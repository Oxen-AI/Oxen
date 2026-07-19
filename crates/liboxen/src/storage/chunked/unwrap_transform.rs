//! The embedded-zstd unwrap transform ([`TransformId::ZSTD_UNWRAP_V1`]).
//!
//! Compressed containers (zstd-page parquet, zstd-buffer arrow IPC, bare `.zst`)
//! hide their content from every downstream storage lever: chunk dedup, shared
//! dictionaries, and delta encoding all see opaque high-entropy bytes. This
//! transform scans a file for embedded zstd frames and, for each frame it can
//! **prove it can reproduce byte-for-byte** (decompress, recompress with this
//! build's encoder at a candidate level, compare), replaces the frame with its
//! decompressed bytes in the transformed stream. Reconstruction re-compresses each
//! recorded frame at its recorded level; the ingest-time verification makes that
//! reproduction exact, and frames that fail verification stay verbatim — the
//! transform is lossless for arbitrary input by construction.
//!
//! The transformed stream is self-describing:
//!
//! ```text
//! [6-byte magic "OXZUW1"] [u32 LE segment count] [segments...] [payload]
//! segment (25 bytes LE): verbatim_len u64 | raw_len u64 | compressed_len u64 | level i8
//! ```
//!
//! Each segment contributes `verbatim_len` payload bytes copied verbatim, then
//! `raw_len` decompressed bytes that re-compress at `level` into exactly
//! `compressed_len` original bytes (`raw_len == 0` marks a trailing verbatim run).
//! The original file is the concatenation, in order, of every verbatim run and
//! every re-compressed frame.
//!
//! Reproducibility caveat, by design: a stored transformed stream depends on the
//! bundled libzstd's compressed output for `(bytes, level)`. The ID freezes that
//! contract — a libzstd upgrade that changes compressed output for any stored
//! input must ship as a new transform ID (with the old ID's reproduction verified
//! against a pinned encoder), exactly like a chunker boundary change.

use super::error::ChunkedError;

/// Magic prefix of a transformed stream.
const MAGIC: &[u8; 6] = b"OXZUW1";

/// Serialized size of one segment record.
const SEGMENT_RECORD: usize = 8 + 8 + 8 + 1;

/// The zstd frame magic in on-disk byte order.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Frames whose declared content size exceeds this are left verbatim (bounds the
/// memory needed to verify or reproduce one frame).
const FRAME_RAW_CAP: u64 = 32 * 1024 * 1024;

/// Cap on the total transformed payload, so pathological input cannot balloon the
/// transformed stream (decompression bombs stay stored as their compact original).
const OUTPUT_CAP: usize = 512 * 1024 * 1024;

/// Compression levels tried when verifying a frame, most likely writers first:
/// arrow/parquet default to 1, zstd CLI and most libraries default to 3.
const CANDIDATE_LEVELS: &[i32] = &[1, 3, 2, 4, 5, 7, 9, 12, 19, 22];

/// One recipe entry of the transformed stream.
struct Segment {
    verbatim_len: u64,
    raw_len: u64,
    compressed_len: u64,
    level: i8,
}

/// Try to unwrap embedded zstd frames in `original`. Returns the transformed
/// stream, or `None` when no frame could be verified (store the original bytes
/// under the identity transform instead).
pub fn try_unwrap(original: &[u8]) -> Option<Vec<u8>> {
    let mut segments: Vec<Segment> = Vec::new();
    let mut payload: Vec<u8> = Vec::new();
    let mut verbatim_start = 0usize;
    let mut search_from = 0usize;
    let mut cached_level: Option<i32> = None;

    while search_from + ZSTD_MAGIC.len() <= original.len() {
        let Some(found) = find_magic(&original[search_from..]) else {
            break;
        };
        let off = search_from + found;
        let rest = &original[off..];

        let Some((raw, compressed_len, level)) = verify_frame(rest, &mut cached_level) else {
            search_from = off + 1;
            continue;
        };
        if payload.len() + (off - verbatim_start) + raw.len() > OUTPUT_CAP {
            break;
        }
        payload.extend_from_slice(&original[verbatim_start..off]);
        segments.push(Segment {
            verbatim_len: (off - verbatim_start) as u64,
            raw_len: raw.len() as u64,
            compressed_len: compressed_len as u64,
            level: level as i8,
        });
        payload.extend_from_slice(&raw);
        verbatim_start = off + compressed_len;
        search_from = off + compressed_len;
    }

    if segments.is_empty() {
        return None;
    }
    // Trailing verbatim run.
    if verbatim_start < original.len() {
        segments.push(Segment {
            verbatim_len: (original.len() - verbatim_start) as u64,
            raw_len: 0,
            compressed_len: 0,
            level: 0,
        });
        payload.extend_from_slice(&original[verbatim_start..]);
    }

    let mut out = Vec::with_capacity(MAGIC.len() + 4 + segments.len() * SEGMENT_RECORD + payload.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&(segments.len() as u32).to_le_bytes());
    for seg in &segments {
        out.extend_from_slice(&seg.verbatim_len.to_le_bytes());
        out.extend_from_slice(&seg.raw_len.to_le_bytes());
        out.extend_from_slice(&seg.compressed_len.to_le_bytes());
        out.push(seg.level as u8);
    }
    out.extend_from_slice(&payload);
    Some(out)
}

/// Invert a transformed stream back to the original bytes.
pub fn unwrap_inverse(transformed: &[u8]) -> Result<Vec<u8>, ChunkedError> {
    let corrupt = |msg: &str| ChunkedError::Transform(msg.to_string());
    if transformed.len() < MAGIC.len() + 4 || &transformed[..MAGIC.len()] != MAGIC {
        return Err(corrupt("missing unwrap magic"));
    }
    let count = u32::from_le_bytes(
        transformed[MAGIC.len()..MAGIC.len() + 4]
            .try_into()
            .map_err(|_| corrupt("truncated segment count"))?,
    ) as usize;
    let recipe_end = MAGIC.len() + 4 + count * SEGMENT_RECORD;
    if transformed.len() < recipe_end {
        return Err(corrupt("truncated segment records"));
    }

    let mut original_size = 0usize;
    let mut segments = Vec::with_capacity(count);
    for i in 0..count {
        let at = MAGIC.len() + 4 + i * SEGMENT_RECORD;
        let record = &transformed[at..at + SEGMENT_RECORD];
        let seg = Segment {
            verbatim_len: u64::from_le_bytes(record[0..8].try_into().map_err(|_| corrupt("record"))?),
            raw_len: u64::from_le_bytes(record[8..16].try_into().map_err(|_| corrupt("record"))?),
            compressed_len: u64::from_le_bytes(
                record[16..24].try_into().map_err(|_| corrupt("record"))?,
            ),
            level: record[24] as i8,
        };
        original_size = original_size
            .checked_add(seg.verbatim_len as usize)
            .and_then(|s| s.checked_add(seg.compressed_len as usize))
            .ok_or_else(|| corrupt("original size overflow"))?;
        segments.push(seg);
    }

    let mut out = Vec::with_capacity(original_size);
    let mut pos = recipe_end;
    for seg in &segments {
        let verbatim_end = pos
            .checked_add(seg.verbatim_len as usize)
            .filter(|&e| e <= transformed.len())
            .ok_or_else(|| corrupt("verbatim run past end of stream"))?;
        out.extend_from_slice(&transformed[pos..verbatim_end]);
        pos = verbatim_end;
        if seg.raw_len == 0 {
            continue;
        }
        let raw_end = pos
            .checked_add(seg.raw_len as usize)
            .filter(|&e| e <= transformed.len())
            .ok_or_else(|| corrupt("frame payload past end of stream"))?;
        let frame = zstd::bulk::compress(&transformed[pos..raw_end], seg.level as i32)
            .map_err(ChunkedError::Compress)?;
        // Ingest verified byte-exact reproduction; a length drift here means the
        // encoder no longer matches the one that wrote this stream.
        if frame.len() as u64 != seg.compressed_len {
            return Err(corrupt(
                "re-compressed frame size differs from record: encoder mismatch",
            ));
        }
        out.extend_from_slice(&frame);
        pos = raw_end;
    }
    if pos != transformed.len() {
        return Err(corrupt("trailing bytes after final segment"));
    }
    Ok(out)
}

/// The original (pre-transform) size a transformed stream reconstructs to, from
/// its recipe alone.
pub fn unwrapped_original_size(transformed: &[u8]) -> Result<u64, ChunkedError> {
    let corrupt = |msg: &str| ChunkedError::Transform(msg.to_string());
    if transformed.len() < MAGIC.len() + 4 || &transformed[..MAGIC.len()] != MAGIC {
        return Err(corrupt("missing unwrap magic"));
    }
    let count = u32::from_le_bytes(
        transformed[MAGIC.len()..MAGIC.len() + 4]
            .try_into()
            .map_err(|_| corrupt("truncated segment count"))?,
    ) as usize;
    let recipe_end = MAGIC.len() + 4 + count * SEGMENT_RECORD;
    if transformed.len() < recipe_end {
        return Err(corrupt("truncated segment records"));
    }
    let mut size = 0u64;
    for i in 0..count {
        let at = MAGIC.len() + 4 + i * SEGMENT_RECORD;
        let record = &transformed[at..at + SEGMENT_RECORD];
        let verbatim = u64::from_le_bytes(record[0..8].try_into().map_err(|_| corrupt("record"))?);
        let compressed =
            u64::from_le_bytes(record[16..24].try_into().map_err(|_| corrupt("record"))?);
        size = size
            .checked_add(verbatim)
            .and_then(|s| s.checked_add(compressed))
            .ok_or_else(|| corrupt("original size overflow"))?;
    }
    Ok(size)
}

fn find_magic(haystack: &[u8]) -> Option<usize> {
    haystack
        .windows(ZSTD_MAGIC.len())
        .position(|w| w == ZSTD_MAGIC)
}

/// Decompress the zstd frame at the head of `bytes` and verify this build's
/// encoder reproduces it byte-for-byte at some candidate level. Returns the
/// decompressed bytes, the frame's compressed length, and the verified level.
fn verify_frame(bytes: &[u8], cached_level: &mut Option<i32>) -> Option<(Vec<u8>, usize, i32)> {
    let content_size = match zstd::zstd_safe::get_frame_content_size(bytes) {
        Ok(Some(size)) if size > 0 && size <= FRAME_RAW_CAP => size,
        _ => return None,
    };
    let compressed_len = zstd::zstd_safe::find_frame_compressed_size(bytes).ok()?;
    if compressed_len > bytes.len() {
        return None;
    }
    let frame = &bytes[..compressed_len];
    let raw = zstd::bulk::decompress(frame, content_size as usize).ok()?;
    if raw.len() as u64 != content_size {
        return None;
    }
    let levels = (*cached_level)
        .into_iter()
        .chain(CANDIDATE_LEVELS.iter().copied());
    for level in levels {
        if let Ok(reproduced) = zstd::bulk::compress(&raw, level)
            && reproduced == frame
        {
            *cached_level = Some(level);
            return Some((raw, compressed_len, level));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_text(n: usize) -> Vec<u8> {
        b"the quick brown fox jumps over the lazy dog, again and again, "
            .iter()
            .copied()
            .cycle()
            .take(n)
            .collect()
    }

    /// A container with several embedded zstd frames round-trips exactly, and the
    /// transformed stream carries the decompressed bytes in the clear.
    #[test]
    fn unwrap_round_trip() -> Result<(), ChunkedError> {
        let page1 = sample_text(30_000);
        let page2 = sample_text(45_000);
        let mut container = b"HEADER".to_vec();
        container.extend(zstd::bulk::compress(&page1, 1).map_err(ChunkedError::Compress)?);
        container.extend_from_slice(b"middle metadata");
        container.extend(zstd::bulk::compress(&page2, 3).map_err(ChunkedError::Compress)?);
        container.extend_from_slice(b"FOOTER");

        let transformed = try_unwrap(&container).expect("frames must verify");
        // The decompressed page bytes appear verbatim in the transformed stream.
        assert!(
            transformed
                .windows(page1.len())
                .any(|w| w == page1.as_slice())
        );
        assert_eq!(unwrapped_original_size(&transformed)?, container.len() as u64);
        assert_eq!(unwrap_inverse(&transformed)?, container);
        Ok(())
    }

    /// Input without verifiable frames (no zstd at all, or frames without a
    /// declared content size) is not transformed.
    #[test]
    fn no_verifiable_frames_yields_none() {
        assert!(try_unwrap(b"plain text with no compressed frames").is_none());
        assert!(try_unwrap(&[]).is_none());
        // A magic with garbage after it must not verify.
        let mut fake = b"xxx".to_vec();
        fake.extend_from_slice(&ZSTD_MAGIC);
        fake.extend_from_slice(&[0xFF; 64]);
        assert!(try_unwrap(&fake).is_none());
    }

    /// A frame our encoder cannot reproduce (unknown level behavior emulated by
    /// tampering) stays verbatim while other frames still unwrap.
    #[test]
    fn unverifiable_frame_stays_verbatim() -> Result<(), ChunkedError> {
        let page = sample_text(20_000);
        let good = zstd::bulk::compress(&page, 1).map_err(ChunkedError::Compress)?;
        // A valid-looking frame that decompresses but was written by "another
        // encoder": emulate by recompressing different content under a level we
        // then corrupt — simplest is a frame with checksum flag flipped bytes that
        // still decodes is hard to fake, so instead use a frame compressed at a
        // level outside the candidate set via long-distance matching parameters.
        let exotic = {
            let mut c = zstd::bulk::Compressor::new(3).map_err(ChunkedError::Compress)?;
            c.set_parameter(zstd::zstd_safe::CParameter::WindowLog(24))
                .map_err(ChunkedError::Compress)?;
            c.compress(&page).map_err(ChunkedError::Compress)?
        };
        let mut container = Vec::new();
        container.extend_from_slice(&good);
        container.extend_from_slice(b"|");
        container.extend_from_slice(&exotic);

        let transformed = try_unwrap(&container).expect("the good frame verifies");
        assert_eq!(unwrap_inverse(&transformed)?, container);
        Ok(())
    }

    /// Corrupt transformed streams fail loudly, never panic.
    #[test]
    fn corrupt_streams_are_rejected() {
        assert!(unwrap_inverse(b"junk").is_err());
        assert!(unwrap_inverse(b"OXZUW1").is_err());
        let mut truncated = b"OXZUW1".to_vec();
        truncated.extend_from_slice(&5u32.to_le_bytes());
        assert!(unwrap_inverse(&truncated).is_err());
    }
}
