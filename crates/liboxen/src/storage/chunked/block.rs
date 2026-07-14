//! The block format: an immutable, content-addressed pack of chunk payloads — the
//! unit of storage and bulk transfer.
//!
//! Layout, written in one streaming pass:
//!
//! ```text
//! [payload payload payload ...]
//! [footer: n × (chunk_hash u128, offset u32, stored_len u32, raw_len u32, flags u8)]
//! [footer_len u32][version u8][magic b"OXBK"]
//! ```
//!
//! All integers are little-endian. The block ID is the xxh3-128 of the complete block
//! bytes, so storage is idempotent and any transfer is verifiable. The footer makes
//! blocks self-describing: the chunk index is rebuildable by scanning footers, and
//! fsck can validate a block standalone.
//!
//! The parser treats input as untrusted (blocks arrive over the wire): checked
//! arithmetic everywhere, payload ranges must tile the payload region exactly,
//! declared raw lengths are capped, and decoders never allocate past the declared
//! output size. [`verify_block`] additionally decodes every chunk and checks it
//! against its claimed hash (design decision 16) — the block hash covers the bytes,
//! but only that step makes the footer's *claims* trustworthy.

use bytes::Bytes;

use crate::util::hasher::hash_buffer_128bit;

use super::compressor::{EncodedChunk, decode_chunk};
use super::error::ChunkedError;
use super::registry::CodecId;
use super::{MAX_BLOCK_SIZE, MAX_CHUNK_SIZE};

/// Block format magic, the final four bytes of every block.
pub const BLOCK_MAGIC: &[u8; 4] = b"OXBK";
/// Current block format version.
pub const BLOCK_VERSION: u8 = 1;
/// Serialized size of one footer entry.
const FOOTER_ENTRY_SIZE: usize = 16 + 4 + 4 + 4 + 1;
/// Serialized size of the fixed trailer: footer_len u32 + version u8 + magic.
const TRAILER_SIZE: usize = 4 + 1 + 4;

/// One chunk's placement and encoding within a block, as recorded in the footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockChunk {
    /// xxh3-128 of the raw (uncompressed) chunk bytes.
    pub chunk_hash: u128,
    /// Byte offset of the stored payload within the block.
    pub offset: u32,
    /// Stored (possibly compressed) payload length in bytes.
    pub stored_len: u32,
    /// Raw (uncompressed) chunk length in bytes.
    pub raw_len: u32,
    /// The codec the payload was encoded with.
    pub codec: CodecId,
}

/// A complete, hashed, immutable block ready to publish.
#[derive(Debug, Clone)]
pub struct SealedBlock {
    /// xxh3-128 of `data` — the block's content address.
    pub hash: u128,
    /// The complete block bytes (payloads + footer + trailer).
    pub data: Bytes,
    /// The chunks packed in this block, in payload order.
    pub chunks: Vec<BlockChunk>,
}

/// Accumulates encoded chunk payloads and seals them into a block.
///
/// Blocks are never appended to after sealing; the packer seals at
/// [`MAX_BLOCK_SIZE`] or at the end of the operation's batch, whichever comes first.
#[derive(Debug, Default)]
pub struct BlockWriter {
    payloads: Vec<u8>,
    chunks: Vec<BlockChunk>,
}

impl BlockWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    /// The complete encoded size of the block if sealed now.
    pub fn encoded_size(&self) -> u64 {
        (self.payloads.len() + self.chunks.len() * FOOTER_ENTRY_SIZE + TRAILER_SIZE) as u64
    }

    /// Whether appending a payload of `stored_len` bytes would push the sealed block
    /// past [`MAX_BLOCK_SIZE`]. The packer seals and starts a new block instead.
    pub fn would_exceed_max_size(&self, stored_len: usize) -> bool {
        self.encoded_size() + (stored_len + FOOTER_ENTRY_SIZE) as u64 > MAX_BLOCK_SIZE
    }

    /// Append one encoded chunk payload.
    ///
    /// `chunk_hash` is the xxh3-128 of the raw bytes and `raw_len` their length;
    /// `encoded` carries the stored payload and the codec that produced it.
    pub fn append(
        &mut self,
        chunk_hash: u128,
        raw_len: u32,
        encoded: &EncodedChunk,
    ) -> Result<(), ChunkedError> {
        if raw_len == 0 || raw_len > MAX_CHUNK_SIZE {
            return Err(ChunkedError::CorruptBlock(format!(
                "cannot pack chunk with raw length {raw_len}"
            )));
        }
        if self.would_exceed_max_size(encoded.data.len()) && !self.is_empty() {
            return Err(ChunkedError::CorruptBlock(
                "block writer overfilled: caller must seal at would_exceed_max_size".to_string(),
            ));
        }
        let offset = u32::try_from(self.payloads.len()).map_err(|_| {
            ChunkedError::CorruptBlock("block payload region exceeds u32 range".to_string())
        })?;
        let stored_len = u32::try_from(encoded.data.len()).map_err(|_| {
            ChunkedError::CorruptBlock("chunk payload exceeds u32 range".to_string())
        })?;
        self.payloads.extend_from_slice(&encoded.data);
        self.chunks.push(BlockChunk {
            chunk_hash,
            offset,
            stored_len,
            raw_len,
            codec: encoded.codec,
        });
        Ok(())
    }

    /// Seal: append the footer and trailer, hash the complete bytes, and return the
    /// immutable block.
    pub fn seal(self) -> Result<SealedBlock, ChunkedError> {
        if self.chunks.is_empty() {
            return Err(ChunkedError::CorruptBlock(
                "refusing to seal an empty block".to_string(),
            ));
        }
        let mut data = self.payloads;
        let footer_len = self.chunks.len() * FOOTER_ENTRY_SIZE;
        data.reserve(footer_len + TRAILER_SIZE);
        for chunk in &self.chunks {
            data.extend_from_slice(&chunk.chunk_hash.to_le_bytes());
            data.extend_from_slice(&chunk.offset.to_le_bytes());
            data.extend_from_slice(&chunk.stored_len.to_le_bytes());
            data.extend_from_slice(&chunk.raw_len.to_le_bytes());
            data.push(chunk.codec.as_u8());
        }
        let footer_len = u32::try_from(footer_len).map_err(|_| {
            ChunkedError::CorruptBlock("block footer exceeds u32 range".to_string())
        })?;
        data.extend_from_slice(&footer_len.to_le_bytes());
        data.push(BLOCK_VERSION);
        data.extend_from_slice(BLOCK_MAGIC);

        let hash = hash_buffer_128bit(&data);
        Ok(SealedBlock {
            hash,
            data: Bytes::from(data),
            chunks: self.chunks,
        })
    }
}

/// Parse and structurally validate a block's footer. Input is untrusted.
///
/// This checks the *shape* — magic, version, bounds, exact payload tiling, known
/// codecs, sane lengths — but does not decode payloads or verify chunk hashes; use
/// [`verify_block`] for that before trusting the footer's claims.
pub fn parse_block_footer(block: &[u8]) -> Result<Vec<BlockChunk>, ChunkedError> {
    if block.len() as u64 > MAX_BLOCK_SIZE {
        return Err(ChunkedError::CorruptBlock(format!(
            "block of {} bytes exceeds maximum {MAX_BLOCK_SIZE}",
            block.len()
        )));
    }
    if block.len() < TRAILER_SIZE {
        return Err(ChunkedError::BadBlockMagic);
    }
    let (rest, trailer) = block.split_at(block.len() - TRAILER_SIZE);
    if &trailer[5..9] != BLOCK_MAGIC {
        return Err(ChunkedError::BadBlockMagic);
    }
    if trailer[4] != BLOCK_VERSION {
        return Err(ChunkedError::UnsupportedBlockVersion(trailer[4]));
    }
    let footer_len = u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]) as usize;
    if footer_len > rest.len() {
        return Err(ChunkedError::CorruptBlock(format!(
            "footer length {footer_len} exceeds block size {}",
            block.len()
        )));
    }
    if footer_len == 0 || !footer_len.is_multiple_of(FOOTER_ENTRY_SIZE) {
        return Err(ChunkedError::CorruptBlock(format!(
            "footer length {footer_len} is not a positive multiple of {FOOTER_ENTRY_SIZE}"
        )));
    }
    let payload_len = rest.len() - footer_len;
    let footer = &rest[payload_len..];

    let mut chunks = Vec::with_capacity(footer_len / FOOTER_ENTRY_SIZE);
    let mut expected_offset = 0usize;
    for (i, entry) in footer.chunks_exact(FOOTER_ENTRY_SIZE).enumerate() {
        let chunk_hash = u128::from_le_bytes(
            entry[0..16]
                .try_into()
                .map_err(|_| ChunkedError::CorruptBlock("footer entry truncated".to_string()))?,
        );
        let offset = u32::from_le_bytes([entry[16], entry[17], entry[18], entry[19]]);
        let stored_len = u32::from_le_bytes([entry[20], entry[21], entry[22], entry[23]]);
        let raw_len = u32::from_le_bytes([entry[24], entry[25], entry[26], entry[27]]);
        let codec = CodecId::from_u8(entry[28])?;

        // Payloads must tile the payload region exactly — no gaps, overlaps, or
        // out-of-bounds ranges, and no smuggled bytes the footer doesn't claim.
        if offset as usize != expected_offset {
            return Err(ChunkedError::CorruptBlock(format!(
                "chunk {i} payload at offset {offset} but previous payloads end at {expected_offset}"
            )));
        }
        if stored_len == 0 {
            return Err(ChunkedError::CorruptBlock(format!(
                "chunk {i} has zero stored length"
            )));
        }
        if raw_len == 0 || raw_len > MAX_CHUNK_SIZE {
            return Err(ChunkedError::CorruptBlock(format!(
                "chunk {i} declares raw length {raw_len} outside (0, {MAX_CHUNK_SIZE}]"
            )));
        }
        expected_offset = expected_offset
            .checked_add(stored_len as usize)
            .filter(|end| *end <= payload_len)
            .ok_or_else(|| {
                ChunkedError::CorruptBlock(format!(
                    "chunk {i} payload range {offset}+{stored_len} exceeds payload region {payload_len}"
                ))
            })?;

        chunks.push(BlockChunk {
            chunk_hash,
            offset,
            stored_len,
            raw_len,
            codec,
        });
    }
    if expected_offset != payload_len {
        return Err(ChunkedError::CorruptBlock(format!(
            "payloads end at {expected_offset} but payload region is {payload_len} bytes"
        )));
    }
    Ok(chunks)
}

/// Fully verify an untrusted block before ingest (design decision 16): parse the
/// footer, then decode every payload and check it against its claimed chunk hash.
///
/// Returns the verified footer entries. The caller separately checks the block's
/// own content hash against the name it arrived under.
pub fn verify_block(block: &[u8]) -> Result<Vec<BlockChunk>, ChunkedError> {
    let chunks = parse_block_footer(block)?;
    for chunk in &chunks {
        let payload = &block[chunk.offset as usize..(chunk.offset + chunk.stored_len) as usize];
        let raw = decode_chunk(chunk.codec, payload, chunk.raw_len as usize)?;
        let actual = hash_buffer_128bit(&raw);
        if actual != chunk.chunk_hash {
            return Err(ChunkedError::ChunkHashMismatch {
                expected: chunk.chunk_hash,
                actual,
            });
        }
    }
    Ok(chunks)
}

/// Decode one chunk's raw bytes from its stored payload slice, as located by a
/// verified footer entry or chunk-index record.
pub fn decode_block_payload(chunk: &BlockChunk, payload: &[u8]) -> Result<Vec<u8>, ChunkedError> {
    if payload.len() != chunk.stored_len as usize {
        return Err(ChunkedError::CorruptBlock(format!(
            "payload slice of {} bytes does not match stored length {}",
            payload.len(),
            chunk.stored_len
        )));
    }
    decode_chunk(chunk.codec, payload, chunk.raw_len as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::chunked::compressor::encode_chunk;

    /// Build a sealed block from raw chunk payloads, exercising the same
    /// hash/encode path production uses.
    fn build_block(raw_chunks: &[&[u8]], codec: CodecId) -> SealedBlock {
        let mut writer = BlockWriter::new();
        for raw in raw_chunks {
            let encoded = encode_chunk(codec, raw).expect("encode chunk");
            writer
                .append(hash_buffer_128bit(raw), raw.len() as u32, &encoded)
                .expect("append chunk");
        }
        writer.seal().expect("seal block")
    }

    fn text_chunk(fill: u8, len: usize) -> Vec<u8> {
        // Low-entropy but nonuniform so zstd compresses without being trivial.
        (0..len)
            .map(|i| fill.wrapping_add((i % 13) as u8))
            .collect()
    }

    /// A sealed block round-trips: parse the footer, verify every chunk, decode
    /// every payload back to the original bytes.
    #[test]
    fn seal_parse_verify_round_trip() -> Result<(), ChunkedError> {
        let chunk_a = text_chunk(b'a', 70_000);
        let chunk_b = text_chunk(b'b', 8_192);
        let chunk_c: Vec<u8> = (0..90_000u32)
            .flat_map(|i| i.to_le_bytes())
            .take(90_000)
            .collect();
        let block = build_block(&[&chunk_a, &chunk_b, &chunk_c], CodecId::ZSTD);

        assert_eq!(block.hash, hash_buffer_128bit(&block.data));

        let parsed = parse_block_footer(&block.data)?;
        assert_eq!(parsed, block.chunks);

        let verified = verify_block(&block.data)?;
        assert_eq!(verified.len(), 3);
        for (chunk, raw) in verified.iter().zip([&chunk_a, &chunk_b, &chunk_c]) {
            let payload =
                &block.data[chunk.offset as usize..(chunk.offset + chunk.stored_len) as usize];
            assert_eq!(&decode_block_payload(chunk, payload)?, raw);
            assert_eq!(chunk.chunk_hash, hash_buffer_128bit(raw));
        }
        Ok(())
    }

    /// Blocks are deterministic: same chunks in ⇒ same bytes and same content hash.
    #[test]
    fn sealing_is_deterministic() {
        let chunk = text_chunk(b'x', 30_000);
        let one = build_block(&[&chunk], CodecId::ZSTD);
        let two = build_block(&[&chunk], CodecId::ZSTD);
        assert_eq!(one.data, two.data);
        assert_eq!(one.hash, two.hash);
    }

    /// Golden block fixture: pins the v1 byte layout. If this fails, the block
    /// format changed — that requires a version bump, not an in-place edit.
    #[test]
    fn golden_block_encoding() {
        let chunk_a = text_chunk(b'g', 10_000);
        let chunk_b = text_chunk(b'h', 20_000);
        let block = build_block(&[&chunk_a, &chunk_b], CodecId::ZSTD);
        // GOLDEN: xxh3-128 of the sealed v1 block bytes for these two chunks.
        assert_eq!(
            format!("{:x}", block.hash),
            "48641fba9af2a8c3cd029d3afc850d63"
        );
    }

    /// Every trailing-structure corruption fails structurally, and a single
    /// flipped payload bit fails chunk-hash verification.
    #[test]
    fn corrupt_blocks_are_rejected() {
        let chunk = text_chunk(b'z', 40_000);
        let block = build_block(&[&chunk], CodecId::ZSTD);
        let data = block.data.to_vec();

        // Truncated to nothing / below trailer size.
        assert!(matches!(
            parse_block_footer(&[]),
            Err(ChunkedError::BadBlockMagic)
        ));
        assert!(matches!(
            parse_block_footer(&data[..TRAILER_SIZE - 1]),
            Err(ChunkedError::BadBlockMagic)
        ));

        // Bad magic.
        let mut bad = data.clone();
        let len = bad.len();
        bad[len - 1] = b'X';
        assert!(matches!(
            parse_block_footer(&bad),
            Err(ChunkedError::BadBlockMagic)
        ));

        // Future version.
        let mut bad = data.clone();
        let len = bad.len();
        bad[len - 5] = 9;
        assert!(matches!(
            parse_block_footer(&bad),
            Err(ChunkedError::UnsupportedBlockVersion(9))
        ));

        // Footer length exceeding the block.
        let mut bad = data.clone();
        let len = bad.len();
        bad[len - 9..len - 5].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            parse_block_footer(&bad),
            Err(ChunkedError::CorruptBlock(_))
        ));

        // Footer length not a multiple of the entry size.
        let mut bad = data.clone();
        let len = bad.len();
        bad[len - 9..len - 5].copy_from_slice(&7u32.to_le_bytes());
        assert!(matches!(
            parse_block_footer(&bad),
            Err(ChunkedError::CorruptBlock(_))
        ));

        // Unknown codec in the footer entry's flags byte.
        let mut bad = data.clone();
        let len = bad.len();
        bad[len - TRAILER_SIZE - 1] = 250;
        assert!(matches!(
            parse_block_footer(&bad),
            Err(ChunkedError::UnknownCodecId(250))
        ));

        // A flipped payload bit passes structural parsing but fails verification.
        let mut bad = data.clone();
        let payload_len = bad.len() - TRAILER_SIZE - FOOTER_ENTRY_SIZE;
        bad[payload_len / 2] ^= 0x01;
        assert!(parse_block_footer(&bad).is_ok());
        assert!(matches!(
            verify_block(&bad),
            Err(ChunkedError::ChunkHashMismatch { .. })
                | Err(ChunkedError::Decompress(_))
                | Err(ChunkedError::DecodedLenMismatch { .. })
        ));

        // A footer entry lying about the chunk hash fails verification even though
        // the structure is intact. The hash field starts the last footer entry.
        let mut bad = data.clone();
        let len = bad.len();
        let entry_start = len - TRAILER_SIZE - FOOTER_ENTRY_SIZE;
        bad[entry_start] ^= 0xFF;
        assert!(matches!(
            verify_block(&bad),
            Err(ChunkedError::ChunkHashMismatch { .. })
        ));
    }

    /// A footer whose declared raw length exceeds the format cap is rejected
    /// before any decode — the bound that stops decompression bombs.
    #[test]
    fn oversized_raw_len_is_rejected() {
        let chunk = text_chunk(b'q', 1000);
        let block = build_block(&[&chunk], CodecId::ZSTD);
        let mut bad = block.data.to_vec();
        let len = bad.len();
        // raw_len sits at bytes 24..28 of the footer entry.
        let entry_start = len - TRAILER_SIZE - FOOTER_ENTRY_SIZE;
        bad[entry_start + 24..entry_start + 28]
            .copy_from_slice(&(MAX_CHUNK_SIZE + 1).to_le_bytes());
        assert!(matches!(
            parse_block_footer(&bad),
            Err(ChunkedError::CorruptBlock(_))
        ));
    }

    /// Payload ranges must tile the payload region exactly: an entry whose range
    /// stops short (leaving unclaimed bytes) is rejected.
    #[test]
    fn payload_gaps_are_rejected() {
        let chunk = text_chunk(b'r', 4096);
        let raw_block = build_block(&[&chunk], CodecId::RAW);
        let mut bad = raw_block.data.to_vec();
        let len = bad.len();
        // Shrink the single entry's stored_len by one: payloads no longer tile.
        let entry_start = len - TRAILER_SIZE - FOOTER_ENTRY_SIZE;
        let stored_len = u32::from_le_bytes(
            bad[entry_start + 20..entry_start + 24]
                .try_into()
                .expect("4 bytes"),
        );
        bad[entry_start + 20..entry_start + 24].copy_from_slice(&(stored_len - 1).to_le_bytes());
        assert!(matches!(
            parse_block_footer(&bad),
            Err(ChunkedError::CorruptBlock(_))
        ));
    }

    /// The writer seals at the size cap: `would_exceed_max_size` flags the append
    /// that would overflow, and appending anyway is an error.
    #[test]
    fn writer_enforces_max_size() -> Result<(), ChunkedError> {
        let raw = vec![0xABu8; MAX_CHUNK_SIZE as usize];
        let encoded = encode_chunk(CodecId::RAW, &raw)?;
        let mut writer = BlockWriter::new();
        let mut appended = 0u64;
        while !writer.would_exceed_max_size(encoded.data.len()) {
            writer.append(hash_buffer_128bit(&raw), raw.len() as u32, &encoded)?;
            appended += 1;
        }
        // A 64 MiB block holds exactly 64 MiB / 128 KiB chunks minus footer slack.
        assert!(appended >= MAX_BLOCK_SIZE / (MAX_CHUNK_SIZE as u64) - 2);
        assert!(writer.encoded_size() <= MAX_BLOCK_SIZE);
        assert!(
            writer
                .append(hash_buffer_128bit(&raw), raw.len() as u32, &encoded)
                .is_err()
        );
        Ok(())
    }

    /// Empty blocks cannot be sealed, and zero/oversized raw lengths cannot be
    /// appended.
    #[test]
    fn writer_input_validation() -> Result<(), ChunkedError> {
        assert!(BlockWriter::new().seal().is_err());

        let encoded = encode_chunk(CodecId::RAW, b"x")?;
        let mut writer = BlockWriter::new();
        assert!(writer.append(1, 0, &encoded).is_err());
        assert!(writer.append(1, MAX_CHUNK_SIZE + 1, &encoded).is_err());
        Ok(())
    }
}
