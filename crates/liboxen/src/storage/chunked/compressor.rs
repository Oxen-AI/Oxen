//! Per-chunk compression: the [`Compressor`] trait, the raw and zstd codecs, and the
//! encode/decode helpers that apply the universal raw fallback.
//!
//! Compression is per-chunk, inside the block, so every chunk stays independently
//! fetchable and decodable — a range read never decompresses a whole block. The codec
//! actually used is recorded per chunk in the block footer's `flags` byte; it is
//! store-local and never crosses the wire as a durable reference.

use super::error::ChunkedError;
use super::registry::{CodecId, codec};

/// Zstd compression level for [`CodecId::ZSTD`]. Chosen for the add-path hot loop:
/// good text/tabular ratios at high throughput.
const ZSTD_LEVEL: i32 = 9;

/// Encodes and decodes a chunk payload.
///
/// `decompress` treats `stored` as untrusted input: output is bounded by the declared
/// `raw_len`, and anything else (truncation, trailing garbage, a length mismatch) is
/// an error, never a panic or an unbounded allocation.
pub trait Compressor: Send + Sync {
    /// The registered ID recorded in block footers for chunks encoded with this codec.
    fn id(&self) -> CodecId;

    /// Encode raw chunk bytes for storage.
    fn compress(&self, raw: &[u8]) -> Result<Vec<u8>, ChunkedError>;

    /// Decode stored bytes back to exactly `raw_len` raw bytes.
    fn decompress(&self, stored: &[u8], raw_len: usize) -> Result<Vec<u8>, ChunkedError>;
}

/// [`CodecId::RAW`]: bytes stored verbatim. The universal fallback — every store can
/// always decode it, and incompressible chunks pay zero overhead.
pub struct RawCodec;

impl Compressor for RawCodec {
    fn id(&self) -> CodecId {
        CodecId::RAW
    }

    fn compress(&self, raw: &[u8]) -> Result<Vec<u8>, ChunkedError> {
        Ok(raw.to_vec())
    }

    fn decompress(&self, stored: &[u8], raw_len: usize) -> Result<Vec<u8>, ChunkedError> {
        if stored.len() != raw_len {
            return Err(ChunkedError::DecodedLenMismatch {
                expected: raw_len,
                actual: stored.len(),
            });
        }
        Ok(stored.to_vec())
    }
}

/// [`CodecId::ZSTD`]: Zstandard at level 3.
pub struct ZstdCodec;

impl Compressor for ZstdCodec {
    fn id(&self) -> CodecId {
        CodecId::ZSTD
    }

    fn compress(&self, raw: &[u8]) -> Result<Vec<u8>, ChunkedError> {
        zstd::bulk::compress(raw, ZSTD_LEVEL).map_err(ChunkedError::Compress)
    }

    fn decompress(&self, stored: &[u8], raw_len: usize) -> Result<Vec<u8>, ChunkedError> {
        // Capacity is fixed to the declared raw length, so a decompression bomb can
        // never allocate past it; a short or long result is rejected below.
        let decoded = zstd::bulk::decompress(stored, raw_len).map_err(ChunkedError::Decompress)?;
        if decoded.len() != raw_len {
            return Err(ChunkedError::DecodedLenMismatch {
                expected: raw_len,
                actual: decoded.len(),
            });
        }
        Ok(decoded)
    }
}

/// A chunk payload ready to be written into a block: the codec actually used (after
/// raw fallback) and the encoded bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedChunk {
    pub codec: CodecId,
    pub data: Vec<u8>,
}

/// Encode a chunk with the policy-selected codec, falling back to raw storage
/// whenever the encoded form is not strictly smaller than the input.
pub fn encode_chunk(codec_id: CodecId, raw: &[u8]) -> Result<EncodedChunk, ChunkedError> {
    if codec_id != CodecId::RAW {
        let compressed = codec(codec_id)?.compress(raw)?;
        if compressed.len() < raw.len() {
            return Ok(EncodedChunk {
                codec: codec_id,
                data: compressed,
            });
        }
    }
    Ok(EncodedChunk {
        codec: CodecId::RAW,
        data: raw.to_vec(),
    })
}

/// Decode a stored chunk payload back to exactly `raw_len` raw bytes, using the codec
/// recorded for it at write time.
pub fn decode_chunk(
    codec_id: CodecId,
    stored: &[u8],
    raw_len: usize,
) -> Result<Vec<u8>, ChunkedError> {
    codec(codec_id)?.decompress(stored, raw_len)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compressible data round-trips through zstd and actually shrinks.
    #[test]
    fn zstd_round_trip() -> Result<(), ChunkedError> {
        let raw: Vec<u8> = b"city,country,population\n"
            .iter()
            .copied()
            .cycle()
            .take(64 * 1024)
            .collect();
        let encoded = encode_chunk(CodecId::ZSTD, &raw)?;
        assert_eq!(encoded.codec, CodecId::ZSTD);
        assert!(encoded.data.len() < raw.len());
        assert_eq!(decode_chunk(encoded.codec, &encoded.data, raw.len())?, raw);
        Ok(())
    }

    /// Incompressible data falls back to raw storage — never expands.
    #[test]
    fn raw_fallback_on_incompressible_data() -> Result<(), ChunkedError> {
        // High-entropy bytes: a simple xorshift stream doesn't compress under zstd.
        let mut state = 0x9E3779B97F4A7C15u64;
        let raw: Vec<u8> = std::iter::repeat_with(|| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()
        })
        .flatten()
        .take(64 * 1024)
        .collect();

        let encoded = encode_chunk(CodecId::ZSTD, &raw)?;
        assert_eq!(encoded.codec, CodecId::RAW);
        assert_eq!(encoded.data, raw);
        assert_eq!(decode_chunk(encoded.codec, &encoded.data, raw.len())?, raw);
        Ok(())
    }

    /// The empty chunk edge case: encodes as raw, decodes to empty.
    #[test]
    fn empty_chunk_round_trip() -> Result<(), ChunkedError> {
        let encoded = encode_chunk(CodecId::ZSTD, &[])?;
        assert_eq!(encoded.codec, CodecId::RAW);
        assert!(decode_chunk(encoded.codec, &encoded.data, 0)?.is_empty());
        Ok(())
    }

    /// A declared raw length that doesn't match the decoded output is rejected for
    /// both codecs — the declared length is untrusted input.
    #[test]
    fn decoded_len_mismatch_is_rejected() -> Result<(), ChunkedError> {
        let raw = vec![7u8; 1000];

        assert!(matches!(
            decode_chunk(CodecId::RAW, &raw, 999),
            Err(ChunkedError::DecodedLenMismatch { .. })
        ));

        let compressed = codec(CodecId::ZSTD)?.compress(&raw)?;
        assert!(matches!(
            decode_chunk(CodecId::ZSTD, &compressed, 999),
            Err(ChunkedError::DecodedLenMismatch { .. }) | Err(ChunkedError::Decompress(_))
        ));
        Ok(())
    }

    /// Corrupt compressed bytes fail cleanly.
    #[test]
    fn corrupt_zstd_bytes_fail() {
        assert!(matches!(
            decode_chunk(CodecId::ZSTD, b"definitely not zstd", 100),
            Err(ChunkedError::Decompress(_))
        ));
    }
}
