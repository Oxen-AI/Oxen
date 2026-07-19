//! The chunk manifest: the ordered file→chunk mapping for one file version.
//!
//! A manifest is **pure content**: deterministic from the file bytes and the chunker,
//! never carrying block placement (placement lives only in the store-local chunk
//! index). Same file bytes ⇒ same manifest, so manifests dedup themselves, are
//! identical across stores, and survive any repack or GC untouched.
//!
//! Serialized as versioned msgpack. A published manifest is immutable and is never
//! overwritten.

use serde::{Deserialize, Serialize};

use crate::model::MerkleHash;

use super::MAX_CHUNK_SIZE;
use super::error::ChunkedError;
use super::registry::{ChunkerId, TransformId};

/// Current manifest format version.
pub const MANIFEST_VERSION: u8 = 1;

/// One chunk of the reconstructed file: its identity (xxh3-128 of the **raw**,
/// uncompressed chunk bytes) and where it lands in the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkEntry {
    /// xxh3-128 of the raw chunk bytes — the chunk's identity for dedup.
    pub hash: u128,
    /// Byte offset of this chunk in the reconstructed file.
    pub offset: u64,
    /// Raw (uncompressed) length of this chunk in bytes.
    pub len: u32,
}

/// The file→chunk mapping for one file version, keyed in the version store by the
/// file's whole-content hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkManifest {
    /// Manifest format version ([`MANIFEST_VERSION`]).
    pub version: u8,
    /// xxh3-128 of the complete original file — the logical file identity.
    pub file_hash: MerkleHash,
    /// Size of the complete original file in bytes.
    pub file_size: u64,
    /// The boundary function that produced these chunks.
    pub chunker_id: ChunkerId,
    /// Reversible pre-chunking transform. v1 always [`TransformId::IDENTITY`]; any
    /// other value is rejected on read with an upgrade-required error.
    pub transform_id: TransformId,
    /// The chunks, ordered by offset, tiling the file exactly.
    pub chunks: Vec<ChunkEntry>,
}

impl ChunkManifest {
    /// Serialize for storage. The caller is responsible for validating before
    /// publishing (see [`Self::validate`]).
    pub fn to_bytes(&self) -> Result<Vec<u8>, ChunkedError> {
        Ok(rmp_serde::to_vec(self)?)
    }

    /// Deserialize and structurally validate a stored manifest.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ChunkedError> {
        let manifest: ChunkManifest = rmp_serde::from_slice(bytes)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Serialize for at-rest storage: the msgpack encoding wrapped in a zstd
    /// frame. Manifest entries share long runs (offsets, lengths, msgpack
    /// framing), so the at-rest form is meaningfully smaller than the logical
    /// encoding; the wire and validation formats are unchanged.
    pub fn to_stored_bytes(&self) -> Result<Vec<u8>, ChunkedError> {
        let logical = self.to_bytes()?;
        zstd::bulk::compress(&logical, 19).map_err(ChunkedError::Compress)
    }

    /// Magic prefix of the delta at-rest form: `[OXMD][16-byte base blob hash
    /// LE][zstd frame dictionary-compressed against the base blob's bytes]`.
    pub const DELTA_MAGIC: [u8; 4] = *b"OXMD";

    /// Serialize as a delta against a base manifest's logical bytes. Returns
    /// `None` when the delta form is not smaller than `full_form`.
    pub fn to_delta_bytes(
        &self,
        base_hash: u128,
        base_logical: &[u8],
        full_form_len: usize,
    ) -> Result<Option<Vec<u8>>, ChunkedError> {
        let logical = self.to_bytes()?;
        let mut compressor = zstd::bulk::Compressor::with_dictionary(19, base_logical)
            .map_err(ChunkedError::Compress)?;
        let frame = compressor
            .compress(&logical)
            .map_err(ChunkedError::Compress)?;
        let total = 4 + 16 + frame.len();
        // Rotate to a fresh full base once drift makes deltas fat.
        if total >= full_form_len || total * 2 >= full_form_len {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&Self::DELTA_MAGIC);
        out.extend_from_slice(&base_hash.to_le_bytes());
        out.extend_from_slice(&frame);
        Ok(Some(out))
    }

    /// Parse a manifest from its at-rest form. Accepts the delta form (resolving
    /// its base blob through `load_base`), the zstd-wrapped full encoding, and
    /// the bare msgpack encoding, so stores written before compression remain
    /// readable.
    pub fn from_stored_bytes(
        bytes: &[u8],
        load_base: impl FnOnce(u128) -> Result<Vec<u8>, ChunkedError>,
    ) -> Result<Self, ChunkedError> {
        const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
        // Manifest size is bounded in practice (entries for a < 2^32-chunk
        // file); cap decompression at a generous fixed bound.
        const MAX_MANIFEST_BYTES: usize = 256 * 1024 * 1024;
        if bytes.len() >= 20 && bytes[..4] == Self::DELTA_MAGIC {
            let base_hash = u128::from_le_bytes(bytes[4..20].try_into().map_err(|_| {
                ChunkedError::InvalidManifest("truncated delta base header".to_string())
            })?);
            let base = load_base(base_hash)?;
            let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&base)
                .map_err(ChunkedError::Decompress)?;
            let logical = decompressor
                .decompress(&bytes[20..], MAX_MANIFEST_BYTES)
                .map_err(ChunkedError::Decompress)?;
            return Self::from_bytes(&logical);
        }
        if bytes.len() >= 4 && bytes[..4] == ZSTD_MAGIC {
            let logical = zstd::bulk::decompress(bytes, MAX_MANIFEST_BYTES)
                .map_err(ChunkedError::Decompress)?;
            return Self::from_bytes(&logical);
        }
        Self::from_bytes(bytes)
    }

    /// Structural validation: version and transform are supported, and the chunk
    /// list tiles the file exactly (contiguous offsets from zero, non-zero lengths
    /// within the chunk-size cap, lengths summing to `file_size`).
    ///
    /// An unknown `chunker_id` is deliberately **not** rejected here: reconstruction
    /// never re-chunks, and a store must accept valid manifests produced by clients
    /// with newer chunker policies.
    pub fn validate(&self) -> Result<(), ChunkedError> {
        if self.version != MANIFEST_VERSION {
            return Err(ChunkedError::UnsupportedManifestVersion(self.version));
        }
        if self.transform_id != TransformId::IDENTITY {
            return Err(ChunkedError::UnknownTransformId(self.transform_id.as_u8()));
        }
        if self.chunks.is_empty() != (self.file_size == 0) {
            return Err(ChunkedError::InvalidManifest(format!(
                "{} chunks for file size {}",
                self.chunks.len(),
                self.file_size
            )));
        }

        let mut expected_offset = 0u64;
        for (i, chunk) in self.chunks.iter().enumerate() {
            if chunk.offset != expected_offset {
                return Err(ChunkedError::InvalidManifest(format!(
                    "chunk {i} at offset {} but previous chunks end at {expected_offset}",
                    chunk.offset
                )));
            }
            if chunk.len == 0 || chunk.len > MAX_CHUNK_SIZE {
                return Err(ChunkedError::InvalidManifest(format!(
                    "chunk {i} has invalid length {}",
                    chunk.len
                )));
            }
            expected_offset = expected_offset
                .checked_add(chunk.len as u64)
                .ok_or_else(|| {
                    ChunkedError::InvalidManifest(format!("chunk {i} overflows total size"))
                })?;
        }
        if expected_offset != self.file_size {
            return Err(ChunkedError::InvalidManifest(format!(
                "chunk lengths sum to {expected_offset} but file size is {}",
                self.file_size
            )));
        }
        Ok(())
    }

    /// The chunk covering file offset `pos`, found by binary search. `None` at or
    /// past EOF.
    pub fn chunk_at(&self, pos: u64) -> Option<&ChunkEntry> {
        self.chunk_index_at(pos).map(|idx| &self.chunks[idx])
    }

    /// The index of the chunk covering file offset `pos`. `None` at or past EOF.
    pub fn chunk_index_at(&self, pos: u64) -> Option<usize> {
        if pos >= self.file_size {
            return None;
        }
        let idx = self
            .chunks
            .partition_point(|chunk| chunk.offset + chunk.len as u64 <= pos);
        (idx < self.chunks.len()).then_some(idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_with_chunks(chunks: Vec<ChunkEntry>) -> ChunkManifest {
        let file_size = chunks.iter().map(|c| c.len as u64).sum();
        ChunkManifest {
            version: MANIFEST_VERSION,
            file_hash: MerkleHash::new(0xABCD),
            file_size,
            chunker_id: ChunkerId::GENERIC_FASTCDC_V1,
            transform_id: TransformId::IDENTITY,
            chunks,
        }
    }

    fn three_chunks() -> Vec<ChunkEntry> {
        vec![
            ChunkEntry {
                hash: 1,
                offset: 0,
                len: 65536,
            },
            ChunkEntry {
                hash: 2,
                offset: 65536,
                len: 8192,
            },
            ChunkEntry {
                hash: 3,
                offset: 73728,
                len: 100,
            },
        ]
    }

    /// A valid manifest round-trips through msgpack unchanged.
    #[test]
    fn round_trip() -> Result<(), ChunkedError> {
        let manifest = manifest_with_chunks(three_chunks());
        let bytes = manifest.to_bytes()?;
        assert_eq!(ChunkManifest::from_bytes(&bytes)?, manifest);
        Ok(())
    }

    /// The zero-byte file: no chunks, size zero — valid.
    #[test]
    fn empty_file_manifest() -> Result<(), ChunkedError> {
        let manifest = manifest_with_chunks(vec![]);
        manifest.validate()?;
        let bytes = manifest.to_bytes()?;
        assert_eq!(ChunkManifest::from_bytes(&bytes)?, manifest);
        Ok(())
    }

    /// Unknown future versions and transforms fail with structured
    /// upgrade-required errors, never a silent fallback.
    #[test]
    fn future_version_and_transform_are_rejected() {
        let mut manifest = manifest_with_chunks(three_chunks());
        manifest.version = 2;
        assert!(matches!(
            manifest.validate(),
            Err(ChunkedError::UnsupportedManifestVersion(2))
        ));

        let mut manifest = manifest_with_chunks(three_chunks());
        manifest.transform_id = TransformId::IDENTITY; // reset
        let bytes = {
            // Round-trip through serde to forge a non-identity transform ID, which
            // can't be constructed directly outside the registry.
            let mut raw = rmp_serde::to_vec(&manifest).expect("serialize test manifest");
            // transform_id is the 5th field in array encoding; forging via byte
            // surgery is brittle, so decode into a tuple-shaped value instead.
            let mut decoded: (u8, u128, u64, u8, u8, Vec<(u128, u64, u32)>) =
                rmp_serde::from_slice(&raw).expect("decode test manifest");
            decoded.4 = 9;
            raw = rmp_serde::to_vec(&decoded).expect("re-encode test manifest");
            raw
        };
        assert!(matches!(
            ChunkManifest::from_bytes(&bytes),
            Err(ChunkedError::UnknownTransformId(9))
        ));
    }

    /// Structural corruption is rejected: gaps, overlaps, zero-length chunks,
    /// oversized chunks, and size mismatches.
    #[test]
    fn corrupt_chunk_lists_are_rejected() {
        // Gap between chunks.
        let mut manifest = manifest_with_chunks(three_chunks());
        manifest.chunks[1].offset += 1;
        assert!(matches!(
            manifest.validate(),
            Err(ChunkedError::InvalidManifest(_))
        ));

        // Zero-length chunk.
        let mut manifest = manifest_with_chunks(three_chunks());
        manifest.chunks[2].len = 0;
        manifest.file_size -= 100;
        assert!(matches!(
            manifest.validate(),
            Err(ChunkedError::InvalidManifest(_))
        ));

        // Chunk over the format cap.
        let manifest = manifest_with_chunks(vec![ChunkEntry {
            hash: 1,
            offset: 0,
            len: MAX_CHUNK_SIZE + 1,
        }]);
        assert!(matches!(
            manifest.validate(),
            Err(ChunkedError::InvalidManifest(_))
        ));

        // Lengths don't sum to file_size.
        let mut manifest = manifest_with_chunks(three_chunks());
        manifest.file_size += 1;
        assert!(matches!(
            manifest.validate(),
            Err(ChunkedError::InvalidManifest(_))
        ));

        // Chunks claimed for an empty file.
        let mut manifest = manifest_with_chunks(three_chunks());
        manifest.file_size = 0;
        manifest.chunks.truncate(0);
        manifest.validate().expect("empty is valid");
        let mut manifest = manifest_with_chunks(vec![]);
        manifest.file_size = 10;
        assert!(matches!(
            manifest.validate(),
            Err(ChunkedError::InvalidManifest(_))
        ));
    }

    /// Golden manifest fixture: pins the v1 msgpack encoding. If this fails, the
    /// manifest format changed — that requires a version bump, not an in-place edit.
    #[test]
    fn golden_manifest_encoding() {
        let bytes = manifest_with_chunks(three_chunks())
            .to_bytes()
            .expect("serialize golden manifest");
        // GOLDEN: xxh3-128 of the v1 msgpack bytes for the three-chunk manifest.
        assert_eq!(
            format!("{:x}", crate::util::hasher::hash_buffer_128bit(&bytes)),
            "4e9dc5166cda4542d4e61b88da4bde9"
        );
    }

    /// Arbitrary junk bytes fail to decode without panicking.
    #[test]
    fn junk_bytes_fail_to_decode() {
        assert!(ChunkManifest::from_bytes(b"not a manifest").is_err());
        assert!(ChunkManifest::from_bytes(&[]).is_err());
    }

    /// `chunk_at` finds the covering chunk at the first byte, at chunk boundaries,
    /// at the final byte, and returns None at EOF.
    #[test]
    fn chunk_at_boundaries() {
        let manifest = manifest_with_chunks(three_chunks());
        assert_eq!(manifest.chunk_at(0).map(|c| c.hash), Some(1));
        assert_eq!(manifest.chunk_at(65535).map(|c| c.hash), Some(1));
        assert_eq!(manifest.chunk_at(65536).map(|c| c.hash), Some(2));
        assert_eq!(manifest.chunk_at(73727).map(|c| c.hash), Some(2));
        assert_eq!(manifest.chunk_at(73728).map(|c| c.hash), Some(3));
        assert_eq!(manifest.chunk_at(73827).map(|c| c.hash), Some(3));
        assert_eq!(manifest.chunk_at(73828), None);
        assert_eq!(manifest.chunk_at(u64::MAX), None);

        let empty = manifest_with_chunks(vec![]);
        assert_eq!(empty.chunk_at(0), None);
    }
}
