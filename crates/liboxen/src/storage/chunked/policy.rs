//! The single policy function: which files get chunked, and with which chunker,
//! codec, and transform.
//!
//! Behavior is versioned, not ambient: repo config selects a stable *named* policy
//! (v1 ships only `generic-fastcdc-v1`), and a new mapping ships as a new named
//! policy ID — never a silent change to an existing one.

use std::sync::LazyLock;

use crate::model::EntryDataType;

use super::registry::{ChunkerId, CodecId, TransformId};

/// Files at or above this size are chunked; smaller files keep the whole-file path
/// (chunking them is pure overhead). 1 MiB deliberately includes the small-but-hot
/// labeled-CSV workload this feature targets.
pub const DEDUP_MIN_FILE_SIZE: u64 = 1024 * 1024;

/// The chunker/codec/transform selection for one file, from
/// [`encode_policy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodePolicy {
    pub chunker: ChunkerId,
    pub codec: CodecId,
    pub transform: TransformId,
}

/// Returns the active minimum file size for chunking.
///
/// Reads `OXEN_DEDUP_MIN_FILE_SIZE` once at first call and caches it for the process
/// — a test-infra override (same pattern as `OXEN_STREAM_SEGMENT_SIZE`), not a
/// user-facing knob. The env var must be set before this is first invoked.
pub fn dedup_min_file_size() -> u64 {
    static CACHED: LazyLock<u64> = LazyLock::new(|| {
        std::env::var("OXEN_DEDUP_MIN_FILE_SIZE")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEDUP_MIN_FILE_SIZE)
    });
    *CACHED
}

/// Whether a file of `size` bytes should be stored chunked.
///
/// v1 policy is size-only — every file at or above the floor chunks, any data type.
/// `data_type` is part of the signature so future format-aware policies slot in
/// without changing call sites.
pub fn should_chunk(_data_type: &EntryDataType, size: u64) -> bool {
    size >= dedup_min_file_size()
}

/// File extensions whose contents are already-compressed containers: a zstd pass
/// over them is wasted CPU that raw fallback then discards, so the codec policy
/// skips the attempt.
const COMPRESSED_CONTAINER_EXTENSIONS: &[&str] = &[
    "parquet", "arrow", "feather", "gz", "gzip", "zip", "zst", "bz2", "xz", "7z",
];

/// Select the chunker, codec, and transform for a file — the one place the
/// `(EntryDataType, extension)` → pipeline mapping lives (`generic-fastcdc-v1`).
///
/// Text-like content (text; tabular in text encodings such as CSV/TSV/JSONL) tries
/// zstd with raw fallback. Already-compressed media, binaries, and compressed
/// containers (notably parquet, which is `Tabular` by data type) skip the attempt.
pub fn encode_policy(data_type: &EntryDataType, extension: &str) -> EncodePolicy {
    let extension = extension.to_lowercase();
    let compressed_container = COMPRESSED_CONTAINER_EXTENSIONS.contains(&extension.as_str());

    let codec = match data_type {
        EntryDataType::Text | EntryDataType::Tabular if !compressed_container => CodecId::ZSTD,
        _ => CodecId::RAW,
    };

    EncodePolicy {
        chunker: ChunkerId::GENERIC_FASTCDC_V1,
        codec,
        transform: TransformId::IDENTITY,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// v1 chunking policy is size-only: the floor is inclusive and applies to every
    /// data type.
    #[test]
    fn should_chunk_is_size_only() {
        let floor = dedup_min_file_size();
        for data_type in [
            EntryDataType::Text,
            EntryDataType::Tabular,
            EntryDataType::Binary,
            EntryDataType::Image,
        ] {
            assert!(should_chunk(&data_type, floor));
            assert!(should_chunk(&data_type, floor + 1));
            assert!(!should_chunk(&data_type, floor - 1));
            assert!(!should_chunk(&data_type, 0));
        }
    }

    /// Text-like content compresses; parquet (Tabular by data type but an
    /// already-compressed container by extension) and media/binary skip zstd.
    #[test]
    fn codec_policy_by_type_and_extension() {
        let policy = |dt: &EntryDataType, ext: &str| encode_policy(dt, ext).codec;

        assert_eq!(policy(&EntryDataType::Tabular, "csv"), CodecId::ZSTD);
        assert_eq!(policy(&EntryDataType::Tabular, "tsv"), CodecId::ZSTD);
        assert_eq!(policy(&EntryDataType::Tabular, "jsonl"), CodecId::ZSTD);
        assert_eq!(policy(&EntryDataType::Text, "txt"), CodecId::ZSTD);

        assert_eq!(policy(&EntryDataType::Tabular, "parquet"), CodecId::RAW);
        assert_eq!(policy(&EntryDataType::Tabular, "PARQUET"), CodecId::RAW);
        assert_eq!(policy(&EntryDataType::Tabular, "arrow"), CodecId::RAW);
        assert_eq!(policy(&EntryDataType::Text, "gz"), CodecId::RAW);
        assert_eq!(policy(&EntryDataType::Binary, "bin"), CodecId::RAW);
        assert_eq!(policy(&EntryDataType::Image, "png"), CodecId::RAW);
        assert_eq!(policy(&EntryDataType::Video, "mp4"), CodecId::RAW);
        assert_eq!(policy(&EntryDataType::Audio, "wav"), CodecId::RAW);
    }

    /// Every policy result uses registered IDs and the identity transform in v1.
    #[test]
    fn policy_pins_chunker_and_transform() {
        let policy = encode_policy(&EntryDataType::Tabular, "csv");
        assert_eq!(policy.chunker, ChunkerId::GENERIC_FASTCDC_V1);
        assert_eq!(policy.transform, TransformId::IDENTITY);
    }
}
