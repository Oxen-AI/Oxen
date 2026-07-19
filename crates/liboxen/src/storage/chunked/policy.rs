//! The single policy function: which files get chunked, and with which chunker,
//! codec, and transform.
//!
//! Behavior is versioned, not ambient: an explicit [`StorageProfile`] mark (from
//! the repo config's `[[storage.profiles]]` rules) selects a stable named
//! pipeline, extension sniffing fills in the default, and any behavior change
//! ships as a new profile name / registry ID — never a silent change to an
//! existing one.

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
    /// Shared-dictionary content class: dictionaries are trained and applied per
    /// class so one format's patterns never dilute another's. Derived from the
    /// extension family; the engine refines it with the auto chunker's row-size
    /// sniff (long-row and small-row line-delimited JSON are distinct families).
    pub dict_class: u8,
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

/// Extensions treated as line-delimited JSON: rows are records, and records that
/// grow do so by appending to nested arrays (chat sessions, agent traces). These
/// route to the structure-anchored trace chunker so dedup granularity follows the
/// data's row/message structure instead of raw byte windows.
const LINE_DELIMITED_JSON_EXTENSIONS: &[&str] = &["jsonl", "ndjson"];

/// An explicit, user-set mark selecting a file's chunking + compression pipeline,
/// overriding data-type/extension sniffing. Marks are attached per path (repo config
/// `[[storage.profiles]]` rules; see `StorageConfig::profile_for_path`) and resolved
/// at ingest.
///
/// Profile names are part of the repo-config contract: **append-only, never
/// renamed**. A profile pins which pipeline a mark selects; behavior changes ship as
/// a new profile name, never a silent remap of an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProfile {
    /// Byte-oriented content-defined chunking (FastCDC) with sniffed codec — the
    /// default pipeline, and the explicit opt-out from any smarter routing.
    Generic,
    /// Structure-anchored chunking for line-delimited JSON traces whose rows grow
    /// by appending (chat sessions, agent traces). See `TraceJsonlChunker`.
    TraceJsonl,
}

impl StorageProfile {
    /// Parse a profile mark by name. `None` for unknown names — callers surface
    /// that loudly (a typo'd mark must never silently fall back to sniffing).
    pub fn from_name(name: &str) -> Option<StorageProfile> {
        match name {
            "generic" => Some(StorageProfile::Generic),
            "trace-jsonl" => Some(StorageProfile::TraceJsonl),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            StorageProfile::Generic => "generic",
            StorageProfile::TraceJsonl => "trace-jsonl",
        }
    }

    /// Every registered profile name, for error messages.
    pub fn known_names() -> &'static [&'static str] {
        &["generic", "trace-jsonl"]
    }
}

/// Select the chunker, codec, and transform for a file — the one place the
/// `(profile, EntryDataType, extension)` → pipeline mapping lives.
///
/// An explicit [`StorageProfile`] mark decides the chunker outright. Without one,
/// the extension routes line-delimited JSON to the structure-anchored trace chunker
/// and everything else to generic FastCDC. The codec is sniffed either way:
/// text-like content (text; tabular in text encodings such as CSV/TSV/JSONL) tries
/// zstd with raw fallback, while already-compressed media, binaries, and compressed
/// containers (notably parquet, which is `Tabular` by data type) skip the attempt —
/// the universal raw fallback makes the codec choice safe for any bytes.
pub fn encode_policy(
    profile: Option<StorageProfile>,
    data_type: &EntryDataType,
    extension: &str,
) -> EncodePolicy {
    let extension = extension.to_lowercase();
    let compressed_container = COMPRESSED_CONTAINER_EXTENSIONS.contains(&extension.as_str());

    let codec = match data_type {
        EntryDataType::Text | EntryDataType::Tabular if !compressed_container => CodecId::ZSTD,
        _ => CodecId::RAW,
    };

    let chunker = match profile {
        Some(StorageProfile::Generic) => ChunkerId::GENERIC_FASTCDC_V1,
        Some(StorageProfile::TraceJsonl) => ChunkerId::TRACE_JSONL_V1,
        None if LINE_DELIMITED_JSON_EXTENSIONS.contains(&extension.as_str()) => {
            ChunkerId::TRACE_AUTO_V1
        }
        None => ChunkerId::GENERIC_FASTCDC_V1,
    };

    let dict_class = if LINE_DELIMITED_JSON_EXTENSIONS.contains(&extension.as_str()) {
        2
    } else if matches!(extension.as_str(), "csv" | "tsv") {
        1
    } else {
        0
    };

    EncodePolicy {
        chunker,
        codec,
        transform: TransformId::IDENTITY,
        dict_class,
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
        let policy = |dt: &EntryDataType, ext: &str| encode_policy(None, dt, ext).codec;

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
        let policy = encode_policy(None, &EntryDataType::Tabular, "csv");
        assert_eq!(policy.chunker, ChunkerId::GENERIC_FASTCDC_V1);
        assert_eq!(policy.transform, TransformId::IDENTITY);
    }

    /// An explicit storage-profile mark decides the chunker outright, overriding
    /// extension sniffing in both directions.
    #[test]
    fn explicit_profile_overrides_sniffing() {
        // Mark a .txt file as a trace: trace chunker despite the extension.
        let marked_trace = encode_policy(
            Some(StorageProfile::TraceJsonl),
            &EntryDataType::Text,
            "txt",
        );
        assert_eq!(marked_trace.chunker, ChunkerId::TRACE_JSONL_V1);

        // Mark a .jsonl file generic: opts out of the trace chunker.
        let marked_generic = encode_policy(
            Some(StorageProfile::Generic),
            &EntryDataType::Tabular,
            "jsonl",
        );
        assert_eq!(marked_generic.chunker, ChunkerId::GENERIC_FASTCDC_V1);
    }

    /// Profile names are a stable, append-only contract; unknown names parse to
    /// `None` so callers can fail loudly.
    #[test]
    fn profile_names_round_trip() {
        for name in StorageProfile::known_names() {
            let profile = StorageProfile::from_name(name).expect("known name parses");
            assert_eq!(profile.name(), *name);
        }
        assert_eq!(StorageProfile::from_name("trace-parquet"), None);
        assert_eq!(StorageProfile::from_name(""), None);
    }

    /// Line-delimited JSON routes to the structure-anchored trace chunker (any data
    /// type — the chunker is safe on arbitrary bytes); everything else stays on
    /// generic FastCDC.
    #[test]
    fn line_delimited_json_uses_trace_chunker() {
        let chunker = |dt: &EntryDataType, ext: &str| encode_policy(None, dt, ext).chunker;

        assert_eq!(
            chunker(&EntryDataType::Tabular, "jsonl"),
            ChunkerId::TRACE_AUTO_V1
        );
        assert_eq!(
            chunker(&EntryDataType::Tabular, "JSONL"),
            ChunkerId::TRACE_AUTO_V1
        );
        assert_eq!(
            chunker(&EntryDataType::Text, "ndjson"),
            ChunkerId::TRACE_AUTO_V1
        );
        assert_eq!(
            chunker(&EntryDataType::Binary, "jsonl"),
            ChunkerId::TRACE_AUTO_V1
        );

        // Whole-file JSON is not line-delimited; CSV rows are tiny and stay generic.
        assert_eq!(
            chunker(&EntryDataType::Text, "json"),
            ChunkerId::GENERIC_FASTCDC_V1
        );
        assert_eq!(
            chunker(&EntryDataType::Tabular, "csv"),
            ChunkerId::GENERIC_FASTCDC_V1
        );
        assert_eq!(
            chunker(&EntryDataType::Tabular, "parquet"),
            ChunkerId::GENERIC_FASTCDC_V1
        );
    }
}
