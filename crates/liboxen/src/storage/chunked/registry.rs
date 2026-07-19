//! The single registry of chunker, codec, and transform IDs.
//!
//! IDs are `u8`, **append-only, never reused**. Each ID pins an exact byte-level
//! behavior forever: a chunker ID freezes a boundary function, a codec ID freezes an
//! encoding, a transform ID freezes a reversible whole-file transform. Any
//! behavior-affecting change ships as a **new** ID — never an in-place upgrade —
//! because data written under the old ID must stay readable and reproducible.
//!
//! An unknown ID on read fails with a structured upgrade-required
//! [`ChunkedError`] — never a panic, never a silent fallback.
//!
//! Contributor checklist for a new chunker or codec: implement the trait, add the ID
//! constant here, register the instance in [`chunker`]/[`codec`], extend the policy in
//! `policy.rs` (or add a named policy), add golden fixtures, and run the shared
//! conformance suite.

use serde::{Deserialize, Serialize};

use super::chunker::{Chunker, FastCdc2020Chunker};
use super::compressor::{Compressor, RawCodec, ZstdCodec};
use super::error::ChunkedError;
use super::trace_chunker::{AutoTraceChunker, TraceJsonlChunker};

/// Identifies the exact chunk-boundary function used to produce a manifest.
///
/// Recorded in every manifest: same file bytes + same `ChunkerId` ⇒ same manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChunkerId(u8);

impl ChunkerId {
    /// FastCDC v2020 (normalized), min 8 KiB / target 64 KiB / max 128 KiB.
    ///
    /// This freezes the *boundary function*, not the crate: golden boundary fixtures
    /// pin it, and a `fastcdc` upgrade that shifts boundaries must ship as a new ID.
    pub const GENERIC_FASTCDC_V1: ChunkerId = ChunkerId(1);

    /// Structure-anchored chunking for line-delimited JSON traces: cuts only at row
    /// boundaries and depth-2 array element separators, with prefix-stable intra-row
    /// cuts. Built for chat/agent-trace tables whose rows grow by appending.
    pub const TRACE_JSONL_V1: ChunkerId = ChunkerId(2);

    /// Content-adaptive line-delimited JSON chunking: sniffs average row size and
    /// delegates to [`Self::TRACE_JSONL_V1`] for long-trace rows or
    /// [`Self::GENERIC_FASTCDC_V1`] for small rows. The sniff rule is part of the
    /// frozen boundary function.
    pub const TRACE_AUTO_V1: ChunkerId = ChunkerId(3);

    // 0 is permanently reserved as "never a valid chunker" so zeroed data can't
    // masquerade as a manifest.

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

/// Identifies the per-chunk encoding stored inside a block.
///
/// Recorded in the block footer's per-chunk `flags` byte. Store-local: a codec ID
/// never crosses the wire as a durable reference, so a store may adopt a new codec
/// for new blocks (or repack old ones) without any manifest or wire change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CodecId(u8);

impl CodecId {
    /// Chunk bytes stored verbatim (the universal fallback).
    pub const RAW: CodecId = CodecId(0);
    /// Zstandard.
    pub const ZSTD: CodecId = CodecId(1);
    /// Zstandard against a store-local shared dictionary. Payload =
    /// `[16-byte dictionary hash LE][zstd frame]`. Decoded by the block engine
    /// (which owns dictionary storage); never crosses the wire — transfer packing
    /// re-encodes these chunks as plain [`CodecId::ZSTD`].
    pub const ZSTD_DICT: CodecId = CodecId(2);
    /// Zstandard delta against a base chunk already stored in this store. Payload =
    /// `[16-byte base chunk hash LE][zstd frame using the base's raw bytes as
    /// dictionary]`. Chain depth is at most one (a base is never itself a delta).
    /// Store-local; transfer packing re-encodes as plain [`CodecId::ZSTD`].
    pub const ZSTD_DELTA: CodecId = CodecId(3);

    pub fn as_u8(&self) -> u8 {
        self.0
    }

    /// Parse a codec ID read from stored data, rejecting IDs this build doesn't know.
    pub fn from_u8(id: u8) -> Result<CodecId, ChunkedError> {
        codec(CodecId(id)).map(|c| c.id())
    }
}

/// Identifies the reversible whole-file transform applied before chunking.
///
/// Recorded in every manifest. v1 only ever writes [`TransformId::IDENTITY`]; the
/// slot exists so future transforms (e.g. reframing already-compressed containers)
/// are a manifest format bump, not a redesign.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransformId(u8);

impl TransformId {
    /// No transform: chunk the original file bytes.
    pub const IDENTITY: TransformId = TransformId(0);

    /// Embedded-zstd unwrap: zstd frames found in the file (parquet pages, arrow
    /// buffers, bare `.zst`) whose bytes this build's encoder provably reproduces
    /// are stored decompressed, with a recipe to re-compress them on
    /// reconstruction. See `unwrap_transform`.
    pub const ZSTD_UNWRAP_V1: TransformId = TransformId(1);

    pub fn as_u8(&self) -> u8 {
        self.0
    }
}

/// Look up the chunker for `id`.
pub fn chunker(id: ChunkerId) -> Result<&'static dyn Chunker, ChunkedError> {
    static FASTCDC: FastCdc2020Chunker = FastCdc2020Chunker;
    static TRACE_JSONL: TraceJsonlChunker = TraceJsonlChunker;
    static TRACE_AUTO: AutoTraceChunker = AutoTraceChunker;
    match id {
        ChunkerId::GENERIC_FASTCDC_V1 => Ok(&FASTCDC),
        ChunkerId::TRACE_JSONL_V1 => Ok(&TRACE_JSONL),
        ChunkerId::TRACE_AUTO_V1 => Ok(&TRACE_AUTO),
        ChunkerId(other) => Err(ChunkedError::UnknownChunkerId(other)),
    }
}

/// Look up the codec for `id`.
pub fn codec(id: CodecId) -> Result<&'static dyn Compressor, ChunkedError> {
    static RAW: RawCodec = RawCodec;
    static ZSTD: ZstdCodec = ZstdCodec;
    static ZSTD_DICT: ZstdDictMarker = ZstdDictMarker;
    static ZSTD_DELTA: ZstdDeltaMarker = ZstdDeltaMarker;
    match id {
        CodecId::RAW => Ok(&RAW),
        CodecId::ZSTD => Ok(&ZSTD),
        CodecId::ZSTD_DICT => Ok(&ZSTD_DICT),
        CodecId::ZSTD_DELTA => Ok(&ZSTD_DELTA),
        CodecId(other) => Err(ChunkedError::UnknownCodecId(other)),
    }
}

/// Registry marker for [`CodecId::ZSTD_DICT`]: the ID is valid (footer parsing and
/// index round-trips accept it), but encode/decode require dictionary context that
/// only the block engine holds — a context-free decode attempt (e.g. a transfer
/// block claiming this codec) fails loudly instead of guessing.
struct ZstdDictMarker;

impl crate::storage::chunked::compressor::Compressor for ZstdDictMarker {
    fn id(&self) -> CodecId {
        CodecId::ZSTD_DICT
    }

    fn compress(&self, _raw: &[u8]) -> Result<Vec<u8>, ChunkedError> {
        Err(ChunkedError::Compress(std::io::Error::other(
            "dictionary-compressed chunks are encoded only by the block engine",
        )))
    }

    fn decompress(&self, _stored: &[u8], _raw_len: usize) -> Result<Vec<u8>, ChunkedError> {
        Err(ChunkedError::Decompress(std::io::Error::other(
            "dictionary-compressed chunks require store-local dictionary context",
        )))
    }
}

/// Registry marker for [`CodecId::ZSTD_DELTA`]: same contract as
/// [`ZstdDictMarker`] — the ID is valid, but decoding requires the base chunk,
/// which only the block engine can resolve.
struct ZstdDeltaMarker;

impl crate::storage::chunked::compressor::Compressor for ZstdDeltaMarker {
    fn id(&self) -> CodecId {
        CodecId::ZSTD_DELTA
    }

    fn compress(&self, _raw: &[u8]) -> Result<Vec<u8>, ChunkedError> {
        Err(ChunkedError::Compress(std::io::Error::other(
            "delta chunks are encoded only by the block engine",
        )))
    }

    fn decompress(&self, _stored: &[u8], _raw_len: usize) -> Result<Vec<u8>, ChunkedError> {
        Err(ChunkedError::Decompress(std::io::Error::other(
            "delta chunks require their base chunk to decode",
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Registered IDs resolve to implementations that report the same ID back;
    /// unknown IDs fail with the structured upgrade-required error.
    #[test]
    fn registry_round_trips_ids() -> Result<(), ChunkedError> {
        assert_eq!(
            chunker(ChunkerId::GENERIC_FASTCDC_V1)?.id(),
            ChunkerId::GENERIC_FASTCDC_V1
        );
        assert_eq!(
            chunker(ChunkerId::TRACE_JSONL_V1)?.id(),
            ChunkerId::TRACE_JSONL_V1
        );
        assert_eq!(
            chunker(ChunkerId::TRACE_AUTO_V1)?.id(),
            ChunkerId::TRACE_AUTO_V1
        );
        assert_eq!(codec(CodecId::RAW)?.id(), CodecId::RAW);
        assert_eq!(codec(CodecId::ZSTD)?.id(), CodecId::ZSTD);

        assert!(matches!(
            chunker(ChunkerId(0)),
            Err(ChunkedError::UnknownChunkerId(0))
        ));
        assert!(matches!(
            chunker(ChunkerId(200)),
            Err(ChunkedError::UnknownChunkerId(200))
        ));
        assert!(matches!(
            codec(CodecId(200)),
            Err(ChunkedError::UnknownCodecId(200))
        ));
        assert!(matches!(
            CodecId::from_u8(200),
            Err(ChunkedError::UnknownCodecId(200))
        ));
        Ok(())
    }

    /// The on-disk / on-wire numeric values are a stable contract.
    #[test]
    fn ids_are_stable() {
        assert_eq!(ChunkerId::GENERIC_FASTCDC_V1.as_u8(), 1);
        assert_eq!(ChunkerId::TRACE_JSONL_V1.as_u8(), 2);
        assert_eq!(ChunkerId::TRACE_AUTO_V1.as_u8(), 3);
        assert_eq!(CodecId::RAW.as_u8(), 0);
        assert_eq!(CodecId::ZSTD.as_u8(), 1);
        assert_eq!(CodecId::ZSTD_DICT.as_u8(), 2);
        assert_eq!(CodecId::ZSTD_DELTA.as_u8(), 3);
        assert_eq!(TransformId::IDENTITY.as_u8(), 0);
        assert_eq!(TransformId::ZSTD_UNWRAP_V1.as_u8(), 1);
    }
}
