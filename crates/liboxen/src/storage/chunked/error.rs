//! Local error enum for the block-level dedup storage layer, bridged into
//! [`OxenError`](crate::error::OxenError) via `#[from]`.

use thiserror::Error;

/// Errors from chunking, compression, and the block/manifest formats.
///
/// The `Unknown*Id` variants are structured upgrade-required errors: they mean the
/// repository contains data written by a newer oxen with an ID this build does not
/// know (IDs are append-only, never reused — see `registry`). They must never be
/// downgraded to a silent fallback.
#[derive(Debug, Error)]
pub enum ChunkedError {
    #[error(
        "unknown chunker id {0}: this repository was written by a newer version of oxen, please upgrade"
    )]
    UnknownChunkerId(u8),

    #[error(
        "unknown codec id {0}: this repository was written by a newer version of oxen, please upgrade"
    )]
    UnknownCodecId(u8),

    #[error(
        "unknown transform id {0}: this repository was written by a newer version of oxen, please upgrade"
    )]
    UnknownTransformId(u8),

    #[error("failed to read data while chunking: {0}")]
    ChunkRead(std::io::Error),

    #[error("failed to compress chunk: {0}")]
    Compress(std::io::Error),

    #[error("failed to decompress chunk: {0}")]
    Decompress(std::io::Error),

    #[error("decompressed chunk length {actual} does not match declared raw length {expected}")]
    DecodedLenMismatch { expected: usize, actual: usize },
}
