//! Errors from the dedup layer.
//! crate::error absorbs these into OxenError through a #[from] declared there.

use thiserror::Error;

/// Failures from chunking.
#[derive(Debug, Error)]
pub enum ChunkedError {
    #[error(
        "Unknown chunker id {0}. This repository was written by a newer version of oxen, or its metadata is corrupt"
    )]
    UnknownChunkerId(u8),

    #[error("Failed to read data while chunking: {0}")]
    ChunkRead(std::io::Error),
}
