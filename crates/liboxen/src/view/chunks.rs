//! Request/response types for the block-dedup wire protocol (chunk negotiation
//! and block/manifest transfer). Chunk and block hashes travel as lowercase hex
//! strings of their xxh3-128 values.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::OxenError;

use super::StatusMessage;

/// Body of `POST /chunks/missing`: the chunk hashes the client wants to negotiate.
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct ChunkHashesRequest {
    pub hashes: Vec<String>,
}

/// Response to `POST /chunks/missing`: the subset of the requested chunk hashes
/// this store does not have.
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct MissingChunksResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub missing: Vec<String>,
}

/// Parse a wire chunk/block hash (lowercase hex xxh3-128) into its u128 value.
pub fn parse_chunk_hash(hash: &str) -> Result<u128, OxenError> {
    u128::from_str_radix(hash, 16)
        .map_err(|_| OxenError::basic_str(format!("invalid chunk hash: {hash}")))
}

/// Render a chunk/block hash for the wire (unpadded lowercase hex, matching the
/// repo-wide hash-string convention).
pub fn format_chunk_hash(hash: u128) -> String {
    format!("{hash:x}")
}
