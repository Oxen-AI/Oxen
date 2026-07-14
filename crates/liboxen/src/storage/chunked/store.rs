//! The [`ChunkedVersionStore`] capability trait: what a version store that can hold
//! block-backed (chunked) versions exposes, beyond the transparent [`VersionStore`]
//! read surface.
//!
//! Reads stay transparent — `get_version_stream`, `copy_version_to_path`,
//! `get_version_chunk`, `version_exists`, … work identically on chunked versions —
//! so this trait carries only the *explicit* operations: chunked ingest, manifest
//! exchange, chunk negotiation, and block transfer. A store advertises the
//! capability through [`VersionStore::chunked`]; a store that returns `None` never
//! stores chunked versions.
//!
//! [`VersionStore`]: crate::storage::version_store::VersionStore
//! [`VersionStore::chunked`]: crate::storage::version_store::VersionStore::chunked

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncRead;

use crate::error::OxenError;
use crate::model::EntryDataType;

use super::manifest::ChunkManifest;

/// Explicit chunked-version operations for stores that support block-backed storage.
///
/// On such a store, a chunked version is a published manifest plus blocks holding
/// its chunks; **manifest existence marks the version as chunked** (no FileNode
/// field ever does). Both representations — whole-file blob and manifest+blocks —
/// may coexist in one store; per-version, exactly one is used.
#[async_trait]
pub trait ChunkedVersionStore: Send + Sync {
    /// Chunk, compress, pack, and index a new version in one streaming pass, then
    /// validate and publish its manifest.
    ///
    /// `hash` is the claimed whole-file content hash; the pass hashes while
    /// chunking and refuses to publish on mismatch (blocks already written are
    /// reclaimable orphans, never referenced). Idempotent: if a manifest for
    /// `hash` is already published, the reader is not consumed and the existing
    /// manifest is returned — a published manifest is never overwritten.
    ///
    /// `data_type` and `extension` drive the codec policy (e.g. parquet skips the
    /// zstd attempt).
    async fn store_version_chunked(
        &self,
        hash: &str,
        data_type: &EntryDataType,
        extension: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<ChunkManifest, OxenError>;

    /// The published manifest for `hash`, or `None` if this store does not hold
    /// that version chunked.
    async fn get_manifest(&self, hash: &str) -> Result<Option<ChunkManifest>, OxenError>;

    /// Validate and publish a manifest produced elsewhere (push/pull).
    ///
    /// Validation is full streamed reconstruction: every referenced chunk must be
    /// present, and the reconstructed bytes must match `file_size` and the
    /// manifest's file hash — without this, a buggy sender's manifest is only
    /// discovered at checkout. Publication is atomic. If a validated manifest
    /// already exists for the file hash, this is a no-op success (recipes from
    /// different chunker policies may legitimately differ; the existing manifest
    /// stays canonical).
    async fn put_manifest(&self, manifest: &ChunkManifest) -> Result<(), OxenError>;

    /// The subset of `hashes` this store does not have chunks for, in input order
    /// (dedup probes and push negotiation).
    async fn missing_chunks(&self, hashes: &[u128]) -> Result<Vec<u128>, OxenError>;

    /// Verify a complete transfer block against `hash` and its own footer claims
    /// (every chunk decoded and hash-checked), then store and index it. Idempotent.
    async fn store_block(&self, hash: &str, data: Bytes) -> Result<(), OxenError>;

    /// Rebuild the chunk index from block footers (the index is disposable derived
    /// state). Returns the number of blocks scanned.
    async fn rebuild_chunk_index(&self) -> Result<u64, OxenError>;
}
