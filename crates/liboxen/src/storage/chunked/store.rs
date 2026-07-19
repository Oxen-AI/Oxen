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

use super::block::SealedBlock;
use super::manifest::ChunkManifest;
use super::policy::StorageProfile;
use super::seekable::SeekableVersionReader;

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
    /// `profile` is the file's explicit storage-profile mark, if any; with
    /// `data_type` and `extension` it drives the encode policy (chunker selection,
    /// and e.g. parquet skipping the zstd attempt).
    async fn store_version_chunked(
        &self,
        hash: &str,
        profile: Option<StorageProfile>,
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

    /// Pack exactly the requested chunks into fresh transfer blocks (stored
    /// payloads copied as-is — compressed chunks travel compressed). Callers bound
    /// their batches (e.g. by summed raw length) to keep memory bounded.
    async fn pack_chunks(&self, hashes: &[u128]) -> Result<Vec<SealedBlock>, OxenError>;

    /// Open a sync `Read + Seek` over the reconstructed bytes of a chunked version
    /// — the random-access reader for consumers like eager Parquet/IPC readers.
    /// The consumer should run inside one `spawn_blocking`.
    async fn open_seekable(&self, hash: &str) -> Result<SeekableVersionReader, OxenError>;

    /// Delete a version's whole-file blob, leaving its chunked representation
    /// (migration to block-v1, after the manifest is durably published).
    ///
    /// Refuses unless a published manifest exists for `hash` — a version's last
    /// representation can never be deleted through this method. Idempotent once
    /// the manifest exists.
    async fn delete_whole_file_blob(&self, hash: &str) -> Result<(), OxenError>;

    /// Delete a version's manifest, leaving its whole-file blob (migration back
    /// to legacy, after the reconstructed blob is durably published). Blocks are
    /// left in place for GC — other manifests may share their chunks.
    ///
    /// Refuses unless the whole-file blob exists for `hash` — a version's last
    /// representation can never be deleted through this method. Idempotent once
    /// the blob exists.
    async fn delete_manifest(&self, hash: &str) -> Result<(), OxenError>;

    /// Rebuild the chunk index from block footers (the index is disposable derived
    /// state). Returns the number of blocks scanned.
    async fn rebuild_chunk_index(&self) -> Result<u64, OxenError>;

    /// Reclaim persistent page slack in the store-local chunk index (no-op when
    /// there is little to reclaim, and for backends without a compactable local
    /// index). Call only at the end of a write batch — index writes made by this
    /// process after compaction may be invisible to later opens, which the index
    /// tolerates because it is rebuildable derived state.
    async fn compact_chunk_index(&self) -> Result<(), OxenError> {
        Ok(())
    }

    /// Clear the disposable chunk index while leaving blocks and manifests
    /// untouched. Reverse migration uses this before allowing pre-block binaries
    /// to open the repository, so a later forward migration cannot trust stale
    /// locations for blocks an older cleaner removed.
    async fn clear_chunk_index(&self) -> Result<(), OxenError> {
        Err(OxenError::basic_str(
            "this chunked storage backend cannot clear its chunk index",
        ))
    }
}
