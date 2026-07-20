//! The backend seam for Merkle tree node storage: an engine-agnostic trait over the *bytes* of a
//! node.
//!
//! A node is stored as two opaque byte blobs keyed by its [`MerkleHash`]:
//! - the `node` blob — the node's own metadata plus the lookup table describing its children
//!   (offset + length into the `children` blob), and
//! - the `children` blob — the serialized child nodes concatenated together.
//!
//! This is deliberately the *same* encoding [`MerkleNodeDB`](super::merkle_node_db) has always
//! produced: the trait isolates only the question of *where the two blobs live* (e.g. two files on
//! disk vs. two keys in an embedded KV store), leaving the msgpack + lookup-table framing
//! untouched. Keeping the bytes identical across engines is what makes a future engine-to-engine
//! bridge trivial.
//!
//! [`MerkleNodeDB`](super::merkle_node_db) reads and writes through a `MerkleNodeStore`;
//! [`FsMerkleNodeStore`](super::fs_merkle_node_store) is the filesystem implementation of `MerkleNodeStore`
//! [`LmdbMerkleNodeStore`](super::lmdb_merkle_node_store) is the LMDB implementation of `MerkleNodeStore`

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::error::OxenError;
use crate::model::MerkleHash;

use super::fs_merkle_node_store::FsMerkleNodeStore;
use super::lmdb_merkle_node_store::LmdbMerkleNodeStore;
use super::merkle_node_db::MerkleDbError;

/// Which engine backs a repo's Merkle node store. The backend determines the on-disk format, so it
/// is a fixed property of a repo for its lifetime — it cannot be flipped on an existing repo
/// without a migration.
///
/// Persisted per-repo as `merkle_node_backend` in `config.toml` (serialized lowercase:
/// `"filesystem"` / `"lmdb"`); see [`create_merkle_node_store`] for how a repo's backend is
/// resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MerkleNodeBackend {
    /// Two files per node under `.oxen/tree/nodes` (the historical, default backend).
    Filesystem,
    /// One LMDB env under `.oxen/tree/nodes_lmdb`.
    Lmdb,
}

/// The backend a newly created repo uses when the caller doesn't request a specific one.
pub(crate) const DEFAULT_MERKLE_NODE_BACKEND: MerkleNodeBackend = MerkleNodeBackend::Filesystem;

/// Engine-agnostic persistence for Merkle tree node bytes, keyed by [`MerkleHash`]. A node is two
/// blobs (`node` + `children`); see the module docs for the layout. Implementations persist and
/// retrieve those blobs and nothing more — the framing lives in [`MerkleNodeDB`](super::merkle_node_db).
///
/// `read_node` / `read_children` return [`MerkleDbError::MissingNodeDir`] when the node is absent,
/// matching the file backend's "open a node that was never written" behavior; callers gate reads
/// with [`MerkleNodeStore::exists`].
///
/// `Debug` is required (like [`VersionStore`](crate::storage::VersionStore)) so a store can be held
/// by `#[derive(Debug)]` types such as `LocalRepository`.
pub(crate) trait MerkleNodeStore: Debug + Send + Sync {
    /// Whether a node has been written for `hash`.
    fn exists(&self, hash: &MerkleHash) -> Result<bool, MerkleDbError>;

    /// The `node` blob for `hash` (metadata + children lookup table).
    fn read_node(&self, hash: &MerkleHash) -> Result<Bytes, MerkleDbError>;

    /// The `children` blob for `hash` (concatenated child nodes); empty for a childless node.
    fn read_children(&self, hash: &MerkleHash) -> Result<Bytes, MerkleDbError>;

    /// The byte lengths of the `(node, children)` blobs for `hash`, without materializing the
    /// blobs. Returns [`MerkleDbError::MissingNodeDir`] when the node is absent. Lets the
    /// transport-size estimate size the wire payload without reading every node into memory.
    fn node_byte_sizes(&self, hash: &MerkleHash) -> Result<(u64, u64), MerkleDbError>;

    /// The hashes of every node currently persisted. Ordering is unspecified. Used by the
    /// whole-tree transport path to enumerate what to pack, so there is exactly one path from
    /// stored bytes to the wire — no backend-specific directory walking outside the store.
    fn list_hashes(&self) -> Result<Vec<MerkleHash>, MerkleDbError>;

    /// Persist both blobs for `hash` as one unit. Implementations make this atomic so a node is
    /// never observable with only one of its two blobs present.
    fn write_node(
        &self,
        hash: &MerkleHash,
        node: Bytes,
        children: Bytes,
    ) -> Result<(), MerkleDbError>;

    /// Persist many nodes at once and return the hashes actually written. With `overwrite_existing`
    /// false, a node already present is left untouched and kept out of the returned set.
    ///
    /// Unpacking a commit's tree writes one node for every directory and vnode. A large repo has
    /// tens of thousands of them, and writing them one at a time means tens of thousands of
    /// separate trips to the store. Each backend implements batching: the filesystem backend writes
    /// files in parallel across threads; the LMDB backend commits the batch in a single transaction.
    fn write_nodes(
        &self,
        nodes: Vec<(MerkleHash, Bytes, Bytes)>,
        overwrite_existing: bool,
    ) -> Result<Vec<MerkleHash>, MerkleDbError>;

    /// Remove the node for `hash` (both blobs). Idempotent: deleting an absent node is `Ok`.
    fn delete(&self, hash: &MerkleHash) -> Result<(), MerkleDbError>;

    /// Copy the store's durable state into `dst_dir` for archiving, returning the file written.
    ///
    /// The copy is point-in-time consistent even while the store is in use and omits any
    /// runtime-only files (e.g. an LMDB lock file). Backends whose on-disk representation is a tree
    /// of plain files (the filesystem backend) return `None` — those files are archived directly
    /// from their location and need no separate snapshot.
    fn snapshot_for_archive(&self, dst_dir: &Path) -> Result<Option<PathBuf>, MerkleDbError>;
}

/// Build the node store for the repo rooted at `repo_path`, choosing the backend once at repo
/// construction (mirroring [`create_version_store`](crate::storage::create_version_store)).
/// `configured` is the repo's persisted `merkle_node_backend` from `config.toml`. Returns the store
/// alongside the backend it resolved to, so the caller can persist the choice.
///
/// The persisted config is authoritative. When it is absent (`None`, for a repo whose config
/// predates the field) the backend is inferred from on-disk data: an FS node tree under
/// `.oxen/tree/nodes` means the filesystem backend, otherwise LMDB. New repos record their backend
/// at `init` ([`DEFAULT_MERKLE_NODE_BACKEND`]), so this inference only applies to legacy repos.
pub(crate) fn create_merkle_node_store(
    repo_path: &Path,
    configured: Option<MerkleNodeBackend>,
) -> Result<(Arc<dyn MerkleNodeStore>, MerkleNodeBackend), OxenError> {
    let backend = resolve_load_backend(configured, repo_path);
    let store: Arc<dyn MerkleNodeStore> = match backend {
        MerkleNodeBackend::Lmdb => Arc::new(LmdbMerkleNodeStore::new(repo_path)?),
        MerkleNodeBackend::Filesystem => Arc::new(FsMerkleNodeStore::new(repo_path)),
    };
    Ok((store, backend))
}

/// Resolve an existing repo's backend: the persisted `configured` value if present, else inferred
/// from on-disk data — an FS node tree ⇒ filesystem, otherwise LMDB.
fn resolve_load_backend(
    configured: Option<MerkleNodeBackend>,
    repo_path: &Path,
) -> MerkleNodeBackend {
    configured.unwrap_or_else(|| {
        if FsMerkleNodeStore::exists_on_disk(repo_path) {
            MerkleNodeBackend::Filesystem
        } else {
            MerkleNodeBackend::Lmdb
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The persisted config is authoritative when present, regardless of on-disk data.
    #[test]
    fn resolve_load_backend_uses_config_when_set() {
        let dir = tempfile::tempdir().expect("create temp dir");
        assert_eq!(
            resolve_load_backend(Some(MerkleNodeBackend::Lmdb), dir.path()),
            MerkleNodeBackend::Lmdb
        );
        assert_eq!(
            resolve_load_backend(Some(MerkleNodeBackend::Filesystem), dir.path()),
            MerkleNodeBackend::Filesystem
        );
    }

    /// With no persisted config, an on-disk FS node tree resolves to the filesystem backend.
    #[test]
    fn resolve_load_backend_detects_fs_tree() -> Result<(), OxenError> {
        let dir = tempfile::tempdir().expect("create temp dir");
        // Writing a node creates the `.oxen/tree/nodes` tree the detector looks for.
        FsMerkleNodeStore::new(dir.path()).write_node(
            &MerkleHash::new(0x1),
            Bytes::from_static(b"n"),
            Bytes::new(),
        )?;
        assert_eq!(
            resolve_load_backend(None, dir.path()),
            MerkleNodeBackend::Filesystem
        );
        Ok(())
    }

    /// With no persisted config and no FS node tree, resolution falls back to LMDB.
    #[test]
    fn resolve_load_backend_falls_back_to_lmdb() {
        let dir = tempfile::tempdir().expect("create temp dir");
        assert_eq!(
            resolve_load_backend(None, dir.path()),
            MerkleNodeBackend::Lmdb
        );
    }

    /// `MerkleNodeBackend` serializes to the lowercase tokens persisted in `config.toml`.
    #[test]
    fn backend_serializes_lowercase() {
        assert_eq!(
            toml::Value::try_from(MerkleNodeBackend::Lmdb).expect("serialize"),
            toml::Value::String("lmdb".to_string())
        );
        assert_eq!(
            toml::Value::try_from(MerkleNodeBackend::Filesystem).expect("serialize"),
            toml::Value::String("filesystem".to_string())
        );
    }
}
