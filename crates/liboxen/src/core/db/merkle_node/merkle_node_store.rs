//! The backend seam for Merkle tree node storage: an engine-agnostic trait over the *bytes* of a
//! node.
//!
//! A node is stored as two opaque byte blobs keyed by its [`MerkleHash`]:
//! - the `node` blob — the node's own metadata plus the lookup table describing its children
//!   (offset + length into the `children` blob), and
//! - the `children` blob — the serialized child nodes concatenated together.
//!
//! This is deliberately the *same* encoding [`MerkleNodeDB`](super::merkle_node_db) has always
//! produced: the trait isolates only the question of *where the two blobs live*, leaving the
//! msgpack + lookup-table framing untouched.
//!
//! [`MerkleNodeDB`](super::merkle_node_db) reads and writes through a `MerkleNodeStore`;
//! [`LmdbMerkleNodeStore`](super::lmdb_merkle_node_store) is its implementation.

use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::error::OxenError;
use crate::model::MerkleHash;

use super::lmdb_merkle_node_store::LmdbMerkleNodeStore;
use super::merkle_node_db::MerkleDbError;

/// The Merkle node backend a repo's `config.toml` records as `merkle_node_backend` (serialized
/// lowercase: `"filesystem"` / `"lmdb"`). Every repo this build opens is on LMDB. See
/// [`create_merkle_node_store`] for how a repo recording the filesystem backend is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MerkleNodeBackend {
    /// Two files per node under `.oxen/tree/nodes`, which this build no longer reads.
    Filesystem,
    /// One LMDB env under `.oxen/tree/nodes_lmdb`.
    Lmdb,
}

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

    /// Persist many nodes at once and return the hashes actually written. With `overwrite_existing`
    /// false, a node already present is left untouched and kept out of the returned set.
    ///
    /// Implementations make each node atomic so it is never observable with only one of its two
    /// blobs present.
    ///
    /// Unpacking a commit's tree writes one node for every directory and vnode. A large repo has
    /// tens of thousands of them, and writing them one at a time means tens of thousands of
    /// separate trips to the store, so a batch is committed in a single transaction.
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
    /// runtime-only files (e.g. an LMDB lock file).
    fn snapshot_for_archive(&self, dst_dir: &Path) -> Result<PathBuf, MerkleDbError>;
}

/// Build the node store for the repo rooted at `repo_path`. `configured` is the repo's persisted
/// `merkle_node_backend` from `config.toml`.
///
/// Refuses a repo still on the filesystem backend: one whose config records it, or, for a config
/// that predates the field, one with a `.oxen/tree/nodes` node tree on disk.
pub(crate) fn create_merkle_node_store(
    repo_path: &Path,
    configured: Option<MerkleNodeBackend>,
) -> Result<Arc<dyn MerkleNodeStore>, OxenError> {
    let on_filesystem = match configured {
        Some(backend) => backend == MerkleNodeBackend::Filesystem,
        None => repo_path
            .join(OXEN_HIDDEN_DIR)
            .join(TREE_DIR)
            .join(NODES_DIR)
            .is_dir(),
    };
    if on_filesystem {
        return Err(OxenError::MerkleNodesOnFilesystem(repo_path.to_path_buf()));
    }
    Ok(Arc::new(LmdbMerkleNodeStore::new(repo_path)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A repo recording the filesystem backend is refused whatever is on disk, and one recording
    /// LMDB opens.
    #[test]
    fn create_merkle_node_store_follows_the_recorded_backend() {
        let dir = tempfile::tempdir().expect("create temp dir");
        assert!(matches!(
            create_merkle_node_store(dir.path(), Some(MerkleNodeBackend::Filesystem)),
            Err(OxenError::MerkleNodesOnFilesystem(_))
        ));
        assert!(create_merkle_node_store(dir.path(), Some(MerkleNodeBackend::Lmdb)).is_ok());
    }

    /// With no recorded backend, a filesystem node tree on disk is refused and its absence opens.
    #[test]
    fn create_merkle_node_store_detects_an_unrecorded_filesystem_tree() -> Result<(), OxenError> {
        let dir = tempfile::tempdir().expect("create temp dir");
        assert!(
            create_merkle_node_store(dir.path(), None).is_ok(),
            "a tree with no nodes written yet opens on LMDB"
        );

        std::fs::create_dir_all(
            dir.path()
                .join(OXEN_HIDDEN_DIR)
                .join(TREE_DIR)
                .join(NODES_DIR),
        )?;
        assert!(matches!(
            create_merkle_node_store(dir.path(), None),
            Err(OxenError::MerkleNodesOnFilesystem(_))
        ));
        Ok(())
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
