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
//! [`FsMerkleNodeStore`](super::fs_merkle_node_store) is the only implementation today.

use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::constants;
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
/// `"filesystem"` / `"lmdb"`), which is the authoritative record of a repo's backend — see
/// [`create_merkle_node_store`] for how it's resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MerkleNodeBackend {
    /// Two files per node under `.oxen/tree/nodes` (the historical, default backend).
    Filesystem,
    /// One LMDB env under `.oxen/tree/nodes_lmdb`.
    Lmdb,
}

/// The global default backend for *newly created* repos, read once from the
/// `OXEN_MERKLE_NODE_BACKEND` environment variable (`"lmdb"` selects LMDB; anything else, or
/// unset, selects the filesystem backend). This is the global configuration switch; it only
/// affects repos that don't already have node data on disk — an existing repo always resolves to
/// the backend its data is already in (see [`create_merkle_node_store`]).
const MERKLE_NODE_BACKEND_ENV: &str = "OXEN_MERKLE_NODE_BACKEND";

impl MerkleNodeBackend {
    /// The global default backend from the environment (defaults to [`Filesystem`]).
    fn from_env() -> Self {
        match std::env::var(MERKLE_NODE_BACKEND_ENV) {
            Ok(value) if value.eq_ignore_ascii_case("lmdb") => MerkleNodeBackend::Lmdb,
            _ => MerkleNodeBackend::Filesystem,
        }
    }
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

    /// Persist both blobs for `hash` as one unit. Implementations make this atomic so a node is
    /// never observable with only one of its two blobs present.
    fn write_node(
        &self,
        hash: &MerkleHash,
        node: Bytes,
        children: Bytes,
    ) -> Result<(), MerkleDbError>;

    /// Remove the node for `hash` (both blobs). Idempotent: deleting an absent node is `Ok`.
    fn delete(&self, hash: &MerkleHash) -> Result<(), MerkleDbError>;
}

/// Build the node store for the repo rooted at `repo_path`, choosing the backend once at repo
/// construction (mirroring [`create_version_store`](crate::storage::create_version_store)).
/// `configured` is the repo's persisted `merkle_node_backend` from `config.toml` (`None` for repos
/// whose config predates the field). Returns the store alongside the backend it resolved to, so the
/// caller can persist the choice.
///
/// Resolution is config first, then on-disk evidence, then the global default — so a repo always
/// resolves to the backend its data is already in, and the global [`OXEN_MERKLE_NODE_BACKEND`]
/// switch can never silently strand an existing repo's nodes:
///   1. The persisted `config.toml` backend, if set — the authoritative record.
///   2. Otherwise an existing LMDB env on disk → LMDB.
///   3. Otherwise an existing on-disk filesystem node tree → filesystem.
///   4. Otherwise (a fresh repo with no node data yet) → the global default from the environment.
pub(crate) fn create_merkle_node_store(
    repo_path: &Path,
    configured: Option<MerkleNodeBackend>,
) -> Result<(Arc<dyn MerkleNodeStore>, MerkleNodeBackend), OxenError> {
    let backend = resolve_backend(repo_path, configured);
    let store: Arc<dyn MerkleNodeStore> = match backend {
        MerkleNodeBackend::Lmdb => Arc::new(LmdbMerkleNodeStore::new(repo_path)?),
        MerkleNodeBackend::Filesystem => Arc::new(FsMerkleNodeStore::new(repo_path)),
    };
    Ok((store, backend))
}

/// Decide a repo's backend: the persisted config value if set, else on-disk evidence, else the
/// global default for a repo with no node data yet. See [`create_merkle_node_store`] for the
/// ordering rationale.
fn resolve_backend(repo_path: &Path, configured: Option<MerkleNodeBackend>) -> MerkleNodeBackend {
    if let Some(backend) = configured {
        backend
    } else if LmdbMerkleNodeStore::exists_on_disk(repo_path) {
        MerkleNodeBackend::Lmdb
    } else if fs_nodes_exist_on_disk(repo_path) {
        MerkleNodeBackend::Filesystem
    } else {
        MerkleNodeBackend::from_env()
    }
}

/// Whether the repo has filesystem-backend node data on disk. The `.oxen/tree/nodes` dir is created
/// lazily on the first node write, so its presence with any entry means the repo is using (or has
/// used) the filesystem backend.
fn fs_nodes_exist_on_disk(repo_path: &Path) -> bool {
    let nodes_dir = repo_path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(constants::NODES_DIR);
    std::fs::read_dir(&nodes_dir)
        .map(|mut entries| entries.next().is_some())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::model::MerkleHash;

    /// With no persisted config (`None`), `resolve_backend` picks from on-disk evidence: an existing
    /// LMDB env resolves to LMDB, and an existing filesystem node tree resolves to the filesystem
    /// backend — regardless of the global default. (The fresh-repo case falls through to `from_env`,
    /// exercised implicitly elsewhere.)
    #[test]
    fn resolve_backend_follows_on_disk_evidence() {
        // An LMDB env on disk → Lmdb.
        let lmdb_dir = tempfile::tempdir().expect("create temp dir");
        LmdbMerkleNodeStore::new(lmdb_dir.path()).expect("open lmdb store");
        assert_eq!(
            resolve_backend(lmdb_dir.path(), None),
            MerkleNodeBackend::Lmdb
        );

        // A written filesystem node → Filesystem.
        let fs_dir = tempfile::tempdir().expect("create temp dir");
        let fs = FsMerkleNodeStore::new(fs_dir.path());
        fs.write_node(
            &MerkleHash::new(0x1234),
            Bytes::from_static(b"node"),
            Bytes::from_static(b"children"),
        )
        .expect("write fs node");
        assert_eq!(
            resolve_backend(fs_dir.path(), None),
            MerkleNodeBackend::Filesystem
        );
    }

    /// A persisted config backend is authoritative: it wins over on-disk evidence pointing the other
    /// way. (The migration relies on this to switch a repo's backend by writing config.)
    #[test]
    fn resolve_backend_config_wins_over_evidence() {
        // LMDB env on disk, but config says Filesystem → Filesystem.
        let lmdb_dir = tempfile::tempdir().expect("create temp dir");
        LmdbMerkleNodeStore::new(lmdb_dir.path()).expect("open lmdb store");
        assert_eq!(
            resolve_backend(lmdb_dir.path(), Some(MerkleNodeBackend::Filesystem)),
            MerkleNodeBackend::Filesystem
        );

        // Filesystem node on disk, but config says Lmdb → Lmdb.
        let fs_dir = tempfile::tempdir().expect("create temp dir");
        FsMerkleNodeStore::new(fs_dir.path())
            .write_node(
                &MerkleHash::new(0x1234),
                Bytes::from_static(b"node"),
                Bytes::from_static(b"children"),
            )
            .expect("write fs node");
        assert_eq!(
            resolve_backend(fs_dir.path(), Some(MerkleNodeBackend::Lmdb)),
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
