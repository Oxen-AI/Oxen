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
/// `"filesystem"` / `"lmdb"`), which records a repo's backend unless overridden by the
/// `OXEN_MERKLE_NODE_BACKEND` environment variable — see [`create_merkle_node_store`] for how it's
/// resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MerkleNodeBackend {
    /// Two files per node under `.oxen/tree/nodes` (the historical, default backend).
    Filesystem,
    /// One LMDB env under `.oxen/tree/nodes_lmdb`.
    Lmdb,
}

/// The global backend override, read from the `OXEN_MERKLE_NODE_BACKEND` environment variable
/// (`"lmdb"` selects LMDB, `"filesystem"` selects the filesystem backend). When set, it takes
/// precedence over every per-repo signal — the persisted config and the on-disk node data alike
/// (see [`create_merkle_node_store`]). Setting it to a backend that disagrees with where a repo's
/// nodes actually live will make that repo read from an empty store, so it is a deliberate global
/// switch, not a safety net.
const MERKLE_NODE_BACKEND_ENV: &str = "OXEN_MERKLE_NODE_BACKEND";

impl MerkleNodeBackend {
    /// The backend named by the environment, or `None` when the variable is unset or empty.
    /// `"lmdb"` selects LMDB; any other non-empty value selects the filesystem backend.
    fn from_env() -> Option<Self> {
        match std::env::var(MERKLE_NODE_BACKEND_ENV) {
            Ok(value) if value.trim().is_empty() => None,
            Ok(value) if value.eq_ignore_ascii_case("lmdb") => Some(MerkleNodeBackend::Lmdb),
            Ok(_) => Some(MerkleNodeBackend::Filesystem),
            Err(_) => None,
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
/// The backend comes from exactly two places: the [`OXEN_MERKLE_NODE_BACKEND`] environment override
/// and the persisted `config.toml` value. The environment override wins when set; otherwise the
/// config value is used; otherwise the filesystem default applies. The choice is never inferred from
/// node data on disk, so pointing either knob at a backend that disagrees with where a repo's nodes
/// actually live makes the repo read from an empty store.
pub(crate) fn create_merkle_node_store(
    repo_path: &Path,
    configured: Option<MerkleNodeBackend>,
) -> Result<(Arc<dyn MerkleNodeStore>, MerkleNodeBackend), OxenError> {
    let backend = resolve_backend(MerkleNodeBackend::from_env(), configured);
    let store: Arc<dyn MerkleNodeStore> = match backend {
        MerkleNodeBackend::Lmdb => Arc::new(LmdbMerkleNodeStore::new(repo_path)?),
        MerkleNodeBackend::Filesystem => Arc::new(FsMerkleNodeStore::new(repo_path)),
    };
    Ok((store, backend))
}

/// Decide a repo's backend: the environment override if set, else the persisted config value, else
/// the filesystem default. See [`create_merkle_node_store`] for the rationale.
fn resolve_backend(
    env_override: Option<MerkleNodeBackend>,
    configured: Option<MerkleNodeBackend>,
) -> MerkleNodeBackend {
    env_override
        .or(configured)
        .unwrap_or(MerkleNodeBackend::Filesystem)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The environment override wins over the persisted config, and over both backends.
    #[test]
    fn resolve_backend_env_override_wins() {
        assert_eq!(
            resolve_backend(
                Some(MerkleNodeBackend::Filesystem),
                Some(MerkleNodeBackend::Lmdb)
            ),
            MerkleNodeBackend::Filesystem
        );
        assert_eq!(
            resolve_backend(
                Some(MerkleNodeBackend::Lmdb),
                Some(MerkleNodeBackend::Filesystem)
            ),
            MerkleNodeBackend::Lmdb
        );
    }

    /// With no environment override, the persisted config decides; with neither, the filesystem
    /// default applies.
    #[test]
    fn resolve_backend_falls_back_to_config_then_default() {
        assert_eq!(
            resolve_backend(None, Some(MerkleNodeBackend::Lmdb)),
            MerkleNodeBackend::Lmdb
        );
        assert_eq!(
            resolve_backend(None, Some(MerkleNodeBackend::Filesystem)),
            MerkleNodeBackend::Filesystem
        );
        assert_eq!(resolve_backend(None, None), MerkleNodeBackend::Filesystem);
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
