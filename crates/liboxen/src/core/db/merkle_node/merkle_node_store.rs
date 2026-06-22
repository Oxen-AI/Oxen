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

use bytes::Bytes;

use crate::model::MerkleHash;

use super::merkle_node_db::MerkleDbError;

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

    /// Persist both blobs for `hash` as one unit. Implementations make this atomic so a node is
    /// never observable with only one of its two blobs present.
    fn write_node(
        &self,
        hash: &MerkleHash,
        node: Bytes,
        children: Bytes,
    ) -> Result<(), MerkleDbError>;
}
