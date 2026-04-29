pub mod merkle_hash;
pub mod merkle_reader;
pub mod merkle_transport;
pub mod merkle_writer;
pub mod node;
pub mod node_type;

pub use crate::model::merkle_tree::merkle_hash::MerkleHash;
pub use crate::model::merkle_tree::merkle_reader::MerkleReader;
pub use crate::model::merkle_tree::merkle_transport::{
    MerklePacker, MerkleTransport, MerkleUnpacker, PackOptions, UnpackOptions,
};
pub use crate::model::merkle_tree::merkle_writer::MerkleWriter;
pub use crate::model::merkle_tree::node::merkle_tree_node_cache;
pub use crate::model::merkle_tree::node_type::{
    MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode,
};

/// A complete Merkle tree store supports reading and writing with a shared error type.
pub trait MerkleStore: MerkleReader + MerkleWriter<Error = <Self as MerkleReader>::Error> {}

/// Any type that implements the Merkle reading and writing traits is automatically an instance
/// of a MerkleStore, provided that the error types in both the reader & writer align.
impl<T> MerkleStore for T where T: MerkleReader + MerkleWriter<Error = <T as MerkleReader>::Error> {}

/// A Merkle tree backend that can also be transported (packed/unpacked) over the wire.
///
/// The store-side and transport-side `Error` types are intentionally allowed to differ —
/// transport-layer errors (tar parsing, gzip framing, path-traversal rejection) have a
/// different shape than store-layer errors (filesystem / database faults), and pinning
/// them together would force one surface to absorb the other's variants. See
/// [`MerkleTransport`] for more detail.
pub trait TransportableMerkleStore: MerkleStore + MerkleTransport {}

/// Any type that implements both [`MerkleStore`] and [`MerkleTransport`] is automatically
/// a [`TransportableMerkleStore`].
impl<T> TransportableMerkleStore for T where T: MerkleStore + MerkleTransport {}
