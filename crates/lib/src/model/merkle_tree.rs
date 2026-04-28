pub mod merkle_hash;
pub mod merkle_reader;
pub mod merkle_writer;
pub mod node;
pub mod node_type;

pub use crate::model::merkle_tree::merkle_hash::MerkleHash;
pub use crate::model::merkle_tree::merkle_reader::MerkleReader;
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
