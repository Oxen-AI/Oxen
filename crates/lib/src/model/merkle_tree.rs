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

/// A complete Merkle tree store supports reading and writing.
///
/// Object-safe via the dyn-compatible [`MerkleReader`] and [`MerkleWriter`].
/// Both sides return [`OxenError`] at the trait surface, so callers can use `?`
/// anywhere they're already returning `Result<_, OxenError>`.
///
/// [`OxenError`]: crate::error::OxenError
pub trait MerkleStore: MerkleReader + MerkleWriter {}

/// Any type that implements both the Merkle reading and writing traits is
/// automatically a [`MerkleStore`]. The `?Sized` bound lets the marker apply
/// to `dyn MerkleStore` itself.
impl<T: MerkleReader + MerkleWriter + ?Sized> MerkleStore for T {}
