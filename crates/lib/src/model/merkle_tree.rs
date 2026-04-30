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

/// A [`MerkleStore`] that can also pack and unpack the canonical tar-gz wire format.
///
/// All four sub-traits — [`MerkleReader`], [`MerkleWriter`], [`MerklePacker`],
/// [`MerkleUnpacker`] — return [`OxenError`] uniformly, so this super-trait carries
/// no associated-type bounds. Object-safe; callers typically hold it as
/// `Box<dyn TransportableMerkleStore + '_>`.
///
/// [`OxenError`]: crate::error::OxenError
pub trait TransportableMerkleStore: MerkleStore + MerkleTransport {}

/// Any type that is a [`MerkleStore`] and a [`MerkleTransport`] is automatically a
/// [`TransportableMerkleStore`]. The `?Sized` bound lets the marker apply to
/// `dyn TransportableMerkleStore` itself.
impl<T: MerkleStore + MerkleTransport + ?Sized> TransportableMerkleStore for T {}
