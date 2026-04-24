pub mod merkle_hash;
pub mod merkle_reader;
pub mod merkle_transport;
pub mod merkle_writer;
pub mod node;
pub mod node_type;

pub use crate::model::merkle_tree::merkle_hash::MerkleHash;
pub use crate::model::merkle_tree::merkle_reader::MerkleReader;
pub use crate::model::merkle_tree::merkle_transport::{
    MerklePacker, MerkleTransport, MerkleUnpacker,
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

/// A [`MerkleStore`] that can also pack and unpack the canonical tar-gz wire format.
///
/// The extra [`MerkleTransport`] bound lets callers obtain transport-ready bytes from the
/// store or install bytes received over the wire. All four associated error types
/// ([`MerkleReader::Error`], [`MerkleWriter::Error`], [`MerklePacker::Error`],
/// [`MerkleUnpacker::Error`]) must be the same type so that callers see one concrete
/// backend error funneled through [`IntoOxenError`]. Pinning [`MerklePacker::Error`] is
/// sufficient because [`MerkleTransport`] already pins [`MerkleUnpacker::Error`] to
/// [`MerklePacker::Error`].
///
/// [`IntoOxenError`]: crate::error::IntoOxenError
pub trait TransportableMerkleStore:
    MerkleStore + MerkleTransport + MerklePacker<Error = <Self as MerkleReader>::Error>
{
}

/// Any type that is a [`MerkleStore`] and a [`MerkleTransport`] with a shared error type is
/// automatically a [`TransportableMerkleStore`].
impl<T> TransportableMerkleStore for T where
    T: MerkleStore + MerkleTransport + MerklePacker<Error = <T as MerkleReader>::Error>
{
}
