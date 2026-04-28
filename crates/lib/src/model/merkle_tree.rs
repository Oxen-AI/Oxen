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

/// A [`MerkleStore`] that can also pack and unpack the canonical tar-gz wire format.
///
/// The store side ([`MerkleStore`] = [`MerkleReader`] + [`MerkleWriter`]) uses one error type,
/// and the transport side ([`MerkleTransport`] = [`MerklePacker`] + [`MerkleUnpacker`]) uses
/// one error type, but those two sides are **not** required to share the same type. That gives
/// backends room to use a transport-specific error that extends their store error with
/// tar/gzip/parse variants, rather than having to fold every transport concern into the
/// store error. Both sides still implement [`IntoOxenError`], so `?` at call sites converts
/// each error directly to [`OxenError`] with no further plumbing.
///
/// [`IntoOxenError`]: crate::error::IntoOxenError
/// [`OxenError`]: crate::error::OxenError
pub trait TransportableMerkleStore: MerkleStore + MerkleTransport {}

/// Any type that is a [`MerkleStore`] and a [`MerkleTransport`] is automatically a
/// [`TransportableMerkleStore`]. The store-side and transport-side error types can differ.
impl<T> TransportableMerkleStore for T where T: MerkleStore + MerkleTransport {}
