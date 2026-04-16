use serde::Serialize;

use crate::explore::{hash::HasHash, lazy_merkle::MerkleTreeL};

/// Trait for writing Merkle tree data to a durable store.
pub trait MerkleWriter: Sized {
    type Error: std::error::Error;
    type Session<'a>: WriteSession<'a, Error = Self::Error>
    where
        Self: 'a;

    /// Open a write transaction for storing Merkle tree nodes.
    /// Allows multiple nodes to be queued for writing.
    fn node_writer<'a>(&'a self) -> Result<Self::Session<'a>, Self::Error>;


}

/// The [MerkleWriter] creates a session in which multiple nodes _can_ be queued for writing.
/// Implementations can take advantage of, but are not required to, batch writes for efficiency.
/// The [finish] method ends the session and ensures all writes are presisted.
pub trait WriteSession<'a, Node: HasHash + Serialize> {
    type Error: std::error::Error;

    /// Queue the node for writing in the transaction.
    fn queue_write(&mut self, node: &Node) -> Result<(), Self::Error>;

    /// Commit the transaction, writing all queued nodes.
    fn finish(self) -> Result<(), Self::Error>;
}
