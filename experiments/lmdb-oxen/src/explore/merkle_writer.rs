use crate::explore::lazy_merkle::{MerkleTreeL, Root};

/// Trait for writing Merkle tree data to a durable store.
pub trait MerkleWriter: Sized {
    type Error: std::error::Error;
    type Session<'a>: WriteSession<'a, Error = Self::Error>
    where
        Self: 'a;

    /// Open a write transaction for storing Merkle tree nodes.
    /// Allows multiple nodes to be queued for writing.
    fn write_session<'a>(&'a self) -> Result<Self::Session<'a>, Self::Error>;
}

/// The [MerkleWriter] creates a session in which multiple nodes _can_ be queued for writing.
/// Implementations can take advantage of, but are not required to, batch writes for efficiency.
/// The [finish] method ends the session and ensures all writes are presisted.
pub trait WriteSession<'a> {
    type Error: std::error::Error;

    /// Queue the node for writing in this session.
    fn queue_node(&mut self, node: &MerkleTreeL) -> Result<(), Self::Error>;

    /// Queue a commit node for writing in this session.
    fn queue_commit(&mut self, commit: &Root) -> Result<(), Self::Error>;

    /// End the session and ensure that all queued nodes and commits are presisted to the store.
    fn finish(self) -> Result<(), Self::Error>;
}
