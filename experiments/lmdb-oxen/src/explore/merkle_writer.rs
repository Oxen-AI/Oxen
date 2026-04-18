use crate::explore::hash::{ContentHash, LocationHash};
use crate::explore::lazy_merkle::{MerkleTreeL, NodeContent, Root};

/// Trait for writing Merkle tree data to a durable store.
pub trait MerkleWriter: Sized {
    type Error: std::error::Error;
    type Session<'a>: WriteSession<'a, Error = Self::Error>
    where
        Self: 'a;

    /// Open a write transaction for storing Merkle tree nodes.
    /// Allows multiple nodes to be queued for writing.
    fn write_session(&self) -> Result<Self::Session<'_>, Self::Error>;
}

/// The [MerkleWriter] creates a session in which multiple records can be
/// queued for writing. Implementations can batch writes for efficiency.
/// [`finish`] ends the session and ensures all writes are persisted.
pub trait WriteSession<'a> {
    type Error: std::error::Error;

    /// Queue a per-occurrence node record keyed by its [`LocationHash`].
    fn queue_location(&mut self, key: LocationHash, node: &MerkleTreeL) -> Result<(), Self::Error>;

    /// Queue an intrinsic content record keyed by its [`ContentHash`].
    /// Safe to call repeatedly for the same key — the caller guards with
    /// `content_exists` if avoiding redundant writes matters.
    fn queue_content(&mut self, key: ContentHash, content: &NodeContent)
    -> Result<(), Self::Error>;

    /// Queue a commit record. The commit's key is its own content hash
    /// (`Root::content_hash()`).
    fn queue_commit(&mut self, commit: &Root) -> Result<(), Self::Error>;

    /// End the session and ensure that all queued records are persisted.
    fn finish(self) -> Result<(), Self::Error>;
}
