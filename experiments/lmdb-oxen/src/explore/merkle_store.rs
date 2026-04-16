use crate::explore::{merkle_reader::MerkleReader, merkle_writer::{MerkleWriter, WriteSession}};

/// A complete Merkle tree store supports reading and writing with a shared error type.
pub trait MerkleStore: MerkleReader + MerkleWriter
where
    <Self as MerkleWriter>::Session: WriteSession<Error=<Self as MerkleReader>::Error>,
{}

/// Any type that implements the Merkle reading and writing traits is automatically an instance
/// of a MerkleStore, provided that the error types in both the reader & writer align.
impl<T> MerkleStore for T
where
    T: MerkleReader + MerkleWriter,
    <T as MerkleWriter>::Session: WriteSession<Error=<T as MerkleReader>::Error>,
{}
