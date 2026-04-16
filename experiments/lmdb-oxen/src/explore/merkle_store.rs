use crate::explore::{
    hash::Hash,
    lazy_merkle::{MerkleTreeL, Root},
    merkle_reader::MerkleReader,
    merkle_writer::MerkleWriter,
};

#[derive(Debug, thiserror::Error)]
pub enum MerkleStoreError<R: MerkleReader> {
    #[error("Parent commit {0} does not exist.")]
    ParentCommitNotFound(Hash),
    #[error("{0}")]
    StoreError(R::Error)
}

/// A complete Merkle tree store supports reading and writing with a shared error type.
pub trait MerkleStore: MerkleReader + MerkleWriter<Error = <Self as MerkleReader>::Error> {
    fn commit_changes<'a>(
        &self,
        parent: Hash,
        updates: &'a [MerkleTreeL],
    ) -> Result<Root, MerkleStoreError<Self>> {
        let Some(parent_commit) = self.commit(parent)? else {
            return Err(MerkleStoreError::ParentCommitNotFound(parent));
        };

        self.write(updates.iter())?;

        let commit = Root::new(self.repository(), children);

    }
}

/// Any type that implements the Merkle reading and writing traits is automatically an instance
/// of a MerkleStore, provided that the error types in both the reader & writer align.
impl<T> MerkleStore for T where T: MerkleReader + MerkleWriter<Error = <T as MerkleReader>::Error> {}
