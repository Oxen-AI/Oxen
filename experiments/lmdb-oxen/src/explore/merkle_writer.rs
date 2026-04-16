use crate::explore::lazy_merkle::MerkleTreeL;

pub trait MerkleWriter: Sized {
    type Error: std::error::Error;
    type Session<'a>: WriteSession<'a, Error = Self::Error>
    where
        Self: 'a;

    /// Open a write transaction for storing Merkle tree nodes.
    /// Allows multiple nodes to be queued for writing.
    fn write_session<'a>(&'a self) -> Result<Self::Session<'a>, Self::Error>;

    /// Durably store a batch of Merkle tree nodes.
    fn write<'n>(&self, nodes: impl Iterator<Item = &'n MerkleTreeL>) -> Result<(), Self::Error> {
        let mut tx = self.write_session()?;
        for node in nodes {
            tx.queue_write(node)?;
        }
        tx.finish()?;
        Ok(())
    }
}

pub trait WriteSession<'a> {
    type Error: std::error::Error;

    /// Queue the node for writing in the transaction.
    fn queue_write(&mut self, node: &MerkleTreeL) -> Result<(), Self::Error>;

    /// Commit the transaction, writing all queued nodes.
    fn finish(self) -> Result<(), Self::Error>;
}
