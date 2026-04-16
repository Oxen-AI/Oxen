use crate::explore::lazy_merkle::MerkleTreeL;

pub trait MerkleWriter: Sized {
    type Session: WriteSession;

    /// Open a write transaction for storing Merkle tree nodes.
    /// Allows multiple nodes to be queued for writing.
    fn write_transaction(&self) -> Self::Session;

    /// Durably store a batch of Merkle tree nodes.
    fn write<'a>(
        &self,
        nodes: impl Iterator<Item = &'a MerkleTreeL>,
    ) -> Result<(), <Self::Session as WriteSession>::Error> {
        let mut tx = self.write_transaction();
        for node in nodes {
            tx.queue_write(&node)?;
        }
        tx.finish()
    }
}


pub trait WriteSession {
    type Error: std::error::Error;

    /// Queue the node for writing in the transaction.
    fn queue_write(&mut self, node: &MerkleTreeL) -> Result<(), Self::Error>;

    /// Commit the transaction, writing all queued nodes.
    fn finish(self) -> Result<(), Self::Error>;
}
