use std::io;

trait Transaction {
    type Node;

    /// Queue the node for writing in the transaction.
    fn queue_write<'a>(&'a mut self, node: &'a Self::Node);

    /// Commit the transaction, writing all queued nodes.
    fn commit(self) -> io::Result<()>;
}

trait MerkleWriter: Sized {
    type T: Transaction;

    /// Open a write transaction for storing Merkle tree nodes.
    /// Allows multiple nodes to be queued for writing.
    fn write_transaction(&self) -> Self::T;

    /// Durably store a batch of Merkle tree nodes.
    fn write(&self, nodes: impl Iterator<Item = <Self::T as Transaction>::Node>) -> io::Result<()> {
        let mut tx = self.write_transaction();
        for node in nodes {
            tx.queue_write(&node);
        }
        tx.commit()
    }
}
