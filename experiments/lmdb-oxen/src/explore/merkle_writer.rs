use std::io;

use async_trait::async_trait;

#[async_trait]
trait Transaction {
    type Node: Send + Sync + 'static;

    /// Queue the node for writing in the transaction.
    fn queue_write<'a>(&'a mut self, node: &'a Self::Node);

    /// Commit the transaction, writing all queued nodes.
    async fn commit(self) -> io::Result<()>;
}

#[async_trait]
trait MerkleWriter: Sized {
    type T: Transaction + Send + Sync + 'static;

    /// Open a write transaction for storing Merkle tree nodes.
    /// Allows multiple nodes to be queued for writing.
    fn write_transaction(&self) -> Self::T;

    /// Durably store a batch of Merkle tree nodes.
    async fn write(
        &self,
        nodes: impl Iterator<Item = <Self::T as Transaction>::Node> + Send,
    ) -> io::Result<()> {
        let mut tx = self.write_transaction();
        for node in nodes {
            tx.queue_write(&node);
        }
        tx.commit().await
    }
}
