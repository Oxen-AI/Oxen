use crate::explore::{
    hash::{HasHash, Hash},
    lazy_merkle::{LazyData, LazyNode, MerkleTreeB, MerkleTreeL, Root, UncomittedRoot},
    merkle_reader::MerkleReader,
    merkle_writer::{MerkleWriter, WriteSession},
};

/// A complete Merkle tree store supports reading and writing with a shared error type.
pub trait MerkleStore: MerkleReader + MerkleWriter<Error = <Self as MerkleReader>::Error> {
    /// Use the reader + writer to commit a repository's changes to the underlying store.
    /// Produces the commit hash on a successful write.
    fn commit_changes<'a>(
        &self,
        updates: UncomittedRoot,
    ) -> Result<Root, <Self as MerkleReader>::Error> {
        let UncomittedRoot {
            parent,
            repository,
            children,
        } = updates;

        let mut writer = self.write_session()?;

        let commit_root = Root::new(
            repository,
            children.iter().map(|n| n.into()).collect(),
            parent,
        );

        for node in children {
            write_tree(self, &mut writer, commit_root.hash(), node)?;
        }

        writer.queue_commit(&commit_root)?;

        writer.finish()?;

        Ok(commit_root)
    }
}

// Recursively writes an entire Merkle tree from the given node to all of its leaves.
// This is the helper function that `MerkleStore::commit_changes` uses to write an
// entire commit tree to the store.
fn write_tree<'a, M: MerkleStore>(
    store: &M,
    write_session: &'a mut <M as MerkleWriter>::Session<'a>,
    parent: Hash,
    node: MerkleTreeB,
) -> Result<(), <M as MerkleReader>::Error> {
    // We only need to do something if this node doesn't exist in the store yet.
    //
    // If the node exists in the store, then it doesn't need to be written.
    //
    // If its a directory node, then we don't need to handle its children because
    // by definition a dir node's hash is the concatonation of its children's hashes.
    // If any child changed then its hash would be different. Since it's not different,
    // we are certiain that its children haven't changed either.
    if !store.exists(node.hash())? {
        // write this node to the store
        match node {
            MerkleTreeB::Dir {
                hash,
                name,
                children,
            } => {
                let lazy_children = children.iter().map(|n| n.into()).collect();
                let lazy_node = MerkleTreeL::Dir {
                    hash,
                    name,
                    parent: Some(LazyNode::new(parent)),
                    children: lazy_children,
                };
                write_session.queue_node(&lazy_node)?;

                for c in children {
                    write_tree(store, write_session, hash, *c)?;
                }
            }
            MerkleTreeB::File { hash, name } => {
                let lazy_node = MerkleTreeL::File {
                    name,
                    parent: Some(LazyNode::new(parent)),
                    content: LazyData::new(hash),
                };
                write_session.queue_node(&lazy_node)?;
            }
        }
    }
    Ok(())
}

/// Any type that implements the Merkle reading and writing traits is automatically an instance
/// of a MerkleStore, provided that the error types in both the reader & writer align.
impl<T> MerkleStore for T where T: MerkleReader + MerkleWriter<Error = <T as MerkleReader>::Error> {}
