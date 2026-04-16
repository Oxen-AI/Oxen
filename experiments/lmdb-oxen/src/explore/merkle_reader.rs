use crate::explore::lazy_merkle::{MerkleTreeL, Root};
use crate::explore::new_path::{RelativePath};
use crate::explore::scratch::{Hash, Repository};

//
// M e r k l e   T r e e   D a t a b a s e   S t o r e
//

pub trait MerkleReader: Sized {
    type Error: std::error::Error;

    /// If true, then there is a node in the Merkle tree that has this hash.
    ///
    /// This always means that either `self.node()` xor `self.commit()` will return a
    /// non-`None` value. Note that this is a mutually exclusive relationship: exactly
    /// one of `node` or `commit` is non-`None` if `exists` is `true`.
    fn exists(&self, hash: Hash) -> Result<bool, Self::Error>;

    /// Obtains a reference to the Merkle tree node for the given hash.
    ///
    /// Corresponds to a real file or directory under version control.
    /// None means there is no node with that hash.
    fn node(&self, hash: Hash) -> Result<Option<MerkleTreeL>, Self::Error>;

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Result<Option<Root>, Self::Error>;

    /// The repository for which this trait is managing the Merkle tree.
    fn repository(&self) -> &Repository;

    /// The repository relative path to the file or directory indicated by the given hash.
    /// None means that the hash does not appear in the Merkle tree.
    fn path(&self, hash: Hash) -> Result<Option<RelativePath>, Self::Error> {
        let rel_path = {
            let mut reverse_path = Vec::new();

            let mut current_hash = hash;
            loop {
                let next_node = self.node(current_hash)?;
                if let Some(next) = next_node {
                    reverse_path.push(next.name().to_string());
                    if let Some(parent) = next.parent() {
                        // if let Some(parent_node_loaded) = self.node(parent.hash())? {
                        //     current_hash = parent_node_loaded.hash();
                        // }
                        current_hash = parent.hash()
                    } else {
                        break;
                    }
                } else {
                    // In case the parent is the commit node / root => we check and
                    // treat this as the end of the path.
                    if self.exists(current_hash)? {
                        break;
                    } else {
                        return Ok(None);
                    }
                }
            }

            reverse_path.reverse();
            reverse_path
        };

        if rel_path.is_empty() {
            Ok(None)
        } else {
            let components = rel_path.iter().map(|s| s.to_string()).collect();
            // SAFETY: we know that each component we've collected in `rel_path` is an actual
            //         file or directory name. We also know that this forms a relative path
            //         within the repository.
            Ok(Some(RelativePath::from_parts(components)))
        }
    }
}
