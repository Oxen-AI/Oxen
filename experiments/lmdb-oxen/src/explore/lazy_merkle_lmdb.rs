use std::path::PathBuf;

use crate::explore::new_path::{AbsolutePath, RelativePath};
use crate::explore::scratch::{Hash, HexHash, Repository};

//
// M e r k l e   T r e e   D a t a b a s e   S t o r e
//

pub trait MerkleMetadataStore: Sized {
    /// If true, then there is a node in the Merkle tree that has this hash.
    ///
    /// This always means that either `self.node()` xor `self.commit()` will return a
    /// non-`None` value. Note that this is a mutually exclusive relationship: exactly
    /// one of `node` or `commit` is non-`None` if `exists` is `true`.
    fn exists(&self, hash: Hash) -> bool;

    /// Obtains a reference to the Merkle tree node for the given hash.
    ///
    /// Corresponds to a real file or directory under version control.
    /// None means there is no node with that hash.
    fn node(&self, hash: Hash) -> Option<&MerkleTreeL<Self>>;

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Option<&Root<Self>>;

    /// The repository for which this trait is managing the Merkle tree.
    fn repository(&self) -> &Repository;

    /// The repository relative path to the file or directory indicated by the given hash.
    /// None means that the hash does not appear in the Merkle tree.
    fn path(&self, hash: Hash) -> Option<RelativePath> {
        let rel_path = {
            let mut reverse_path = Vec::new();

            let mut current_hash = hash;
            loop {
                let next_node = self.node(current_hash);
                if let Some(next) = next_node {
                    reverse_path.push(next.name());
                    if let Some(parent) = next.parent() {
                        current_hash = parent.hash();
                    } else {
                        break;
                    }
                } else {
                    // In case the parent is the commit node / root => we check and
                    // treat this as the end of the path.
                    if self.exists(current_hash) {
                        break;
                    } else {
                        return None;
                    }
                }
            }

            reverse_path.reverse();
            reverse_path
        };

        if rel_path.is_empty() {
            None
        } else {
            let components = rel_path.iter().map(|s| s.to_string()).collect();
            // SAFETY: we know that each component we've collected in `rel_path` is an actual
            //         file or directory name. We also know that this forms a relative path
            //         within the repository.
            Some(unsafe { RelativePath::from_parts(components) })
        }
    }
}

pub struct InnerMerkle<DB: MerkleMetadataStore> {
    hash: Hash,
    name: String,
    parent: Option<Box<MerkleTreeL<DB>>>,
}

pub enum MerkleTreeL<DB: MerkleMetadataStore> {
    Dir {
        inner: InnerMerkle<DB>,
        children: Vec<LazyNode<DB>>,
    },
    File {
        inner: InnerMerkle<DB>,
        content: LazyData<DB>,
    },
}

pub struct LazyNode<DB: MerkleMetadataStore> {
    pub db: DB,
}

pub struct LazyData<DB: MerkleMetadataStore> {
    pub db: DB,
    pub me: Hash,
}

impl<DB: MerkleMetadataStore> LazyData<DB> {
    pub async fn load(&self) -> Result<Vec<u8>, std::io::Error> {
        let Some(rel_path) = self.db.path(self.me) else {
            eprintln!(
                "[ERROR] cannot determine relative path with hash: {}",
                HexHash::from(self.me)
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "No path found for hash",
            ));
        };

        let absolute_path = AbsolutePath::from(self.db.repository(), &rel_path).consume();

        tokio::fs::read(absolute_path).await
    }
}

impl<DB: MerkleMetadataStore> MerkleTreeL<DB> {
    pub fn hash(&self) -> Hash {
        let inner = match self {
            MerkleTreeL::Dir { inner, .. } => inner,
            MerkleTreeL::File { inner, .. } => inner,
        };
        inner.hash.clone()
    }

    pub fn name(&self) -> &str {
        let inner = match self {
            MerkleTreeL::Dir { inner, .. } => inner,
            MerkleTreeL::File { inner, .. } => inner,
        };
        &inner.name
    }

    pub fn parent(&self) -> Option<&MerkleTreeL<DB>> {
        let inner = match self {
            MerkleTreeL::Dir { inner, .. } => inner,
            MerkleTreeL::File { inner, .. } => inner,
        };
        inner.parent.as_deref()
    }
}

pub struct Root<DB: MerkleMetadataStore> {
    pub root: PathBuf,
    pub hash: Hash,
    pub children: Vec<LazyNode<DB>>,
}
