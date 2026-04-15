use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::explore::new_path::{AbsolutePath, RelativePath};
use crate::explore::scratch::{Hash, HexHash, Repository};

//
// M e r k l e   T r e e   D a t a b a s e   S t o r e
//

pub trait MerkleMetadataStore: Sized {
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
    fn node(&self, hash: Hash) -> Result<Option<&MerkleTreeL<Self>>, Self::Error>;

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Result<Option<&Root<Self>>, Self::Error>;

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
                    reverse_path.push(next.name());
                    if let Some(parent) = next.parent() {
                        current_hash = parent.load()?.hash();
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

#[derive(Debug, Serialize, Deserialize)]
pub enum MerkleTreeL<DB: MerkleMetadataStore> {
    Dir {
        hash: Hash,
        name: String,
        parent: Option<LazyNode<DB>>,
        children: Vec<LazyNode<DB>>,
    },
    File {
        // hash is in content (LazyData)
        name: String,
        parent: Option<LazyNode<DB>>,
        content: LazyData<DB>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LazyNode<DB: MerkleMetadataStore> {
    db: DB,
    me: Hash,
}

impl<DB: MerkleMetadataStore> LazyNode<DB> {
    pub fn load(&self) -> Result<&MerkleTreeL<DB>, DB::Error> {
        let Some(node) = self.db.node(self.me)? else {
            panic!(
                "[ERROR] merkle tree node stored incorrectly, cannot find node with hash: {}",
                HexHash::from(self.me)
            );
        };
        Ok(node)
    }

    pub fn hash(&self) -> Hash {
        self.me
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LazyData<DB: MerkleMetadataStore> {
    db: DB,
    me: Hash,
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError<DB: MerkleMetadataStore> {
    #[error("Error from Merkle tree data store: {0}")]
    DBError(DB::Error),

    #[error("Cannot determine relative path for hash: {0}")]
    PathError(HexHash),

    #[error("{0}")]
    ReadError(std::io::Error),
}

impl<DB: MerkleMetadataStore> LazyData<DB> {
    /// Reconstructs the relative path to this file node's data
    /// and reads it from storage.
    pub async fn load(&self) -> Result<Vec<u8>, LoadError<DB>> {
        let Some(rel_path) = self.db.path(self.me).map_err(LoadError::DBError)? else {
            return Err(LoadError::PathError(HexHash::from(self.me)));
        };

        let path = AbsolutePath::from(self.db.repository(), &rel_path);
        tokio::fs::read(path.as_path())
            .await
            .map_err(LoadError::ReadError)
    }

    pub fn hash(&self) -> Hash {
        self.me
    }
}

impl<DB: MerkleMetadataStore> MerkleTreeL<DB> {
    pub fn hash(&self) -> Hash {
        match self {
            MerkleTreeL::Dir { hash, .. } => *hash,
            MerkleTreeL::File { content, .. } => content.hash(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            MerkleTreeL::Dir { name, .. } => name,
            MerkleTreeL::File { name, .. } => name,
        }
    }

    pub fn parent(&self) -> Option<&LazyNode<DB>> {
        match self {
            MerkleTreeL::Dir { parent, .. } => parent,
            MerkleTreeL::File { parent, .. } => parent,
        }
        .as_ref()
    }
}

pub struct Root<DB: MerkleMetadataStore> {
    root: AbsolutePath,
    hash: Hash,
    children: Vec<LazyNode<DB>>,
}

impl<DB: MerkleMetadataStore> Root<DB> {
    pub fn hash(&self) -> Hash {
        self.hash
    }

    pub fn children(&self) -> impl Iterator<Item = &LazyNode<DB>> {
        self.children.iter()
    }
}
