use std::fmt;

use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

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
    fn node(&self, hash: Hash) -> Result<Option<MerkleTreeL>, Self::Error>;

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Result<Option<&Root>, Self::Error>;

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

#[derive(Debug, Serialize, Deserialize)]
pub enum MerkleTreeL {
    Dir {
        hash: Hash,
        name: String,
        parent: Option<LazyNode>,
        children: Vec<LazyNode>,
    },
    File {
        // hash is in content (LazyData)
        name: String,
        parent: Option<LazyNode>,
        content: LazyData,
    },
}

impl MerkleTreeL {
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

    pub fn parent(&self) -> Option<&LazyNode> {
        match self {
            MerkleTreeL::Dir { parent, .. } => parent,
            MerkleTreeL::File { parent, .. } => parent,
        }
        .as_ref()
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LazyNode {
    me: Hash,
}

impl LazyNode {
    pub fn hash(&self) -> Hash {
        self.me
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LazyData {
    me: Hash,
}

impl LazyData {
    /// Reconstructs the relative path to this file node's data
    /// and reads it from storage.
    pub async fn load<DB: MerkleMetadataStore>(&self, db: &DB) -> Result<Vec<u8>, LoadError<DB>> {
        let Some(rel_path) = db.path(self.me).map_err(LoadError::DBError)? else {
            return Err(LoadError::PathError(HexHash::from(self.me)));
        };

        let path = AbsolutePath::from(db.repository(), &rel_path);
        tokio::fs::read(path.as_path())
            .await
            .map_err(LoadError::ReadError)
    }

    pub fn hash(&self) -> Hash {
        self.me
    }
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Root {
    root: AbsolutePath,
    hash: Hash,
    children: Vec<LazyNode>,
}

impl Root {
    pub fn hash(&self) -> Hash {
        self.hash
    }

    pub fn children(&self) -> impl Iterator<Item = &LazyNode> {
        self.children.iter()
    }
}
