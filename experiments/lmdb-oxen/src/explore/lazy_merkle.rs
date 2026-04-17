use serde::{Deserialize, Serialize};

use crate::explore::hash::{HasHash, Hash, HexHash};
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::paths::{AbsolutePath, Name};

#[derive(Debug, Serialize, Deserialize)]
pub enum MerkleTreeL {
    Dir {
        hash: Hash,
        name: Name,
        parent: Option<LazyNode>,
        children: Vec<LazyNode>,
    },
    File {
        // hash is in content (LazyData)
        name: Name,
        parent: Option<LazyNode>,
        content: LazyData,
    },
}

pub trait HasName {
    fn name(&self) -> &Name;
}

impl MerkleTreeL {
    pub fn parent(&self) -> Option<&LazyNode> {
        match self {
            MerkleTreeL::Dir { parent, .. } => parent,
            MerkleTreeL::File { parent, .. } => parent,
        }
        .as_ref()
    }
}

impl HasHash for MerkleTreeL {
    #[inline]
    fn hash(&self) -> Hash {
        match self {
            MerkleTreeL::Dir { hash, .. } => *hash,
            MerkleTreeL::File { content, .. } => content.hash(),
        }
    }
}

impl HasName for MerkleTreeL {
    #[inline(always)]
    fn name(&self) -> &Name {
        match self {
            MerkleTreeL::Dir { name, .. } => name,
            MerkleTreeL::File { name, .. } => name,
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LazyNode(Hash);

impl LazyNode {
    #[inline(always)]
    pub(crate) fn new(hash: Hash) -> Self {
        Self(hash)
    }
}

impl HasHash for LazyNode {
    #[inline(always)]
    fn hash(&self) -> Hash {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LazyData(Hash);

impl LazyData {
    #[inline(always)]
    pub(crate) fn new(hash: Hash) -> Self {
        Self(hash)
    }

    /// Reconstructs the relative path to this file node's data
    /// and reads it from storage.
    pub async fn load<DB: MerkleReader>(&self, db: &DB) -> Result<Vec<u8>, LoadError<DB>> {
        let Some(rel_path) = db.path(self.hash()).map_err(LoadError::DBError)? else {
            return Err(LoadError::PathError(HexHash::from(self.hash())));
        };

        let path = AbsolutePath::from(db.repository(), &rel_path);
        tokio::fs::read(path.as_path())
            .await
            .map_err(LoadError::ReadError)
    }
}

impl HasHash for LazyData {
    #[inline(always)]
    fn hash(&self) -> Hash {
        self.0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError<DB: MerkleReader> {
    #[error("Error from Merkle tree data store: {0}")]
    DBError(DB::Error),

    #[error("Cannot determine relative path for hash: {0}")]
    PathError(HexHash),

    #[error("{0}")]
    ReadError(std::io::Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Root {
    repository: AbsolutePath,
    hash: Hash,
    children: Vec<LazyNode>,
    parent: Option<Hash>,
}

impl Root {
    /// Computes the hash of the children on creation.
    pub fn new(repository: AbsolutePath, children: Vec<LazyNode>, parent: Option<Hash>) -> Self {
        Self {
            repository,
            hash: Hash::hash_of_hashes(children.iter()),
            children,
            parent,
        }
    }

    pub fn children(&self) -> impl Iterator<Item = &LazyNode> {
        self.children.iter()
    }
}

impl HasHash for Root {
    #[inline(always)]
    fn hash(&self) -> Hash {
        self.hash
    }
}

pub struct UncomittedRoot {
    pub parent: Option<Hash>,
    pub repository: AbsolutePath,
    pub children: Vec<MerkleTreeB>,
}

#[derive(Debug)]
pub enum MerkleTreeB {
    Dir {
        hash: Hash,
        name: Name,
        children: Vec<Self>,
    },
    File {
        hash: Hash,
        name: Name,
    },
}

macro_rules! impl_into_lazy_node {
    ($type:path) => {
        impl From<$type> for LazyNode {
            fn from(tree: $type) -> Self {
                Self::new(tree.hash())
            }
        }

        impl<'a> From<&'a $type> for LazyNode {
            fn from(tree: &'a $type) -> Self {
                Self::new(tree.hash())
            }
        }
    };
}
impl_into_lazy_node!(MerkleTreeB);
impl_into_lazy_node!(Box<MerkleTreeB>);

impl HasHash for MerkleTreeB {
    #[inline(always)]
    fn hash(&self) -> Hash {
        match self {
            MerkleTreeB::Dir { hash, .. } => *hash,
            MerkleTreeB::File { hash, .. } => *hash,
        }
    }
}

impl HasName for MerkleTreeB {
    #[inline(always)]
    fn name(&self) -> &Name {
        match self {
            MerkleTreeB::Dir { name, .. } => name,
            MerkleTreeB::File { name, .. } => name,
        }
    }
}
