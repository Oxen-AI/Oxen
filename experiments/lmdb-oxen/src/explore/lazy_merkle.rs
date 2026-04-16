use serde::{Deserialize, Serialize};

use crate::explore::merkle_reader::MerkleReader;
use crate::explore::new_path::AbsolutePath;
use crate::explore::scratch::{Hash, HexHash};


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
pub struct LazyNode(Hash);

impl LazyNode {
    #[inline(always)]
    pub fn hash(&self) -> Hash {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LazyData(Hash);

impl LazyData {
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

    #[inline(always)]
    pub fn hash(&self) -> Hash {
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
