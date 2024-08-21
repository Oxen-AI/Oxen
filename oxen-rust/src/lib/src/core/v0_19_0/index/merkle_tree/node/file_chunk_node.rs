//! This is a compact representation of a merkle tree file chunk node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

use std::fmt;

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct FileChunkNode {
    pub data: Vec<u8>,
    pub dtype: MerkleTreeNodeType,
    pub hash: MerkleHash,
}

impl Default for FileChunkNode {
    fn default() -> Self {
        FileChunkNode {
            data: vec![],
            dtype: MerkleTreeNodeType::FileChunk,
            hash: MerkleHash::new(0),
        }
    }
}

impl MerkleTreeNodeIdType for FileChunkNode {
    fn dtype(&self) -> MerkleTreeNodeType {
        self.dtype
    }

    fn hash(&self) -> MerkleHash {
        self.hash
    }
}

impl TMerkleTreeNode for FileChunkNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for FileChunkNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileChunkNode({})", self.hash.to_string())
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for FileChunkNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileChunkNode({})", self.hash.to_string())
    }
}
