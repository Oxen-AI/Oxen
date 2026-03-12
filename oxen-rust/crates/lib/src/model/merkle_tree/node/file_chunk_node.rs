//! This is a compact representation of a merkle tree file chunk node
//! that is stored in on disk
//!

use serde::{Deserialize, Serialize};

use crate::error::OxenError;
use crate::model::{MerkleHash, MerkleTreeNodeIdType, MerkleTreeNodeType, TMerkleTreeNode};

use std::fmt;

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct FileChunkNode {
    pub data: Vec<u8>,
    pub node_type: MerkleTreeNodeType,
    pub hash: MerkleHash,
}

impl FileChunkNode {
    pub fn deserialize(data: &[u8]) -> Result<FileChunkNode, OxenError> {
        rmp_serde::from_slice(data)
            .map_err(|e| OxenError::basic_str(format!("Error deserializing file chunk node: {e}")))
    }
}

impl Default for FileChunkNode {
    fn default() -> Self {
        FileChunkNode {
            data: vec![],
            node_type: MerkleTreeNodeType::FileChunk,
            hash: MerkleHash::new(0),
        }
    }
}

impl MerkleTreeNodeIdType for FileChunkNode {
    fn node_type(&self) -> MerkleTreeNodeType {
        self.node_type
    }

    fn hash(&self) -> MerkleHash {
        self.hash
    }
}

impl TMerkleTreeNode for FileChunkNode {}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for FileChunkNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileChunkNode({})", self.hash)
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for FileChunkNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FileChunkNode({})", self.hash)
    }
}
