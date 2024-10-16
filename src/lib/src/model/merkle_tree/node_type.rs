//! This is the type of node that we are storing in the merkle tree
//!
//! There are only 6 node types as of now, so can store in a u8, and would
//! need a migration to change anyways.
//!
//! This value is stored at the top of a merkle tree db file
//! to know how to deserialize the node type
//!

use crate::model::merkle_tree::merkle_hash::MerkleHash;

use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Copy)]
pub enum MerkleTreeNodeType {
    Commit,
    File,
    Dir,
    VNode,
    Schema,
    FileChunk,
}

impl MerkleTreeNodeType {
    pub fn to_u8(&self) -> u8 {
        match self {
            MerkleTreeNodeType::Commit => 0u8,
            MerkleTreeNodeType::Dir => 1u8,
            MerkleTreeNodeType::VNode => 2u8,
            MerkleTreeNodeType::File => 3u8,
            MerkleTreeNodeType::Schema => 4u8,
            MerkleTreeNodeType::FileChunk => 5u8,
        }
    }

    pub fn from_u8(val: u8) -> MerkleTreeNodeType {
        match val {
            0u8 => MerkleTreeNodeType::Commit,
            1u8 => MerkleTreeNodeType::Dir,
            2u8 => MerkleTreeNodeType::VNode,
            3u8 => MerkleTreeNodeType::File,
            4u8 => MerkleTreeNodeType::Schema,
            5u8 => MerkleTreeNodeType::FileChunk,
            _ => panic!("Invalid MerkleTreeNodeType: {}", val),
        }
    }
}

pub trait MerkleTreeNodeIdType {
    fn dtype(&self) -> MerkleTreeNodeType;
    fn hash(&self) -> MerkleHash;
}

pub trait TMerkleTreeNode: MerkleTreeNodeIdType + Serialize + Debug + Display {}
