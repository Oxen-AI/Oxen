//! This is a compact representation of a merkle tree vnode
//! that is stored in on disk
//!

use crate::core::versions::MinOxenVersion;
use crate::model::merkle_tree::node::vnode::TVNode;
use crate::model::{MerkleHash, MerkleTreeNodeType};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct VNodeData {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
    pub num_entries: u64,
}

impl TVNode for VNodeData {
    fn version(&self) -> MinOxenVersion {
        MinOxenVersion::LATEST
    }

    fn hash(&self) -> &MerkleHash {
        &self.hash
    }

    fn node_type(&self) -> &MerkleTreeNodeType {
        &self.node_type
    }

    fn num_entries(&self) -> u64 {
        self.num_entries
    }

    fn set_num_entries(&mut self, num_entries: u64) {
        self.num_entries = num_entries;
    }
}
