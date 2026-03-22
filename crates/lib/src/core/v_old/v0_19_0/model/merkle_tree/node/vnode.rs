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
}

impl TVNode for VNodeData {
    fn version(&self) -> MinOxenVersion {
        MinOxenVersion::V0_19_0
    }

    fn hash(&self) -> &MerkleHash {
        &self.hash
    }

    fn node_type(&self) -> &MerkleTreeNodeType {
        &self.node_type
    }

    fn num_entries(&self) -> u64 {
        // log::warn!("VNodeData(0.19.0) does not have num_entries");
        0
    }

    fn set_num_entries(&mut self, _: u64) {
        // log::warn!("VNodeData(0.19.0) does not have num_entries");
    }
}
