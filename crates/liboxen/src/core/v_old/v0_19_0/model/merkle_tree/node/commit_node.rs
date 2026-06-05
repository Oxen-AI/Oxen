use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::core::versions::MinOxenVersion;
use crate::model::merkle_tree::node::commit_node::TCommitNode;
use crate::model::{MerkleHash, MerkleTreeNodeType};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
pub struct CommitNodeData {
    pub hash: MerkleHash,
    pub node_type: MerkleTreeNodeType,
    pub parent_ids: Vec<MerkleHash>,
    pub message: String,
    pub author: String,
    pub email: String,
    pub timestamp: OffsetDateTime,
}

impl TCommitNode for CommitNodeData {
    fn node_type(&self) -> &MerkleTreeNodeType {
        &self.node_type
    }

    fn version(&self) -> MinOxenVersion {
        MinOxenVersion::V0_19_0
    }

    fn hash(&self) -> &MerkleHash {
        &self.hash
    }

    fn parent_ids(&self) -> &Vec<MerkleHash> {
        &self.parent_ids
    }

    fn message(&self) -> &str {
        &self.message
    }

    fn author(&self) -> &str {
        &self.author
    }

    fn email(&self) -> &str {
        &self.email
    }

    fn timestamp(&self) -> &OffsetDateTime {
        &self.timestamp
    }
}
