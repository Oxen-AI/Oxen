use crate::model::StagedEntryStatus;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StagedMerkleTreeNode {
    pub status: StagedEntryStatus,
    pub node: MerkleTreeNode,
}

impl StagedMerkleTreeNode {
    /// The file node staged for a file's addition or edit, or `None` for any other entry.
    pub(crate) fn into_staged_file(self) -> Option<FileNode> {
        match (self.status, self.node.node) {
            (
                StagedEntryStatus::Added | StagedEntryStatus::Modified,
                EMerkleTreeNode::File(file_node),
            ) => Some(file_node),
            _ => None,
        }
    }
}

impl Default for StagedMerkleTreeNode {
    fn default() -> Self {
        StagedMerkleTreeNode {
            status: StagedEntryStatus::Unmodified,
            node: MerkleTreeNode::default(),
        }
    }
}

impl Display for StagedMerkleTreeNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StagedMerkleTreeNode {{ hash: {}, data_type: {:?}, metadata: {:?}, name: {:?}, status: {:?} }}",
            self.node.hash,
            self.node.node.node_type(),
            self.node.node.metadata(),
            self.node.maybe_path(),
            self.status
        )
    }
}

impl Eq for StagedMerkleTreeNode {}

impl PartialEq for StagedMerkleTreeNode {
    fn eq(&self, other: &Self) -> bool {
        if let Ok(path) = self.node.maybe_path()
            && let Ok(other_path) = other.node.maybe_path()
        {
            return path == other_path;
        }

        self.node.hash == other.node.hash
    }
}

impl Hash for StagedMerkleTreeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if let Ok(path) = self.node.maybe_path() {
            path.hash(state);
        } else {
            self.node.hash(state);
        }
    }
}
