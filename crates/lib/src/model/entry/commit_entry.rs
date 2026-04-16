use crate::model::Commit;
use crate::model::merkle_tree::node::{DirNode, EMerkleTreeNode, FileNode};

use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitPath {
    pub commit: Option<Commit>,
    pub path: PathBuf,
}

/// Represents a file or directory entry at a specific commit.
///
/// `Hash` and `Eq` are based on the content hash field, so `HashSet<CommitEntry>`
/// deduplicates by file content. This is used during fetch and push to avoid
/// transferring the same content twice.
#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct CommitEntry {
    pub commit_id: String,
    #[schema(value_type = String)]
    pub path: PathBuf,
    pub hash: String,
    pub num_bytes: u64,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
}

// TODONOW - maybe rename or reorg, this isn't an "entry" as such
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CompareEntry {
    pub commit_entry: Option<CommitEntry>,
    pub path: PathBuf,
}

impl PartialEq for CommitEntry {
    fn eq(&self, other: &CommitEntry) -> bool {
        self.hash == other.hash
    }
}

impl Eq for CommitEntry {}

impl Hash for CommitEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl CommitEntry {
    pub fn from_node(node: &EMerkleTreeNode) -> CommitEntry {
        match node {
            EMerkleTreeNode::Directory(dir_node) => CommitEntry::from_dir_node(dir_node),
            EMerkleTreeNode::File(file_node) => CommitEntry::from_file_node(file_node),
            _ => panic!("Cannot convert EMerkleTreeNode to CommitEntry"),
        }
    }

    pub fn from_file_node(file_node: &FileNode) -> CommitEntry {
        CommitEntry {
            commit_id: file_node.last_commit_id().to_string(),
            path: PathBuf::from(file_node.name()),
            hash: file_node.hash().to_string(),
            num_bytes: file_node.num_bytes(),
            last_modified_seconds: file_node.last_modified_seconds(),
            last_modified_nanoseconds: file_node.last_modified_nanoseconds(),
        }
    }

    pub fn from_dir_node(dir_node: &DirNode) -> CommitEntry {
        CommitEntry {
            commit_id: dir_node.last_commit_id().to_string(),
            path: PathBuf::from(dir_node.name()),
            hash: dir_node.hash().to_string(),
            num_bytes: dir_node.num_bytes(),
            last_modified_seconds: dir_node.last_modified_seconds(),
            last_modified_nanoseconds: dir_node.last_modified_nanoseconds(),
        }
    }
}
