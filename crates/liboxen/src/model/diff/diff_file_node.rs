use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::merkle_tree::node::FileNode;

use super::diff_entry_status::DiffEntryStatus;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiffFileNode {
    pub status: DiffEntryStatus,
    // path for sorting so we don't have to dive into the optional commit entries
    pub path: PathBuf,

    // FileNode(s)
    pub head_entry: Option<FileNode>,
    pub base_entry: Option<FileNode>,
}
