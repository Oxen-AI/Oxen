use std::path::{Path, PathBuf};

use crate::model::CommitEntry;
use serde::{Deserialize, Serialize};

use super::merkle_tree::node::FileNode;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EntryMergeConflict {
    pub lca_entry: CommitEntry,   // Least Common Ancestor Entry
    pub base_entry: CommitEntry,  // Entry that existed in the base commit
    pub merge_entry: CommitEntry, // Entry we are trying to merge in
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NodeMergeConflict {
    pub lca_entry: (FileNode, PathBuf),  // Least Common Ancestor Entry
    pub base_entry: (FileNode, PathBuf), // Entry that existed in the base commit
    pub merge_entry: (FileNode, PathBuf), // Entry we are trying to merge in
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MergeConflict {
    pub lca_entry: MergeConflictEntry,
    pub base_entry: MergeConflictEntry,
    pub merge_entry: MergeConflictEntry,
}

impl MergeConflict {
    pub fn to_entry_merge_conflict(&self) -> EntryMergeConflict {
        EntryMergeConflict {
            lca_entry: self.lca_entry.to_commit_entry(),
            base_entry: self.base_entry.to_commit_entry(),
            merge_entry: self.merge_entry.to_commit_entry(),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MergeConflictEntry {
    pub path: PathBuf,
    pub filename: String,
    pub hash: String,
    pub commit_id: String,
}

impl MergeConflictEntry {
    pub fn to_commit_entry(&self) -> CommitEntry {
        CommitEntry {
            path: self.path.clone(),
            hash: self.hash.clone(),
            commit_id: self.commit_id.clone(),
            // TODO: We need to get rid of CommitEntry after the v19 migration
            num_bytes: 0,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
        }
    }
}

impl EntryMergeConflict {
    pub fn to_merge_conflict(&self) -> MergeConflict {
        MergeConflict {
            lca_entry: self.lca_entry.to_merge_conflict_entry(),
            base_entry: self.base_entry.to_merge_conflict_entry(),
            merge_entry: self.merge_entry.to_merge_conflict_entry(),
        }
    }
}

impl NodeMergeConflict {
    pub fn to_merge_conflict(&self) -> MergeConflict {
        MergeConflict {
            lca_entry: to_merge_conflict_entry(&self.lca_entry.0, &self.lca_entry.1),
            base_entry: to_merge_conflict_entry(&self.base_entry.0, &self.base_entry.1),
            merge_entry: to_merge_conflict_entry(&self.merge_entry.0, &self.merge_entry.1),
        }
    }
}

impl CommitEntry {
    fn to_merge_conflict_entry(&self) -> MergeConflictEntry {
        MergeConflictEntry {
            path: self.path.clone(),
            filename: self
                .path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            hash: self.hash.clone(),
            commit_id: self.commit_id.clone(),
        }
    }
}

fn to_merge_conflict_entry(node: &FileNode, path: &Path) -> MergeConflictEntry {
    MergeConflictEntry {
        path: path.to_path_buf(),
        filename: path.file_name().unwrap().to_string_lossy().into_owned(),
        hash: node.hash.to_string(),
        commit_id: node.last_commit_id.to_string(),
    }
}
