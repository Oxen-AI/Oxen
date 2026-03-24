use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::model::CommitEntry;

use super::diff_entry_status::DiffEntryStatus;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct DiffCommitEntry {
    pub status: DiffEntryStatus,
    // path for sorting so we don't have to dive into the optional commit entries
    pub path: PathBuf,

    // CommitEntry
    pub head_entry: Option<CommitEntry>,
    pub base_entry: Option<CommitEntry>,
}
