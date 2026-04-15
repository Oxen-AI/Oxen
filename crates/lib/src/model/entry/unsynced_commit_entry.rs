use crate::model::{Commit, CommitEntry};

#[derive(Debug)]
pub struct UnsyncedCommitEntries {
    pub commit: Commit,
    pub entries: Vec<CommitEntry>,
}
