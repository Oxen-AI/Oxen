use crate::model::Commit;
use crate::model::entry::commit_entry::Entry;

#[derive(Debug)]
pub struct UnsyncedCommitEntries {
    pub commit: Commit,
    pub entries: Vec<Entry>,
}
