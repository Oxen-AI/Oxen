use crate::model::entries::commit_entry::Entry;
use crate::model::Commit;

#[derive(Debug)]
pub struct UnsyncedCommitEntries {
    pub commit: Commit,
    pub entries: Vec<Entry>,
}
