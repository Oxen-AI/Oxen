#[derive(Debug, PartialEq, Eq, Clone)]
pub enum EntryStatus {
    Added,
    Untracked,
    Modified,
    Removed,
}
