use crate::core::progress::sync_progress::ProgressSender;

#[derive(Clone, Debug, Default)]
pub struct PushOpts {
    pub remote: String,
    pub branch: String,
    pub delete: bool,
    pub force: bool,
    pub missing_files: bool,
    pub missing_files_commit_id: Option<String>,
    pub revalidate: bool,
    /// Sent how many files and bytes the push has uploaded, as it uploads them. A push with
    /// `missing_files` or `revalidate` set sends nothing. This field's sender lives as long as the
    /// opts do, so a receiver learns that the push ended when the push returns, not from
    /// `ProgressReceiver::changed`.
    pub progress: Option<ProgressSender>,
}
