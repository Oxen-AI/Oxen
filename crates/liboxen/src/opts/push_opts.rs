use crate::core::progress::sync_progress::ProgressObserver;

#[derive(Clone, Debug, Default)]
pub struct PushOpts {
    pub remote: String,
    pub branch: String,
    pub delete: bool,
    pub force: bool,
    pub missing_files: bool,
    pub missing_files_commit_id: Option<String>,
    pub revalidate: bool,
    /// Called with how many files and bytes the push has uploaded, as it uploads them, on a
    /// blocking-pool thread of its own, so it may block. The push returns only after the observer
    /// has returned from the last update, whether or not the push succeeded. A push dropped
    /// partway keeps calling it until the uploads it started have stopped. A push with nothing to
    /// upload, or with `missing_files` or `revalidate` set, never calls it.
    pub progress: Option<ProgressObserver>,
}
