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
    /// Told how many files and bytes the push has uploaded, as it uploads them. A push with
    /// `missing_files` or `revalidate` set reports nothing.
    pub progress: Option<ProgressObserver>,
}
