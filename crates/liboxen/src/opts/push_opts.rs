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
    /// Sent how many files and bytes the push has uploaded, as it uploads them. The push closes
    /// the channel when it ends, whether or not it succeeded, so `ProgressReceiver::changed`
    /// returns `None` after the last update. A push that fails or is dropped partway closes it once
    /// the uploads it started have stopped. A push with nothing to upload, or with `missing_files`
    /// or `revalidate` set, sends no update and only closes the channel. A sender serves one push.
    pub progress: Option<ProgressSender>,
}
