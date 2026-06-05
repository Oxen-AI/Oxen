use std::path::PathBuf;

#[derive(Clone, Debug, Default)]
pub struct CleanOpts {
    /// Scope the cleanup to these paths (relative to cwd or absolute).
    /// Empty means the whole working tree.
    pub paths: Vec<PathBuf>,
    /// Without `force`, `clean` runs as a dry-run and makes no filesystem changes.
    pub force: bool,
}
