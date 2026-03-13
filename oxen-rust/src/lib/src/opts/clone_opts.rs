use std::path::{Path, PathBuf};

use crate::opts::FetchOpts;
use crate::opts::StorageOpts;

#[derive(Clone, Debug, Default)]
pub struct CloneOpts {
    // The url of the remote repository to clone
    pub url: String,
    // The local destination path to clone the repository to
    pub dst: PathBuf,
    // FetchOpts
    pub fetch_opts: FetchOpts,
    // StorageOpts
    pub storage_opts: StorageOpts,
    // Flag for cloning on VFS
    pub is_vfs: bool,
    // Flag for remote mode
    pub is_remote: bool,
}

impl CloneOpts {
    /// Sets `branch` to `DEFAULT_BRANCH_NAME` and defaults `all` to `false`
    pub fn new(url: &str, dst: &Path) -> CloneOpts {
        CloneOpts {
            url: url.to_string(),
            dst: dst.to_path_buf(),
            fetch_opts: FetchOpts::new(),
            storage_opts: StorageOpts::from_path(dst, true),
            is_vfs: false,
            is_remote: false,
        }
    }

    pub fn from_branch(url: &str, dst: &Path, branch: &str) -> CloneOpts {
        CloneOpts {
            fetch_opts: FetchOpts::from_branch(branch),
            storage_opts: StorageOpts::from_path(dst, true),
            is_vfs: false,
            is_remote: false,
            ..CloneOpts::new(url, dst)
        }
    }
}
