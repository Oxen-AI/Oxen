use std::path::{Path, PathBuf};

use crate::opts::fetch_opts::FetchOpts;

#[derive(Clone, Debug)]
pub struct CloneOpts {
    // The url of the remote repository to clone
    pub url: String,
    // The local destination path to clone the repository to
    pub dst: PathBuf,
    // FetchOpts
    pub fetch_opts: FetchOpts,
    // Flag for remote mode
    pub is_remote: bool,
}

impl CloneOpts {
    /// Sets `branch` to `DEFAULT_BRANCH_NAME` and defaults `all` to `false`
    pub fn new(url: impl AsRef<str>, dst: impl AsRef<Path>) -> CloneOpts {
        CloneOpts {
            url: url.as_ref().to_string(),
            dst: dst.as_ref().to_path_buf(),
            fetch_opts: FetchOpts::new(),
            is_remote: false,
        }
    }

    pub fn from_branch(
        url: impl AsRef<str>,
        dst: impl AsRef<Path>,
        branch: impl AsRef<str>,
    ) -> CloneOpts {
        CloneOpts {
            fetch_opts: FetchOpts::from_branch(branch.as_ref()),
            is_remote: false,
            ..CloneOpts::new(url, dst)
        }
    }
}
