use std::path::PathBuf;

use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};

#[derive(Clone, Debug)]
pub struct FetchOpts {
    // The remote to fetch from
    pub remote: String,
    // The branch to clone
    pub branch: String,
    // If you only want to clone a subdirectory / tree, you can specify it here
    pub subtree_paths: Option<Vec<PathBuf>>,
    // The depth at which to clone the subtree.
    pub depth: Option<i32>,
    // If true, recursively clones the whole repository history
    // by default, only the head commit is cloned to save time and disk space
    pub all: bool,
    // Defaults to true, but on pull we want to only update the branch head if there are no conflicts
    pub should_update_branch_head: bool,
    // If true, re-scan all commits for missing version files and download them
    pub missing_files: bool,
}

impl Default for FetchOpts {
    fn default() -> Self {
        Self::new()
    }
}

impl FetchOpts {
    /// Sets `branch` to `DEFAULT_BRANCH_NAME` and defaults `all` to `false`
    pub fn new() -> FetchOpts {
        FetchOpts {
            remote: DEFAULT_REMOTE_NAME.to_string(),
            branch: DEFAULT_BRANCH_NAME.to_string(),
            subtree_paths: None,
            depth: None,
            all: false,
            should_update_branch_head: true,
            missing_files: false,
        }
    }

    pub fn from_branch(branch: impl AsRef<str>) -> FetchOpts {
        FetchOpts {
            branch: branch.as_ref().to_string(),
            ..FetchOpts::new()
        }
    }
}
