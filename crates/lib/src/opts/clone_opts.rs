use std::path::{Path, PathBuf};

use crate::config::repository_config::MerkleStoreKind;
use crate::opts::FetchOpts;

#[derive(Clone, Debug, Default)]
pub struct CloneOpts {
    // The url of the remote repository to clone
    pub url: String,
    // The local destination path to clone the repository to
    pub dst: PathBuf,
    // FetchOpts
    pub fetch_opts: FetchOpts,
    // Flag for cloning on VFS
    pub is_vfs: bool,
    // Flag for remote mode
    pub is_remote: bool,
    // Which Merkle tree store to use locally for the cloned repo. A per-disk choice
    // (like the one `oxen init` lets the caller make) — independent of the remote's
    // backend. Defaults to [`MerkleStoreKind::default()`] (File) so existing callers
    // are byte-for-byte unchanged.
    pub merkle_store_kind: MerkleStoreKind,
}

impl CloneOpts {
    /// Sets `branch` to `DEFAULT_BRANCH_NAME` and defaults `all` to `false`
    pub fn new(url: impl AsRef<str>, dst: impl AsRef<Path>) -> CloneOpts {
        CloneOpts {
            url: url.as_ref().to_string(),
            dst: dst.as_ref().to_path_buf(),
            fetch_opts: FetchOpts::new(),
            is_vfs: false,
            is_remote: false,
            merkle_store_kind: MerkleStoreKind::default(),
        }
    }

    pub fn from_branch(
        url: impl AsRef<str>,
        dst: impl AsRef<Path>,
        branch: impl AsRef<str>,
    ) -> CloneOpts {
        CloneOpts {
            fetch_opts: FetchOpts::from_branch(branch.as_ref()),
            is_vfs: false,
            is_remote: false,
            merkle_store_kind: MerkleStoreKind::default(),
            ..CloneOpts::new(url, dst)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `CloneOpts::default()` (also reached via `..Default::default()` from
    /// callers like `oxen-py`) picks the type-level default for
    /// `merkle_store_kind`, which is `File`. This preserves every existing
    /// caller byte-for-byte after the Phase I plumbing change.
    #[test]
    fn test_clone_opts_default_uses_file_merkle_store() {
        let opts = CloneOpts::default();
        assert_eq!(opts.merkle_store_kind, MerkleStoreKind::File);
        assert!(!opts.is_vfs);
    }

    /// `CloneOpts::new` is the most commonly used constructor — confirm it
    /// also defaults to File so existing call sites are unchanged.
    #[test]
    fn test_clone_opts_new_uses_file_merkle_store() {
        let opts = CloneOpts::new("http://example.com/ns/repo", "/tmp/dst");
        assert_eq!(opts.merkle_store_kind, MerkleStoreKind::File);
    }
}
