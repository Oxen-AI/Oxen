//! Prune orphaned nodes and version files from the repository

use crate::core::repo_locks;
use crate::core::v_latest::prune::{
    PruneStats, prune as prune_impl, prune_remote as prune_remote_impl,
};
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};

/// Prune orphaned nodes and version files from the repository
///
/// This function removes nodes and version files that are not referenced by any commit
/// in the repository history.
///
/// # Arguments
/// * `repo` - The local repository to prune
/// * `dry_run` - If true, only report what would be removed without actually removing it
///
/// # Returns
/// Statistics about the prune operation
pub async fn prune(repo: &LocalRepository, dry_run: bool) -> Result<PruneStats, OxenError> {
    let work = prune_impl(repo, dry_run);
    if dry_run {
        // Dry run only reports what would be removed; no exclusive lock needed.
        work.await
    } else {
        // The sweep deletes nodes and version files; hold the whole-repo exclusive lock so no
        // write lands mid-prune.
        repo_locks::with_repo_exclusive(repo, work).await
    }
}

/// Prune orphaned nodes and version files from a remote repository
///
/// This function triggers a prune operation on the remote server.
///
/// # Arguments
/// * `remote_repo` - The remote repository to prune
/// * `dry_run` - If true, only report what would be removed without actually removing it
///
/// # Returns
/// Statistics about the prune operation
pub async fn prune_remote(
    remote_repo: &RemoteRepository,
    dry_run: bool,
) -> Result<PruneStats, OxenError> {
    prune_remote_impl(remote_repo, dry_run).await
}
