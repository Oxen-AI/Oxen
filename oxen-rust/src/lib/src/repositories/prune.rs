//! Prune orphaned nodes and version files from the repository

use crate::core::v_latest::prune::{prune as prune_impl, PruneStats};
use crate::error::OxenError;
use crate::model::LocalRepository;

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
    prune_impl(repo, dry_run).await
}
