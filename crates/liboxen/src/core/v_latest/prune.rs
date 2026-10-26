//! Prune orphaned nodes and version files from the repository
//!
//! This module provides functionality to clean up nodes and version files that are not
//! referenced by any commit in the repository history.

use std::collections::HashSet;

use crate::api;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{LocalRepository, MerkleHash, RemoteRepository};
use crate::repositories;

/// Statistics about the prune operation
#[derive(Debug, Clone)]
pub struct PruneStats {
    pub nodes_scanned: usize,
    pub nodes_kept: usize,
    pub nodes_removed: usize,
    pub versions_scanned: usize,
    pub versions_kept: usize,
    pub versions_removed: usize,
    pub bytes_freed: u64,
}

impl PruneStats {
    pub fn new() -> Self {
        Self {
            nodes_scanned: 0,
            nodes_kept: 0,
            nodes_removed: 0,
            versions_scanned: 0,
            versions_kept: 0,
            versions_removed: 0,
            bytes_freed: 0,
        }
    }
}

impl Default for PruneStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Prune orphaned nodes and version files from the repository
///
/// This function:
/// 1. Lists all commits in the repository
/// 2. Traverses all nodes referenced by those commits
/// 3. Collects all version file hashes referenced by file nodes
/// 4. Removes nodes and version files that are not referenced
///
/// # Arguments
/// * `repo` - The local repository to prune
/// * `dry_run` - If true, only report what would be removed without actually removing it
///
/// # Returns
/// Statistics about the prune operation
pub async fn prune(repo: &LocalRepository, dry_run: bool) -> Result<PruneStats, OxenError> {
    let start = std::time::Instant::now();
    let mut stats = PruneStats::new();

    log::info!("Starting prune operation (dry_run: {dry_run})");

    // Step 1: Collect all referenced nodes and version hashes from all commits
    log::info!("Collecting referenced nodes and version files from commit history...");
    let (referenced_nodes, referenced_versions) = collect_referenced_hashes(repo)?;
    log::info!(
        "Found {} referenced nodes and {} referenced version files",
        referenced_nodes.len(),
        referenced_versions.len()
    );

    // Step 2: Prune orphaned nodes
    log::info!("Pruning orphaned nodes...");
    prune_nodes(repo, &referenced_nodes, &mut stats, dry_run)?;

    // Step 3: Prune orphaned version files
    log::info!("Pruning orphaned version files...");
    prune_versions(repo, &referenced_versions, &mut stats, dry_run).await?;

    let duration = start.elapsed();
    log::info!("Prune operation complete in {duration:.2?}");
    log::info!(
        "Nodes: scanned={}, kept={}, removed={}",
        stats.nodes_scanned,
        stats.nodes_kept,
        stats.nodes_removed
    );
    log::info!(
        "Versions: scanned={}, kept={}, removed={}",
        stats.versions_scanned,
        stats.versions_kept,
        stats.versions_removed
    );
    log::info!("Bytes freed: {}", bytesize::ByteSize::b(stats.bytes_freed));

    Ok(stats)
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
    log::info!(
        "Starting remote prune operation on {} (dry_run: {})",
        remote_repo.url(),
        dry_run
    );

    let stats_response = api::client::prune::prune(remote_repo, dry_run).await?;

    // Convert the API response stats to our local PruneStats type
    let stats = PruneStats {
        nodes_scanned: stats_response.nodes_scanned,
        nodes_kept: stats_response.nodes_kept,
        nodes_removed: stats_response.nodes_removed,
        versions_scanned: stats_response.versions_scanned,
        versions_kept: stats_response.versions_kept,
        versions_removed: stats_response.versions_removed,
        bytes_freed: stats_response.bytes_freed,
    };

    log::info!("Remote prune operation complete");
    log::info!(
        "Nodes: scanned={}, kept={}, removed={}",
        stats.nodes_scanned,
        stats.nodes_kept,
        stats.nodes_removed
    );
    log::info!(
        "Versions: scanned={}, kept={}, removed={}",
        stats.versions_scanned,
        stats.versions_kept,
        stats.versions_removed
    );
    log::info!("Bytes freed: {}", bytesize::ByteSize::b(stats.bytes_freed));

    Ok(stats)
}

/// Collect all referenced node hashes and version file hashes from all commits
fn collect_referenced_hashes(
    repo: &LocalRepository,
) -> Result<(HashSet<MerkleHash>, HashSet<String>), OxenError> {
    let mut referenced_nodes = HashSet::new();
    let mut referenced_versions = HashSet::new();

    // Get all commits in the repository
    let all_commits = repositories::commits::list_all(repo)?;
    log::debug!("Processing {} commits", all_commits.len());

    for commit in all_commits {
        // Get the commit node and traverse it
        if let Some(commit_node) = repositories::tree::get_root_with_children(repo, &commit)? {
            collect_node_hashes(
                &commit_node,
                &mut referenced_nodes,
                &mut referenced_versions,
            )?;
        }
    }

    Ok((referenced_nodes, referenced_versions))
}

/// Recursively collect all node hashes and version file hashes from a node tree
fn collect_node_hashes(
    node: &MerkleTreeNode,
    referenced_nodes: &mut HashSet<MerkleHash>,
    referenced_versions: &mut HashSet<String>,
) -> Result<(), OxenError> {
    // Add this node's hash
    referenced_nodes.insert(node.hash);

    // If this is a file node, collect its version hash
    if let EMerkleTreeNode::File(file_node) = &node.node {
        let version_hash = file_node.hash().to_string();
        referenced_versions.insert(version_hash);

        // Also collect chunk hashes if present
        for chunk_hash in file_node.chunk_hashes() {
            referenced_versions.insert(format!("{chunk_hash:032x}"));
        }
    }

    // Recursively process children
    for child in &node.children {
        collect_node_hashes(child, referenced_nodes, referenced_versions)?;
    }

    Ok(())
}

/// Prune orphaned nodes from the repository
fn prune_nodes(
    repo: &LocalRepository,
    referenced_nodes: &HashSet<MerkleHash>,
    stats: &mut PruneStats,
    dry_run: bool,
) -> Result<(), OxenError> {
    // Enumerate and delete through the store so pruning is backend-agnostic, mirroring how
    // `prune_versions` drives the version store below.
    let store = repo.merkle_node_store();
    let all_nodes = store.list_hashes()?;
    stats.nodes_scanned += all_nodes.len();

    for hash in all_nodes {
        if referenced_nodes.contains(&hash) {
            stats.nodes_kept += 1;
        } else {
            // This node is orphaned
            stats.nodes_removed += 1;
            if dry_run {
                log::debug!("Would remove orphaned node: {hash}");
            } else {
                log::debug!("Removing orphaned node: {hash}");
                store.delete(&hash)?;
            }
        }
    }

    Ok(())
}

/// Prune orphaned version files from the repository
async fn prune_versions(
    repo: &LocalRepository,
    referenced_versions: &HashSet<String>,
    stats: &mut PruneStats,
    dry_run: bool,
) -> Result<(), OxenError> {
    let version_store = repo.version_store();

    let all_versions = version_store.list_versions().await?;
    stats.versions_scanned = all_versions.len();

    for version_hash in all_versions {
        if referenced_versions.contains(&version_hash) {
            stats.versions_kept += 1;
        } else {
            // This version is orphaned
            stats.versions_removed += 1;

            // Get the size before deleting
            if let Ok(file_size) = version_store.get_version_size(&version_hash).await {
                stats.bytes_freed += file_size;
            }

            if dry_run {
                log::debug!("Would remove orphaned version: {version_hash}");
            } else {
                log::debug!("Removing orphaned version: {version_hash}");
                version_store.delete_version(&version_hash).await?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::test;
    use crate::util::fs::create_dir_all;

    #[tokio::test]
    async fn test_prune_removes_only_unreachable_commits() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let stats = prune(&repo, false).await?;
            assert_eq!(stats.nodes_scanned, 0);
            assert_eq!(stats.nodes_removed, 0);
            assert_eq!(stats.versions_scanned, 0);
            assert_eq!(stats.versions_removed, 0);
            let stats = prune(&repo, true).await?;
            assert_eq!(
                stats.nodes_scanned, 0,
                "a dry run of an empty repo scans nothing"
            );
            assert_eq!(stats.versions_scanned, 0);

            let train_dir = repo.path.join("train");
            create_dir_all(&train_dir)?;
            test::add_txt_file_to_dir(&train_dir, "file1.txt")?;
            test::add_txt_file_to_dir(&train_dir, "file2.txt")?;
            let root_file = repo.path.join("file1.txt");
            tokio::fs::write(&root_file, "file1").await?;
            repositories::add(&repo, &train_dir).await?;
            repositories::add(&repo, &root_file).await?;
            let initial_commit = repositories::commit(&repo, "Add files")?;
            let stats = prune(&repo, false).await?;
            assert_eq!(stats.nodes_removed, 0, "committed data is referenced");
            assert_eq!(stats.versions_removed, 0);

            repositories::branches::create(&repo, "test-branch", &initial_commit.id)?;
            let stats = prune(&repo, false).await?;
            assert_eq!(stats.nodes_removed, 0, "a second branch adds no garbage");
            assert_eq!(stats.versions_removed, 0);

            // A feature branch merged into main and then deleted leaves nothing to remove.
            let branch_name = "feature";
            repositories::branches::create(&repo, branch_name, &initial_commit.id)?;
            repositories::checkout(&repo, branch_name).await?;
            let file3 = repo.path.join("file3.txt");
            tokio::fs::write(&file3, "file3").await?;
            repositories::add(&repo, &file3).await?;
            repositories::commit(&repo, "Add file3")?;
            repositories::checkout(&repo, DEFAULT_BRANCH_NAME).await?;
            repositories::merge::merge(&repo, branch_name).await?;
            repositories::branches::delete(&repo, branch_name)?;

            let commits_before = repositories::commits::list_all(&repo)?;
            let referenced_before = collect_referenced_hashes(&repo)?;
            let stats = prune(&repo, false).await?;
            assert_eq!(
                stats.nodes_removed, 0,
                "merged data is referenced from main"
            );
            assert_eq!(stats.versions_removed, 0);
            assert_eq!(collect_referenced_hashes(&repo)?, referenced_before);
            assert_eq!(repositories::commits::list_all(&repo)?, commits_before);

            // Commits reachable only from deleted branches are removed.
            let head = repositories::commits::head_commit(&repo)?;
            repositories::branches::create(&repo, "branch-1", &head.id)?;
            repositories::checkout(&repo, "branch-1").await?;
            let file = test::add_txt_file_to_dir(&train_dir, "file4.txt")?;
            repositories::add(&repo, &file).await?;
            let branch_1_commit = repositories::commit(&repo, "Commit on branch 1")?;
            repositories::branches::create(&repo, "branch-2", &branch_1_commit.id)?;
            repositories::checkout(&repo, "branch-2").await?;
            let file = test::add_txt_file_to_dir(&train_dir, "file5.txt")?;
            repositories::add(&repo, &file).await?;
            let branch_2_commit = repositories::commit(&repo, "Commit on branch 2")?;
            repositories::checkout(&repo, DEFAULT_BRANCH_NAME).await?;
            repositories::branches::force_delete(&repo, "branch-1")?;
            repositories::branches::force_delete(&repo, "branch-2")?;

            let stats = prune(&repo, true).await?;
            assert_eq!(
                stats.versions_removed, 2,
                "a dry run reports the two branch-only files"
            );
            assert!(stats.nodes_removed > 0);
            for commit in [&branch_1_commit, &branch_2_commit] {
                assert!(
                    repositories::commits::get_by_id(&repo, &commit.id)?.is_some(),
                    "a dry run removes nothing"
                );
            }

            let stats = prune(&repo, false).await?;
            assert_eq!(
                stats.versions_removed, 2,
                "the two branch-only files' versions are removed"
            );
            assert!(stats.nodes_removed > 0);
            for commit in [&branch_1_commit, &branch_2_commit] {
                assert!(
                    repositories::commits::get_by_id(&repo, &commit.id)?.is_none(),
                    "a commit only a deleted branch reached is removed"
                );
            }
            assert_eq!(repositories::commits::list_all(&repo)?, commits_before);

            Ok(())
        })
        .await
    }
}
