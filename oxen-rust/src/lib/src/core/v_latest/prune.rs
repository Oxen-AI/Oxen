//! Prune orphaned nodes and version files from the repository
//!
//! This module provides functionality to clean up nodes and version files that are not
//! referenced by any commit in the repository history.

use std::collections::HashSet;
use std::str::FromStr;

use crate::api;
use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{LocalRepository, MerkleHash, RemoteRepository};
use crate::opts::StorageOpts;
use crate::repositories;
use crate::storage::version_store::create_version_store;

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
    let nodes_dir = repo
        .path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);

    if !nodes_dir.exists() {
        log::debug!("Nodes directory does not exist: {nodes_dir:?}");
        return Ok(());
    }

    // Walk through the nodes directory structure
    // Nodes are stored in a two-level directory structure: prefix/suffix
    for top_entry in std::fs::read_dir(&nodes_dir)? {
        let top_entry = top_entry?;
        if !top_entry.file_type()?.is_dir() {
            continue;
        }

        for sub_entry in std::fs::read_dir(top_entry.path())? {
            let sub_entry = sub_entry?;
            if !sub_entry.file_type()?.is_dir() {
                continue;
            }

            stats.nodes_scanned += 1;

            // Reconstruct the hash from the directory structure
            let top_name = top_entry.file_name();
            let sub_name = sub_entry.file_name();
            let hash_str = format!(
                "{}{}",
                top_name.to_string_lossy(),
                sub_name.to_string_lossy()
            );

            // Try to parse the hash
            if let Ok(hash) = MerkleHash::from_str(&hash_str) {
                if referenced_nodes.contains(&hash) {
                    stats.nodes_kept += 1;
                } else {
                    // This node is orphaned
                    stats.nodes_removed += 1;
                    let node_path = sub_entry.path();

                    if dry_run {
                        log::debug!("Would remove orphaned node: {node_path:?}");
                    } else {
                        log::debug!("Removing orphaned node: {node_path:?}");
                        std::fs::remove_dir_all(&node_path)?;
                    }
                }
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
    let storage_opts = StorageOpts::from_path(&repo.path, true);
    let version_store = create_version_store(&storage_opts)?;

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
    use crate::test;

    #[tokio::test]
    async fn test_prune_empty_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let stats = prune(&repo, false).await?;
            assert_eq!(stats.nodes_scanned, 0);
            assert_eq!(stats.nodes_removed, 0);
            assert_eq!(stats.versions_scanned, 0);
            assert_eq!(stats.versions_removed, 0);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_prune_with_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Add and commit some files
            let train_dir = repo.path.join("train");
            test::add_txt_file_to_dir(&train_dir, "file1.txt")?;
            test::add_txt_file_to_dir(&train_dir, "file2.txt")?;
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "Add files")?;

            // Run prune - should not remove anything
            let stats = prune(&repo, false).await?;
            assert_eq!(stats.nodes_removed, 0);
            assert_eq!(stats.versions_removed, 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_prune_branch() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let train_dir = repo.path.join("train");
            test::add_txt_file_to_dir(&train_dir, "file1.txt")?;
            test::add_txt_file_to_dir(&train_dir, "file2.txt")?;
            repositories::add(&repo, &train_dir).await?;
            let initial_commit = repositories::commit(&repo, "Add files")?;

            // Create a new branch
            let branch_name = "test-branch";
            repositories::branches::create(&repo, branch_name, &initial_commit.id)?;

            // Run prune - should not remove anything
            let stats = prune(&repo, false).await?;
            assert_eq!(stats.nodes_removed, 0);
            assert_eq!(stats.versions_removed, 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_prune_dry_run() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let stats = prune(&repo, true).await?;
            // Dry run should not actually remove anything
            assert_eq!(stats.nodes_scanned, 0);
            assert_eq!(stats.versions_scanned, 0);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_prune_deleted_branch_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Initial commit
            let train_dir = repo.path.join("train");
            let initial_file = test::add_txt_file_to_dir(&train_dir, "file1.txt")?;
            repositories::add(&repo, &initial_file).await?;
            let initial_commit = repositories::commit(&repo, "Add file1")?;

            // Create a new branch and add a commit
            let branch_1_name = "branch-1";
            repositories::branches::create(&repo, branch_1_name, &initial_commit.id)?;
            repositories::checkout(&repo, branch_1_name).await?;
            let branch_1_file = test::add_txt_file_to_dir(&train_dir, "file2.txt")?;
            repositories::add(&repo, &branch_1_file).await?;
            let branch_1_commit = repositories::commit(&repo, "Commit on branch 1")?;

            // Create a second branch and add another commit
            let branch_2_name = "branch-2";
            repositories::branches::create(&repo, branch_2_name, &branch_1_commit.id)?;
            repositories::checkout(&repo, branch_2_name).await?;
            let branch_2_file = test::add_txt_file_to_dir(&train_dir, "file3.txt")?;
            repositories::add(&repo, &branch_2_file).await?;
            let branch_2_commit = repositories::commit(&repo, "Commit on branch 2")?;

            // Checkout main and delete the branches
            repositories::checkout(&repo, "main").await?;
            repositories::branches::force_delete(&repo, branch_1_name)?;
            repositories::branches::force_delete(&repo, branch_2_name)?;

            // Prune the repo
            prune(&repo, false).await?;

            // The commits from the deleted branches should be gone
            let all_commits = repositories::commits::list_all(&repo)?;
            assert_eq!(all_commits.len(), 1);
            assert!(all_commits.contains(&initial_commit));
            assert!(!all_commits.contains(&branch_1_commit));
            assert!(!all_commits.contains(&branch_2_commit));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_prune_does_not_delete_referenced_data() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Initial commit on main
            let file1 = repo.path.join("file1.txt");
            tokio::fs::write(&file1, "file1").await?;
            repositories::add(&repo, &file1).await?;
            let initial_commit = repositories::commit(&repo, "Add file1")?;

            // Create a feature branch and dd a commit to the feature branch
            let branch_name = "feature";
            repositories::branches::create(&repo, branch_name, &initial_commit.id)?;

            repositories::checkout(&repo, branch_name).await?;
            let file2 = repo.path.join("file2.txt");
            tokio::fs::write(&file2, "file2").await?;
            repositories::add(&repo, &file2).await?;
            repositories::commit(&repo, "Add file2")?;

            // Checkout main and merge the feature branch
            repositories::checkout(&repo, "main").await?;
            repositories::merge::merge(&repo, branch_name).await?;

            // Delete the feature branch
            repositories::branches::delete(&repo, branch_name)?;

            // Get all referenced nodes and versions before pruning
            let (referenced_nodes_before, referenced_versions_before) =
                collect_referenced_hashes(&repo)?;

            // Prune the repo
            let stats = prune(&repo, false).await?;

            // No nodes or versions should be removed
            assert_eq!(stats.nodes_removed, 0);
            assert_eq!(stats.versions_removed, 0);

            // Get all referenced nodes and versions after pruning
            let (referenced_nodes_after, referenced_versions_after) =
                collect_referenced_hashes(&repo)?;

            // The set of referenced hashes should be the same
            assert_eq!(referenced_nodes_before, referenced_nodes_after);
            assert_eq!(referenced_versions_before, referenced_versions_after);

            // All commits should still be present
            let all_commits = repositories::commits::list_all(&repo)?;
            assert_eq!(all_commits.len(), 2); // Initial commit + merge commit

            Ok(())
        })
        .await
    }
}
