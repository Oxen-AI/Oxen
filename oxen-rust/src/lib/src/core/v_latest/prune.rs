//! Prune orphaned nodes and version files from the repository
//!
//! This module provides functionality to clean up nodes and version files that are not
//! referenced by any commit in the repository history.

use std::collections::HashSet;
use std::str::FromStr;

use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{LocalRepository, MerkleHash};
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
    let mut stats = PruneStats::new();

    log::info!("Starting prune operation (dry_run: {})", dry_run);

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

    log::info!("Prune operation complete");
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
            referenced_versions.insert(format!("{:032x}", chunk_hash));
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
        log::debug!("Nodes directory does not exist: {:?}", nodes_dir);
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
                        log::debug!("Would remove orphaned node: {:?}", node_path);
                    } else {
                        log::debug!("Removing orphaned node: {:?}", node_path);
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
    let version_store = create_version_store(&repo.path, None)?;

    let all_versions = version_store.list_versions().await?;
    stats.versions_scanned = all_versions.len();

    for version_hash in all_versions {
        if referenced_versions.contains(&version_hash) {
            stats.versions_kept += 1;
        } else {
            // This version is orphaned
            stats.versions_removed += 1;

            // Get the size before deleting
            if let Ok(metadata) = version_store.get_version_metadata(&version_hash).await {
                stats.bytes_freed += metadata.len();
            }

            if dry_run {
                log::debug!("Would remove orphaned version: {}", version_hash);
            } else {
                log::debug!("Removing orphaned version: {}", version_hash);
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
            repositories::add(&repo, &train_dir)?;
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
    async fn test_prune_dry_run() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let stats = prune(&repo, true).await?;
            // Dry run should not actually remove anything
            assert_eq!(stats.nodes_scanned, 0);
            assert_eq!(stats.versions_scanned, 0);
            Ok(())
        })
        .await
    }
}
