use crate::core::v0_19_0::index::commit_merkle_tree::CommitMerkleTree;
use crate::core::v0_19_0::{commits, fetch};
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::{Commit, CommitEntry, LocalRepository, MerkleTreeNodeType};
use crate::repositories;
use crate::util;

use std::collections::HashSet;
use std::path::Path;

pub fn list_entry_versions_for_commit(
    local_repo: &LocalRepository,
    commit_id: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    log::debug!("list_entry_versions_for_commit {} -> {:?}", commit_id, path);
    let mut branch_commits = repositories::commits::list_from(local_repo, commit_id)?;

    // Sort on timestamp oldest to newest
    branch_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut result: Vec<(Commit, CommitEntry)> = Vec::new();
    let mut seen_hashes: HashSet<String> = HashSet::new();

    for commit in branch_commits {
        log::debug!("list_entry_versions_for_commit {}", commit);
        let tree = repositories::tree::get_by_commit(local_repo, &commit)?;
        let node = tree.get_by_path(path)?;
        tree.print();

        if let Some(node) = node {
            if !seen_hashes.contains(&node.node.hash().to_string()) {
                log::debug!(
                    "list_entry_versions_for_commit adding {} -> {}",
                    commit,
                    node
                );
                seen_hashes.insert(node.node.hash().to_string());

                match node.node {
                    EMerkleTreeNode::File(file_node) => {
                        let entry = CommitEntry::from_file_node(&file_node);
                        result.push((commit, entry));
                    }
                    EMerkleTreeNode::Directory(dir_node) => {
                        let entry = CommitEntry::from_dir_node(&dir_node);
                        result.push((commit, entry));
                    }
                    _ => {}
                }
            } else {
                log::debug!("list_entry_versions_for_commit already seen {}", node);
            }
        }
    }

    result.reverse();

    Ok(result)
}

pub async fn checkout(repo: &LocalRepository, branch_name: &str) -> Result<(), OxenError> {
    log::debug!("checkout {branch_name}");
    let branch = repositories::branches::get_by_name(repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;

    checkout_commit_id(repo, &branch.commit_id).await?;

    Ok(())
}

pub async fn checkout_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    log::debug!("checkout_commit_id {}", commit_id.as_ref());
    let commit = repositories::commits::get_by_id(repo, &commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&commit_id))?;

    // Fetch entries if needed
    fetch::maybe_fetch_missing_entries(repo, &commit).await?;

    // Set working repo to commit
    set_working_repo_to_commit(repo, &commit, true).await?;

    Ok(())
}

pub async fn set_working_repo_to_commit(
    repo: &LocalRepository,
    commit: &Commit,
    force: bool,
) -> Result<(), OxenError> {
    log::debug!("set_working_repo_to_commit {}", commit);
    if !force {
        if let Some(head_commit) = commits::head_commit_maybe(repo)? {
            if head_commit.id == commit.id {
                log::debug!(
                    "set_working_repo_to_commit, do nothing... head commit == commit_id {}",
                    commit.id
                );
                return Ok(());
            }
        }
    }

    let tree = CommitMerkleTree::from_commit(repo, commit)?;

    cleanup_removed_files(repo, &tree)?;
    r_restore_missing_or_modified_files(repo, &tree.root, Path::new(""))?;

    Ok(())
}

fn cleanup_removed_files(
    repo: &LocalRepository,
    target_tree: &CommitMerkleTree,
) -> Result<(), OxenError> {
    // Get the head commit, and the merkle tree for that commit
    // Compare the nodes in the head tree to the nodes in the target tree
    // If the file node is in the head tree, but not in the target tree, remove it
    // If we don't have a head commit, there isn't anything to clean up (i.e., new clone)
    if let Some(head_commit) = commits::head_commit_maybe(repo)? {
        log::debug!("cleanup_removed_files head_commit {:?}", head_commit.id);

        let head_tree = CommitMerkleTree::from_commit(repo, &head_commit)?;
        let head_root_dir_node = CommitMerkleTree::get_root_dir_from_commit(&head_tree.root)?;

        r_remove_if_not_in_target(repo, head_root_dir_node, target_tree, Path::new(""))?;
    }

    Ok(())
}

fn r_remove_if_not_in_target(
    repo: &LocalRepository,
    head_node: &MerkleTreeNode,
    target_tree: &CommitMerkleTree,
    current_path: &Path,
) -> Result<(), OxenError> {
    log::debug!(
        "r_remove_if_not_in_target head_node: {:?} current_path: {:?}",
        head_node.hash,
        current_path
    );
    match &head_node.node.dtype() {
        MerkleTreeNodeType::File => {
            let file_node = head_node.file()?;
            let file_path = current_path.join(&file_node.name);
            if target_tree.get_by_path(&file_path)?.is_none() {
                let full_path = repo.path.join(&file_path);
                if full_path.exists() {
                    log::debug!("Removing file: {:?}", file_path);
                    util::fs::remove_file(&full_path)?;
                }
            }
        }
        MerkleTreeNodeType::Dir => {
            // TODO: can we also check if the directory is in the target tree,
            // and potentially remove the whole directory?
            let dir_node = head_node.dir()?;
            let dir_path = current_path.join(&dir_node.name);
            let children = CommitMerkleTree::node_files_and_folders(head_node)?;
            for child in children {
                r_remove_if_not_in_target(repo, &child, target_tree, &dir_path)?;
            }
            // Remove directory if it's empty
            let full_dir_path = repo.path.join(&dir_path);
            if full_dir_path.exists() && full_dir_path.read_dir()?.next().is_none() {
                log::debug!("Removing empty directory: {:?}", dir_path);
                util::fs::remove_dir_all(&full_dir_path)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn r_restore_missing_or_modified_files(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    path: &Path, // relative path
) -> Result<(), OxenError> {
    // Recursively iterate through the tree, checking each file against the working repo
    // If the file is not in the working repo, restore it from the commit
    // If the file is in the working repo, but the hash does not match, overwrite the file in the working repo with the file from the commit
    // If the file is in the working repo, and the hash matches, do nothing

    match &node.node.dtype() {
        MerkleTreeNodeType::File => {
            let file_node = node.file().unwrap();
            let rel_path = path.join(&file_node.name);
            let full_path = repo.path.join(&rel_path);
            if !full_path.exists() {
                // File doesn't exist, restore it
                log::debug!("Restoring missing file: {:?}", rel_path);
                restore_file(repo, &file_node, &full_path)?;
            } else {
                // File exists, check if it needs to be updated
                let current_hash = util::hasher::hash_file_contents(&full_path)?;
                if current_hash != file_node.hash.to_string() {
                    log::debug!("Updating modified file: {:?}", rel_path);
                    restore_file(repo, &file_node, &full_path)?;
                }
            }
        }
        MerkleTreeNodeType::Dir => {
            // Recursively call for each file and directory
            let children = CommitMerkleTree::node_files_and_folders(node)?;
            let dir_node = node.dir().unwrap();
            let dir_path = path.join(dir_node.name);
            for child_node in children {
                r_restore_missing_or_modified_files(repo, &child_node, &dir_path)?;
            }
        }
        MerkleTreeNodeType::Commit => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = CommitMerkleTree::get_root_dir_from_commit(node)?;
            r_restore_missing_or_modified_files(repo, root_dir, path)?;
        }
        _ => {
            return Err(OxenError::basic_str(
                "Got an unexpected node type during checkout",
            ));
        }
    }
    Ok(())
}

pub fn restore_file(
    repo: &LocalRepository,
    file_node: &FileNode,
    dst_path: &Path, // absolute path
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path_from_hash(repo, file_node.hash.to_string());
    if !version_path.exists() {
        return Err(OxenError::basic_str(format!(
            "Source file not found in versions directory: {:?}",
            version_path
        )));
    }

    if let Some(parent) = dst_path.parent() {
        if !parent.exists() {
            util::fs::create_dir_all(parent)?;
        }
    }

    util::fs::copy(version_path, dst_path)?;

    let last_modified_seconds = file_node.last_modified_seconds;
    let last_modified_nanoseconds = file_node.last_modified_nanoseconds;
    let last_modified = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(last_modified_seconds as u64)
        + std::time::Duration::from_nanos(last_modified_nanoseconds as u64);
    filetime::set_file_mtime(
        dst_path,
        filetime::FileTime::from_system_time(last_modified),
    )?;

    Ok(())
}
