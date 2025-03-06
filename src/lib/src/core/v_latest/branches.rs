use indicatif::{ProgressBar, ProgressStyle};

use crate::core::v_latest::index::restore::{self, FileToRestore};
use crate::core::v_latest::{fetch, index};
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::repositories;
use crate::util;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

struct CheckoutProgressBar {
    revision: String,
    progress: ProgressBar,
    num_restored: usize,
    num_modified: usize,
    num_removed: usize,
}

impl CheckoutProgressBar {
    pub fn new(revision: String) -> Self {
        let progress = ProgressBar::new_spinner();
        progress.set_style(ProgressStyle::default_spinner());
        progress.enable_steady_tick(Duration::from_millis(100));

        Self {
            revision,
            progress,
            num_restored: 0,
            num_modified: 0,
            num_removed: 0,
        }
    }

    pub fn increment_restored(&mut self) {
        self.num_restored += 1;
        self.update_message();
    }

    pub fn increment_modified(&mut self) {
        self.num_modified += 1;
        self.update_message();
    }

    pub fn increment_removed(&mut self) {
        self.num_removed += 1;
        self.update_message();
    }

    fn update_message(&mut self) {
        self.progress.set_message(format!(
            "🐂 checkout '{}' restored {}, modified {}, removed {}",
            self.revision, self.num_restored, self.num_modified, self.num_removed
        ));
    }
}

pub fn list_entry_versions_for_commit(
    repo: &LocalRepository,
    commit_id: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    log::debug!(
        "list_entry_versions_for_commit {} for file: {:?}",
        commit_id,
        path
    );
    let mut branch_commits = repositories::commits::list_from(repo, commit_id)?;

    // Sort on timestamp oldest to newest
    branch_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut result: Vec<(Commit, CommitEntry)> = Vec::new();
    let mut seen_hashes: HashSet<String> = HashSet::new();

    for commit in branch_commits {
        log::debug!("list_entry_versions_for_commit {}", commit);
        let node = repositories::tree::get_node_by_path(repo, &commit, path)?;

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

pub async fn checkout(
    repo: &LocalRepository,
    branch_name: &str,
    from_commit: &Option<Commit>,
) -> Result<(), OxenError> {
    log::debug!("checkout {branch_name}");
    let branch = repositories::branches::get_by_name(repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;

    let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&branch.commit_id))?;

    checkout_commit(repo, &commit, from_commit).await?;

    Ok(())
}

pub async fn checkout_subtrees(
    repo: &LocalRepository,
    from_commit: &Commit,
    subtree_paths: &[PathBuf],
    depth: i32,
) -> Result<(), OxenError> {
    for subtree_path in subtree_paths {
        let Some(target_root) = repositories::tree::get_subtree_by_depth(
            repo,
            from_commit,
            &Some(subtree_path.to_path_buf()),
            &Some(depth),
        )?
        else {
            log::error!("Cannot get subtree for commit: {}", from_commit);
            continue;
        };

        let mut progress = CheckoutProgressBar::new(from_commit.id.clone());
        let from_tree = None;
        let parent_path = subtree_path.parent().unwrap_or(Path::new(""));
        let mut files_to_restore: Vec<FileToRestore> = vec![];
        let mut cannot_overwrite_entries: Vec<PathBuf> = vec![];
        r_restore_missing_or_modified_files(
            repo,
            &target_root,
            &from_tree,
            parent_path,
            &mut files_to_restore,
            &mut cannot_overwrite_entries,
            &mut progress,
        )?;

        if !cannot_overwrite_entries.is_empty() {
            return Err(OxenError::cannot_overwrite_files(&cannot_overwrite_entries));
        }

        let version_store = repo.version_store()?;
        for file_to_restore in files_to_restore {
            restore::restore_file(
                repo,
                &file_to_restore.file_node,
                &file_to_restore.path,
                &version_store,
            )?;
        }
    }

    Ok(())
}

pub async fn checkout_commit(
    repo: &LocalRepository,
    to_commit: &Commit,
    from_commit: &Option<Commit>,
) -> Result<(), OxenError> {
    log::debug!("checkout_commit to {} from {:?}", to_commit, from_commit);

    if let Some(from_commit) = from_commit {
        if from_commit.id == to_commit.id {
            return Ok(());
        }
    }

    // Fetch entries if needed
    fetch::maybe_fetch_missing_entries(repo, to_commit).await?;

    // Set working repo to commit
    set_working_repo_to_commit(repo, to_commit, from_commit).await?;

    Ok(())
}

pub async fn set_working_repo_to_commit(
    repo: &LocalRepository,
    to_commit: &Commit,
    maybe_from_commit: &Option<Commit>,
) -> Result<(), OxenError> {
    let mut progress = CheckoutProgressBar::new(to_commit.id.clone());

    let Some(target_node) = repositories::tree::get_root_with_children(repo, to_commit)? else {
        return Err(OxenError::basic_str(
            "Cannot get root node for target commit",
        ));
    };
    let from_node = if let Some(from_commit) = maybe_from_commit {
        if from_commit.id == to_commit.id {
            return Ok(());
        }

        // Only cleanup removed files if we are checking out from an existing tree
        let Some(from_node) = repositories::tree::get_root_with_children(repo, from_commit)? else {
            return Err(OxenError::basic_str("Cannot get root node for base commit"));
        };
        cleanup_removed_files(repo, &target_node, &from_node, &mut progress)?;
        Some(from_node)
    } else {
        None
    };

    // You may be thinking, why do we not do this in one pass?
    // It's because when removing files, we are iterating over the from tree
    // And when we restore files, we are iterating over the target tree

    // If we did it in one pass, we would not know if we should remove the file
    // or restore it.
    let mut files_to_restore: Vec<FileToRestore> = vec![];
    let mut cannot_overwrite_entries: Vec<PathBuf> = vec![];
    r_restore_missing_or_modified_files(
        repo,
        &target_node,
        &from_node,
        Path::new(""),
        &mut files_to_restore,
        &mut cannot_overwrite_entries,
        &mut progress,
    )?;

    // If there are conflicts, return an error without restoring anything
    if !cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(&cannot_overwrite_entries));
    }

    let version_store = repo.version_store()?;
    for file_to_restore in files_to_restore {
        restore::restore_file(
            repo,
            &file_to_restore.file_node,
            &file_to_restore.path,
            &version_store,
        )?;
    }

    Ok(())
}

fn cleanup_removed_files(
    repo: &LocalRepository,
    target_node: &MerkleTreeNode,
    from_node: &MerkleTreeNode,
    progress: &mut CheckoutProgressBar,
) -> Result<(), OxenError> {
    // Compare the nodes in the from tree to the nodes in the target tree
    // If the file node is in the from tree, but not in the target tree, remove it
    let from_root_dir_node = repositories::tree::get_root_dir(from_node)?;
    log::debug!("cleanup_removed_files from_commit {}", from_root_dir_node);

    let mut paths_to_remove: Vec<PathBuf> = vec![];
    let mut cannot_overwrite_entries: Vec<PathBuf> = vec![];
    r_remove_if_not_in_target(
        repo,
        from_root_dir_node,
        from_node,
        target_node,
        Path::new(""),
        &mut paths_to_remove,
        &mut cannot_overwrite_entries,
    )?;

    if !cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(&cannot_overwrite_entries));
    }

    for full_path in paths_to_remove {
        // If it's a directory, and it's empty, remove it
        if full_path.is_dir() && full_path.read_dir()?.next().is_none() {
            log::debug!("Removing dir: {:?}", full_path);
            util::fs::remove_dir_all(&full_path)?;
        } else if full_path.is_file() {
            log::debug!("Removing file: {:?}", full_path);
            util::fs::remove_file(&full_path)?;
        }
        progress.increment_removed();
    }

    Ok(())
}

fn r_remove_if_not_in_target(
    repo: &LocalRepository,
    head_node: &MerkleTreeNode,
    from_tree_root: &MerkleTreeNode,
    target_tree_root: &MerkleTreeNode,
    current_path: &Path,
    paths_to_remove: &mut Vec<PathBuf>,
    cannot_overwrite_entries: &mut Vec<PathBuf>,
) -> Result<(), OxenError> {
    match &head_node.node {
        EMerkleTreeNode::File(file_node) => {
            let file_path = current_path.join(file_node.name());
            let target_node = target_tree_root.get_by_path(&file_path)?;
            let from_node = from_tree_root.get_by_path(&file_path)?;

            if target_node.is_none() && from_node.is_some() {
                let full_path = repo.path.join(&file_path);
                if full_path.exists() {
                    // Verify that the file is not in a modified state
                    let current_hash = util::hasher::hash_file_contents(&full_path)?;
                    if current_hash != head_node.node.hash().to_string() {
                        cannot_overwrite_entries.push(file_path.clone());
                    } else {
                        log::debug!("Removing file: {:?}", full_path);
                        paths_to_remove.push(full_path.clone());
                    }
                }
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            // TODO: can we also check if the directory is in the target tree,
            // and potentially remove the whole directory?
            let dir_path = current_path.join(dir_node.name());

            // Check if the directory is the same in the from and target trees
            // If it is, we don't need to do anything
            if let Some(target_node) = target_tree_root.get_by_path(&dir_path)? {
                if target_node.node.hash() == dir_node.hash() {
                    return Ok(());
                }
            }

            let from_dir = from_tree_root
                .get_by_path(&dir_path)?
                .ok_or(OxenError::basic_str(format!(
                    "Could not find directory in from tree: {:?}",
                    dir_path
                )))?;
            let from_children = repositories::tree::list_files_and_folders(&from_dir)?;
            for child in from_children {
                // If the hashes match, we don't need to check if we need to remove any children
                // because the subdirectory will be the same content-wise
                r_remove_if_not_in_target(
                    repo,
                    &child,
                    from_tree_root,
                    target_tree_root,
                    &dir_path,
                    paths_to_remove,
                    cannot_overwrite_entries,
                )?;
            }

            // Remove directory if it's empty
            let full_dir_path = repo.path.join(&dir_path);
            if full_dir_path.exists() {
                paths_to_remove.push(full_dir_path.clone());
            }
        }
        _ => {}
    }
    Ok(())
}

fn r_restore_missing_or_modified_files(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    from_tree: &Option<MerkleTreeNode>,
    path: &Path, // relative path
    files_to_restore: &mut Vec<FileToRestore>,
    cannot_overwrite_entries: &mut Vec<PathBuf>,
    progress: &mut CheckoutProgressBar,
) -> Result<(), OxenError> {
    // Recursively iterate through the tree, checking each file against the working repo
    // If the file is not in the working repo, restore it from the commit
    // If the file is in the working repo, but the hash does not match, overwrite the file in the working repo with the file from the commit
    // If the file is in the working repo, and the hash matches, do nothing

    match &node.node {
        EMerkleTreeNode::File(file_node) => {
            let rel_path = path.join(file_node.name());
            let full_path = repo.path.join(&rel_path);
            if !full_path.exists() {
                // File doesn't exist, restore it
                log::debug!("Restoring missing file: {:?}", rel_path);
                if index::restore::should_restore_file(repo, None, file_node, &rel_path)? {
                    files_to_restore.push(FileToRestore {
                        file_node: file_node.clone(),
                        path: rel_path.clone(),
                    });
                } else {
                    cannot_overwrite_entries.push(rel_path.clone());
                }
                progress.increment_restored();
            } else {
                // File exists, check if it needs to be updated
                let current_hash = util::hasher::hash_file_contents(&full_path)?;
                if current_hash != file_node.hash().to_string() {
                    let mut from_node: Option<FileNode> = None;
                    if let Some(from_tree) = from_tree {
                        if let Some(node_from_tree) = from_tree.get_by_path(&rel_path)? {
                            if let EMerkleTreeNode::File(file_node) = &node_from_tree.node {
                                from_node = Some(file_node.clone());
                            }
                        }
                    }

                    if index::restore::should_restore_file(repo, from_node, file_node, &rel_path)? {
                        log::debug!("Updating modified file: {:?}", rel_path);
                        files_to_restore.push(FileToRestore {
                            file_node: file_node.clone(),
                            path: rel_path.clone(),
                        });
                    } else {
                        cannot_overwrite_entries.push(rel_path.clone());
                    }
                    progress.increment_modified();
                }
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            // Early exit if the directory is the same in the from and target trees
            if let Some(from_tree) = from_tree {
                if let Some(from_node) = from_tree.get_by_path(path)? {
                    if from_node.node.hash() == dir_node.hash() {
                        log::debug!("r_restore_missing_or_modified_files path {:?} is the same as from_tree", path);
                        return Ok(());
                    }
                }
            }

            // Recursively call for each file and directory
            let children = repositories::tree::list_files_and_folders(node)?;
            let dir_path = path.join(dir_node.name());
            for child_node in children {
                r_restore_missing_or_modified_files(
                    repo,
                    &child_node,
                    from_tree,
                    &dir_path,
                    files_to_restore,
                    cannot_overwrite_entries,
                    progress,
                )?;
            }
        }
        EMerkleTreeNode::Commit(_) => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = repositories::tree::get_root_dir(node)?;
            r_restore_missing_or_modified_files(
                repo,
                root_dir,
                from_tree,
                path,
                files_to_restore,
                cannot_overwrite_entries,
                progress,
            )?;
        }
        _ => {
            return Err(OxenError::basic_str(
                "Got an unexpected node type during checkout",
            ));
        }
    }
    Ok(())
}
