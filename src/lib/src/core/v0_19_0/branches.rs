use indicatif::{ProgressBar, ProgressStyle};

use crate::core::v0_19_0::fetch;
use crate::core::v0_19_0::index::commit_merkle_tree::CommitMerkleTree;
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
    local_repo: &LocalRepository,
    commit_id: &str,
    path: &Path,
) -> Result<Vec<(Commit, CommitEntry)>, OxenError> {
    log::debug!(
        "list_entry_versions_for_commit {} for file: {:?}",
        commit_id,
        path
    );
    let mut branch_commits = repositories::commits::list_from(local_repo, commit_id)?;

    // Sort on timestamp oldest to newest
    branch_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut result: Vec<(Commit, CommitEntry)> = Vec::new();
    let mut seen_hashes: HashSet<String> = HashSet::new();

    for commit in branch_commits {
        log::debug!("list_entry_versions_for_commit {}", commit);
        let tree = repositories::tree::get_by_commit(local_repo, &commit)?;
        let node = tree.get_by_path(path)?;
        // tree.print();

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
        let target_tree = repositories::tree::get_subtree_by_depth(
            repo,
            from_commit,
            &Some(subtree_path.to_path_buf()),
            &Some(depth),
        )?;

        let mut progress = CheckoutProgressBar::new(from_commit.id.clone());
        let from_tree = None;
        let parent_path = subtree_path.parent().unwrap_or(Path::new(""));
        r_restore_missing_or_modified_files(
            repo,
            &target_tree.root,
            &from_tree,
            parent_path,
            &mut progress,
        )?;
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

    log::debug!(
        "set_working_repo_to_commit to_commit {} from_commit {:?}",
        to_commit,
        maybe_from_commit
    );
    let target_tree = CommitMerkleTree::from_commit(repo, to_commit)?;
    let from_tree = if let Some(from_commit) = maybe_from_commit {
        if from_commit.id == to_commit.id {
            log::debug!(
                "set_working_repo_to_commit, do nothing... to_commit == from_commit {}",
                to_commit
            );
            return Ok(());
        }

        // Only cleanup removed files if we are checking out from an existing tree
        let from_tree = CommitMerkleTree::from_commit(repo, from_commit)?;
        cleanup_removed_files(repo, &target_tree, &from_tree, &mut progress)?;
        Some(from_tree)
    } else {
        None
    };

    // You may be thinking, why do we not do this in one pass?
    // It's because when removing files, we are iterating over the from tree
    // And when we restore files, we are iterating over the target tree

    // If we did it in one pass, we would not know if we should remove the file
    // or restore it.
    r_restore_missing_or_modified_files(
        repo,
        &target_tree.root,
        &from_tree,
        Path::new(""),
        &mut progress,
    )?;

    Ok(())
}

fn cleanup_removed_files(
    repo: &LocalRepository,
    target_tree: &CommitMerkleTree,
    from_tree: &CommitMerkleTree,
    progress: &mut CheckoutProgressBar,
) -> Result<(), OxenError> {
    // Compare the nodes in the from tree to the nodes in the target tree
    // If the file node is in the from tree, but not in the target tree, remove it
    let from_root_dir_node = CommitMerkleTree::get_root_dir_from_commit(&from_tree.root)?;
    log::debug!("cleanup_removed_files from_commit {}", from_root_dir_node);

    r_remove_if_not_in_target(
        repo,
        from_root_dir_node,
        from_tree,
        target_tree,
        Path::new(""),
        progress,
    )?;

    Ok(())
}

fn r_remove_if_not_in_target(
    repo: &LocalRepository,
    head_node: &MerkleTreeNode,
    from_tree: &CommitMerkleTree,
    target_tree: &CommitMerkleTree,
    current_path: &Path,
    progress: &mut CheckoutProgressBar,
) -> Result<(), OxenError> {
    log::debug!(
        "r_remove_if_not_in_target current_path: {:?} head_node: {}",
        current_path,
        head_node
    );
    match &head_node.node {
        EMerkleTreeNode::File(file_node) => {
            let file_path = current_path.join(&file_node.name);
            log::debug!(
                "r_remove_if_not_in_target looking up file_path {:?} from current_path {:?}",
                file_path,
                current_path
            );
            let target_node = target_tree.get_by_path(&file_path)?;
            let from_node = from_tree.get_by_path(&file_path)?;
            log::debug!(
                "r_remove_if_not_in_target target_node.is_none() {} from_node.is_some() {}",
                target_node.is_none(),
                from_node.is_some()
            );

            if target_node.is_none() && from_node.is_some() {
                log::debug!("r_remove_if_not_in_target removing file: {:?}", file_path);
                let full_path = repo.path.join(&file_path);
                if full_path.exists() {
                    log::debug!("Removing file: {:?}", file_path);
                    util::fs::remove_file(&full_path)?;
                    progress.increment_removed();
                }
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            // TODO: can we also check if the directory is in the target tree,
            // and potentially remove the whole directory?
            let dir_path = current_path.join(&dir_node.name);

            // Check if the directory is the same in the from and target trees
            // If it is, we don't need to do anything
            if let Some(target_node) = target_tree.get_by_path(&dir_path)? {
                log::debug!(
                    "r_remove_if_not_in_target dir_path {:?} from_node {} === target_node {}",
                    dir_path,
                    target_node,
                    dir_node
                );
                if target_node.node.hash() == dir_node.hash {
                    log::debug!(
                        "r_remove_if_not_in_target dir_path {:?} is the same as target_tree",
                        dir_path
                    );
                    return Ok(());
                }
            }

            let from_children = from_tree.files_and_folders(&dir_path)?;
            for child in from_children {
                // If the hashes match, we don't need to check if we need to remove any children
                // because the subdirectory will be the same content-wise
                log::debug!(
                    "r_remove_if_not_in_target dir_path {:?} child {} dir_node {}",
                    dir_path,
                    dir_node,
                    child,
                );
                r_remove_if_not_in_target(
                    repo,
                    &child,
                    from_tree,
                    target_tree,
                    &dir_path,
                    progress,
                )?;
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
    from_tree: &Option<CommitMerkleTree>,
    path: &Path, // relative path
    progress: &mut CheckoutProgressBar,
) -> Result<(), OxenError> {
    // Recursively iterate through the tree, checking each file against the working repo
    // If the file is not in the working repo, restore it from the commit
    // If the file is in the working repo, but the hash does not match, overwrite the file in the working repo with the file from the commit
    // If the file is in the working repo, and the hash matches, do nothing

    match &node.node {
        EMerkleTreeNode::File(file_node) => {
            let rel_path = path.join(&file_node.name);
            let full_path = repo.path.join(&rel_path);
            if !full_path.exists() {
                // File doesn't exist, restore it
                log::debug!("Restoring missing file: {:?}", rel_path);
                restore_file(repo, file_node, &full_path)?;
                progress.increment_restored();
            } else {
                // File exists, check if it needs to be updated
                let current_hash = util::hasher::hash_file_contents(&full_path)?;
                if current_hash != file_node.hash.to_string() {
                    log::debug!("Updating modified file: {:?}", rel_path);
                    restore_file(repo, file_node, &full_path)?;
                    progress.increment_modified();
                }
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            // Early exit if the directory is the same in the from and target trees
            if let Some(from_tree) = from_tree {
                if let Some(from_node) = from_tree.get_by_path(path)? {
                    if from_node.node.hash() == dir_node.hash {
                        log::debug!("r_restore_missing_or_modified_files path {:?} is the same as from_tree", path);
                        return Ok(());
                    }
                }
            }

            // Recursively call for each file and directory
            let children = CommitMerkleTree::node_files_and_folders(node)?;
            let dir_path = path.join(&dir_node.name);
            for child_node in children {
                r_restore_missing_or_modified_files(
                    repo,
                    &child_node,
                    from_tree,
                    &dir_path,
                    progress,
                )?;
            }
        }
        EMerkleTreeNode::Commit(_) => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = CommitMerkleTree::get_root_dir_from_commit(node)?;
            r_restore_missing_or_modified_files(repo, root_dir, from_tree, path, progress)?;
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
            "Restoring file, source file {} not found in versions directory: {:?} when restoring to {:?}",
            file_node.name,
            version_path,
            dst_path
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
