use indicatif::{ProgressBar, ProgressStyle};

use crate::core::v_latest::fetch;
use crate::core::v_latest::index::restore::{self, FileToRestore};
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Commit, CommitEntry, LocalRepository, MerkleHash, PartialNode};
use crate::repositories;
use crate::util;

use filetime::FileTime;
use std::collections::{HashMap, HashSet};
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

// Structs grouping related fields to reduce the number of arguments fed into the recursive functions

// files_to_restore: files present in the target tree but not the from tree
// cannot_overwrite_entries: files that would be restored, but are modified from the from_tree, and thus would erase work if overwritten
struct CheckoutResult {
    pub files_to_restore: Vec<FileToRestore>,
    pub cannot_overwrite_entries: Vec<PathBuf>,
}

impl CheckoutResult {
    pub fn new() -> Self {
        CheckoutResult {
            files_to_restore: vec![],
            cannot_overwrite_entries: vec![],
        }
    }
}

// seen_files: HashMap of MerkleHashes and PathBufs, removing the need to check files against the target tree in r_remove_if_not_in_target
// common_nodes: HashSet of the hashes of all the dirs and vnodes that are common between the trees, removing the need to look up dirs and vnodes in the recursive functions
struct CheckoutHashes {
    pub seen_hashes: HashSet<MerkleHash>,
    pub seen_paths: HashSet<PathBuf>,
    pub common_nodes: HashSet<MerkleHash>,
}

impl CheckoutHashes {
    // Not currently in use
    /*
    pub fn new() -> Self {
        CheckoutHashes {
            seen_hashes: HashSet::new(),
            seen_paths: HashSet::new(),
            common_nodes: HashSet::new(),
        }
    }
    */

    pub fn from_hashes(common_nodes: HashSet<MerkleHash>) -> Self {
        CheckoutHashes {
            seen_hashes: HashSet::new(),
            seen_paths: HashSet::new(),
            common_nodes,
        }
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
    to_commit: &Commit,
    subtree_paths: &[PathBuf],
    depth: i32,
) -> Result<(), OxenError> {
    for subtree_path in subtree_paths {
        let mut progress = CheckoutProgressBar::new(to_commit.id.clone());
        let mut target_hashes = HashSet::new();
        let target_root = if let Some(target_root) =
            CommitMerkleTree::from_path_depth_children_and_hashes(
                repo,
                to_commit,
                subtree_path.clone(),
                depth,
                &mut target_hashes,
            )? {
            target_root
        } else {
            log::error!("Cannot get subtree for commit: {}", to_commit);
            continue;
        };

        // Load in the target tree, collecting every dir and vnode hash for comparison with the from tree
        let mut shared_hashes = HashSet::new();
        let mut partial_nodes = HashMap::new();

        let maybe_from_commit = repositories::commits::head_commit_maybe(repo)?;

        let from_root = if maybe_from_commit.is_some() {
            log::debug!("from id: {:?}", maybe_from_commit.as_ref().unwrap().id);
            log::debug!("to id: {:?}", to_commit.id);
            CommitMerkleTree::root_with_unique_children(
                repo,
                maybe_from_commit.as_ref().unwrap(),
                &mut target_hashes,
                &mut shared_hashes,
                &mut partial_nodes,
            )
            .map_err(|e| {
                OxenError::basic_str(format!("Cannot get root node for base commit: {:?}", e))
            })?
        } else {
            log::warn!("head commit missing, might be a clone");
            None
        };

        let parent_path = subtree_path.parent().unwrap_or(Path::new(""));

        let mut results = CheckoutResult::new();
        let mut hashes = CheckoutHashes::from_hashes(shared_hashes);
        let version_store = repo.version_store()?;

        r_restore_missing_or_modified_files(
            repo,
            &target_root,
            parent_path,
            &mut results,
            &mut progress,
            &mut partial_nodes,
            &mut hashes,
            depth,
        )?;

        // If there are conflicts, return an error without restoring anything
        if !results.cannot_overwrite_entries.is_empty() {
            return Err(OxenError::cannot_overwrite_files(
                &results.cannot_overwrite_entries,
            ));
        }

        if from_root.is_some() {
            log::debug!("🔥 from node: {:?}", from_root);
            cleanup_removed_files(repo, &from_root.unwrap(), &mut progress, &mut hashes).await?;
        } else {
            log::debug!("head commit missing, no cleanup");
        }

        if repo.is_remote_mode() {
            for file_to_restore in results.files_to_restore {
                //let file_hash = format!("{}", &file_to_restore.file_node.hash());

                // In remote-mode repos, only restore files that are present in version store
                if version_store.version_exists(&file_to_restore.file_node.hash().to_string())? {
                    restore::restore_file(
                        repo,
                        &file_to_restore.file_node,
                        &file_to_restore.path,
                        &version_store,
                    )
                    .await?;
                }
            }
        } else {
            for file_to_restore in results.files_to_restore {
                restore::restore_file(
                    repo,
                    &file_to_restore.file_node,
                    &file_to_restore.path,
                    &version_store,
                )
                .await?;
            }
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

// Notes for future optimizations:
// If a dir or a vnode is shared between the trees, then all files under it will also be shared exactly
// However, shared file nodes may not always fall under the same dirs and vnodes between the trees
// Hence, it's necessary to traverse all unique paths in each tree at least once
pub async fn set_working_repo_to_commit(
    repo: &LocalRepository,
    to_commit: &Commit,
    maybe_from_commit: &Option<Commit>,
) -> Result<(), OxenError> {
    let mut progress = CheckoutProgressBar::new(to_commit.id.clone());

    // Load in the target tree, collecting every dir and vnode hash for comparison with the from tree
    let mut target_hashes = HashSet::new();
    let Some(target_tree) =
        CommitMerkleTree::root_with_children_and_hashes(repo, to_commit, &mut target_hashes)?
    else {
        return Err(OxenError::basic_str(
            "Cannot get root node for target commit",
        ));
    };

    // If the from tree exists, load in the nodes not found in the target tree
    // Also collects a 'PartialNode' of every file node unique to the from tree
    // This is used to determine missing or modified files in the recursive function
    let mut shared_hashes = HashSet::new();
    let mut partial_nodes = HashMap::new();
    let from_tree = if let Some(from_commit) = maybe_from_commit {
        if from_commit.id == to_commit.id {
            return Ok(());
        }

        log::debug!("from id: {:?}", from_commit.id);
        log::debug!("to id: {:?}", to_commit.id);
        CommitMerkleTree::root_with_unique_children(
            repo,
            from_commit,
            &mut target_hashes,
            &mut shared_hashes,
            &mut partial_nodes,
        )
        .map_err(|_| OxenError::basic_str("Cannot get root node for base commit"))?
    } else {
        None
    };

    let mut results = CheckoutResult::new();
    let mut hashes = CheckoutHashes::from_hashes(shared_hashes);
    let version_store = repo.version_store()?;

    log::debug!("restore_missing_or_modified_files");
    // Restore files present in the target commit
    r_restore_missing_or_modified_files(
        repo,
        &target_tree,
        Path::new(""),
        &mut results,
        &mut progress,
        &mut partial_nodes,
        &mut hashes,
        i32::MAX,
    )?;

    // If there are conflicts, return an error without restoring anything
    if !results.cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(
            &results.cannot_overwrite_entries,
        ));
    }

    // Cleanup files if checking out from another commit
    if maybe_from_commit.is_some() {
        log::debug!("Cleanup_removed_files");
        cleanup_removed_files(repo, &from_tree.unwrap(), &mut progress, &mut hashes).await?;
    }

    for file_to_restore in results.files_to_restore {
        restore::restore_file(
            repo,
            &file_to_restore.file_node,
            &file_to_restore.path,
            &version_store,
        )
        .await?;
    }

    Ok(())
}

// Only called if checking out from an existant commit

async fn cleanup_removed_files(
    repo: &LocalRepository,
    from_node: &MerkleTreeNode,
    progress: &mut CheckoutProgressBar,
    hashes: &mut CheckoutHashes,
) -> Result<(), OxenError> {
    // Compare the nodes in the from tree to the nodes in the target tree
    // If the file node is in the from tree, but not in the target tree, remove it

    let mut paths_to_remove: Vec<PathBuf> = vec![];
    let mut files_to_store: Vec<(MerkleHash, PathBuf)> = vec![];
    let mut cannot_overwrite_entries: Vec<PathBuf> = vec![];

    r_remove_if_not_in_target(
        repo,
        from_node,
        Path::new(""),
        &mut paths_to_remove,
        &mut files_to_store,
        &mut cannot_overwrite_entries,
        hashes,
    )?;

    if !cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(&cannot_overwrite_entries));
    }

    // If in remote mode, need to store committed paths before removal
    if repo.is_remote_mode() {
        let version_store = repo.version_store()?;
        for (hash, full_path) in files_to_store {
            version_store
                .store_version_from_path(&hash.to_string(), &full_path)
                .await?;
        }
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

// Note: If we're willing to duplicate the recursive logic, we could make synchronous version for regular repos
// And have the async only for remote-mode repos
fn r_remove_if_not_in_target(
    repo: &LocalRepository,
    from_node: &MerkleTreeNode,
    current_path: &Path,
    paths_to_remove: &mut Vec<PathBuf>,
    files_to_store: &mut Vec<(MerkleHash, PathBuf)>,
    cannot_overwrite_entries: &mut Vec<PathBuf>,
    hashes: &mut CheckoutHashes,
) -> Result<(), OxenError> {
    // Iterate through the from tree, removing files not present in the target tree
    match &from_node.node {
        EMerkleTreeNode::File(file_node) => {
            // Only consider files not seen while traversing the target tree
            if !hashes.seen_hashes.contains(&from_node.hash) {
                let file_path = current_path.join(file_node.name());
                let full_path = repo.path.join(&file_path);
                // Before staging for removal, verify the path exists, doesn't refer to a different file in the target tree, and isn't modified

                if full_path.exists() && !hashes.seen_paths.contains(&file_path) {
                    if util::fs::is_modified_from_node(&full_path, file_node)? {
                        cannot_overwrite_entries.push(file_path.clone());
                    } else {
                        // If in remote mode, save file to version store before removing
                        if repo.is_remote_mode() {
                            files_to_store.push((from_node.hash, full_path.clone()))
                        }

                        paths_to_remove.push(full_path.clone());
                    }
                }
            }
        }

        EMerkleTreeNode::Directory(dir_node) => {
            let dir_path = current_path.join(dir_node.name());
            if hashes.common_nodes.contains(&from_node.hash) {
                return Ok(());
            };

            let children = {
                // Get vnodes for the from dir node
                let dir_vnodes = &from_node.children;

                // Only iterate through vnodes not shared between the trees
                let mut unique_nodes = Vec::new();
                for vnode in dir_vnodes {
                    if !hashes.common_nodes.contains(&vnode.hash) {
                        unique_nodes.extend(vnode.children.iter().cloned());
                    }
                }

                unique_nodes
            };

            for child in &children {
                r_remove_if_not_in_target(
                    repo,
                    child,
                    &dir_path,
                    paths_to_remove,
                    files_to_store,
                    cannot_overwrite_entries,
                    hashes,
                )?;
            }
            log::debug!(
                "r_remove_if_not_in_target checked {:?} paths",
                children.len()
            );

            // Remove directory if it's empty
            let full_dir_path = repo.path.join(&dir_path);
            if full_dir_path.exists() {
                paths_to_remove.push(full_dir_path.clone());
            }
        }
        EMerkleTreeNode::Commit(_) => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = repositories::tree::get_root_dir(from_node)?;
            r_remove_if_not_in_target(
                repo,
                root_dir,
                current_path,
                paths_to_remove,
                files_to_store,
                cannot_overwrite_entries,
                hashes,
            )?;
        }
        _ => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn r_restore_missing_or_modified_files(
    repo: &LocalRepository,
    target_node: &MerkleTreeNode,
    path: &Path, // relative path
    results: &mut CheckoutResult,
    progress: &mut CheckoutProgressBar,
    partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    hashes: &mut CheckoutHashes,
    depth: i32,
) -> Result<(), OxenError> {
    // Recursively iterate through the tree, checking each file against the working repo
    // If the file is not in the working repo, restore it from the commit
    // If the file is in the working repo, but the hash does not match, overwrite the file in the working repo with the file from the commit
    // If the file is in the working repo, and the hash matches, do nothing
    if depth < 0 {
        return Ok(());
    }

    match &target_node.node {
        EMerkleTreeNode::File(file_node) => {
            let file_path = path.join(file_node.name());
            let full_path = repo.path.join(&file_path);

            // Collect hash and path for matching in r_remove_if_not_in_target
            hashes.seen_hashes.insert(target_node.hash);
            hashes.seen_paths.insert(file_path.clone());
            if !full_path.exists() {
                // File doesn't exist, restore it
                log::debug!("Restoring missing file: {:?}", file_path);
                results.files_to_restore.push(FileToRestore {
                    file_node: file_node.clone(),
                    path: file_path.clone(),
                });

                progress.increment_restored();
            } else {
                // File exists, check whether it matches the target node or a from node
                // First check last modified times
                let meta = util::fs::metadata(&full_path)?;
                let last_modified = Some(FileTime::from_last_modification_time(&meta));

                // If last_modified matches the target, do nothing
                let target_last_modified = util::fs::last_modified_time(
                    file_node.last_modified_seconds(),
                    file_node.last_modified_nanoseconds(),
                );
                if last_modified == Some(target_last_modified) {
                    return Ok(());
                }

                // If last_modified matches a corresponding from_node, stage it to be restored
                let (from_node, from_last_modified) =
                    if let Some(from_node) = partial_nodes.get(&file_path) {
                        (Some(from_node), Some(from_node.last_modified))
                    } else {
                        (None, None)
                    };

                if last_modified == from_last_modified {
                    log::debug!("Updating modified file: {:?}", file_path);
                    results.files_to_restore.push(FileToRestore {
                        file_node: file_node.clone(),
                        path: file_path.clone(),
                    });
                    progress.increment_modified();
                    return Ok(());
                }

                // Otherwise, check hashes
                let working_hash = Some(util::hasher::get_hash_given_metadata(&full_path, &meta)?);
                //log::debug!("Working hash: {:?}", working_hash);
                let target_hash = target_node.hash.to_u128();
                //log::debug!("Target hash: {:?}", MerkleHash::new(target_hash));
                if working_hash == Some(target_hash) {
                    return Ok(());
                }

                let from_hash = from_node.map(|from_node| from_node.hash.to_u128());
                //log::debug!("from hash: {from_hash:?}");

                if working_hash == from_hash {
                    log::debug!("Updating modified file: {:?}", file_path);

                    results.files_to_restore.push(FileToRestore {
                        file_node: file_node.clone(),
                        path: file_path.clone(),
                    });
                    progress.increment_modified();
                    return Ok(());
                }

                // If neither hash matches, the file is modified in the working directory and cannot be overwritten
                results.cannot_overwrite_entries.push(file_path.clone());
                progress.increment_modified();
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            let dir_path = path.join(dir_node.name());
            // Early exit if the directory is the same in the from and target trees
            if hashes.common_nodes.contains(&target_node.hash) {
                return Ok(());
            };

            let children = {
                // Get vnodes for the from dir node
                let dir_vnodes = &target_node.children;

                // Only iterate through vnodes not shared between the trees
                let mut unique_nodes = Vec::new();
                for vnode in dir_vnodes {
                    if !hashes.common_nodes.contains(&vnode.hash) {
                        unique_nodes.extend(vnode.children.iter().cloned());
                    }
                }

                unique_nodes
            };

            for child_node in children {
                r_restore_missing_or_modified_files(
                    repo,
                    &child_node,
                    &dir_path,
                    results,
                    progress,
                    partial_nodes,
                    hashes,
                    depth - 1,
                )?;
            }
        }
        EMerkleTreeNode::Commit(_) => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = repositories::tree::get_root_dir(target_node)?;
            r_restore_missing_or_modified_files(
                repo,
                root_dir,
                path,
                results,
                progress,
                partial_nodes,
                hashes,
                depth - 1,
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
