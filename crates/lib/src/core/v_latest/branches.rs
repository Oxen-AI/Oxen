use indicatif::{ProgressBar, ProgressStyle};

use crate::core::v_latest::fetch;
use crate::core::v_latest::index::restore::{self, FileToRestore};
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Commit, CommitEntry, LocalRepository, MerkleHash, PartialNode};
use crate::repositories;
use crate::util;

use filetime::FileTime;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// What to do when a checkout would overwrite a working-tree file that matches neither the source
/// nor the target commit (i.e., the user has uncommitted changes or a crashed merge left the file
/// in an intermediate state).
#[derive(Debug, Clone, Copy)]
pub enum OnConflict {
    /// Return `OxenError::cannot_overwrite_files` and leave the working tree untouched.
    Abort,
    /// Discard the working-tree state and restore the target's version from the version store.
    /// Only pass this when the user has explicitly asked to discard their working tree
    /// (e.g. `oxen merge --abort`).
    Overwrite,
}

impl OnConflict {
    pub fn is_abort(self) -> bool {
        matches!(self, Self::Abort)
    }
}

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

struct CheckoutResult {
    /// files_to_restore: files present in the target tree but not the from tree
    pub files_to_restore: Vec<FileToRestore>,
    /// cannot_overwrite_entries: files that would be restored, but are modified from the from_tree, and thus would erase work if overwritten
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

struct CheckoutHashes {
    /// seen_paths: HashSet of PathBufs seen while traversing the target tree, used in r_remove_if_not_in_target to identify files not in the target
    pub seen_paths: HashSet<PathBuf>,
    /// common_nodes: HashSet of the hashes of all the dirs and vnodes that are common between the trees, removing the need to look up dirs and vnodes in the recursive functions
    pub common_nodes: HashSet<MerkleHash>,
}

impl CheckoutHashes {
    pub fn from_hashes(common_nodes: HashSet<MerkleHash>) -> Self {
        CheckoutHashes {
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
    log::debug!("list_entry_versions_for_commit {commit_id} for file: {path:?}");
    let mut branch_commits = repositories::commits::list_from(repo, commit_id)?;

    // Sort on timestamp oldest to newest
    branch_commits.sort_by_key(|a| a.timestamp);

    let mut result: Vec<(Commit, CommitEntry)> = Vec::new();
    let mut seen_hashes: HashSet<String> = HashSet::new();

    for commit in branch_commits {
        log::debug!("list_entry_versions_for_commit {commit}");
        let node = repositories::tree::get_node_by_path(repo, &commit, path)?;

        if let Some(node) = node {
            if !seen_hashes.contains(&node.node.hash().to_string()) {
                log::debug!("list_entry_versions_for_commit adding {commit} -> {node}");
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
                log::debug!("list_entry_versions_for_commit already seen {node}");
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
    let branch = repositories::branches::get_by_name(repo, branch_name)?;

    let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?
        .ok_or_else(|| OxenError::commit_id_does_not_exist(&branch.commit_id))?;

    checkout_commit(repo, &commit, from_commit, OnConflict::Abort).await?;

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
            repositories::tree::get_subtree_by_depth_with_unique_children(
                repo,
                to_commit,
                subtree_path.clone(),
                None,
                Some(&mut target_hashes),
                None,
                depth,
            )? {
            target_root
        } else {
            log::error!("Cannot get subtree for commit: {to_commit}");
            continue;
        };

        // Load in the target tree, collecting every dir and vnode hash for comparison with the from tree
        let mut shared_hashes = HashSet::new();
        let mut partial_nodes = HashMap::new();

        let maybe_from_commit = repositories::commits::head_commit_maybe(repo)?;

        let from_root = if let Some(from_commit) = &maybe_from_commit {
            log::debug!("from id: {:?}", from_commit.id);
            log::debug!("to id: {:?}", to_commit.id);
            repositories::tree::get_root_with_children_and_partial_nodes(
                repo,
                from_commit,
                Some(&target_hashes),
                None,
                Some(&mut shared_hashes),
                &mut partial_nodes,
            )
            .map_err(|e| {
                OxenError::basic_str(format!("Cannot get root node for base commit: {e:?}"))
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
            OnConflict::Abort,
        )?;

        // If there are conflicts, return an error without restoring anything
        if !results.cannot_overwrite_entries.is_empty() {
            return Err(OxenError::cannot_overwrite_files(
                &results.cannot_overwrite_entries,
            ));
        }

        if let Some(root) = from_root {
            log::debug!("Cleanup_removed_files");
            cleanup_removed_files(repo, &root, &mut progress, &hashes, OnConflict::Abort).await?;
        } else {
            log::debug!("head commit missing, no cleanup");
        }

        if repo.is_remote_mode() {
            for file_to_restore in results.files_to_restore {
                log::debug!("file_to_restore: {:?}", file_to_restore.file_node);
                // In remote-mode repos, only restore files that are present in version store
                let file_hash = format!("{}", &file_to_restore.file_node.hash());
                if version_store.version_exists(&file_hash).await? {
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
    on_conflict: OnConflict,
) -> Result<(), OxenError> {
    log::debug!("checkout_commit to {to_commit} from {from_commit:?} on_conflict={on_conflict:?}");

    if let Some(from_commit) = from_commit
        && from_commit.id == to_commit.id
    {
        return Ok(());
    }

    // Fetch entries if needed
    fetch::maybe_fetch_missing_entries(repo, to_commit).await?;

    // Set working repo to commit
    set_working_repo_to_commit(repo, to_commit, from_commit, on_conflict).await?;

    Ok(())
}

// Notes for future optimizations:
// If a dir or a vnode is shared between the trees, then all files under it will also be shared exactly
// However, shared file nodes may not always fall under the same dirs and vnodes between the trees
// Hence, it's necessary to traverse all unique paths in each tree at least once
/// Bring the working tree into line with `to_commit`, optionally using `maybe_from_commit` as a
/// hint about the current on-disk state to skip unchanged files. `on_conflict` decides whether to
/// abort or overwrite when the working tree has diverged from both commits (see [`OnConflict`]).
pub async fn set_working_repo_to_commit(
    repo: &LocalRepository,
    to_commit: &Commit,
    maybe_from_commit: &Option<Commit>,
    on_conflict: OnConflict,
) -> Result<(), OxenError> {
    let mut progress = CheckoutProgressBar::new(to_commit.id.clone());

    // Load in the target tree, collecting every dir and vnode hash for comparison with the from tree
    let mut target_hashes = HashSet::new();
    let Some(target_tree) = repositories::tree::get_root_with_children_and_node_hashes(
        repo,
        to_commit,
        None,
        Some(&mut target_hashes),
        None,
    )?
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
        repositories::tree::get_root_with_children_and_partial_nodes(
            repo,
            from_commit,
            Some(&target_hashes),
            None,
            Some(&mut shared_hashes),
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
        on_conflict,
    )?;

    // If there are conflicts, return an error without restoring anything
    if !results.cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(
            &results.cannot_overwrite_entries,
        ));
    }

    // Cleanup files if checking out fr om another commit
    if let Some(from_tree) = from_tree {
        log::debug!("Cleanup_removed_files");
        cleanup_removed_files(repo, &from_tree, &mut progress, &hashes, on_conflict).await?;
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
    hashes: &CheckoutHashes,
    on_conflict: OnConflict,
) -> Result<(), OxenError> {
    let candidates = walk_from_tree(repo, from_node, hashes, on_conflict).await?;

    if !candidates.cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(
            &candidates.cannot_overwrite_entries,
        ));
    }

    // If in remote mode, need to store committed paths before removal
    if repo.is_remote_mode() {
        let version_store = repo.version_store()?;
        for (hash, full_path) in candidates.files_to_store {
            log::debug!("Storing hash {hash:?} and path {full_path:?}");
            let file = tokio::fs::File::open(&full_path).await?;
            let size = file.metadata().await?.len();
            let reader = tokio::io::BufReader::new(file);
            version_store
                .store_version_from_reader(&hash.to_string(), Box::new(reader), size)
                .await?;
        }
    }

    for full_path in candidates.paths_to_remove {
        // If it's a directory, and it's empty, remove it
        if full_path.is_dir() && full_path.read_dir()?.next().is_none() {
            log::debug!("Removing dir: {full_path:?}");
            util::fs::remove_dir_all(&full_path)?;
        } else if full_path.is_file() {
            log::debug!("Removing file: {full_path:?}");
            util::fs::remove_file(&full_path)?;
        }
        progress.increment_removed();
    }

    Ok(())
}

/// Files and directories the cleanup pass might remove, plus blockers it found.
#[derive(Default)]
struct CleanupCandidates {
    /// Paths to remove, in post-order (children before their parent dir) so that by the
    /// time we get to a directory entry its files have already been removed and the
    /// emptiness check in `cleanup_removed_files` succeeds.
    paths_to_remove: Vec<PathBuf>,
    /// (hash, full_path) pairs to store in the version store before removal — only
    /// populated in remote-mode repos.
    files_to_store: Vec<(MerkleHash, PathBuf)>,
    /// Files in HEAD that don't appear in the target tree but have local modifications;
    /// `OnConflict::Abort` upgrades these to a `cannot_overwrite_files` error.
    cannot_overwrite_entries: Vec<PathBuf>,
}

/// Stack item for the iterative depth-first search in `walk_from_tree`. `Visit` is the
/// usual "process this node next"; `FinalizeDir` runs after a directory's subtree is
/// fully processed so we can append the directory itself to `paths_to_remove` in
/// post-order. Pushed BEFORE the directory's children so the LIFO `pop()` returns it last.
enum WalkFromItem<'a> {
    Visit(PathBuf, &'a MerkleTreeNode),
    FinalizeDir(PathBuf),
}

/// Walk the from tree (HEAD) and gather files-and-dirs to remove (anything HEAD has that
/// the target tree doesn't), files to back up to the version store, and conflict blockers.
/// Iterative depth-first search over an explicit stack so the file branch can `.await`
/// `repo.is_modified_from_node` — same shape as `walk_target_tree` and the merge-side
/// walkers.
async fn walk_from_tree<'a>(
    repo: &LocalRepository,
    from_root: &'a MerkleTreeNode,
    hashes: &CheckoutHashes,
    on_conflict: OnConflict,
) -> Result<CleanupCandidates, OxenError> {
    let mut candidates = CleanupCandidates::default();
    let mut stack: Vec<WalkFromItem<'a>> = vec![WalkFromItem::Visit(PathBuf::new(), from_root)];

    while let Some(item) = stack.pop() {
        match item {
            WalkFromItem::Visit(path, node) => match &node.node {
                EMerkleTreeNode::File(file_node) => {
                    let file_path = path.join(file_node.name());
                    let full_path = repo.path.join(&file_path);

                    // Only consider files whose path is not in the target tree (using
                    // path-based check instead of hash-based, because different files at
                    // different paths can share the same content hash).
                    if !hashes.seen_paths.contains(&file_path) {
                        if full_path.exists() {
                            let modified_locally =
                                repo.is_modified_from_node(&full_path, file_node).await?;
                            if on_conflict.is_abort() && modified_locally {
                                candidates.cannot_overwrite_entries.push(file_path);
                            } else {
                                // In remote mode, back up the file under `node.hash` before we
                                // remove it so future checkouts can restore from the version store.
                                // Only safe when the on-disk bytes match `node.hash`. The remaining
                                // case (`OnConflict::Overwrite` + `modified_locally`) is the user
                                // discarding their working state, so storing those bytes under the
                                // committed hash would pollute the content-addressable store with
                                // mismatched content.
                                if repo.is_remote_mode() && !modified_locally {
                                    candidates
                                        .files_to_store
                                        .push((node.hash, full_path.clone()));
                                }
                                candidates.paths_to_remove.push(full_path);
                            }
                        }
                    } else if full_path.exists() && repo.is_remote_mode() {
                        // File exists in both trees at the same path — it may be overwritten by the
                        // restore step. Same gate as above: back up the on-disk bytes only when
                        // they match `node.hash`. If the user modified the file locally, those
                        // bytes would pollute the content-addressable store under the wrong hash.
                        if !repo.is_modified_from_node(&full_path, file_node).await? {
                            candidates.files_to_store.push((node.hash, full_path));
                        }
                    }
                }
                EMerkleTreeNode::Directory(dir_node) => {
                    if hashes.common_nodes.contains(&node.hash) {
                        continue;
                    }
                    let dir_path = path.join(dir_node.name());

                    // Post-order: schedule the directory's "remove if empty" finalize task
                    // FIRST so that after the LIFO walks every child the FinalizeDir item
                    // pops last.
                    stack.push(WalkFromItem::FinalizeDir(dir_path.clone()));

                    for vnode in &node.children {
                        if !hashes.common_nodes.contains(&vnode.hash) {
                            for child in &vnode.children {
                                stack.push(WalkFromItem::Visit(dir_path.clone(), child));
                            }
                        }
                    }
                }
                EMerkleTreeNode::Commit(_) => {
                    let root_dir = repositories::tree::get_root_dir(node)?;
                    stack.push(WalkFromItem::Visit(path, root_dir));
                }
                _ => {}
            },
            WalkFromItem::FinalizeDir(dir_path) => {
                let full_dir_path = repo.path.join(&dir_path);
                if full_dir_path.exists() {
                    candidates.paths_to_remove.push(full_dir_path);
                }
            }
        }
    }

    Ok(candidates)
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
    on_conflict: OnConflict,
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

            // Collect path for matching in r_remove_if_not_in_target
            hashes.seen_paths.insert(file_path.clone());
            if !full_path.exists() {
                // Before restoring, check if the user intentionally deleted this file
                // If the file existed in the from tree (tracked in partial_nodes), it was
                // deleted in the working directory without being committed
                if let Some(from_node) = partial_nodes.get(&file_path) {
                    if from_node.hash == target_node.hash {
                        // Same content in both trees - preserve the user's deletion
                        log::debug!("Preserving uncommitted deletion of file: {file_path:?}");
                        return Ok(());
                    } else if on_conflict.is_abort() {
                        // Different content - this is a conflict
                        log::debug!(
                            "Conflict: uncommitted deletion of modified file: {file_path:?}"
                        );
                        results.cannot_overwrite_entries.push(file_path.clone());
                        return Ok(());
                    }
                    // Overwrite: fall through and restore the target's version anyway.
                }

                // File is new in the target commit, restore it
                log::debug!("Restoring missing file: {file_path:?}");
                results.files_to_restore.push(FileToRestore {
                    file_node: file_node.clone(),
                    path: file_path.clone(),
                });

                progress.increment_restored();
            } else {
                // TODO: Refactor this check into a separate module
                // We don't have a module for a 3-way is_modified_from_node right now

                // File exists, check whether it matches the target node or a from node
                // First, check the metadata
                let meta = util::fs::metadata(&full_path)?;
                let last_modified = Some(FileTime::from_last_modification_time(&meta));
                let size = Some(meta.len());

                let target_last_modified = util::fs::last_modified_time(
                    file_node.last_modified_seconds(),
                    file_node.last_modified_nanoseconds(),
                );

                let target_size = file_node.num_bytes();

                // If this matches the target, do nothing
                if last_modified == Some(target_last_modified) && size == Some(target_size) {
                    return Ok(());
                }

                // If the metadata matches a corresponding from_node, stage it to be restored
                let (from_node, from_last_modified, from_size) =
                    if let Some(from_node) = partial_nodes.get(&file_path) {
                        (
                            Some(from_node),
                            Some(from_node.last_modified),
                            Some(from_node.size),
                        )
                    } else {
                        (None, None, None)
                    };

                if last_modified == from_last_modified && size == from_size {
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
                    results.files_to_restore.push(FileToRestore {
                        file_node: file_node.clone(),
                        path: file_path.clone(),
                    });
                    progress.increment_modified();
                    return Ok(());
                }

                // Neither hash matches: the working file has been modified (or is mid-write,
                // e.g. after a crashed merge). Normally a conflict — but with
                // `OnConflict::Overwrite` (e.g. `oxen merge --abort`), discard the working state
                // and restore the target's version.
                match on_conflict {
                    OnConflict::Abort => {
                        results.cannot_overwrite_entries.push(file_path.clone());
                    }
                    OnConflict::Overwrite => {
                        results.files_to_restore.push(FileToRestore {
                            file_node: file_node.clone(),
                            path: file_path.clone(),
                        });
                    }
                }
                progress.increment_modified();
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            let dir_path = path.join(dir_node.name());
            let full_dir_path = repo.path.join(&dir_path);
            // If something exists at this path but is not a directory (e.g. the
            // user replaced a dir with a file), remove it so restoration can proceed.
            if full_dir_path.exists() && !full_dir_path.is_dir() {
                std::fs::remove_file(&full_dir_path)?;
            }

            // Early exit if the directory is the same in the from and target trees
            // AND it still exists on disk as a directory (if deleted or replaced, we need to restore it)
            if hashes.common_nodes.contains(&target_node.hash) && full_dir_path.is_dir() {
                return Ok(());
            };

            // If the directory doesn't exist on disk, we need to walk all vnodes
            // (including shared ones) to restore all missing files
            let walk_all = !full_dir_path.is_dir();

            let children = {
                // Get vnodes for the from dir node
                let dir_vnodes = &target_node.children;

                // Only iterate through vnodes not shared between the trees
                // unless walk_all is set (directory deleted from disk)
                let mut unique_nodes = Vec::new();
                for vnode in dir_vnodes {
                    if walk_all || !hashes.common_nodes.contains(&vnode.hash) {
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
                    on_conflict,
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
                on_conflict,
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
