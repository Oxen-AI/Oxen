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
    /// Working-tree paths that hold a non-directory where the target tree has a directory.
    /// These are removed only after `cannot_overwrite_entries` has been confirmed empty so an
    /// aborted checkout never mutates the working tree.
    pub dir_replacements: Vec<DirReplacement>,
}

impl CheckoutResult {
    pub fn new() -> Self {
        CheckoutResult {
            files_to_restore: vec![],
            cannot_overwrite_entries: vec![],
            dir_replacements: vec![],
        }
    }
}

/// A working-tree non-directory entry blocking a target-tree directory. Application is deferred
/// until after conflict resolution, mirroring the file-vs-file flow.
struct DirReplacement {
    /// Absolute on-disk path to the blocking entry.
    full_path: PathBuf,
    /// Hash of the entry as it existed in the from tree, or `None` if the path was untracked.
    /// Used in remote-mode to back up the file's content before deletion.
    from_hash: Option<MerkleHash>,
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
        let mut hashes = CheckoutHashes::from_hashes(shared_hashes);
        let version_store = repo.version_store()?;

        let results = walk_target_tree(
            repo,
            &target_root,
            parent_path,
            &mut progress,
            &partial_nodes,
            &mut hashes,
            depth,
            OnConflict::Abort,
        )
        .await?;

        // If there are conflicts, return an error without restoring anything
        if !results.cannot_overwrite_entries.is_empty() {
            return Err(OxenError::cannot_overwrite_files(
                &results.cannot_overwrite_entries,
            ));
        }

        apply_dir_replacements(repo, &results.dir_replacements).await?;

        if let Some(root) = from_root {
            log::debug!("Cleanup_removed_files");
            cleanup_removed_files(repo, &root, &mut progress, &mut hashes, OnConflict::Abort)
                .await?;
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

    let mut hashes = CheckoutHashes::from_hashes(shared_hashes);
    let version_store = repo.version_store()?;

    log::debug!("walk_target_tree");
    // Restore files present in the target commit
    let results = walk_target_tree(
        repo,
        &target_tree,
        Path::new(""),
        &mut progress,
        &partial_nodes,
        &mut hashes,
        i32::MAX,
        on_conflict,
    )
    .await?;

    // If there are conflicts, return an error without restoring anything
    if !results.cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(
            &results.cannot_overwrite_entries,
        ));
    }

    apply_dir_replacements(repo, &results.dir_replacements).await?;

    // Cleanup files if checking out fr om another commit
    if let Some(from_tree) = from_tree {
        log::debug!("Cleanup_removed_files");
        cleanup_removed_files(repo, &from_tree, &mut progress, &mut hashes, on_conflict).await?;
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
    on_conflict: OnConflict,
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
        on_conflict,
    )?;

    if !cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(&cannot_overwrite_entries));
    }

    // If in remote mode, need to store committed paths before removal
    if repo.is_remote_mode() {
        let version_store = repo.version_store()?;
        for (hash, full_path) in files_to_store {
            log::debug!("Storing hash {hash:?} and path {full_path:?}");
            let file = tokio::fs::File::open(&full_path).await?;
            let size = file.metadata().await?.len();
            let reader = tokio::io::BufReader::new(file);
            version_store
                .store_version_from_reader(&hash.to_string(), Box::new(reader), size)
                .await?;
        }
    }

    for full_path in paths_to_remove {
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

#[allow(clippy::too_many_arguments)]
fn r_remove_if_not_in_target(
    repo: &LocalRepository,
    from_node: &MerkleTreeNode,
    current_path: &Path,
    paths_to_remove: &mut Vec<PathBuf>,
    files_to_store: &mut Vec<(MerkleHash, PathBuf)>,
    cannot_overwrite_entries: &mut Vec<PathBuf>,
    hashes: &mut CheckoutHashes,
    on_conflict: OnConflict,
) -> Result<(), OxenError> {
    // Iterate through the from tree, removing files not present in the target tree
    match &from_node.node {
        EMerkleTreeNode::File(file_node) => {
            let file_path = current_path.join(file_node.name());
            let full_path = repo.path.join(&file_path);

            // Only consider files whose path is not in the target tree
            // (using path-based check instead of hash-based, because different
            // files at different paths can share the same content hash)
            if !hashes.seen_paths.contains(&file_path) {
                // Before staging for removal, verify the path exists and isn't modified
                if full_path.exists() {
                    let modified_locally = util::fs::is_modified_from_node(&full_path, file_node)?;
                    if on_conflict.is_abort() && modified_locally {
                        cannot_overwrite_entries.push(file_path.clone());
                    } else {
                        // If in remote mode, save file to version store before removing
                        if repo.is_remote_mode() {
                            files_to_store.push((from_node.hash, full_path.clone()))
                        }

                        paths_to_remove.push(full_path.clone());
                    }
                }
            } else if full_path.exists() && repo.is_remote_mode() {
                // File exists in both trees at the same path — it may be overwritten
                // during restore. Store the current version so future checkouts can
                // restore it from the version store.
                files_to_store.push((from_node.hash, full_path.clone()))
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
                    on_conflict,
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
                on_conflict,
            )?;
        }
        _ => {}
    }
    Ok(())
}

/// Apply working-tree replacements where the target tree has a directory but the working tree
/// has a non-directory entry. Runs after `cannot_overwrite_entries` is confirmed empty so an
/// aborted checkout never mutates the working tree. In remote mode, the file's content is
/// stored under its from-tree hash before deletion so future checkouts can restore it.
async fn apply_dir_replacements(
    repo: &LocalRepository,
    replacements: &[DirReplacement],
) -> Result<(), OxenError> {
    if replacements.is_empty() {
        return Ok(());
    }
    let version_store = if repo.is_remote_mode() {
        Some(repo.version_store()?)
    } else {
        None
    };
    for replacement in replacements {
        if !replacement.full_path.exists() {
            continue;
        }
        if let (Some(version_store), Some(from_hash)) =
            (version_store.as_ref(), replacement.from_hash)
        {
            let file = tokio::fs::File::open(&replacement.full_path).await?;
            let size = file.metadata().await?.len();
            let reader = tokio::io::BufReader::new(file);
            version_store
                .store_version_from_reader(&from_hash.to_string(), Box::new(reader), size)
                .await?;
        }
        util::fs::remove_file(&replacement.full_path)?;
    }
    Ok(())
}

/// Walk the target tree and stage every file that is missing from disk or differs from the
/// target node. Iterative depth-first search over an explicit stack so the file branch can
/// `.await` `repo.mtime_matches` — same shape as the merge-side walkers.
///
/// Also populates `hashes.seen_paths` with every path in the target tree, which the cleanup
/// walker (`r_remove_if_not_in_target`) consumes to identify HEAD files that aren't in the
/// target (i.e., deletions to apply).
#[allow(clippy::too_many_arguments)]
async fn walk_target_tree<'a>(
    repo: &LocalRepository,
    target_root: &'a MerkleTreeNode,
    starting_path: &Path,
    progress: &mut CheckoutProgressBar,
    partial_nodes: &HashMap<PathBuf, PartialNode>,
    hashes: &mut CheckoutHashes,
    starting_depth: i32,
    on_conflict: OnConflict,
) -> Result<CheckoutResult, OxenError> {
    let mut results = CheckoutResult::new();
    let mut stack: Vec<(PathBuf, &'a MerkleTreeNode, i32)> =
        vec![(starting_path.to_path_buf(), target_root, starting_depth)];

    while let Some((path, node, depth)) = stack.pop() {
        if depth < 0 {
            continue;
        }

        match &node.node {
            EMerkleTreeNode::File(file_node) => {
                let file_path = path.join(file_node.name());
                let full_path = repo.path.join(&file_path);

                // Collect path for matching in r_remove_if_not_in_target
                hashes.seen_paths.insert(file_path.clone());

                if !full_path.exists() {
                    // Before restoring, check if the user intentionally deleted this file. If
                    // the file existed in the from tree (tracked in partial_nodes), it was
                    // deleted in the working directory without being committed.
                    if let Some(from_node) = partial_nodes.get(&file_path) {
                        if from_node.hash == node.hash {
                            // Same content in both trees - preserve the user's deletion
                            log::debug!("Preserving uncommitted deletion of file: {file_path:?}");
                            continue;
                        } else if on_conflict.is_abort() {
                            log::debug!(
                                "Conflict: uncommitted deletion of modified file: {file_path:?}"
                            );
                            results.cannot_overwrite_entries.push(file_path.clone());
                            continue;
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
                    continue;
                }

                // TODO: Refactor this check into a separate module — there is no module for
                // a 3-way is_modified_from_node right now.

                // File exists. Check whether it matches the target node or a from node.
                let meta = util::fs::metadata(&full_path)?;
                let disk_mtime = FileTime::from_last_modification_time(&meta);
                let disk_size = meta.len();

                let target_mtime = util::fs::last_modified_time(
                    file_node.last_modified_seconds(),
                    file_node.last_modified_nanoseconds(),
                );
                let target_size = file_node.num_bytes();

                // If this matches the target, do nothing. `mtime_matches` honors the FS's
                // rounding tolerance — without it, a file that `restore_file`'s fast path
                // would skip looks "modified" here on coarse-mtime mounts (FAT/exFAT, HFS+,
                // some NFS).
                if repo.mtime_matches(disk_mtime, target_mtime).await && disk_size == target_size {
                    continue;
                }

                let from_node = partial_nodes.get(&file_path);

                // If the metadata matches a corresponding from_node, stage it to be restored.
                if let Some(from) = from_node
                    && repo.mtime_matches(disk_mtime, from.last_modified).await
                    && disk_size == from.size
                {
                    results.files_to_restore.push(FileToRestore {
                        file_node: file_node.clone(),
                        path: file_path.clone(),
                    });
                    progress.increment_modified();
                    continue;
                }

                // Otherwise, check hashes.
                let working_hash = util::hasher::get_hash_given_metadata(&full_path, &meta)?;
                let target_hash = node.hash.to_u128();
                if working_hash == target_hash {
                    continue;
                }

                let from_hash = from_node.map(|n| n.hash.to_u128());
                if Some(working_hash) == from_hash {
                    results.files_to_restore.push(FileToRestore {
                        file_node: file_node.clone(),
                        path: file_path.clone(),
                    });
                    progress.increment_modified();
                    continue;
                }

                // Neither hash matches: the working file has diverged from both. Normally a
                // conflict — but with `OnConflict::Overwrite` (e.g. `oxen merge --abort`),
                // discard the working state and restore the target's version.
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
            EMerkleTreeNode::Directory(dir_node) => {
                let dir_path = path.join(dir_node.name());
                let full_dir_path = repo.path.join(&dir_path);
                // Something exists at this path but is not a directory (e.g. the user
                // replaced a dir with a file). Stage it for replacement instead of removing
                // eagerly.
                if full_dir_path.exists() && !full_dir_path.is_dir() {
                    // Only block when the from tree had a *file* at this path and the disk
                    // copy diverges from it — the case where eager removal would silently
                    // destroy committed-then-locally-modified work. Other shapes (untracked
                    // content, or a tracked directory the user destructively replaced with a
                    // file) were silently overwritten before this fix; preserve that
                    // contract since `partial_nodes` only tracks file paths and we cannot
                    // cheaply distinguish those cases here.
                    let from = partial_nodes.get(&dir_path);
                    if let Some(from) = from {
                        let meta = util::fs::metadata(&full_dir_path)?;
                        let disk_mtime = FileTime::from_last_modification_time(&meta);
                        let disk_size = meta.len();
                        let unmodified = if repo.mtime_matches(disk_mtime, from.last_modified).await
                            && disk_size == from.size
                        {
                            true
                        } else {
                            let working_hash =
                                util::hasher::get_hash_given_metadata(&full_dir_path, &meta)?;
                            working_hash == from.hash.to_u128()
                        };
                        if !unmodified && on_conflict.is_abort() {
                            results.cannot_overwrite_entries.push(dir_path.clone());
                            // Skip the children walk: the checkout will abort, so queueing
                            // restorations under this path would be wasted work.
                            continue;
                        }
                    }

                    results.dir_replacements.push(DirReplacement {
                        full_path: full_dir_path.clone(),
                        from_hash: from.map(|f| f.hash),
                    });
                }

                // Early exit if the directory is the same in the from and target trees AND
                // it still exists on disk as a directory.
                if hashes.common_nodes.contains(&node.hash) && full_dir_path.is_dir() {
                    continue;
                }

                // If the directory doesn't exist on disk, walk all vnodes (including shared
                // ones) to restore all missing files.
                let walk_all = !full_dir_path.is_dir();

                for vnode in &node.children {
                    if walk_all || !hashes.common_nodes.contains(&vnode.hash) {
                        for child in &vnode.children {
                            stack.push((dir_path.clone(), child, depth - 1));
                        }
                    }
                }
            }
            EMerkleTreeNode::Commit(_) => {
                let root_dir = repositories::tree::get_root_dir(node)?;
                stack.push((path, root_dir, depth - 1));
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Got an unexpected node type during checkout",
                ));
            }
        }
    }

    Ok(results)
}
