use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::oxenignore;
use crate::core::staged::staged_db_manager::get_staged_db_manager;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::staged_data::StagedDataOpts;
use crate::model::{
    Commit, LocalRepository, MerkleHash, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus, StagedSchema, SummarizedStagedDirStats,
};
use crate::{repositories, util};

use ignore::gitignore::Gitignore;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, IteratorMode, SingleThreaded};
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::time::Duration;

use crate::core::v_latest::index::CommitMerkleTree;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;

pub async fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    status_from_dir(repo, &repo.path).await
}

pub async fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    let opts = StagedDataOpts {
        paths: vec![dir.as_ref().to_path_buf()],
        ..StagedDataOpts::default()
    };
    status_from_opts(repo, &opts).await
}

pub async fn status_from_opts(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    //log::debug!("status_from_opts {:?}", opts.paths);
    let staged_db_maybe = open_staged_db(repo)?;
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let dir_hashes = get_dir_hashes(repo, &head_commit)?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let out = walk_paths(
        repo,
        opts,
        StagedSource::Db(&staged_db_maybe),
        MissingClassification::AsRemoved,
        &dir_hashes,
        &read_progress,
    )
    .await?;

    log::debug!("status_from_opts untracked: {:?}", out.untracked);
    log::debug!("status_from_opts modified: {:?}", out.modified);
    log::debug!("status_from_opts removed: {:?}", out.removed);

    let mut staged_data = StagedData::empty();
    staged_data.untracked_dirs = out.untracked.dirs.into_iter().collect();
    staged_data.untracked_files = out.untracked.files;
    staged_data.modified_files = out.modified;
    staged_data.removed_files = out.removed;

    // Find merge conflicts
    let conflicts = repositories::merge::list_conflicts(repo)?;
    //log::debug!("list_conflicts found {} conflicts", conflicts.len());
    for conflict in conflicts {
        staged_data
            .merge_conflicts
            .push(conflict.to_entry_merge_conflict());
    }

    let Some(staged_db) = staged_db_maybe else {
        log::debug!("status_from_dir no staged db, returning early");
        return Ok(staged_data);
    };

    // TODO: Consider moving this to the top to keep track of removed dirs and avoid unnecessary recursion with count_removed_entries
    let mut dir_entries = HashMap::new();
    for dir in opts.paths.iter() {
        let (sub_dir_entries, _) =
            read_staged_entries_below_path(repo, &staged_db, dir, &read_progress)?;
        dir_entries.extend(sub_dir_entries);
        // log::debug!("status_from_dir dir_entries: {:?}", dir_entries);
    }
    read_progress.finish_and_clear();

    status_from_dir_entries(&mut staged_data, dir_entries)
}

// Get status with pre-existing staged data
pub async fn status_from_opts_and_staged_data(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    staged_data: &mut StagedData,
) -> Result<(), OxenError> {
    //log::debug!("status_from_opts {:?}", opts.paths);
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let dir_hashes = get_dir_hashes(repo, &head_commit)?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let out = walk_paths(
        repo,
        opts,
        StagedSource::Data(staged_data),
        MissingClassification::AsUnsynced,
        &dir_hashes,
        &read_progress,
    )
    .await?;

    log::debug!(
        "status_from_opts_and_staged_data untracked: {:?}",
        out.untracked
    );
    log::debug!(
        "status_from_opts_and_staged_data unsynced: {:?}",
        out.unsynced
    );
    log::debug!(
        "status_from_opts_and_staged_data modified: {:?}",
        out.modified
    );
    log::debug!(
        "status_from_opts_and_staged_data removed: {:?}",
        out.removed
    );

    staged_data.untracked_dirs = out.untracked.dirs.into_iter().collect();
    staged_data.untracked_files = out.untracked.files;
    staged_data.unsynced_dirs = out.unsynced.dirs.into_iter().collect();
    staged_data.unsynced_files = out.unsynced.files;
    staged_data.modified_files = out.modified;
    staged_data.removed_files = out.removed;

    // Find merge conflicts
    let conflicts = repositories::merge::list_conflicts(repo)?;
    //log::debug!("list_conflicts found {} conflicts", conflicts.len());
    for conflict in conflicts {
        staged_data
            .merge_conflicts
            .push(conflict.to_entry_merge_conflict());
    }

    Ok(())
}

pub fn status_from_dir_entries(
    staged_data: &mut StagedData,
    dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>>,
) -> Result<StagedData, OxenError> {
    let mut summarized_dir_stats = SummarizedStagedDirStats {
        num_files_staged: 0,
        total_files: 0,
        paths: HashMap::new(),
    };

    // log::debug!("dir_entries.len(): {:?}", dir_entries.len());

    for (dir, entries) in dir_entries {
        log::debug!(
            "dir_entries dir: {:?} entries.len(): {:?}",
            dir,
            entries.len()
        );
        let mut stats = StagedDirStats {
            path: dir.clone(),
            num_files_staged: 0,
            total_files: 0,
            status: StagedEntryStatus::Added,
        };

        let mut removed_stats = StagedDirStats {
            path: dir.clone(),
            num_files_staged: 0,
            total_files: 0,
            status: StagedEntryStatus::Removed,
        };

        let mut is_removed = false;

        for entry in &entries {
            match &entry.node.node {
                EMerkleTreeNode::Directory(node) => {
                    log::debug!("dir_entries dir_node: {node}");
                    // Correction for empty dir status
                    is_removed = true;

                    // Cannot be removed if it's staged
                    if !staged_data.staged_dirs.contains_key(&dir) {
                        staged_data
                            .removed_files
                            .remove(&PathBuf::from(&node.name()));
                    }
                }
                EMerkleTreeNode::File(node) => {
                    // TODO: It's not always added. It could be modified.
                    log::debug!("dir_entries file_node: {entry}");
                    let file_path = PathBuf::from(node.name());
                    if entry.status == StagedEntryStatus::Modified {
                        staged_data.modified_files.insert(file_path.clone());
                    }
                    let staged_entry = StagedEntry {
                        hash: node.hash().to_string(),
                        status: entry.status.clone(),
                    };

                    staged_data
                        .staged_files
                        .insert(file_path.clone(), staged_entry);
                    maybe_add_schemas(node, staged_data)?;

                    // Cannot be removed if it's staged
                    if staged_data.staged_files.contains_key(&file_path) {
                        staged_data.removed_files.remove(&file_path);
                        staged_data.modified_files.remove(&file_path);
                    }

                    if entry.status == StagedEntryStatus::Removed {
                        removed_stats.num_files_staged += 1;
                    } else {
                        stats.num_files_staged += 1;
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(format!(
                        "status_from_dir found unexpected node type: {:?}",
                        entry.node
                    )));
                }
            }
        }

        // Empty dirs should be added to summarized_dir_stats (entries.len() == 0)
        if entries.is_empty() {
            if is_removed || staged_data.removed_files.contains(&dir) {
                summarized_dir_stats.add_stats(&removed_stats);
            } else {
                summarized_dir_stats.add_stats(&stats);
            }
        }

        if stats.num_files_staged > 0 {
            summarized_dir_stats.add_stats(&stats);
        }

        if removed_stats.num_files_staged > 0 {
            summarized_dir_stats.add_stats(&removed_stats);
        }
    }

    staged_data.staged_dirs = summarized_dir_stats;
    find_moved_files(staged_data)?;

    Ok(staged_data.clone())
}

fn find_moved_files(staged_data: &mut StagedData) -> Result<(), OxenError> {
    let files = staged_data.staged_files.clone();
    let files_vec: Vec<(&PathBuf, &StagedEntry)> = files.iter().collect();

    // Find pairs of added-removed with same hash and add them to moved.
    // We won't mutate StagedEntries here, the "moved" property is read-only
    let mut added_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();
    let mut removed_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();

    for (path, entry) in files_vec.iter() {
        match entry.status {
            StagedEntryStatus::Added => {
                added_map.entry(entry.hash.clone()).or_default().push(path);
            }
            StagedEntryStatus::Removed => {
                removed_map
                    .entry(entry.hash.clone())
                    .or_default()
                    .push(path);
            }
            _ => continue,
        }
    }

    for (hash, added_paths) in added_map.iter_mut() {
        if let Some(removed_paths) = removed_map.get_mut(hash) {
            while !added_paths.is_empty() && !removed_paths.is_empty() {
                if let (Some(added_path), Some(removed_path)) =
                    (added_paths.pop(), removed_paths.pop())
                {
                    // moved_entries.push((added_path, removed_path, hash.to_string()));
                    staged_data.moved_files.push((
                        added_path.clone(),
                        removed_path.clone(),
                        hash.to_string(),
                    ));
                }
            }
        }
    }
    Ok(())
}

fn maybe_add_schemas(node: &FileNode, staged_data: &mut StagedData) -> Result<(), OxenError> {
    if let Some(GenericMetadata::MetadataTabular(m)) = &node.metadata() {
        let schema = m.tabular.schema.clone();
        let path = PathBuf::from(node.name());
        let staged_schema = StagedSchema {
            schema,
            status: StagedEntryStatus::Added,
        };
        staged_data.staged_schemas.insert(path, staged_schema);
    }

    Ok(())
}

pub fn read_staged_entries(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    read_staged_entries_below_path(repo, db, Path::new(""), read_progress)
}

/// Duplicate function using staged db manager in workspaces
pub fn read_staged_entries_with_staged_db_manager(
    repo: &LocalRepository,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    read_staged_entries_below_path_with_staged_db_manager(repo, Path::new(""), read_progress)
}

/// Duplicate function using staged db manager in workspaces
pub fn read_staged_entries_below_path_with_staged_db_manager(
    repo: &LocalRepository,
    start_path: impl AsRef<Path>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    let staged_db_manager = get_staged_db_manager(repo)?;
    staged_db_manager.read_staged_entries_below_path(start_path, read_progress)
}

pub fn read_staged_entries_below_path(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    start_path: impl AsRef<Path>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, usize), OxenError> {
    let start_path = util::fs::path_relative_to_dir(start_path.as_ref(), &repo.path)?;
    let mut total_entries = 0;
    let iter = db.iterator(IteratorMode::Start);
    let mut dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>> = HashMap::new();
    for item in iter {
        match item {
            // key = file path, value = EntryMetaData
            Ok((key, value)) => {
                // log::debug!("Key is {key:?}, value is {value:?}");
                let key = str::from_utf8(&key)?;
                let path = Path::new(key);
                if !path.starts_with(&start_path) {
                    continue;
                }

                // Older versions may have a corrupted StagedMerkleTreeNode that was staged
                // Ignore these when reading the staged db
                let entry: Result<StagedMerkleTreeNode, rmp_serde::decode::Error> =
                    rmp_serde::from_slice(&value);
                let Ok(entry) = entry else {
                    log::error!("read_staged_entries error decoding {key} path: {path:?}");
                    continue;
                };
                log::debug!("read_staged_entries key {key} entry: {entry} path: {path:?}");

                if let EMerkleTreeNode::Directory(_) = &entry.node.node {
                    // add the dir as a key in dir_entries
                    log::debug!("read_staged_entries adding dir {path:?}");
                    dir_entries.entry(path.to_path_buf()).or_default();
                }

                // add the file or dir as an entry under its parent dir
                if let Some(parent) = path.parent() {
                    log::debug!("read_staged_entries adding file {path:?} to parent {parent:?}");
                    dir_entries
                        .entry(parent.to_path_buf())
                        .or_default()
                        .push(entry);
                }

                total_entries += 1;
                read_progress.set_message(format!("Found {total_entries} entries"));
            }
            Err(err) => {
                log::error!("Could not get staged entry: {err}");
            }
        }
    }

    log::debug!(
        "read_staged_entries dir_entries.len(): {:?}",
        dir_entries.len()
    );
    if log::max_level() == log::Level::Debug {
        for (dir, entries) in dir_entries.iter() {
            log::debug!("commit dir_entries dir {dir:?}");
            for entry in entries.iter() {
                log::debug!("\tcommit dir_entries entry {entry}");
            }
        }
    }

    Ok((dir_entries, total_entries))
}

/// Source of staging information for the walker. The two callers have different views
/// of the staged set: `status_from_opts` queries the staged-db RocksDB directly, and
/// `status_from_opts_and_staged_data` already has a fully-materialized `StagedData`.
#[derive(Clone, Copy)]
enum StagedSource<'a> {
    Db(&'a Option<DBWithThreadMode<SingleThreaded>>),
    Data(&'a StagedData),
}

impl StagedSource<'_> {
    fn is_path_staged(&self, path: &Path) -> Result<bool, OxenError> {
        match self {
            Self::Db(db) => is_staged(path, db),
            Self::Data(data) => in_staged_data(path, data),
        }
    }

    /// True if `path` is staged for deletion in the in-memory staged-files map. Used at
    /// the tree-side check to gate "missing on disk" → unsynced classification: a file
    /// the user has already staged for delete shouldn't be re-surfaced as unsynced.
    /// A file staged with any other status (e.g. Added/Modified) is not relevant here,
    /// so we check the entry's status rather than mere presence in the map. Always false
    /// in `Db` mode (`status_from_opts` doesn't classify into unsynced).
    fn is_file_deleted(&self, path: &Path) -> bool {
        match self {
            Self::Db(_) => false,
            Self::Data(data) => data
                .staged_files
                .get(path)
                .is_some_and(|entry| entry.status == StagedEntryStatus::Removed),
        }
    }

    /// Like [`Self::is_file_deleted`] but for directories. Returns true when the path's
    /// staged-dir stats include a `Removed` entry (a single dir can have both an
    /// `Added` and a `Removed` rollup if it contains a mix of staged adds and removes).
    fn is_dir_deleted(&self, path: &Path) -> bool {
        match self {
            Self::Db(_) => false,
            Self::Data(data) => {
                data.staged_dirs.paths.get(path).is_some_and(|stats| {
                    stats.iter().any(|s| s.status == StagedEntryStatus::Removed)
                })
            }
        }
    }
}

/// Where to record paths that are in the merkle tree but missing on disk.
#[derive(Clone, Copy)]
enum MissingClassification {
    /// Local-mode (`status_from_opts`): missing files+dirs go to `removed` unconditionally.
    /// Upstream code reconciles against the staged-db afterward.
    AsRemoved,
    /// Remote-mode (`status_from_opts_and_staged_data`): missing files+dirs go to `unsynced`
    /// unless already staged for delete. The subtree-root special case still uses `removed`,
    /// so partially fetched subtree mode surfaces missing files even in unsynced mode.
    AsUnsynced,
}

/// Output of the unified walker. `unsynced` is always empty under
/// [`MissingClassification::AsRemoved`] (the `status_from_opts` caller doesn't
/// distinguish unsynced from removed).
struct WalkOutput {
    untracked: UntrackedData,
    unsynced: UnsyncedData,
    modified: HashSet<PathBuf>,
    removed: HashSet<PathBuf>,
}

impl WalkOutput {
    fn empty() -> Self {
        Self {
            untracked: UntrackedData::new(),
            unsynced: UnsyncedData::new(),
            modified: HashSet::new(),
            removed: HashSet::new(),
        }
    }

    fn merge(&mut self, other: WalkOutput) {
        self.untracked.merge(other.untracked);
        self.unsynced.merge(other.unsynced);
        self.modified.extend(other.modified);
        self.removed.extend(other.removed);
    }
}

/// Read the entries of a directory (one stat-call per entry, parallelized via rayon),
/// or wrap a single non-dir path. Bad-metadata entries are skipped with a debug log
/// rather than failing the whole walk. A non-dir path whose metadata is unreadable
/// (e.g. the user passed a path that's been deleted on disk — `rm_with_staged_db`
/// runs status against just-deleted dirs to check for modifications) yields an empty
/// list; the missing-file case is then surfaced by `walk_status`'s tree-side check
/// for single-file paths (see the block guarded on `!is_dir && !full_path.exists()`).
fn read_dir_entries(
    full_path: &Path,
    is_dir: bool,
) -> Result<Vec<(PathBuf, bool, std::fs::Metadata)>, OxenError> {
    if is_dir {
        let Ok(dir_entries) = std::fs::read_dir(full_path) else {
            return Err(OxenError::basic_str(format!(
                "Could not read dir {full_path:?}"
            )));
        };
        let new_entries: Vec<_> = dir_entries
            .par_bridge()
            .filter_map(|res| {
                let entry = match res {
                    Ok(entry) => entry,
                    Err(err) => {
                        log::debug!("Skipping unreadable entry: {err}");
                        return None;
                    }
                };
                let path = entry.path();
                let metadata = match entry.metadata() {
                    Ok(md) => md,
                    Err(err) => {
                        log::debug!("Skipping entry with unreadable metadata {path:?}: {err}");
                        return None;
                    }
                };
                Some((path, metadata.is_dir(), metadata))
            })
            .collect();
        Ok(new_entries)
    } else {
        let Ok(metadata) = util::fs::metadata(full_path) else {
            return Ok(Vec::new());
        };
        Ok(vec![(full_path.to_owned(), false, metadata)])
    }
}

/// Per-directory state for the iterative depth-first search. Each state accumulates
/// findings from its own files plus any merged-in results from its (already-finalized)
/// children. The walker maintains a `path_stack: Vec<DirState>` — the top is the
/// directory currently being processed, and the rest of the stack is its ancestor
/// chain.
struct DirState {
    search_node_path: PathBuf,
    full_path: PathBuf,
    is_dir: bool,
    untracked: UntrackedData,
    unsynced: UnsyncedData,
    modified: HashSet<PathBuf>,
    removed: HashSet<PathBuf>,
    untracked_count: usize,
    /// The merkle node this path resolves to, if any. Read at FinalizeDir time both to
    /// gate the promote-to-untracked-dir guard (`is_none()`) and, for single-file paths
    /// the user named directly in `opts.paths`, to dispatch on the `File` variant when
    /// the file is missing on disk.
    search_node: Option<MerkleTreeNode>,
}

/// Stack item for the iterative depth-first search. `EnterDir` pushes a fresh
/// `DirState` onto the path stack, classifies the directory's file entries inline,
/// and queues `FinalizeDir` plus an `EnterDir` for each subdirectory. `FinalizeDir`
/// is queued FIRST so LIFO ordering pops it AFTER all the children's states have
/// been finalized — at which point the matching `DirState` is on top of the path
/// stack and can be popped and merged into its parent (the new top).
enum WalkItem {
    EnterDir(PathBuf),
    FinalizeDir,
}

/// Iterative depth-first walker shared between `status_from_opts` and
/// `status_from_opts_and_staged_data` (via `walk_paths`). The two callers differ only
/// in (1) which staged source they consult and (2) where they record paths that are in
/// the merkle tree but missing on disk; both knobs are passed in.
#[allow(clippy::too_many_arguments)]
async fn walk_status(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    starting_path: &Path,
    staged: StagedSource<'_>,
    missing: MissingClassification,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    gitignore: &Option<Gitignore>,
    progress: &ProgressBar,
    total_entries: &mut usize,
) -> Result<WalkOutput, OxenError> {
    let mut path_stack: Vec<DirState> = Vec::new();
    let mut work: Vec<WalkItem> = vec![WalkItem::EnterDir(starting_path.to_path_buf())];

    while let Some(item) = work.pop() {
        match item {
            WalkItem::EnterDir(search_node_path) => {
                let full_path = repo.path.join(&search_node_path);
                // Use `metadata.is_dir()` rather than `full_path.is_dir()` to avoid
                // following symlinks — Oxen does not track symlinks, and a symlink-to-dir
                // at the walker root would otherwise be silently followed.
                let is_dir = tokio::fs::symlink_metadata(&full_path)
                    .await
                    .map(|m| m.is_dir())
                    .unwrap_or(false);
                log::debug!(
                    "walk_status search_node_path: {search_node_path:?} full_path: {full_path:?}"
                );

                if let Some(ignore) = &opts.ignore
                    && (ignore.contains(&search_node_path) || ignore.contains(&full_path))
                {
                    // Ignored — contribute nothing, no state.
                    continue;
                }

                let entries = read_dir_entries(&full_path, is_dir)?;
                let search_node = maybe_get_node(repo, dir_hashes, &search_node_path)?;
                let dir_children = maybe_get_dir_children(&search_node)?;

                path_stack.push(DirState {
                    search_node_path: search_node_path.clone(),
                    full_path,
                    is_dir,
                    untracked: UntrackedData::new(),
                    unsynced: UnsyncedData::new(),
                    modified: HashSet::new(),
                    removed: HashSet::new(),
                    untracked_count: 0,
                    search_node: search_node.clone(),
                });

                // FinalizeDir goes onto the work queue BEFORE the children's EnterDirs
                // so that LIFO ordering pops it after every child has been finalized
                // (and popped from the path stack).
                work.push(WalkItem::FinalizeDir);

                let mut subdirs_to_recurse: Vec<PathBuf> = Vec::new();
                for (path, is_entry_dir, metadata) in entries {
                    progress.set_message(format!(
                        "🐂 checking ({total_entries} files) scanning {search_node_path:?}"
                    ));
                    *total_entries += 1;
                    let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;
                    let node_path =
                        util::fs::path_relative_to_dir(&relative_path, &search_node_path)?;
                    log::debug!(
                        "walk_status entry relative_path: {relative_path:?} in node_path {node_path:?} search_node_path: {search_node_path:?}"
                    );

                    if oxenignore::is_ignored(&relative_path, gitignore, is_entry_dir) {
                        continue;
                    }

                    let current = path_stack
                        .last_mut()
                        .expect("path stack non-empty: just pushed this dir's state");

                    if is_entry_dir {
                        log::debug!("walk_status entry is a directory {path:?}");
                        subdirs_to_recurse.push(relative_path);
                    } else if staged.is_path_staged(&relative_path)? {
                        log::debug!("walk_status entry is staged {path:?}");
                        // Check this after handling directories, because we still need
                        // to recurse into staged directories.
                        current.untracked.all_untracked = false;
                    } else if let Some(node) = maybe_get_child_node(&node_path, &dir_children)? {
                        log::debug!("walk_status entry is a child node {path:?}");
                        // If we have a dir node, it's either tracked (clean) or modified —
                        // either way, this directory is not all_untracked.
                        current.untracked.all_untracked = false;
                        if let EMerkleTreeNode::File(file_node) = &node.node {
                            let is_modified = repo
                                .is_modified_from_node_with_metadata(&path, file_node, Ok(metadata))
                                .await?;
                            log::debug!("is_modified {is_modified} {relative_path:?}");
                            if is_modified {
                                current.modified.insert(relative_path.clone());
                            }
                        }
                    } else {
                        log::debug!("walk_status entry is not a child node {path:?}");
                        // None of the above — check if it's untracked or modified.
                        let mut found_file = false;
                        if let Some(search_node) = &search_node
                            && let EMerkleTreeNode::File(file_node) = &search_node.node
                        {
                            found_file = true;
                            if repo
                                .is_modified_from_node_with_metadata(&path, file_node, Ok(metadata))
                                .await?
                            {
                                current.modified.insert(relative_path.clone());
                            }
                        }
                        log::debug!("walk_status found_file {found_file:?} {path:?}");

                        if !found_file {
                            current.untracked.add_file(relative_path.clone());
                            current.untracked_count += 1;
                        }
                    }
                }

                // Push subdirs in reverse so the LIFO pop order matches the recursive
                // walker's iteration order.
                for subdir in subdirs_to_recurse.into_iter().rev() {
                    work.push(WalkItem::EnterDir(subdir));
                }
            }
            WalkItem::FinalizeDir => {
                let mut dir_state = path_stack
                    .pop()
                    .expect("path stack non-empty: every FinalizeDir matches an EnterDir");

                // Promote an all-untracked directory to a single dir entry, unless
                // it's the root, is itself staged or committed, or isn't actually a
                // directory (single-file walk).
                if dir_state.untracked.all_untracked
                    && dir_state.search_node_path != Path::new("")
                    && !staged.is_path_staged(&dir_state.search_node_path)?
                    && dir_state.is_dir
                    && dir_state.search_node.is_none()
                {
                    dir_state.untracked.add_dir(
                        dir_state.search_node_path.clone(),
                        dir_state.untracked_count,
                    );
                    // Clear individual files as they're now represented by the directory.
                    dir_state.untracked.files.clear();
                }

                // Tree-side check for a single-file path that's missing on disk. The
                // dir-based block below only fires for paths in `dir_hashes` (i.e.
                // directories), so a tracked file passed directly in `opts.paths` and
                // then deleted would otherwise be silently dropped. Classify it the
                // same way the dir-based check classifies missing children.
                if !dir_state.is_dir
                    && !dir_state.full_path.exists()
                    && let Some(node) = &dir_state.search_node
                    && let EMerkleTreeNode::File(_) = &node.node
                {
                    let relative_file_path = dir_state.search_node_path.clone();
                    match missing {
                        MissingClassification::AsRemoved => {
                            dir_state.removed.insert(relative_file_path);
                        }
                        MissingClassification::AsUnsynced => {
                            if !staged.is_file_deleted(&relative_file_path) {
                                dir_state.unsynced.add_file(relative_file_path);
                            }
                        }
                    }
                }

                // Tree-side check for paths that are in the merkle tree but missing on
                // disk. The labeled block lets the subtree gates `break` out without
                // skipping the merge-into-parent step below.
                // TODO: Distinguish 'removed files' from unsynced more precisely.
                'tree_side: {
                    let Some(dir_hash) = dir_hashes.get(&dir_state.search_node_path) else {
                        break 'tree_side;
                    };

                    if let Some(subtree_paths) = repo.subtree_paths() {
                        if !subtree_paths.contains(&dir_state.search_node_path) {
                            // Outside the subtree — skip the tree-side check.
                            break 'tree_side;
                        }
                        if subtree_paths.len() == 1 && subtree_paths[0] == Path::new("") {
                            // Subtree-root special case: surface missing files as
                            // `removed` regardless of `MissingClassification`, so
                            // partially-fetched subtree mode still flags them.
                            let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
                            if let Some(node) = dir_node {
                                for child in repositories::tree::list_files_and_folders(&node)? {
                                    if let EMerkleTreeNode::File(file_node) = &child.node {
                                        let file_path = dir_state.full_path.join(file_node.name());
                                        if !file_path.exists() {
                                            dir_state.removed.insert(
                                                dir_state.search_node_path.join(file_node.name()),
                                            );
                                        }
                                    }
                                }
                            }
                            break 'tree_side;
                        }
                    }

                    let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
                    if let Some(node) = dir_node {
                        for child in repositories::tree::list_files_and_folders(&node)? {
                            if let EMerkleTreeNode::File(file_node) = &child.node {
                                let file_path = dir_state.full_path.join(file_node.name());
                                let relative_file_path =
                                    dir_state.search_node_path.join(file_node.name());
                                if !file_path.exists() {
                                    match missing {
                                        MissingClassification::AsRemoved => {
                                            dir_state.removed.insert(relative_file_path);
                                        }
                                        MissingClassification::AsUnsynced => {
                                            if !staged.is_file_deleted(&relative_file_path) {
                                                dir_state.unsynced.add_file(relative_file_path);
                                            }
                                        }
                                    }
                                }
                            } else if let EMerkleTreeNode::Directory(dir) = &child.node {
                                let dir_path = dir_state.full_path.join(dir.name());
                                let relative_dir_path = dir_state.search_node_path.join(dir.name());
                                if !dir_path.exists() {
                                    // Only do this for non-existent dirs — existing dirs
                                    // already trigger a queued EnterDir.
                                    let dir_deleted = staged.is_dir_deleted(&relative_dir_path);
                                    let should_record = match missing {
                                        MissingClassification::AsRemoved => true,
                                        MissingClassification::AsUnsynced => !dir_deleted,
                                    };
                                    if should_record {
                                        let mut count: usize = 0;
                                        count_removed_entries(
                                            repo,
                                            &relative_dir_path,
                                            dir.hash(),
                                            gitignore,
                                            &mut count,
                                        )?;
                                        *total_entries += count;
                                        match missing {
                                            MissingClassification::AsRemoved => {
                                                dir_state.removed.insert(relative_dir_path);
                                            }
                                            MissingClassification::AsUnsynced => {
                                                dir_state
                                                    .unsynced
                                                    .add_dir(relative_dir_path, count);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Merge accumulated state into the parent (now top of path_stack), or
                // return if this was the root.
                if let Some(parent) = path_stack.last_mut() {
                    parent.untracked.merge(dir_state.untracked);
                    parent.unsynced.merge(dir_state.unsynced);
                    parent.modified.extend(dir_state.modified);
                    parent.removed.extend(dir_state.removed);
                } else {
                    return Ok(WalkOutput {
                        untracked: dir_state.untracked,
                        unsynced: dir_state.unsynced,
                        modified: dir_state.modified,
                        removed: dir_state.removed,
                    });
                }
            }
        }
    }

    // The root's `FinalizeDir` always returns above; reaching here means the root was
    // filtered out (e.g., ignored). Return empty results.
    Ok(WalkOutput::empty())
}

/// Walk every path in `opts.paths` through [`walk_status`] and aggregate the results.
/// Shared between `status_from_opts` (which uses `StagedSource::Db` + `AsRemoved`) and
/// `status_from_opts_and_staged_data` (which uses `StagedSource::Data` + `AsUnsynced`).
async fn walk_paths(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    staged: StagedSource<'_>,
    missing: MissingClassification,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    progress: &ProgressBar,
) -> Result<WalkOutput, OxenError> {
    let gitignore: Option<Gitignore> = oxenignore::create(repo);
    let mut total_entries = 0;
    let mut out = WalkOutput::empty();
    for dir in opts.paths.iter() {
        let relative_dir = util::fs::path_relative_to_dir(dir, &repo.path)?;
        let sub = walk_status(
            repo,
            opts,
            &relative_dir,
            staged,
            missing,
            dir_hashes,
            &gitignore,
            progress,
            &mut total_entries,
        )
        .await?;
        out.merge(sub);
    }
    Ok(out)
}

// Traverse the merkle tree to count removed entries under a dir node
fn count_removed_entries(
    repo: &LocalRepository,
    relative_path: &Path,
    dir_hash: &MerkleHash,
    gitignore: &Option<Gitignore>,
    removed_entries: &mut usize,
) -> Result<(), OxenError> {
    if oxenignore::is_ignored(relative_path, gitignore, true) {
        return Ok(());
    }

    let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
    if let Some(ref node) = dir_node {
        for child in repositories::tree::list_files_and_folders(node)? {
            if let EMerkleTreeNode::File(_) = &child.node {
                // Any files nodes accessed here are children of a removed dir, so they must also be removed
                *removed_entries += 1;
            } else if let EMerkleTreeNode::Directory(dir) = child.node {
                let relative_dir_path = relative_path.join(dir.name());
                count_removed_entries(
                    repo,
                    &relative_dir_path,
                    dir.hash(),
                    gitignore,
                    removed_entries,
                )?;
            }
        }
    }

    Ok(())
}

// Helper functions (implement these based on your existing code)
fn open_staged_db(
    repo: &LocalRepository,
) -> Result<Option<DBWithThreadMode<SingleThreaded>>, OxenError> {
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    if db_path.join("CURRENT").exists() {
        // Read the staged files from the staged db
        let opts = db::key_val::opts::default();
        let db: DBWithThreadMode<SingleThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), true)?;
        Ok(Some(db))
    } else {
        Ok(None)
    }
}

fn get_dir_hashes(
    repo: &LocalRepository,
    head_commit_maybe: &Option<Commit>,
) -> Result<HashMap<PathBuf, MerkleHash>, OxenError> {
    if let Some(head_commit) = head_commit_maybe {
        Ok(CommitMerkleTree::dir_hashes(repo, head_commit)?)
    } else {
        Ok(HashMap::new())
    }
}

fn maybe_get_node(
    repo: &LocalRepository,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let path = path.as_ref();
    if let Some(hash) = dir_hashes.get(path) {
        CommitMerkleTree::read_depth(repo, hash, 1)
    } else {
        CommitMerkleTree::read_file(repo, dir_hashes, path)
    }
}

fn is_staged(
    path: &Path,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
) -> Result<bool, OxenError> {
    if let Some(staged_db) = staged_db {
        let key = path.to_str().unwrap();
        if staged_db.get(key.as_bytes())?.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn in_staged_data(path: &Path, staged_data: &StagedData) -> Result<bool, OxenError> {
    if staged_data.staged_files.contains_key(path)
        || staged_data.staged_dirs.paths.contains_key(path)
    {
        return Ok(true);
    }
    Ok(false)
}

#[derive(Debug)]
struct UntrackedData {
    dirs: HashMap<PathBuf, usize>,
    files: Vec<PathBuf>,
    all_untracked: bool,
}

// TODO: After implementing this I realized that it has a lot in common with
// SummarizedStagedDirStats, and even with the StagedData struct. Since our
// status structure is probably pretty stable at this point, it might be worth
// looking into combining these structs to reduce duplication. However, we do
// handle staged and untracked data differently in a few places, so it might
// be more effort than it's worth.

impl UntrackedData {
    fn new() -> Self {
        Self {
            dirs: HashMap::new(),
            files: Vec::new(),
            all_untracked: true,
        }
    }

    fn add_dir(&mut self, path: PathBuf, count: usize) {
        // Check if this directory is a parent of any existing entries. It will
        // never be a child since we process child directories first.
        let subdirs: Vec<_> = self
            .dirs
            .keys()
            .filter(|k| k.starts_with(&path) && **k != path)
            .cloned()
            .collect();

        let total_count: usize = subdirs.iter().map(|k| self.dirs[k]).sum::<usize>() + count;

        for subdir in subdirs {
            self.dirs.remove(&subdir);
        }

        self.dirs.insert(path, total_count);
    }

    fn add_file(&mut self, file_path: PathBuf) {
        self.files.push(file_path);
    }

    fn merge(&mut self, other: UntrackedData) {
        // Since we process child directories first, we can just extend
        self.dirs.extend(other.dirs);
        self.files.extend(other.files);
        self.all_untracked = self.all_untracked && other.all_untracked;
    }
}

//
#[derive(Debug)]
struct UnsyncedData {
    dirs: HashMap<PathBuf, usize>,
    files: Vec<PathBuf>,
}

impl UnsyncedData {
    fn new() -> Self {
        Self {
            dirs: HashMap::new(),
            files: Vec::new(),
        }
    }

    fn add_dir(&mut self, path: PathBuf, count: usize) {
        // Check if this directory is a parent of any existing entries. It will
        // never be a child since we process child directories first.
        let subdirs: Vec<_> = self
            .dirs
            .keys()
            .filter(|k| k.starts_with(&path) && **k != path)
            .cloned()
            .collect();

        let total_count: usize = subdirs.iter().map(|k| self.dirs[k]).sum::<usize>() + count;

        for subdir in subdirs {
            self.dirs.remove(&subdir);
        }

        self.dirs.insert(path, total_count);
    }

    fn add_file(&mut self, file_path: PathBuf) {
        self.files.push(file_path);
    }

    fn merge(&mut self, other: UnsyncedData) {
        // Since we process child directories first, we can just extend
        self.dirs.extend(other.dirs);
        self.files.extend(other.files);
    }
}

fn maybe_get_child_node(
    path: impl AsRef<Path>,
    dir_children: &Option<HashMap<PathBuf, MerkleTreeNode>>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let Some(children) = dir_children else {
        return Ok(None);
    };

    let child = children.get(path.as_ref());
    Ok(child.cloned())
}

fn maybe_get_dir_children(
    dir_node: &Option<MerkleTreeNode>,
) -> Result<Option<HashMap<PathBuf, MerkleTreeNode>>, OxenError> {
    let Some(node) = dir_node else {
        return Ok(None);
    };

    if let EMerkleTreeNode::Directory(_) = &node.node {
        let children = repositories::tree::list_files_and_folders_map(node)?;
        Ok(Some(children))
    } else {
        Ok(None)
    }
}
