use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::oxenignore;
use crate::core::staged::staged_db_manager::with_staged_db_manager;
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
use std::time::Instant;

use crate::core::v_latest::index::CommitMerkleTree;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use oxen_watcher::client::WatcherClient;
use oxen_watcher::tree::{FileMetadata, FileSystemTree, NodeType};

pub fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    status_from_dir(repo, &repo.path)
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    let opts = StagedDataOpts {
        paths: vec![dir.as_ref().to_path_buf()],
        ..StagedDataOpts::default()
    };
    status_from_opts(repo, &opts)
}

/// Status with optional watcher cache support
pub async fn status_with_cache(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    use_cache: bool,
) -> Result<StagedData, OxenError> {
    // If cache is enabled, try to use the watcher
    if use_cache {
        log::debug!("Attempting to use watcher cache for status");
        let start = Instant::now();

        // Try to connect to watcher
        if let Some(client) = WatcherClient::connect(&repo.path).await {
            log::info!("Connected to watcher");

            // Try to get filesystem tree from watcher
            match client.get_tree(None).await {
                Ok(fs_tree) => {
                    let tree_time = start.elapsed();
                    log::info!("Got tree from watcher in {} ms", tree_time.as_millis());
                    return compute_status_from_tree(repo, opts, fs_tree);
                }
                Err(e) => {
                    log::warn!("Failed to get tree from watcher: {}", e);
                    // Fall through to regular status
                }
            }
        } else {
            log::warn!("Could not connect to watcher");
        }
    } else {
        log::debug!("Cache disabled, using direct scan");
    }

    // Fallback to regular status
    status_from_opts(repo, opts)
}

pub fn status_from_opts(
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

    let mut total_entries = 0;

    let mut untracked = UntrackedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();

    for dir in opts.paths.iter() {
        let relative_dir = util::fs::path_relative_to_dir(dir, &repo.path)?;
        let (sub_untracked, sub_modified, sub_removed) = find_changes(
            repo,
            opts,
            &relative_dir,
            &staged_db_maybe,
            &dir_hashes,
            &read_progress,
            &mut total_entries,
        )?;
        untracked.merge(sub_untracked);
        modified.extend(sub_modified);
        removed.extend(sub_removed);
    }

    log::debug!("find_changes untracked: {untracked:?}");
    log::debug!("find_changes modified: {modified:?}");
    log::debug!("find_changes removed: {removed:?}");

    let mut staged_data = StagedData::empty();
    staged_data.untracked_dirs = untracked.dirs.into_iter().collect();
    staged_data.untracked_files = untracked.files;
    staged_data.modified_files = modified;
    staged_data.removed_files = removed;

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
pub fn status_from_opts_and_staged_data(
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

    let mut total_entries = 0;

    let mut untracked = UntrackedData::new();
    let mut unsynced = UnsyncedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();

    for dir in opts.paths.iter() {
        let relative_dir = util::fs::path_relative_to_dir(dir, &repo.path)?;
        let (sub_untracked, sub_unsynced, sub_modified, sub_removed) = find_local_changes(
            repo,
            opts,
            &relative_dir,
            staged_data,
            &dir_hashes,
            &read_progress,
            &mut total_entries,
        )?;

        untracked.merge(sub_untracked);
        unsynced.merge(sub_unsynced);
        modified.extend(sub_modified);
        removed.extend(sub_removed);
    }

    log::debug!("find_changes untracked: {untracked:?}");
    log::debug!("find_changes unsynced: {unsynced:?}");
    log::debug!("find_changes modified: {modified:?}");
    log::debug!("find_changes removed: {removed:?}");

    staged_data.untracked_dirs = untracked.dirs.into_iter().collect();
    staged_data.untracked_files = untracked.files;
    staged_data.unsynced_dirs = unsynced.dirs.into_iter().collect();
    staged_data.unsynced_files = unsynced.files;
    staged_data.modified_files = modified;
    staged_data.removed_files = removed;

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
    with_staged_db_manager(repo, |staged_db_manager| {
        staged_db_manager.read_staged_entries_below_path(start_path, read_progress)
    })
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

fn find_changes(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    search_node_path: impl AsRef<Path>,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    progress: &ProgressBar,
    total_entries: &mut usize,
) -> Result<(UntrackedData, HashSet<PathBuf>, HashSet<PathBuf>), OxenError> {
    let search_node_path = search_node_path.as_ref();
    let full_path = repo.path.join(search_node_path);
    let is_dir = full_path.is_dir();
    log::debug!("find_changes search_node_path: {search_node_path:?} full_path: {full_path:?}");

    if let Some(ignore) = &opts.ignore {
        if ignore.contains(search_node_path) || ignore.contains(&full_path) {
            return Ok((UntrackedData::new(), HashSet::new(), HashSet::new()));
        }
    }

    let mut untracked = UntrackedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();
    let gitignore: Option<Gitignore> = oxenignore::create(repo);

    let mut entries: Vec<(PathBuf, bool, Result<std::fs::Metadata, OxenError>)> = Vec::new();
    if is_dir {
        let Ok(dir_entries) = std::fs::read_dir(&full_path) else {
            return Err(OxenError::basic_str(format!(
                "Could not read dir {full_path:?}"
            )));
        };
        let new_entries: Vec<_> = dir_entries
            .par_bridge()
            .filter_map(|res| match res {
                Ok(entry) => {
                    let path = entry.path();
                    let is_dir = path.is_dir();
                    let md = match entry.metadata() {
                        Ok(md) => Ok(md),
                        Err(err) => Err(OxenError::basic_str(err.to_string())),
                    };
                    Some((path, is_dir, md))
                }
                Err(err) => {
                    log::debug!("Skipping unreadable entry: {err}");
                    None
                }
            })
            .collect();
        entries.extend(new_entries);
    } else {
        let metadata = util::fs::metadata(&full_path);
        entries.push((full_path.to_owned(), false, metadata));
    }
    let mut untracked_count = 0;
    let search_node = maybe_get_node(repo, dir_hashes, search_node_path)?;
    let dir_children = maybe_get_dir_children(&search_node)?;

    for (path, is_dir, metadata) in entries {
        progress.set_message(format!(
            "🐂 checking ({total_entries} files) scanning {search_node_path:?}"
        ));
        *total_entries += 1;
        let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;
        let node_path = util::fs::path_relative_to_dir(&relative_path, search_node_path)?;
        log::debug!(
            "find_changes entry relative_path: {relative_path:?} in node_path {node_path:?} search_node_path: {search_node_path:?}"
        );

        if oxenignore::is_ignored(&relative_path, &gitignore, is_dir) {
            continue;
        }

        if is_dir {
            log::debug!("find_changes entry is a directory {path:?}");
            // If it's a directory, recursively find changes below it
            let (sub_untracked, sub_modified, sub_removed) = find_changes(
                repo,
                opts,
                &relative_path,
                staged_db,
                dir_hashes,
                progress,
                total_entries,
            )?;
            untracked.merge(sub_untracked);
            modified.extend(sub_modified);
            removed.extend(sub_removed)
        } else if is_staged(&relative_path, staged_db)? {
            log::debug!("find_changes entry is staged {path:?}");
            // check this after handling directories, because we still need to recurse into staged directories
            untracked.all_untracked = false;
            continue;
        } else if let Some(node) = maybe_get_child_node(&node_path, &dir_children)? {
            log::debug!("find_changes entry is a child node {path:?}");
            // If we have a dir node, it's either tracked (clean) or modified
            // Either way, we know the directory is not all_untracked
            untracked.all_untracked = false;
            if let EMerkleTreeNode::File(file_node) = &node.node {
                let is_modified =
                    util::fs::is_modified_from_node_with_metadata(&path, file_node, metadata)?;
                log::debug!("is_modified {is_modified} {relative_path:?}");
                if is_modified {
                    modified.insert(relative_path.clone());
                }
            }
        } else {
            log::debug!("find_changes entry is not a child node {path:?}");
            // If it's none of the above conditions
            // then check if it's untracked or modified
            let mut found_file = false;
            if let Some(search_node) = &search_node {
                if let EMerkleTreeNode::File(file_node) = &search_node.node {
                    found_file = true;
                    if util::fs::is_modified_from_node_with_metadata(&path, file_node, metadata)? {
                        modified.insert(relative_path.clone());
                    }
                }
            }
            log::debug!("find_changes found_file {found_file:?} {path:?}");

            if !found_file {
                untracked.add_file(relative_path.clone());
                untracked_count += 1;
            }
        }
    }

    // Only add the untracked directory if it's not the root directory
    // and it's not staged or committed
    if untracked.all_untracked
        && search_node_path != Path::new("")
        && !is_staged(search_node_path, staged_db)?
        && is_dir
        && search_node.is_none()
    {
        untracked.add_dir(search_node_path.to_path_buf(), untracked_count);
        // Clear individual files as they're now represented by the directory
        untracked.files.clear();
    }

    // Check for removed files
    if let Some(dir_hash) = dir_hashes.get(search_node_path) {
        // if we have subtree paths, don't check for removed files that are outside of the subtree
        if let Some(subtree_paths) = repo.subtree_paths() {
            if !subtree_paths.contains(&search_node_path.to_path_buf()) {
                return Ok((untracked, modified, removed));
            }

            if subtree_paths.len() == 1 && subtree_paths[0] == PathBuf::from("") {
                // If the subtree is the root, we need to check for removed files in the root
                let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
                if let Some(node) = dir_node {
                    for child in CommitMerkleTree::node_files_and_folders(&node)? {
                        if let EMerkleTreeNode::File(file_node) = &child.node {
                            let file_path = full_path.join(file_node.name());
                            if !file_path.exists() {
                                removed.insert(search_node_path.join(file_node.name()));
                            }
                        }
                    }
                }
                return Ok((untracked, modified, removed));
            }
        }

        let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
        if let Some(node) = dir_node {
            for child in CommitMerkleTree::node_files_and_folders(&node)? {
                if let EMerkleTreeNode::File(file_node) = &child.node {
                    let file_path = full_path.join(file_node.name());
                    if !file_path.exists() {
                        removed.insert(search_node_path.join(file_node.name()));
                    }
                } else if let EMerkleTreeNode::Directory(dir) = &child.node {
                    let dir_path = full_path.join(dir.name());
                    let relative_dir_path = search_node_path.join(dir.name());
                    if !dir_path.exists() {
                        // Only call this for non-existant dirs, because existant dirs already trigger a find_changes call

                        let mut count: usize = 0;
                        count_removed_entries(
                            repo,
                            &relative_dir_path,
                            dir.hash(),
                            &gitignore,
                            &mut count,
                        )?;

                        *total_entries += count;
                        removed.insert(relative_dir_path);
                    }
                }
            }
        }
    }

    Ok((untracked, modified, removed))
}

fn find_local_changes(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    search_node_path: impl AsRef<Path>,
    staged_data: &StagedData,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    progress: &ProgressBar,
    total_entries: &mut usize,
) -> Result<
    (
        UntrackedData,
        UnsyncedData,
        HashSet<PathBuf>,
        HashSet<PathBuf>,
    ),
    OxenError,
> {
    let search_node_path = search_node_path.as_ref();
    let full_path = repo.path.join(search_node_path);
    let is_dir = full_path.is_dir();

    log::debug!("find_changes search_node_path: {search_node_path:?} full_path: {full_path:?}");

    if let Some(ignore) = &opts.ignore {
        if ignore.contains(search_node_path) || ignore.contains(&full_path) {
            return Ok((
                UntrackedData::new(),
                UnsyncedData::new(),
                HashSet::new(),
                HashSet::new(),
            ));
        }
    }

    let mut untracked = UntrackedData::new();
    let mut unsynced = UnsyncedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();

    let gitignore: Option<Gitignore> = oxenignore::create(repo);

    let mut entries: Vec<(PathBuf, bool, std::fs::Metadata)> = Vec::new();
    if is_dir {
        let Ok(dir_entries) = std::fs::read_dir(&full_path) else {
            return Err(OxenError::basic_str(format!(
                "Could not read dir {full_path:?}"
            )));
        };
        let metadata: Vec<_> = dir_entries
            .par_bridge()
            .map(|entry| {
                let entry = entry?;
                let path = entry.path();
                let metadata = entry.metadata()?;
                let is_dir = metadata.is_dir();
                Ok((path, is_dir, metadata))
            })
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        // metadata.sort_by_key(|(path, _, _)| path.to_path_buf());
        entries.extend(metadata);
    } else {
        let metadata = util::fs::metadata(&full_path)?;
        entries.push((full_path.to_owned(), false, metadata));
    }
    let mut untracked_count = 0;
    let search_node = maybe_get_node(repo, dir_hashes, search_node_path)?;
    let dir_children = maybe_get_dir_children(&search_node)?;

    for (path, is_dir, _) in entries {
        progress.set_message(format!(
            "🐂 checking ({total_entries} files) scanning {search_node_path:?}"
        ));
        *total_entries += 1;
        let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;
        let node_path = util::fs::path_relative_to_dir(&relative_path, search_node_path)?;
        log::debug!(
            "find_changes entry relative_path: {relative_path:?} in node_path {node_path:?} search_node_path: {search_node_path:?}"
        );

        if oxenignore::is_ignored(&relative_path, &gitignore, is_dir) {
            continue;
        }

        if is_dir {
            log::debug!("find_changes entry is a directory {path:?}");
            // If it's a directory, recursively find changes below it
            let (sub_untracked, sub_unsynced, sub_modified, sub_removed) = find_local_changes(
                repo,
                opts,
                &relative_path,
                staged_data,
                dir_hashes,
                progress,
                total_entries,
            )?;
            untracked.merge(sub_untracked);
            unsynced.merge(sub_unsynced);
            modified.extend(sub_modified);
            removed.extend(sub_removed);
        } else if in_staged_data(&relative_path, staged_data)? {
            log::debug!("find_changes entry is staged {path:?}");
            // check this after handling directories, because we still need to recurse into staged directories
            untracked.all_untracked = false;
            continue;
        } else if let Some(node) = maybe_get_child_node(&node_path, &dir_children)? {
            log::debug!("find_changes entry is a child node {path:?}");
            // If we have a dir node, it's either tracked (clean) or modified
            // Either way, we know the directory is not all_untracked
            untracked.all_untracked = false;
            if let EMerkleTreeNode::File(file_node) = &node.node {
                let is_modified = util::fs::is_modified_from_node(&path, file_node)?;
                log::debug!("is_modified {is_modified} {relative_path:?}");
                if is_modified {
                    modified.insert(relative_path.clone());
                }
            }
        } else {
            log::debug!("find_changes entry is not a child node {path:?}");
            // If it's none of the above conditions
            // then check if it's untracked or modified
            let mut found_file = false;
            if let Some(search_node) = &search_node {
                if let EMerkleTreeNode::File(file_node) = &search_node.node {
                    found_file = true;
                    if util::fs::is_modified_from_node(&path, file_node)? {
                        modified.insert(relative_path.clone());
                    }
                }
            }
            log::debug!("find_changes found_file {found_file:?} {path:?}");

            if !found_file {
                untracked.add_file(relative_path.clone());
                untracked_count += 1;
            }
        }
    }

    // Only add the untracked directory if it's not the root directory
    // and it's not staged or committed
    if untracked.all_untracked
        && search_node_path != Path::new("")
        && !in_staged_data(search_node_path, staged_data)?
        && is_dir
        && search_node.is_none()
    {
        untracked.add_dir(search_node_path.to_path_buf(), untracked_count);
        // Clear individual files as they're now represented by the directory
        untracked.files.clear();
    }

    // Check for unsynced files
    // TODO: Distinguish 'removed files' from unsynced
    if let Some(dir_hash) = dir_hashes.get(search_node_path) {
        // if we have subtree paths, don't check for removed files that are outside of the subtree
        if let Some(subtree_paths) = repo.subtree_paths() {
            if !subtree_paths.contains(&search_node_path.to_path_buf()) {
                return Ok((untracked, unsynced, modified, removed));
            }

            if subtree_paths.len() == 1 && subtree_paths[0] == PathBuf::from("") {
                // If the subtree is the root, we need to check for removed files in the root
                let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
                if let Some(node) = dir_node {
                    for child in CommitMerkleTree::node_files_and_folders(&node)? {
                        if let EMerkleTreeNode::File(file_node) = &child.node {
                            let file_path = full_path.join(file_node.name());
                            if !file_path.exists() && !unsynced.files.contains(&file_path) {
                                removed.insert(search_node_path.join(file_node.name()));
                            }
                        }
                    }
                }
                return Ok((untracked, unsynced, modified, removed));
            }
        }

        let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 1)?;
        if let Some(node) = dir_node {
            for child in CommitMerkleTree::node_files_and_folders(&node)? {
                if let EMerkleTreeNode::File(file_node) = &child.node {
                    let file_path = full_path.join(file_node.name());

                    if !file_path.exists()
                        && !staged_data
                            .staged_files
                            .contains_key(&search_node_path.join(file_node.name()))
                    {
                        unsynced.add_file(search_node_path.join(file_node.name()));
                    }
                } else if let EMerkleTreeNode::Directory(dir) = &child.node {
                    let dir_path = full_path.join(dir.name());
                    let relative_dir_path = search_node_path.join(dir.name());
                    if !dir_path.exists()
                        && !staged_data
                            .staged_dirs
                            .paths
                            .contains_key(&relative_dir_path)
                    {
                        // Only call this for non-existant dirs, because existant dirs already trigger a find_changes call
                        let mut count: usize = 0;
                        count_removed_entries(
                            repo,
                            &relative_dir_path,
                            dir.hash(),
                            &gitignore,
                            &mut count,
                        )?;

                        *total_entries += count;
                        unsynced.add_dir(relative_dir_path, count);
                    }
                }
            }
        }
    }

    Ok((untracked, unsynced, modified, removed))
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
        for child in CommitMerkleTree::node_files_and_folders(node)? {
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

/// Information about a tracked file from the committed version
#[derive(Debug, Clone)]
struct TrackedFileInfo {
    hash: String,
    size: u64,
    mtime: i64,
}

/// Compute status using cached filesystem tree from watcher
fn compute_status_from_tree(
    repo: &LocalRepository,
    opts: &StagedDataOpts,
    fs_tree: FileSystemTree,
) -> Result<StagedData, OxenError> {
    let start = Instant::now();
    let mut staged_data = StagedData::empty();

    // 1. Get tracked files from HEAD commit
    log::debug!("Getting tracked files from HEAD commit");
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let tracked_files = get_tracked_files_map(repo, &head_commit)?;
    let tracked_dirs = get_tracked_dirs_from_files(&tracked_files);

    // 2. Load oxenignore
    let gitignore = oxenignore::create(repo);

    // 3. Process filesystem tree to find untracked and modified files
    let mut untracked_files = HashSet::new();
    let mut untracked_dirs_files: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
    let mut dirs_with_tracked_files: HashSet<PathBuf> = HashSet::new();

    // Iterate through all files in the tree
    log::debug!("Finding changes from cached tree");
    for (path, metadata) in iter_tree_files(&fs_tree) {
        // Apply path filtering if specified
        if !opts.paths.is_empty() {
            let matches = opts.paths.iter().any(|p| {
                let rel_p =
                    util::fs::path_relative_to_dir(p, &repo.path).unwrap_or_else(|_| p.clone());
                path.starts_with(&rel_p) || rel_p.as_os_str().is_empty()
            });
            if !matches {
                continue;
            }
        }

        // Apply oxenignore
        if oxenignore::is_ignored(&path, &gitignore, false) {
            continue;
        }

        if let Some(tracked_entry) = tracked_files.get(&path) {
            // File is tracked - check if modified
            if needs_rehash(tracked_entry, metadata) {
                // Hash the file to confirm modification
                let full_path = repo.path.join(&path);
                match util::hasher::hash_file_contents(&full_path) {
                    Ok(hash) if hash != tracked_entry.hash => {
                        staged_data.modified_files.insert(path.clone());
                    }
                    Err(e) => {
                        log::debug!("Could not hash file {:?}: {}", path, e);
                    }
                    _ => {} // Hash matches, file not modified
                }
            }
            // Mark all parent directories as having tracked files
            let mut parent = path.parent();
            while let Some(p) = parent {
                if p.as_os_str().is_empty() {
                    break;
                }
                dirs_with_tracked_files.insert(p.to_path_buf());
                parent = p.parent();
            }
        } else {
            // File is untracked
            untracked_files.insert(path.clone());

            // Track which directory this file belongs to
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    untracked_dirs_files
                        .entry(parent.to_path_buf())
                        .or_insert_with(Vec::new)
                        .push(path.clone());
                } else {
                    // File in root directory
                    untracked_dirs_files
                        .entry(PathBuf::new())
                        .or_insert_with(Vec::new)
                        .push(path.clone());
                }
            } else {
                // File in root directory
                untracked_dirs_files
                    .entry(PathBuf::new())
                    .or_insert_with(Vec::new)
                    .push(path.clone());
            }
        }
    }

    // 4. Process untracked directories for rollup
    log::debug!("Processing untracked directories for rollup");
    let mut final_untracked_files = Vec::new();
    let mut final_untracked_dirs = Vec::new();

    // Check staged entries to avoid rolling up directories with staged files
    let staged_db = open_staged_db(repo)?;

    for (dir_path, files_in_dir) in &untracked_dirs_files {
        // Check if this directory should be rolled up
        let should_rollup = !dir_path.as_os_str().is_empty() && // Not root directory
            !dirs_with_tracked_files.contains(dir_path) && // No tracked files in dir
            !is_staged(dir_path, &staged_db)? && // Directory not staged
            !tracked_dirs.contains(dir_path) && // Directory not tracked
            check_all_children_untracked(dir_path, &untracked_dirs_files, &dirs_with_tracked_files);

        if should_rollup {
            // Roll up into directory entry
            final_untracked_dirs.push((dir_path.clone(), files_in_dir.len()));
        } else {
            // Keep individual files
            final_untracked_files.extend(files_in_dir.clone());
        }
    }

    // 5. Find removed files and directories
    log::debug!("Finding removed files and directories");
    let mut removed_entries = HashSet::new();
    let mut processed_dirs = HashSet::new();

    // Check for removed directories first
    for dir_path in &tracked_dirs {
        if !tree_contains_path(&fs_tree, dir_path) {
            // Apply path filtering
            if !opts.paths.is_empty() {
                let matches = opts.paths.iter().any(|p| {
                    let rel_p =
                        util::fs::path_relative_to_dir(p, &repo.path).unwrap_or_else(|_| p.clone());
                    dir_path.starts_with(&rel_p)
                        || rel_p.as_os_str().is_empty()
                        || dir_path == &rel_p
                });
                if !matches {
                    continue;
                }
            }

            // Apply oxenignore
            if !oxenignore::is_ignored(dir_path, &gitignore, true) {
                // Entire directory is removed
                removed_entries.insert(dir_path.clone());
                processed_dirs.insert(dir_path.clone());
            }
        }
    }

    // Then check for individual removed files (not in removed directories)
    for (tracked_path, _) in &tracked_files {
        // Skip if file is in a removed directory
        let mut in_removed_dir = false;
        for removed_dir in &processed_dirs {
            if tracked_path.starts_with(removed_dir) {
                in_removed_dir = true;
                break;
            }
        }
        if in_removed_dir {
            continue;
        }

        if !tree_contains_path(&fs_tree, tracked_path) {
            // Apply path filtering
            if !opts.paths.is_empty() {
                let matches = opts.paths.iter().any(|p| {
                    let rel_p =
                        util::fs::path_relative_to_dir(p, &repo.path).unwrap_or_else(|_| p.clone());
                    tracked_path.starts_with(&rel_p) || rel_p.as_os_str().is_empty()
                });
                if !matches {
                    continue;
                }
            }

            // Apply oxenignore
            if !oxenignore::is_ignored(tracked_path, &gitignore, false) {
                removed_entries.insert(tracked_path.clone());
            }
        }
    }

    staged_data.untracked_files = final_untracked_files;
    staged_data.untracked_dirs = final_untracked_dirs;
    staged_data.removed_files = removed_entries;

    // 5. Merge with staged database
    log::debug!("Merging with staged database");
    merge_staged_entries(repo, &mut staged_data, opts)?;

    // 6. Find merge conflicts
    log::debug!("Finding merge conflicts");
    let conflicts = repositories::merge::list_conflicts(repo)?;
    for conflict in conflicts {
        staged_data
            .merge_conflicts
            .push(conflict.to_entry_merge_conflict());
    }

    let compute_status_time = start.elapsed();
    log::info!(
        "Computed status from cache in {} ms",
        compute_status_time.as_millis()
    );
    Ok(staged_data)
}

/// Get tracked files from the HEAD commit
fn get_tracked_files_map(
    repo: &LocalRepository,
    commit: &Option<Commit>,
) -> Result<HashMap<PathBuf, TrackedFileInfo>, OxenError> {
    let Some(commit) = commit else {
        return Ok(HashMap::new());
    };

    // Use merkle tree to get all tracked files
    let tree = CommitMerkleTree::from_commit(repo, commit)?;
    let mut tracked = HashMap::new();

    // Process from the root node
    collect_tracked_files(&tree.root, &PathBuf::new(), &mut tracked)?;

    Ok(tracked)
}

/// Recursively collect tracked files from merkle tree
fn collect_tracked_files(
    node: &MerkleTreeNode,
    current_path: &Path,
    tracked: &mut HashMap<PathBuf, TrackedFileInfo>,
) -> Result<(), OxenError> {
    match &node.node {
        EMerkleTreeNode::File(file_node) => {
            let file_path = if current_path.as_os_str().is_empty() {
                PathBuf::from(file_node.name())
            } else {
                current_path.join(file_node.name())
            };
            tracked.insert(
                file_path,
                TrackedFileInfo {
                    hash: file_node.hash().to_string(),
                    size: file_node.num_bytes(),
                    mtime: file_node.last_modified_seconds(),
                },
            );
        }
        EMerkleTreeNode::Directory(dir_node) => {
            let dir_path = if current_path.as_os_str().is_empty() && dir_node.name() == "." {
                PathBuf::new()
            } else if current_path.as_os_str().is_empty() {
                PathBuf::from(dir_node.name())
            } else {
                current_path.join(dir_node.name())
            };

            // Directory nodes have VNode children which contain the actual files
            for child in &node.children {
                if let EMerkleTreeNode::VNode(_) = &child.node {
                    // VNodes contain the actual file/directory entries
                    for vnode_child in &child.children {
                        collect_tracked_files(vnode_child, &dir_path, tracked)?;
                    }
                } else {
                    // Handle direct children (shouldn't normally happen in directories)
                    collect_tracked_files(child, &dir_path, tracked)?;
                }
            }
        }
        EMerkleTreeNode::VNode(_) => {
            // VNodes are containers, process their children
            for child in &node.children {
                collect_tracked_files(child, current_path, tracked)?;
            }
        }
        EMerkleTreeNode::Commit(_) => {
            // Commit nodes are at the root, process their children (which should be the root directory)
            for child in &node.children {
                collect_tracked_files(child, current_path, tracked)?;
            }
        }
        _ => {} // Skip other node types
    }

    Ok(())
}

/// Check if a file needs to be rehashed based on metadata
fn needs_rehash(tracked: &TrackedFileInfo, metadata: &FileMetadata) -> bool {
    // Check if file has potentially changed based on metadata
    metadata.size != tracked.size
        || metadata
            .mtime
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
            > tracked.mtime
}

/// Iterate through all files in the tree
fn iter_tree_files(tree: &FileSystemTree) -> Vec<(PathBuf, &FileMetadata)> {
    let mut files = Vec::new();
    collect_files_from_node(&tree.root, &PathBuf::new(), &mut files);
    files
}

/// Recursively collect files from a tree node
fn collect_files_from_node<'a>(
    node: &'a oxen_watcher::tree::TreeNode,
    current_path: &Path,
    files: &mut Vec<(PathBuf, &'a FileMetadata)>,
) {
    let node_path = if current_path.as_os_str().is_empty() && node.name == "." {
        PathBuf::new()
    } else if current_path.as_os_str().is_empty() {
        PathBuf::from(&node.name)
    } else {
        current_path.join(&node.name)
    };

    match &node.node_type {
        NodeType::File(metadata) => {
            // Don't include the root "." path for files
            if !node_path.as_os_str().is_empty() || node.name != "." {
                files.push((node_path, metadata));
            }
        }
        NodeType::Directory => {
            // Recursively collect from children
            for child in node.children.values() {
                collect_files_from_node(child, &node_path, files);
            }
        }
    }
}

/// Check if the tree contains a specific path
fn tree_contains_path(tree: &FileSystemTree, path: &Path) -> bool {
    tree.get_node(path).is_some()
}

/// Get all tracked directories from the set of tracked files
fn get_tracked_dirs_from_files(
    tracked_files: &HashMap<PathBuf, TrackedFileInfo>,
) -> HashSet<PathBuf> {
    let mut dirs = HashSet::new();
    for path in tracked_files.keys() {
        let mut parent = path.parent();
        while let Some(p) = parent {
            if p.as_os_str().is_empty() {
                break;
            }
            dirs.insert(p.to_path_buf());
            parent = p.parent();
        }
    }
    dirs
}

/// Check if all children of a directory are untracked
fn check_all_children_untracked(
    dir_path: &Path,
    untracked_dirs_files: &HashMap<PathBuf, Vec<PathBuf>>,
    dirs_with_tracked_files: &HashSet<PathBuf>,
) -> bool {
    // Check if any subdirectory has tracked files
    for (other_dir, _) in untracked_dirs_files {
        if other_dir != dir_path && other_dir.starts_with(dir_path) {
            if dirs_with_tracked_files.contains(other_dir) {
                return false;
            }
        }
    }
    true
}

/// Merge staged entries from the database
fn merge_staged_entries(
    repo: &LocalRepository,
    staged_data: &mut StagedData,
    opts: &StagedDataOpts,
) -> Result<(), OxenError> {
    // Read staged database entries
    let Some(staged_db) = open_staged_db(repo)? else {
        return Ok(());
    };

    let read_progress = ProgressBar::hidden();
    let mut dir_entries = HashMap::new();

    if !opts.paths.is_empty() {
        for path in &opts.paths {
            let (entries, _) =
                read_staged_entries_below_path(repo, &staged_db, path, &read_progress)?;
            dir_entries.extend(entries);
        }
    } else {
        let (entries, _) = read_staged_entries(repo, &staged_db, &read_progress)?;
        dir_entries = entries;
    }

    // Process staged entries
    status_from_dir_entries(staged_data, dir_entries)?;

    // Remove staged files from untracked list
    let staged_paths: HashSet<&PathBuf> = staged_data.staged_files.keys().collect();
    staged_data
        .untracked_files
        .retain(|p| !staged_paths.contains(&p));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use oxen_watcher::tree::TreeNode;
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::time::SystemTime;

    #[test]
    fn test_compute_status_from_tree_empty_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create an empty filesystem tree
            let fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            let opts = StagedDataOpts::default();
            let result = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Empty repo with empty tree should have no changes
            assert!(result.untracked_files.is_empty());
            assert!(result.modified_files.is_empty());
            assert!(result.removed_files.is_empty());

            Ok(())
        })
    }

    #[test]
    fn test_compute_status_from_tree_untracked_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create a tree with some untracked files
            let mut fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            // Add untracked file1.txt
            fs_tree.root.children.insert(
                "file1.txt".to_string(),
                TreeNode {
                    name: "file1.txt".to_string(),
                    path: PathBuf::from("file1.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: 100,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );

            // Add untracked file2.txt
            fs_tree.root.children.insert(
                "file2.txt".to_string(),
                TreeNode {
                    name: "file2.txt".to_string(),
                    path: PathBuf::from("file2.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: 200,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );

            let opts = StagedDataOpts::default();
            let result = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Should have 2 untracked files
            assert_eq!(result.untracked_files.len(), 2);
            assert!(result.untracked_files.contains(&PathBuf::from("file1.txt")));
            assert!(result.untracked_files.contains(&PathBuf::from("file2.txt")));
            assert!(result.modified_files.is_empty());
            assert!(result.removed_files.is_empty());

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_compute_status_from_tree_modified_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create and commit a file
            let file_path = repo.path.join("test.txt");
            test::write_txt_file_to_path(&file_path, "original content")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Initial commit")?;

            // Modify the file with different content
            test::write_txt_file_to_path(&file_path, "this is modified content now")?;

            // Create a tree with the modified file
            let mut fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            // Add the modified file to the tree with updated size and newer mtime
            fs_tree.root.children.insert(
                "test.txt".to_string(),
                TreeNode {
                    name: "test.txt".to_string(),
                    path: PathBuf::from("test.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: "this is modified content now".len() as u64,
                        mtime: SystemTime::now() + std::time::Duration::from_secs(1),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );

            let opts = StagedDataOpts::default();
            let result = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Should have 1 modified file
            assert!(result.untracked_files.is_empty());
            assert_eq!(result.modified_files.len(), 1);
            assert!(result.modified_files.contains(&PathBuf::from("test.txt")));
            assert!(result.removed_files.is_empty());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_compute_status_from_tree_removed_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create and commit a file
            let file_path = repo.path.join("test.txt");
            test::write_txt_file_to_path(&file_path, "content")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Initial commit")?;

            // Create a tree without the file (simulating deletion)
            let fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            let opts = StagedDataOpts::default();
            let result = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Should have 1 removed file
            assert!(result.untracked_files.is_empty());
            assert!(result.modified_files.is_empty());
            assert_eq!(result.removed_files.len(), 1);
            assert!(result.removed_files.contains(&PathBuf::from("test.txt")));

            Ok(())
        })
        .await
    }

    #[test]
    fn test_compute_status_from_tree_with_path_filter() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create a tree with files in different directories
            let mut fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            // Add dir1/file1.txt
            let mut dir1 = TreeNode {
                name: "dir1".to_string(),
                path: PathBuf::from("dir1"),
                node_type: NodeType::Directory,
                children: BTreeMap::new(),
            };
            dir1.children.insert(
                "file1.txt".to_string(),
                TreeNode {
                    name: "file1.txt".to_string(),
                    path: PathBuf::from("dir1/file1.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: 100,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );
            fs_tree.root.children.insert("dir1".to_string(), dir1);

            // Add dir2/file2.txt
            let mut dir2 = TreeNode {
                name: "dir2".to_string(),
                path: PathBuf::from("dir2"),
                node_type: NodeType::Directory,
                children: BTreeMap::new(),
            };
            dir2.children.insert(
                "file2.txt".to_string(),
                TreeNode {
                    name: "file2.txt".to_string(),
                    path: PathBuf::from("dir2/file2.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: 200,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );
            fs_tree.root.children.insert("dir2".to_string(), dir2);

            // Filter to only dir1
            let opts = StagedDataOpts {
                paths: vec![repo.path.join("dir1")],
                ..StagedDataOpts::default()
            };
            let result = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Should only have file from dir1 (or rolled up as directory)
            if result.untracked_dirs.len() == 1 {
                // Directory was rolled up
                assert_eq!(result.untracked_files.len(), 0);
                assert!(result
                    .untracked_dirs
                    .iter()
                    .any(|(path, count)| path == &PathBuf::from("dir1") && *count == 1));
            } else {
                // Individual files
                assert_eq!(result.untracked_files.len(), 1);
                assert!(result
                    .untracked_files
                    .contains(&PathBuf::from("dir1/file1.txt")));
            }
            assert!(!result
                .untracked_files
                .contains(&PathBuf::from("dir2/file2.txt")));

            Ok(())
        })
    }

    #[test]
    fn test_compute_status_from_tree_with_oxenignore() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create .oxenignore file
            let oxenignore_path = repo.path.join(".oxenignore");
            test::write_txt_file_to_path(&oxenignore_path, "*.log\ntemp/\n")?;

            // Create a tree with ignored and non-ignored files
            let mut fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            // Add regular file
            fs_tree.root.children.insert(
                "file.txt".to_string(),
                TreeNode {
                    name: "file.txt".to_string(),
                    path: PathBuf::from("file.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: 100,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );

            // Add ignored log file
            fs_tree.root.children.insert(
                "debug.log".to_string(),
                TreeNode {
                    name: "debug.log".to_string(),
                    path: PathBuf::from("debug.log"),
                    node_type: NodeType::File(FileMetadata {
                        size: 200,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );

            let opts = StagedDataOpts::default();
            let result = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Should only have the non-ignored file
            assert_eq!(result.untracked_files.len(), 1);
            assert!(result.untracked_files.contains(&PathBuf::from("file.txt")));
            assert!(!result.untracked_files.contains(&PathBuf::from("debug.log")));

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_status_comparison_untracked_directory_rollup() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create a directory with multiple untracked files
            let dir_path = repo.path.join("untracked_dir");
            std::fs::create_dir(&dir_path)?;
            test::write_txt_file_to_path(&dir_path.join("file1.txt"), "content1")?;
            test::write_txt_file_to_path(&dir_path.join("file2.txt"), "content2")?;
            test::write_txt_file_to_path(&dir_path.join("file3.txt"), "content3")?;

            // Get status without cache (traditional implementation)
            let opts = StagedDataOpts::default();
            let status_without_cache = status_from_opts(&repo, &opts)?;

            // Create filesystem tree matching the actual files
            let mut fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            // Add the untracked directory with files
            let mut untracked_dir_node = TreeNode {
                name: "untracked_dir".to_string(),
                path: PathBuf::from("untracked_dir"),
                node_type: NodeType::Directory,
                children: BTreeMap::new(),
            };

            for i in 1..=3 {
                let file_name = format!("file{}.txt", i);
                untracked_dir_node.children.insert(
                    file_name.clone(),
                    TreeNode {
                        name: file_name.clone(),
                        path: PathBuf::from(format!("untracked_dir/{}", file_name)),
                        node_type: NodeType::File(FileMetadata {
                            size: 8, // "content1".len()
                            mtime: SystemTime::now(),
                            is_symlink: false,
                        }),
                        children: BTreeMap::new(),
                    },
                );
            }

            fs_tree
                .root
                .children
                .insert("untracked_dir".to_string(), untracked_dir_node);

            // Get status with tree-based implementation
            let status_with_tree = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Compare results - both should roll up the directory
            assert_eq!(
                status_without_cache.untracked_dirs.len(),
                status_with_tree.untracked_dirs.len()
            );
            assert_eq!(
                status_without_cache.untracked_files.len(),
                status_with_tree.untracked_files.len()
            );

            // Should have rolled up into directory entry
            assert_eq!(status_with_tree.untracked_dirs.len(), 1);
            assert_eq!(status_with_tree.untracked_files.len(), 0);

            // Verify the directory entry
            let has_dir = status_with_tree
                .untracked_dirs
                .iter()
                .any(|(path, count)| path == &PathBuf::from("untracked_dir") && *count == 3);
            assert!(has_dir, "Should have untracked_dir with 3 files");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_comparison_mixed_tracked_untracked() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create some files and commit one of them
            let file1_path = repo.path.join("tracked.txt");
            let file2_path = repo.path.join("untracked.txt");
            test::write_txt_file_to_path(&file1_path, "tracked content")?;
            repositories::add(&repo, &file1_path).await?;
            repositories::commit(&repo, "Initial commit")?;

            // Add an untracked file
            test::write_txt_file_to_path(&file2_path, "untracked content")?;

            // Get status without cache
            let opts = StagedDataOpts::default();
            let status_without_cache = status_from_opts(&repo, &opts)?;

            // Create filesystem tree
            let mut fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            // Add both files to tree
            fs_tree.root.children.insert(
                "tracked.txt".to_string(),
                TreeNode {
                    name: "tracked.txt".to_string(),
                    path: PathBuf::from("tracked.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: "tracked content".len() as u64,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );

            fs_tree.root.children.insert(
                "untracked.txt".to_string(),
                TreeNode {
                    name: "untracked.txt".to_string(),
                    path: PathBuf::from("untracked.txt"),
                    node_type: NodeType::File(FileMetadata {
                        size: "untracked content".len() as u64,
                        mtime: SystemTime::now(),
                        is_symlink: false,
                    }),
                    children: BTreeMap::new(),
                },
            );

            // Get status with tree-based implementation
            let status_with_tree = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Compare results
            assert_eq!(
                status_without_cache.untracked_files.len(),
                status_with_tree.untracked_files.len(),
                "Untracked files count should match"
            );
            assert_eq!(
                status_without_cache.modified_files.len(),
                status_with_tree.modified_files.len(),
                "Modified files count should match"
            );

            // Verify specific expectations
            assert_eq!(status_with_tree.untracked_files.len(), 1);
            assert!(status_with_tree
                .untracked_files
                .contains(&PathBuf::from("untracked.txt")));
            assert!(!status_with_tree
                .untracked_files
                .contains(&PathBuf::from("tracked.txt")));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_status_comparison_removed_directory() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create a directory with files and commit them
            let dir_path = repo.path.join("mydir");
            std::fs::create_dir(&dir_path)?;
            test::write_txt_file_to_path(&dir_path.join("file1.txt"), "content1")?;
            test::write_txt_file_to_path(&dir_path.join("file2.txt"), "content2")?;

            repositories::add(&repo, &dir_path).await?;
            repositories::commit(&repo, "Added directory")?;

            // Delete the directory
            std::fs::remove_dir_all(&dir_path)?;

            // Get status without cache
            let opts = StagedDataOpts::default();
            let status_without_cache = status_from_opts(&repo, &opts)?;

            // Create empty filesystem tree (no files since directory is deleted)
            let fs_tree = FileSystemTree {
                root: TreeNode {
                    name: ".".to_string(),
                    path: PathBuf::from("."),
                    node_type: NodeType::Directory,
                    children: BTreeMap::new(),
                },
                last_updated: SystemTime::now(),
                scan_complete: true,
            };

            // Get status with tree-based implementation
            let status_with_tree = compute_status_from_tree(&repo, &opts, fs_tree)?;

            // Both should detect removed files
            assert_eq!(
                status_without_cache.removed_files.len(),
                status_with_tree.removed_files.len(),
                "Removed files count should match"
            );

            // Should detect the removed files or directory
            assert!(
                status_with_tree.removed_files.len() > 0,
                "Should have removed entries"
            );

            Ok(())
        })
        .await
    }
}
