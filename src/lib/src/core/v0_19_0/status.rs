use crate::constants::OXEN_HIDDEN_DIR;
use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::oxenignore;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{
    Commit, LocalRepository, MerkleHash, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus, StagedSchema, SummarizedStagedDirStats,
};
use crate::{repositories, util};

use filetime::FileTime;
use ignore::gitignore::Gitignore;
use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::{DBWithThreadMode, IteratorMode, SingleThreaded};
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::str;
use std::time::Duration;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;

pub fn status(repo: &LocalRepository) -> Result<StagedData, OxenError> {
    status_from_dir(repo, Path::new(""))
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    if repo.is_shallow_clone() {
        return Err(OxenError::basic_str(
            "Cannot run `oxen status` on a shallow clone",
        ));
    }

    log::debug!("status_from_dir {:?}", dir.as_ref());
    let staged_db_maybe = open_staged_db(repo)?;
    let head_commit = repositories::commits::head_commit_maybe(repo)?;
    let dir_hashes = get_dir_hashes(repo, &head_commit)?;
    let relative_dir = util::fs::path_relative_to_dir(dir.as_ref(), &repo.path)?;

    let read_progress = ProgressBar::new_spinner();
    read_progress.set_style(ProgressStyle::default_spinner());
    read_progress.enable_steady_tick(Duration::from_millis(100));

    let (untracked, modified, removed) =
        find_changes(repo, &relative_dir, &staged_db_maybe, &dir_hashes)?;

    log::debug!("find_changes untracked: {:?}", untracked);
    log::debug!("find_changes modified: {:?}", modified);
    log::debug!("find_changes removed: {:?}", removed);

    let mut staged_data = StagedData::empty();
    staged_data.untracked_dirs = untracked.dirs.into_iter().collect();
    staged_data.untracked_files = untracked.files;
    staged_data.modified_files = modified;
    staged_data.removed_files = removed;

    // Find merge conflicts
    let conflicts = repositories::merge::list_conflicts(repo)?;
    log::debug!("list_conflicts found {} conflicts", conflicts.len());
    for conflict in conflicts {
        staged_data
            .merge_conflicts
            .push(conflict.to_entry_merge_conflict());
    }

    let Some(staged_db) = staged_db_maybe else {
        log::debug!("status_from_dir no staged db, returning early");
        return Ok(staged_data);
    };

    let (dir_entries, _) = read_staged_entries_below_path(repo, &staged_db, &dir, &read_progress)?;
    // log::debug!("status_from_dir dir_entries: {:?}", dir_entries);
    read_progress.finish_and_clear();

    status_from_dir_entries(&mut staged_data, dir_entries)
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

    log::debug!("dir_entries.len(): {:?}", dir_entries.len());
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
        for entry in &entries {
            match &entry.node.node {
                EMerkleTreeNode::Directory(node) => {
                    log::debug!("dir_entries dir_node: {}", node);
                    // Cannot be removed if it's staged
                    if !staged_data.staged_dirs.contains_key(&dir) {
                        staged_data.removed_files.remove(&dir);
                    }
                }
                EMerkleTreeNode::File(node) => {
                    // TODO: It's not always added. It could be modified.
                    log::debug!("dir_entries file_node: {}", entry);
                    let file_path = PathBuf::from(&node.name);
                    if entry.status == StagedEntryStatus::Modified {
                        staged_data.modified_files.insert(file_path.clone());
                    }
                    let staged_entry = StagedEntry {
                        hash: node.hash.to_string(),
                        status: entry.status.clone(),
                    };
                    staged_data
                        .staged_files
                        .insert(file_path.clone(), staged_entry);
                    stats.num_files_staged += 1;
                    maybe_add_schemas(node, staged_data)?;

                    // Cannot be removed if it's staged
                    if staged_data.staged_files.contains_key(&file_path) {
                        staged_data.removed_files.remove(&file_path);
                        staged_data.modified_files.remove(&file_path);
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

        // Cannot be removed if it's staged
        if !staged_data.staged_dirs.contains_key(&dir) {
            staged_data.removed_files.remove(&dir);
        }

        // Empty dirs should be added to summarized_dir_stats (entries.len() == 0)
        // Otherwise we are filtering out parent dirs that were added during add
        if stats.num_files_staged > 0 || entries.is_empty() {
            summarized_dir_stats.add_stats(&stats);
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
    if let Some(GenericMetadata::MetadataTabular(m)) = &node.metadata {
        let schema = m.tabular.schema.clone();
        let path = PathBuf::from(&node.name);
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
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, u64), OxenError> {
    read_staged_entries_below_path(repo, db, Path::new(""), read_progress)
}

pub fn read_staged_entries_below_path(
    repo: &LocalRepository,
    db: &DBWithThreadMode<SingleThreaded>,
    start_path: impl AsRef<Path>,
    read_progress: &ProgressBar,
) -> Result<(HashMap<PathBuf, Vec<StagedMerkleTreeNode>>, u64), OxenError> {
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
                let entry: StagedMerkleTreeNode = rmp_serde::from_slice(&value).unwrap();
                log::debug!("read_staged_entries key {key} entry: {entry} path: {path:?}");
                let full_path = repo.path.join(path);

                if full_path.is_dir() {
                    // add the dir as a key in dir_entries
                    log::debug!("read_staged_entries adding dir {:?}", path);
                    dir_entries.entry(path.to_path_buf()).or_default();
                }

                // add the file or dir as an entry under its parent dir
                if let Some(parent) = path.parent() {
                    log::debug!(
                        "read_staged_entries adding file {:?} to parent {:?}",
                        path,
                        parent
                    );
                    dir_entries
                        .entry(parent.to_path_buf())
                        .or_default()
                        .push(entry);
                }

                total_entries += 1;
                read_progress.set_message(format!("Found {} entries", total_entries));
            }
            Err(err) => {
                log::error!("Could not get staged entry: {}", err);
            }
        }
    }

    log::debug!(
        "read_staged_entries dir_entries.len(): {:?}",
        dir_entries.len()
    );
    for (dir, entries) in dir_entries.iter() {
        log::debug!("commit dir_entries dir {:?}", dir);
        for entry in entries.iter() {
            log::debug!("\tcommit dir_entries entry {}", entry);
        }
    }

    Ok((dir_entries, total_entries))
}

fn find_changes(
    repo: &LocalRepository,
    relative_dir: impl AsRef<Path>,
    staged_db: &Option<DBWithThreadMode<SingleThreaded>>,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
) -> Result<(UntrackedData, HashSet<PathBuf>, HashSet<PathBuf>), OxenError> {
    let relative_dir = relative_dir.as_ref();
    log::debug!("find_changes dir: {:?}", relative_dir);
    let mut untracked = UntrackedData::new();
    let mut modified = HashSet::new();
    let mut removed = HashSet::new();
    let gitignore = oxenignore::create(repo);

    let full_dir = repo.path.join(relative_dir);

    let Ok(entries) = std::fs::read_dir(&full_dir) else {
        return Err(OxenError::basic_str(format!(
            "Could not read dir {:?}",
            full_dir
        )));
    };
    let mut untracked_count = 0;
    let dir_node = get_dir_node(repo, dir_hashes, relative_dir)?;

    for entry in entries.flatten() {
        let path = entry.path();
        let relative_path = util::fs::path_relative_to_dir(&path, &repo.path)?;

        if is_ignored(&relative_path, &gitignore, path.is_dir()) {
            continue;
        }

        if path.is_dir() {
            // If it's a directory, recursively find changes below it
            let (sub_untracked, sub_modified, sub_removed) =
                find_changes(repo, &relative_path, staged_db, dir_hashes)?;
            untracked.merge(sub_untracked);
            modified.extend(sub_modified);
            removed.extend(sub_removed);
        } else if is_staged(&relative_path, staged_db)? {
            // check this after handling directories, because we still need to recurse into staged directories
            untracked.all_untracked = false;
            continue;
        } else if let Some(node) =
            maybe_get_child_node(relative_path.file_name().unwrap(), &dir_node)?
        {
            // If we have a dir node, it's either tracked (clean) or modified
            // Either way, we know the directory is not all_untracked
            untracked.all_untracked = false;
            let is_modified = is_modified(&node, &path)?;
            log::debug!("is_modified {} {:?}", is_modified, relative_path);
            if is_modified {
                modified.insert(relative_path.clone());
            }
        } else {
            // If it's none of the above conditions, then it's untracked
            untracked.add_file(relative_path.clone());
            untracked_count += 1;
        }
    }

    // Only add the untracked directory if it's not the root directory
    // and it's not staged
    if untracked.all_untracked
        && relative_dir != Path::new("")
        && !is_staged(relative_dir, staged_db)?
    {
        untracked.add_dir(relative_dir.to_path_buf(), untracked_count);
        // Clear individual files as they're now represented by the directory
        untracked.files.clear();
    }

    // Check for removed files
    if let Some(dir_hash) = dir_hashes.get(relative_dir) {
        let dir_node = CommitMerkleTree::read_depth(repo, dir_hash, 2)?;
        if let Some(node) = dir_node {
            for child in CommitMerkleTree::node_files_and_folders(&node)? {
                if let EMerkleTreeNode::File(file) = &child.node {
                    let file_path = full_dir.join(&file.name);
                    if !file_path.exists() {
                        removed.insert(relative_dir.join(&file.name));
                    }
                } else if let EMerkleTreeNode::Directory(dir) = &child.node {
                    let dir_path = full_dir.join(&dir.name);
                    if !dir_path.exists() {
                        removed.insert(relative_dir.join(&dir.name));
                    }
                }
            }
        }
    }

    Ok((untracked, modified, removed))
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

fn get_dir_node(
    repo: &LocalRepository,
    dir_hashes: &HashMap<PathBuf, MerkleHash>,
    dir: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let dir = dir.as_ref();
    if let Some(hash) = dir_hashes.get(dir) {
        CommitMerkleTree::read_depth(repo, hash, 2)
    } else {
        Ok(None)
    }
}

fn is_ignored(path: &Path, gitignore: &Option<Gitignore>, is_dir: bool) -> bool {
    // Skip hidden .oxen files
    if path.starts_with(OXEN_HIDDEN_DIR) {
        return true;
    }
    if let Some(gitignore) = gitignore {
        if gitignore.matched(path, is_dir).is_ignore() {
            return true;
        }
    }
    false
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

fn maybe_get_child_node(
    path: impl AsRef<Path>,
    dir_node: &Option<MerkleTreeNode>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let Some(node) = dir_node else {
        return Ok(None);
    };

    node.get_by_path(path)
}

fn is_modified(node: &MerkleTreeNode, full_path: impl AsRef<Path>) -> Result<bool, OxenError> {
    // Check the file timestamps vs the commit timestamps
    let metadata = std::fs::metadata(&full_path)?;
    let mtime = FileTime::from_last_modification_time(&metadata);

    let (node_modified_seconds, node_modified_nanoseconds) = match &node.node {
        EMerkleTreeNode::File(file) => {
            let node_modified_seconds = file.last_modified_seconds;
            let node_modified_nanoseconds = file.last_modified_nanoseconds;
            (node_modified_seconds, node_modified_nanoseconds)
        }
        EMerkleTreeNode::Directory(dir) => {
            let node_modified_seconds = dir.last_modified_seconds;
            let node_modified_nanoseconds = dir.last_modified_nanoseconds;
            (node_modified_seconds, node_modified_nanoseconds)
        }
        _ => {
            return Err(OxenError::basic_str("unsupported node type"));
        }
    };

    if node_modified_nanoseconds != mtime.nanoseconds()
        || node_modified_seconds != mtime.unix_seconds()
    {
        log::debug!(
            "is_modified path {:?} modified time mismatch {:?} vs {:?} || {:?} vs {:?}",
            full_path.as_ref(),
            node_modified_seconds,
            mtime.unix_seconds(),
            node_modified_nanoseconds,
            mtime.nanoseconds()
        );
        return Ok(true);
    }

    Ok(false)
}
