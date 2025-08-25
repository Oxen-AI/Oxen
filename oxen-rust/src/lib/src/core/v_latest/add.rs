use filetime::FileTime;
use futures::stream::{self, Stream, StreamExt};
use glob::glob;
use par_stream::prelude::*;
use parking_lot::Mutex;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use walkdir::{DirEntry, WalkDir};

use indicatif::{ProgressBar, ProgressStyle};
use rmp_serde::Serializer;
use serde::Serialize;

use crate::constants::{OXEN_HIDDEN_DIR, STAGED_DIR};
use crate::core;
use crate::core::db;
use crate::core::oxenignore;
use crate::core::staged::staged_db_manager::{with_staged_db_manager, StagedDBManager};
use crate::model::merkle_tree::node::file_node::FileNodeOpts;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{Commit, EntryDataType, MerkleHash, StagedEntryStatus};
use crate::opts::RmOpts;
use crate::storage::version_store::VersionStore;
use crate::{error::OxenError, model::LocalRepository};
use crate::{repositories, util};
use ignore::gitignore::Gitignore;
use pathdiff::diff_paths;
use std::ops::AddAssign;

use crate::core::v_latest::index::CommitMerkleTree;
use crate::model::merkle_tree::node::{
    EMerkleTreeNode, FileNode, MerkleTreeNode, StagedMerkleTreeNode,
};

const FILE_BATCH_SIZE: usize = 64;

#[derive(Clone, Debug)]
pub struct FileStatus {
    pub data_path: PathBuf,
    pub status: StagedEntryStatus,
    pub hash: MerkleHash,

    pub num_bytes: u64,
    pub mtime: FileTime,
    pub previous_metadata: Option<GenericMetadata>,
    pub previous_file_node: Option<FileNode>,
}

#[derive(Clone, Debug, Default)]
pub struct CumulativeStats {
    pub total_files: usize,
    pub total_bytes: u64,
    pub data_type_counts: HashMap<EntryDataType, usize>,
}

impl AddAssign<CumulativeStats> for CumulativeStats {
    fn add_assign(&mut self, other: CumulativeStats) {
        self.total_files += other.total_files;
        self.total_bytes += other.total_bytes;
        for (data_type, count) in other.data_type_counts {
            *self.data_type_counts.entry(data_type).or_insert(0) += count;
        }
    }
}

pub async fn add<T: AsRef<Path>>(
    repo: &LocalRepository,
    paths: impl IntoIterator<Item = T>,
) -> Result<(), OxenError> {
    // Collect paths that match the glob pattern either:
    // 1. In the repo working directory (untracked or modified files)
    // 2. In the commit entry db (removed files)

    // Check if the repo is in the working tree
    let mut repo_path = repo.path.clone();
    let repo_in_working_tree = repo_path.exists();

    let mut expanded_paths: HashSet<PathBuf> = HashSet::new();
    for path in paths {
        let path_str = path
            .as_ref()
            .to_str()
            .ok_or_else(|| OxenError::basic_str("Invalid path string"))?;

        if !repo_in_working_tree {
            // If adding from outside the scope of the repo, only allow absolute paths
            if !util::fs::is_canonical(&path)? {
                return Err(OxenError::basic_str(
                    "Err: Cannot add relative paths from outside repo scope",
                ));
            }

            repo_path = util::fs::full_path_from_child_path(&repo_path, &path)?;
        }

        // TODO: At least on Windows, this is improperly case sensitive
        if util::fs::is_glob_path(path_str) {
            log::debug!("glob path: {}", path_str);
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                expanded_paths.insert(entry?);
            }

            // For removed files?
            if let Some(commit) = repositories::commits::head_commit_maybe(repo)? {
                let pattern_entries =
                    repositories::commits::search_entries(repo, &commit, path_str)?;
                log::debug!("pattern entries: {:?}", pattern_entries);
                expanded_paths.extend(pattern_entries);
            }
        } else {
            // Non-glob path
            expanded_paths.insert(path.as_ref().to_path_buf());
        }
    }

    log::debug!("final repo path: {repo_path:?}");

    // Get the version store from the repository
    let version_store = repo.version_store()?;

    // Open the staged db once at the beginning and reuse the connection
    let opts = db::key_val::opts::default();

    let db_path = repo_path.join(OXEN_HIDDEN_DIR).join(STAGED_DIR);
    log::debug!("staged_db path: {db_path:?}");

    let staged_db: Arc<DBWithThreadMode<MultiThreaded>> =
        Arc::new(DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?);

    let _stats = add_files(repo, &repo_path, &expanded_paths, staged_db, &version_store).await?;

    Ok(())
}

pub async fn add_files(
    repo: &LocalRepository,
    repo_path: &PathBuf,
    paths: &HashSet<PathBuf>, // We assume all paths provided are relative to the repo root
    staged_db: Arc<DBWithThreadMode<MultiThreaded>>,
    version_store: &Arc<dyn VersionStore>,
) -> Result<CumulativeStats, OxenError> {
    log::debug!("add files: {:?}", paths);
    let cwd = std::env::current_dir()?;

    // Start a timer
    let start = std::time::Instant::now();

    // Lookup the head commit
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };
    let excluded_hashes: HashSet<MerkleHash> = HashSet::new();
    let gitignore = oxenignore::create(repo);

    for path in paths {
        let corrected_path = match (path.is_absolute(), repo_path.is_absolute()) {
            (true, true) | (true, false) => path.clone(),
            (false, true) => repo_path.join(path),
            (false, false) => match diff_paths(repo_path, &cwd) {
                Some(correct_path) => correct_path.join(path),
                None => path.clone(),
            },
        };
        log::debug!("corrected path is {corrected_path:?}");

        if corrected_path.is_dir() {
            total += add_dir_inner(
                repo,
                repo_path,
                &maybe_head_commit,
                path.clone(),
                Arc::clone(&staged_db),
                version_store,
                excluded_hashes.clone(),
                &gitignore,
            )
            .await?;
        } else if corrected_path.is_file() {
            if oxenignore::is_ignored(&corrected_path, &gitignore, corrected_path.is_dir()) {
                continue;
            }

            let entry = add_file_inner(
                repo,
                repo_path,
                &maybe_head_commit,
                &corrected_path,
                &Arc::clone(&staged_db),
                version_store,
            )
            .await?;

            if let Some(entry) = entry {
                if let EMerkleTreeNode::File(file_node) = &entry.node.node {
                    let data_type = file_node.data_type();
                    total.total_files += 1;
                    total.total_bytes += file_node.num_bytes();
                    total
                        .data_type_counts
                        .entry(data_type.clone())
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                }
            }
        } else if corrected_path.is_symlink() {
            log::debug!("Skipping symlink: {:?}", corrected_path);
            continue;
        } else {
            log::debug!("Found nonexistent path {path:?}. Staging for removal. Recursive flag set");
            let mut opts = RmOpts::from_path(path);
            opts.recursive = true;
            core::v_latest::rm::rm_with_staged_db(paths, repo, &opts, &staged_db)?;

            // TODO: Make rm_with_staged_db return the stats of the files it removes

            return Ok(total);
        }
    }

    // Stop the timer, and round the duration to the nearest second
    let duration = Duration::from_millis(start.elapsed().as_millis() as u64);
    log::debug!("---END--- oxen add: {:?} duration: {:?}", paths, duration);

    // oxen staged?
    println!(
        "🐂 oxen added {} files ({}) in {}",
        total.total_files,
        bytesize::ByteSize::b(total.total_bytes),
        humantime::format_duration(duration)
    );

    Ok(total)
}

#[allow(clippy::too_many_arguments)]
async fn add_dir_inner(
    repo: &LocalRepository,
    repo_path: &Path,
    maybe_head_commit: &Option<Commit>,
    path: PathBuf,
    staged_db: Arc<DBWithThreadMode<MultiThreaded>>,
    version_store: &Arc<dyn VersionStore>,
    excluded_hashes: HashSet<MerkleHash>,
    gitignore: &Option<Gitignore>,
) -> Result<CumulativeStats, OxenError> {
    process_add_dir(
        repo,
        repo_path,
        maybe_head_commit,
        version_store,
        staged_db,
        path,
        excluded_hashes,
        gitignore,
    )
    .await
}

// Skip all checks on the subdirs contained in excluded_hashes
pub async fn add_dir_except(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: PathBuf,
    excluded_hashes: HashSet<MerkleHash>,
) -> Result<CumulativeStats, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    // Get the version store from the repository
    let version_store = repo.version_store()?;
    let gitignore = None;

    let repo_path = &repo.path;

    add_dir_inner(
        repo,
        repo_path,
        maybe_head_commit,
        path,
        Arc::new(staged_db),
        &version_store,
        excluded_hashes,
        &gitignore,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn process_add_dir(
    repo: &LocalRepository,
    repo_path: &Path,
    maybe_head_commit: &Option<Commit>,
    version_store: &Arc<dyn VersionStore>,
    staged_db: Arc<DBWithThreadMode<MultiThreaded>>,
    path: PathBuf,
    excluded_hashes: HashSet<MerkleHash>,
    gitignore: &Option<Gitignore>,
) -> Result<CumulativeStats, OxenError> {
    let start = std::time::Instant::now();

    let progress_1 = Arc::new(ProgressBar::new_spinner());
    progress_1.set_style(ProgressStyle::default_spinner());
    progress_1.enable_steady_tick(Duration::from_millis(100));

    use std::sync::atomic::{AtomicU64, Ordering};
    let byte_counter = Arc::new(AtomicU64::new(0));
    let added_file_counter = Arc::new(AtomicU64::new(0));
    let unchanged_file_counter = Arc::new(AtomicU64::new(0));

    let mut cumulative_stats = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    // If any dirs are excluded, get the dir_hashes map from the head commit
    let dir_hashes = match maybe_head_commit.clone() {
        Some(head_commit) => Some(CommitMerkleTree::dir_hashes(repo, &head_commit)?),
        None => None,
    };
    let dir_hashes = Arc::new(dir_hashes);
    let excluded_hashes = Arc::new(excluded_hashes.clone());

    let conflicts: HashSet<PathBuf> = repositories::merge::list_conflicts(repo)?
        .into_iter()
        .map(|conflict| conflict.merge_entry.path)
        .collect();

    // Create a stream from the entries
    let gitignore = Arc::new(gitignore.clone());
    let entries_stream = walkdir_async_stream(path.clone(), Arc::clone(&gitignore));

    // Create Arc for the async closure
    let maybe_head_commit = Arc::new(maybe_head_commit.clone());
    let repo = Arc::new(repo.clone());
    let repo_path = Arc::new(repo_path.to_path_buf());
    let conflicts = Arc::new(conflicts);
    let version_store = Arc::new(version_store.clone());
    let start = Arc::new(start);

    let progress_1_clone = Arc::clone(&progress_1);
    let byte_counter_final = Arc::clone(&byte_counter);
    let added_file_counter_final = Arc::clone(&added_file_counter);

    // parallel processing
    entries_stream
        .par_for_each(None, move |entry| {
            // Clone values into the async closure
            let maybe_head_commit = Arc::clone(&maybe_head_commit);

            let dir_hashes = Arc::clone(&dir_hashes);
            let excluded_hashes = Arc::clone(&excluded_hashes);

            let repo = Arc::clone(&repo);
            let repo_path = Arc::clone(&repo_path);
            let staged_db = Arc::clone(&staged_db);
            let byte_counter_clone = Arc::clone(&byte_counter);
            let added_file_counter_clone = Arc::clone(&added_file_counter);
            let unchanged_file_counter_clone = Arc::clone(&unchanged_file_counter);

            let conflicts = Arc::clone(&conflicts);
            let version_store = Arc::clone(&version_store);
            let progress_1 = Arc::clone(&progress_1);
            let start = Arc::clone(&start);
            let gitignore = Arc::clone(&gitignore);

            async move {
                let process_directory = move || async move {
                    let dir = entry.path();
                    let dir_path = util::fs::path_relative_to_dir(dir, &*Arc::clone(&repo_path))?;
                    // Check if the dir is excluded
                    if let Some(dir_hashes_ref) = dir_hashes.as_ref() {
                        if let Some(dir_hash) = dir_hashes_ref.get(&dir_path) {
                            if excluded_hashes.contains(dir_hash) {
                                return Ok::<(), OxenError>(());
                            }
                        }
                    }

                    let dir_node =
                        maybe_load_directory(&repo, &maybe_head_commit, &dir_path, &dir_hashes)?;

                    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
                    let dir_node = Arc::new(dir_node);

                    // Determine the status of the directory compared to HEAD
                    let dir_status = get_dir_status_compared_to_head(
                        &repo,
                        &dir_path,
                        &maybe_head_commit,
                        &dir_hashes,
                    )?;
                    // Only explicitly add the directory to staged_db if it's a new directory.
                    // If it existed in HEAD, it will be implicitly handled if its children change.
                    if dir_status == StagedEntryStatus::Added {
                        add_dir_to_staged_db(&staged_db, &dir_path, &seen_dirs)?;
                    }

                    let entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;

                    // Convert the file entries to a stream and batch them
                    let file_stream = stream::iter(entries).chunks(FILE_BATCH_SIZE);

                    file_stream
                        .par_for_each(num_cpus::get() * 2, move |batch| {
                            let repo = Arc::clone(&repo);
                            let repo_path = Arc::clone(&repo_path);
                            let staged_db = Arc::clone(&staged_db);
                            let byte_counter_clone = Arc::clone(&byte_counter_clone);
                            let added_file_counter_clone = Arc::clone(&added_file_counter_clone);
                            let unchanged_file_counter_clone =
                                Arc::clone(&unchanged_file_counter_clone);

                            let conflicts = Arc::clone(&conflicts);
                            let version_store = Arc::clone(&version_store);
                            let progress_1 = Arc::clone(&progress_1);
                            let start = Arc::clone(&start);
                            let gitignore = Arc::clone(&gitignore);

                            let seen_dirs_clone = Arc::clone(&seen_dirs);
                            let dir_node = Arc::clone(&dir_node);

                            async move {
                                for dir_entry in batch {
                                    let repo = Arc::clone(&repo);
                                    let repo_path = Arc::clone(&repo_path);
                                    let staged_db = Arc::clone(&staged_db);
                                    let byte_counter_clone = Arc::clone(&byte_counter_clone);
                                    let added_file_counter_clone =
                                        Arc::clone(&added_file_counter_clone);
                                    let unchanged_file_counter_clone =
                                        Arc::clone(&unchanged_file_counter_clone);

                                    let conflicts = Arc::clone(&conflicts);
                                    let version_store = Arc::clone(&version_store);
                                    let progress_1 = Arc::clone(&progress_1);
                                    let start = Arc::clone(&start);
                                    let gitignore = Arc::clone(&gitignore);

                                    let seen_dirs_clone = Arc::clone(&seen_dirs_clone);
                                    let dir_node = Arc::clone(&dir_node);
                                    let process_file = move || async move {
                                        log::debug!("Dir Entry is: {dir_entry:?}");
                                        let path = dir_entry.path();

                                        let total_bytes =
                                            byte_counter_clone.load(Ordering::Relaxed);
                                        let duration = start.elapsed().as_secs_f32();
                                        let mbps = (total_bytes as f32 / duration) / 1_000_000.0;

                                        progress_1.set_message(format!(
                                            "🐂 add {} files, {} unchanged ({}) {:.2} MB/s",
                                            added_file_counter_clone.load(Ordering::Relaxed),
                                            unchanged_file_counter_clone.load(Ordering::Relaxed),
                                            bytesize::ByteSize::b(total_bytes),
                                            mbps
                                        ));

                                        if path.is_dir()
                                            || oxenignore::is_ignored(
                                                &path,
                                                &gitignore,
                                                path.is_dir(),
                                            )
                                        {
                                            return Ok::<(), OxenError>(());
                                        }

                                        let file_name =
                                            &path.file_name().unwrap_or_default().to_string_lossy();
                                        let file_status =
                                            core::v_latest::add::determine_file_status(
                                                &dir_node, file_name, &path,
                                            )?;

                                        match process_add_file(
                                            &repo,
                                            &repo_path,
                                            &file_status,
                                            &staged_db,
                                            &path,
                                            &seen_dirs_clone,
                                            &conflicts,
                                        ) {
                                            Ok(Some(node)) => {
                                                version_store
                                                    .store_version_from_path(
                                                        &file_status.hash.to_string(),
                                                        &path,
                                                    )
                                                    .await?;

                                                if let EMerkleTreeNode::File(file_node) =
                                                    &node.node.node
                                                {
                                                    byte_counter_clone.fetch_add(
                                                        file_node.num_bytes(),
                                                        Ordering::Relaxed,
                                                    );
                                                    added_file_counter_clone
                                                        .fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                            Ok(None) => {
                                                unchanged_file_counter_clone
                                                    .fetch_add(1, Ordering::Relaxed);
                                            }
                                            Err(e) => {
                                                log::error!("Error adding file: {:?}", e);
                                            }
                                        }

                                        Ok::<(), OxenError>(())
                                    };

                                    // Capture errors in process_file
                                    if let Err(e) = process_file().await {
                                        log::error!("Error processing file: {}", e);
                                    }
                                }
                            }
                        })
                        .await;

                    Ok::<(), OxenError>(())
                };

                // Capture errors in process_directory
                if let Err(e) = process_directory().await {
                    log::error!("Error processing directory: {}", e);
                }
            }
        })
        .await;

    progress_1_clone.finish_and_clear();
    cumulative_stats.total_files = added_file_counter_final.load(Ordering::Relaxed) as usize;
    cumulative_stats.total_bytes = byte_counter_final.load(Ordering::Relaxed);
    Ok(cumulative_stats)
}

fn walkdir_async_stream(
    path: impl Into<PathBuf> + Send + 'static,
    gitignore: Arc<Option<Gitignore>>,
) -> impl Stream<Item = DirEntry> + Send + 'static {
    let path = path.into();
    let (tx, rx) = mpsc::channel::<DirEntry>(512);

    tokio::task::spawn_blocking(move || {
        for entry in WalkDir::new(&path)
            .into_iter()
            .filter_entry(|e| {
                e.file_type().is_dir()
                    && e.file_name() != OXEN_HIDDEN_DIR
                    && !oxenignore::is_ignored(e.path(), &gitignore, true)
            })
            .filter_map(Result::ok)
        {
            if tx.blocking_send(entry).is_err() {
                break; // downstream dropped
            }
        }
    });

    ReceiverStream::new(rx)
}

fn get_dir_status_compared_to_head(
    repo: &LocalRepository,
    dir_path: &Path, // relative to repo root
    maybe_head_commit: &Option<Commit>,
    dir_hashes: &Option<HashMap<PathBuf, MerkleHash>>,
) -> Result<StagedEntryStatus, OxenError> {
    if let (Some(dir_hashes), Some(_)) = (dir_hashes, maybe_head_commit) {
        // Check if the directory exists in the head commit's tree
        match CommitMerkleTree::dir_without_children_with_dirhash(repo, dir_path, dir_hashes)? {
            Some(_) => {
                // Directory exists in HEAD.
                Ok(StagedEntryStatus::Unmodified)
            }
            None => {
                // Directory does not exist in HEAD, so it's "Added".
                Ok(StagedEntryStatus::Added)
            }
        }
    } else {
        // No head commit, so everything is "Added".
        Ok(StagedEntryStatus::Added)
    }
}

fn maybe_load_directory(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
    dir_hashes: &Option<HashMap<PathBuf, MerkleHash>>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    if let (Some(head_commit), Some(dir_hashes)) = (maybe_head_commit, dir_hashes) {
        let dir_node =
            CommitMerkleTree::dir_with_children_from_dirhash(repo, head_commit, path, dir_hashes)?;
        Ok(dir_node)
    } else {
        Ok(None)
    }
}

pub fn get_file_node(
    dir_node: &Option<MerkleTreeNode>,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    if let Some(node) = dir_node {
        if let Some(node) = node.get_by_path(path)? {
            if let EMerkleTreeNode::File(file_node) = &node.node {
                Ok(Some(file_node.clone()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

async fn add_file_inner(
    repo: &LocalRepository,
    repo_path: &PathBuf,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    version_store: &Arc<dyn VersionStore>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, repo_path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node = CommitMerkleTree::dir_with_children(repo, head_commit, parent_path)?;
    }

    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let file_status = determine_file_status(&maybe_dir_node, &file_name, path)?;
    version_store
        .store_version_from_path(&file_status.hash.to_string(), path)
        .await?;

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    let conflicts: HashSet<PathBuf> = repositories::merge::list_conflicts(repo)?
        .into_iter()
        .map(|conflict| conflict.merge_entry.path)
        .collect();

    process_add_file(
        repo,
        repo_path,
        &file_status,
        staged_db,
        path,
        &seen_dirs,
        &conflicts,
    )
}

pub fn determine_file_status(
    maybe_dir_node: &Option<MerkleTreeNode>,
    file_name: impl AsRef<str>,  // Name of the file in the repository
    data_path: impl AsRef<Path>, // Path to the data file (maybe in the version store)
) -> Result<FileStatus, OxenError> {
    // Check if the file is already in the head commit
    let file_path = file_name.as_ref();
    let data_path = data_path.as_ref();
    log::debug!(
        "determine_file_status data_path {:?} file_name {:?}",
        data_path,
        file_path
    );
    let maybe_file_node = get_file_node(maybe_dir_node, file_path)?;
    let mut previous_oxen_metadata: Option<GenericMetadata> = None;
    // This is ugly - but makes sure we don't have to rehash the file if it hasn't changed
    let (status, hash, num_bytes, mtime) = if let Some(file_node) = &maybe_file_node {
        log::debug!(
            "got existing file_node: {} data_path {:?}",
            file_node,
            data_path
        );

        // first check if the file timestamp is different
        let metadata = util::fs::metadata(data_path)?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        previous_oxen_metadata = file_node.metadata();
        if util::fs::is_modified_from_node(data_path, file_node)? {
            log::debug!("has_different_modification_time true {}", file_node);
            let hash = util::hasher::get_hash_given_metadata(data_path, &metadata)?;
            if file_node.hash().to_u128() != hash {
                log::debug!(
                    "has_different_modification_time hash is different true {}",
                    file_node
                );
                let num_bytes = metadata.len();
                (
                    StagedEntryStatus::Modified,
                    MerkleHash::new(hash),
                    num_bytes,
                    mtime,
                )
            } else {
                (
                    StagedEntryStatus::Unmodified,
                    MerkleHash::new(hash),
                    file_node.num_bytes(),
                    mtime,
                )
            }
        } else {
            (
                StagedEntryStatus::Unmodified,
                MerkleHash::new(file_node.hash().to_u128()),
                file_node.num_bytes(),
                mtime,
            )
        }
    } else {
        let metadata = util::fs::metadata(data_path)?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        let hash = util::hasher::get_hash_given_metadata(data_path, &metadata)?;
        (
            StagedEntryStatus::Added,
            MerkleHash::new(hash),
            metadata.len(),
            mtime,
        )
    };

    Ok(FileStatus {
        data_path: data_path.to_path_buf(),
        status,
        hash,
        num_bytes,
        mtime,
        previous_metadata: previous_oxen_metadata,
        previous_file_node: maybe_file_node,
    })
}

pub fn process_add_file(
    repo: &LocalRepository,
    repo_path: &Path,         // Path to the repository
    file_status: &FileStatus, // All the metadata including if the file is added, modified, or deleted
    staged_db: &DBWithThreadMode<MultiThreaded>,
    path: &Path, // Path to the file in the repository, or path defined by the user
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
    merge_conflicts: &HashSet<PathBuf>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    log::debug!("process_add_file {:?}", path);
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
    let full_path = repo_path.join(&relative_path);

    if !full_path.is_file() {
        // If path is not canonical, we cannot recover the absolute repo path
        log::debug!("file is not a file - skipping add on {:?}", full_path);
        return Ok(Some(StagedMerkleTreeNode {
            status: StagedEntryStatus::Added,
            node: MerkleTreeNode::default_dir(),
        }));
    }

    let mut status = file_status.status.clone();
    let hash = file_status.hash;
    let num_bytes = file_status.num_bytes;
    let mtime = file_status.mtime;
    let maybe_file_node = file_status.previous_file_node.clone();
    let previous_metadata = file_status.previous_metadata.clone();

    log::debug!("status {status:?} hash {hash:?} num_bytes {num_bytes:?} mtime {mtime:?} file_node {maybe_file_node:?}");

    if let Some(_file_node) = &maybe_file_node {
        if merge_conflicts.contains(&relative_path) {
            log::debug!("merge conflict resolved: {relative_path:?}");
            status = StagedEntryStatus::Modified; // Mark as modified if there's a conflict
            repositories::merge::mark_conflict_as_resolved(repo, &relative_path)?;
        }
    }

    // Don't have to add the file to the staged db if it hasn't changed
    if status == StagedEntryStatus::Unmodified {
        log::debug!("file has not changed - skipping add");
        return Ok(None);
    }

    // Get the data type of the file
    let mime_type = util::fs::file_mime_type(path);
    let mut data_type = util::fs::datatype_from_mimetype(path, &mime_type);
    let metadata = match &previous_metadata {
        Some(previous_oxen_metadata) => {
            let df_metadata = repositories::metadata::get_file_metadata(&full_path, &data_type)?;
            maybe_construct_generic_metadata_for_tabular(
                df_metadata,
                previous_oxen_metadata.clone(),
            )
        }
        None => repositories::metadata::get_file_metadata(&full_path, &data_type)?,
    };

    // If the metadata is None, but the data type is tabular, we need to set the data type to binary
    // because this means we failed to parse the metadata from the file
    if metadata.is_none() && data_type == EntryDataType::Tabular {
        data_type = EntryDataType::Binary;
    }

    let file_extension = relative_path
        .extension()
        .unwrap_or_default()
        .to_string_lossy();
    let relative_path_str = relative_path.to_str().unwrap_or_default();
    let (hash, metadata_hash, combined_hash) = if let Some(metadata) = &metadata {
        let metadata_hash = util::hasher::get_metadata_hash(&Some(metadata.clone()))?;
        let metadata_hash = MerkleHash::new(metadata_hash);
        let combined_hash =
            util::hasher::get_combined_hash(Some(metadata_hash.to_u128()), hash.to_u128())?;
        let combined_hash = MerkleHash::new(combined_hash);
        (hash, Some(metadata_hash), combined_hash)
    } else {
        (hash, None, hash)
    };
    let file_node = FileNode::new(
        repo,
        FileNodeOpts {
            name: relative_path_str.to_string(),
            hash,
            combined_hash,
            metadata_hash,
            num_bytes,
            last_modified_seconds: mtime.unix_seconds(),
            last_modified_nanoseconds: mtime.nanoseconds(),
            data_type,
            metadata,
            mime_type: mime_type.clone(),
            extension: file_extension.to_string(),
        },
    )?;

    p_add_file_node_to_staged_db(staged_db, relative_path_str, status, &file_node, seen_dirs)
}

/// Add this function in replace of process_add_file for workspaces staged db to handle concurrent add_file calls
/// TODO: Migrate all staged db actions to use the manager
pub fn process_add_file_with_staged_db_manager(
    repo: &LocalRepository,
    repo_path: &Path,         // Path to the repository
    file_status: &FileStatus, // All the metadata including if the file is added, modified, or deleted
    path: &Path,              // Path to the file in the repository, or path defined by the user
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
    merge_conflicts: &HashSet<PathBuf>,
) -> Result<(), OxenError> {
    log::debug!("process_add_file {:?}", path);
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
    let full_path = repo_path.join(&relative_path);

    if !full_path.is_file() {
        // If it's not a file - no need to add it
        // We handle directories by traversing the parents of files below
        log::debug!("file is not a file - skipping add on {:?}", full_path);
        return Ok(());
    }

    let mut status = file_status.status.clone();
    let hash = file_status.hash;
    let num_bytes = file_status.num_bytes;
    let mtime = file_status.mtime;
    let maybe_file_node = file_status.previous_file_node.clone();
    let previous_metadata = file_status.previous_metadata.clone();

    log::debug!("status {status:?} hash {hash:?} num_bytes {num_bytes:?} mtime {mtime:?} file_node {maybe_file_node:?}");

    if let Some(_file_node) = &maybe_file_node {
        if merge_conflicts.contains(&relative_path) {
            log::debug!("merge conflict resolved: {relative_path:?}");
            status = StagedEntryStatus::Modified; // Mark as modified if there's a conflict
            repositories::merge::mark_conflict_as_resolved(repo, &relative_path)?;
        }
    }

    // Don't have to add the file to the staged db if it hasn't changed
    if status == StagedEntryStatus::Unmodified {
        log::debug!("file has not changed - skipping add");
        return Ok(());
    }

    // Get the data type of the file
    let mime_type = util::fs::file_mime_type(path);
    let mut data_type = util::fs::datatype_from_mimetype(path, &mime_type);
    let metadata = match &previous_metadata {
        Some(previous_oxen_metadata) => {
            let df_metadata = repositories::metadata::get_file_metadata(&full_path, &data_type)?;
            maybe_construct_generic_metadata_for_tabular(
                df_metadata,
                previous_oxen_metadata.clone(),
            )
        }
        None => repositories::metadata::get_file_metadata(&full_path, &data_type)?,
    };

    // If the metadata is None, but the data type is tabular, we need to set the data type to binary
    // because this means we failed to parse the metadata from the file
    if metadata.is_none() && data_type == EntryDataType::Tabular {
        data_type = EntryDataType::Binary;
    }

    let file_extension = relative_path
        .extension()
        .unwrap_or_default()
        .to_string_lossy();
    let relative_path_str = relative_path.to_str().unwrap_or_default();
    let (hash, metadata_hash, combined_hash) = if let Some(metadata) = &metadata {
        let metadata_hash = util::hasher::get_metadata_hash(&Some(metadata.clone()))?;
        let metadata_hash = MerkleHash::new(metadata_hash);
        let combined_hash =
            util::hasher::get_combined_hash(Some(metadata_hash.to_u128()), hash.to_u128())?;
        let combined_hash = MerkleHash::new(combined_hash);
        (hash, Some(metadata_hash), combined_hash)
    } else {
        (hash, None, hash)
    };
    let file_node = FileNode::new(
        repo,
        FileNodeOpts {
            name: relative_path_str.to_string(),
            hash,
            combined_hash,
            metadata_hash,
            num_bytes,
            last_modified_seconds: mtime.unix_seconds(),
            last_modified_nanoseconds: mtime.nanoseconds(),
            data_type,
            metadata,
            mime_type: mime_type.clone(),
            extension: file_extension.to_string(),
        },
    )?;

    add_file_node_to_staged_db(repo, relative_path_str, status, &file_node, seen_dirs)
}

/// Stage file node with staged db manager
pub fn add_file_node_to_staged_db(
    repo: &LocalRepository,
    relative_path: impl AsRef<Path>,
    status: StagedEntryStatus,
    file_node: &FileNode,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<(), OxenError> {
    with_staged_db_manager(repo, |staged_db_manager| {
        add_file_node_and_parent_dir(
            file_node,
            status,
            relative_path,
            staged_db_manager,
            seen_dirs,
        )?;
        Ok(())
    })
}

// seperate data path and dst path in case it's in the version store
pub fn get_status_and_add_file(
    repo: &LocalRepository,
    data_path: &Path,
    dst_path: &Path,
    staged_db_manager: &StagedDBManager,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<(), OxenError> {
    let relative_path = util::fs::path_relative_to_dir(dst_path, &repo.path)?;
    if let Some(parent) = dst_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file_name = dst_path.file_name().unwrap().to_string_lossy();
    let maybe_dir_node = None;
    let file_status =
        core::v_latest::add::determine_file_status(&maybe_dir_node, &file_name, data_path)?;
    let status = file_status.status.clone();
    // Don't have to add the file to the staged db if it hasn't changed
    if status == StagedEntryStatus::Unmodified {
        log::debug!("file has not changed - skipping add");
        return Ok(());
    }
    let file_node = generate_file_node(repo, data_path, dst_path, &file_status)?;

    // Only add the file to the staged db if it has changed
    if let Some(file_node) = file_node {
        let status = file_status.status.clone();
        add_file_node_and_parent_dir(
            &file_node,
            status,
            &relative_path,
            staged_db_manager,
            seen_dirs,
        )?;
    }
    Ok(())
}

/// Stage file node and parent dirs with staged db manager
pub fn add_file_node_and_parent_dir(
    file_node: &FileNode,
    status: StagedEntryStatus,
    relative_path: impl AsRef<Path>,
    staged_db_manager: &StagedDBManager,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<(), OxenError> {
    // Stage the file node
    staged_db_manager.upsert_file_node(&relative_path, status, file_node)?;

    // Add all the parent dirs to the staged db
    let mut parent_path = relative_path.as_ref().to_path_buf();
    while let Some(parent) = parent_path.parent() {
        parent_path = parent.to_path_buf();

        staged_db_manager.add_directory(&parent_path, seen_dirs)?;
        if parent_path == Path::new("") {
            break;
        }
    }

    Ok(())
}

pub fn generate_file_node(
    repo: &LocalRepository,
    version_path: &Path,
    dst_path: &Path,
    file_status: &FileStatus,
) -> Result<Option<FileNode>, OxenError> {
    let status = file_status.status.clone();
    let hash = file_status.hash;
    let num_bytes = file_status.num_bytes;
    let mtime = file_status.mtime;
    let maybe_file_node = file_status.previous_file_node.clone();
    let previous_metadata = file_status.previous_metadata.clone();

    // Normalize the path
    let relative_path = util::fs::path_relative_to_dir(dst_path, &repo.path)?;
    let file_extension = relative_path
        .extension()
        .unwrap_or_default()
        .to_string_lossy();
    let relative_path_str = relative_path.to_str().unwrap_or_default();
    log::debug!("status {status:?} hash {hash:?} num_bytes {num_bytes:?} mtime {mtime:?} file_node {maybe_file_node:?}");

    // version_path is where the file is stored, relative_path is the working directory path that contains the file extension
    let mime_type = util::fs::file_mime_type_from_extension(version_path, &relative_path);
    let mut data_type =
        util::fs::datatype_from_mimetype_from_extension(version_path, &relative_path, &mime_type);
    let metadata = match &previous_metadata {
        Some(previous_oxen_metadata) => {
            let df_metadata = repositories::metadata::get_file_metadata_with_extension(
                version_path,
                &data_type,
                &util::fs::file_extension(&relative_path),
            )?;
            maybe_construct_generic_metadata_for_tabular(
                df_metadata,
                previous_oxen_metadata.clone(),
            )
        }
        None => repositories::metadata::get_file_metadata_with_extension(
            version_path,
            &data_type,
            &util::fs::file_extension(&relative_path),
        )?,
    };

    // If the metadata is None, but the data type is tabular, we need to set the data type to binary
    // because this means we failed to parse the metadata from the file
    if metadata.is_none() && data_type == EntryDataType::Tabular {
        data_type = EntryDataType::Binary;
    }

    let (hash, metadata_hash, combined_hash) = if let Some(metadata) = &metadata {
        let metadata_hash = util::hasher::get_metadata_hash(&Some(metadata.clone()))?;
        let metadata_hash = MerkleHash::new(metadata_hash);
        let combined_hash =
            util::hasher::get_combined_hash(Some(metadata_hash.to_u128()), hash.to_u128())?;
        let combined_hash = MerkleHash::new(combined_hash);
        (hash, Some(metadata_hash), combined_hash)
    } else {
        (hash, None, hash)
    };
    let file_node = FileNode::new(
        repo,
        FileNodeOpts {
            name: relative_path_str.to_string(),
            hash,
            combined_hash,
            metadata_hash,
            num_bytes,
            last_modified_seconds: mtime.unix_seconds(),
            last_modified_nanoseconds: mtime.nanoseconds(),
            data_type,
            metadata,
            mime_type: mime_type.clone(),
            extension: file_extension.to_string(),
        },
    )?;
    Ok(Some(file_node))
}

pub fn maybe_construct_generic_metadata_for_tabular(
    df_metadata: Option<GenericMetadata>,
    previous_oxen_metadata: GenericMetadata,
) -> Option<GenericMetadata> {
    log::debug!(
        "maybe_construct_generic_metadata_for_tabular {:?}",
        df_metadata
    );
    log::debug!("previous_oxen_metadata {:?}", previous_oxen_metadata);

    if let Some(GenericMetadata::MetadataTabular(mut df_metadata)) = df_metadata.clone() {
        if let GenericMetadata::MetadataTabular(ref previous_oxen_metadata) = previous_oxen_metadata
        {
            // Combine the two by using previous_oxen_metadata as the source of truth for metadata,
            // but keeping df_metadata's fields

            for field in &mut df_metadata.tabular.schema.fields {
                if let Some(oxen_field) = previous_oxen_metadata
                    .tabular
                    .schema
                    .fields
                    .iter()
                    .find(|oxen_field| oxen_field.name == field.name)
                {
                    field.metadata = oxen_field.metadata.clone();
                }
            }
            return Some(GenericMetadata::MetadataTabular(df_metadata));
        }
    }
    df_metadata
}

pub fn p_add_file_node_to_staged_db(
    staged_db: &DBWithThreadMode<MultiThreaded>,
    relative_path: impl AsRef<Path>,
    status: StagedEntryStatus,
    file_node: &FileNode,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let relative_path = relative_path.as_ref();

    log::debug!(
        "writing {:?} [{:?}] to staged db: {:?}",
        relative_path,
        status,
        staged_db.path()
    );

    let staged_file_node = StagedMerkleTreeNode {
        status,
        node: MerkleTreeNode::from_file(file_node.clone()),
    };
    log::debug!("writing file: {}", staged_file_node);

    let mut buf = Vec::new();
    staged_file_node
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();

    let relative_path_str = relative_path.to_str().unwrap_or_default();
    log::debug!("writing to staged db {:?}", relative_path_str);
    staged_db.put(relative_path_str, &buf)?;

    // Add all the parent dirs to the staged db
    let mut parent_path = relative_path.to_path_buf();
    while let Some(parent) = parent_path.parent() {
        parent_path = parent.to_path_buf();

        add_dir_to_staged_db(staged_db, &parent_path, seen_dirs)?;

        if parent_path == Path::new("") {
            break;
        }
    }

    Ok(Some(staged_file_node))
}

pub fn add_dir_to_staged_db(
    staged_db: &DBWithThreadMode<MultiThreaded>,
    relative_path: impl AsRef<Path>,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<(), OxenError> {
    let relative_path = relative_path.as_ref();
    let relative_path_str = relative_path.to_str().unwrap();
    let mut seen_dirs = seen_dirs.lock();
    if !seen_dirs.insert(relative_path.to_path_buf()) {
        return Ok(());
    }

    let dir_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Added,
        node: MerkleTreeNode::default_dir_from_path(relative_path),
    };

    let mut buf = Vec::new();
    dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
    staged_db.put(relative_path_str, &buf).unwrap();
    Ok(())
}

pub fn has_different_modification_time(node: &FileNode, time: &FileTime) -> bool {
    node.last_modified_nanoseconds() != time.nanoseconds()
        || node.last_modified_seconds() != time.unix_seconds()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;

    #[tokio::test]
    async fn test_add_respects_oxenignore() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let ignored_file = "ignored.txt";
            let normal_file = "normal.txt";

            let ignored_path = repo.path.join(ignored_file);
            let normal_path = repo.path.join(normal_file);

            test::write_txt_file_to_path(&ignored_path, "This should be ignored")?;
            test::write_txt_file_to_path(&normal_path, "This should be added")?;

            // Create .oxenignore file with the ignored file pattern
            let oxenignore_path = repo.path.join(".oxenignore");
            test::write_txt_file_to_path(&oxenignore_path, ignored_file)?;

            add(&repo, vec![Path::new(&repo.path)]).await?;

            let status = repositories::status(&repo)?;

            // The normal file should be staged
            assert!(status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(normal_file)));

            // The ignored file should not be staged
            assert!(!status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(ignored_file)));

            // The oxenignore file itself should be staged
            assert!(status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(".oxenignore")));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_dot_on_committed_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let dir1 = repo.path.join("dir1");
            let dir2 = repo.path.join("dir2");
            std::fs::create_dir_all(&dir1)?;
            std::fs::create_dir_all(&dir2)?;

            let file1_1 = dir1.join("file1_1.txt");
            let file1_2 = dir1.join("file1_2.txt");
            let file2_1 = dir2.join("file2_1.txt");
            let file_root = repo.path.join("file_root.txt");

            test::write_txt_file_to_path(&file1_1, "dir1/file1_1")?;
            test::write_txt_file_to_path(&file1_2, "dir1/file1_2")?;
            test::write_txt_file_to_path(&file2_1, "dir2/file2_1")?;
            test::write_txt_file_to_path(&file_root, "file_root")?;

            add(&repo, vec![&repo.path]).await?;

            repositories::commits::commit(&repo, "Initial commit with multiple files and dirs")?;

            add(&repo, vec![&repo.path]).await?;

            let status = repositories::status(&repo);
            assert!(status.is_ok());
            let status = status.unwrap();

            assert!(status.staged_files.is_empty(), "No files should be staged");
            assert!(
                status.staged_dirs.is_empty(),
                "No directories should be staged"
            );
            assert!(
                status.untracked_files.is_empty(),
                "No files should be untracked"
            );
            assert!(
                status.untracked_dirs.is_empty(),
                "No directories should be untracked"
            );
            assert!(
                status.modified_files.is_empty(),
                "No files should be modified"
            );
            assert!(
                status.removed_files.is_empty(),
                "No files should be removed"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_respects_dir_ignore_patterns() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let dir_to_ignore = "ignored_dir";
            let normal_dir = "normal_dir";

            let ignored_dir_path = repo.path.join(dir_to_ignore);
            let normal_dir_path = repo.path.join(normal_dir);

            std::fs::create_dir(&ignored_dir_path)?;
            std::fs::create_dir(&normal_dir_path)?;

            // Add files to both directories
            test::write_txt_file_to_path(
                ignored_dir_path.join("file1.txt"),
                "This should be ignored",
            )?;
            test::write_txt_file_to_path(
                ignored_dir_path.join("file2.txt"),
                "This should also be ignored",
            )?;
            test::write_txt_file_to_path(
                normal_dir_path.join("file1.txt"),
                "This should be added",
            )?;
            test::write_txt_file_to_path(
                normal_dir_path.join("file2.txt"),
                "This should also be added",
            )?;

            let oxenignore_path = repo.path.join(".oxenignore");
            test::write_txt_file_to_path(&oxenignore_path, format!("{}/", dir_to_ignore))?;

            add(&repo, vec![Path::new(&repo.path)]).await?;

            let status = repositories::status(&repo)?;

            // Files in normal_dir should be staged
            assert!(status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(format!("{}/file1.txt", normal_dir))));
            assert!(status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(format!("{}/file2.txt", normal_dir))));

            // Files in ignored_dir should not be staged
            assert!(!status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(format!("{}/file1.txt", dir_to_ignore))));
            assert!(!status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(format!("{}/file2.txt", dir_to_ignore))));

            // The oxenignore file itself should be staged
            assert!(status
                .staged_files
                .iter()
                .any(|path| path.0.ends_with(".oxenignore")));

            Ok(())
        })
        .await
    }
}
