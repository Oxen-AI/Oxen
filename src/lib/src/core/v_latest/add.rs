use filetime::FileTime;
use glob::glob;
// use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::time::Duration;
use walkdir::WalkDir;

use indicatif::{ProgressBar, ProgressStyle};
use rmp_serde::Serializer;
use serde::Serialize;

use crate::constants::{OXEN_HIDDEN_DIR, STAGED_DIR};
use crate::core;
use crate::core::db;
use crate::model::merkle_tree::node::file_node::FileNodeOpts;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::{Commit, EntryDataType, MerkleHash, StagedEntryStatus};
use crate::opts::RmOpts;
use crate::storage::version_store::VersionStore;
use crate::{error::OxenError, model::LocalRepository};
use crate::{repositories, util};
use std::ops::AddAssign;

use crate::core::v_latest::index::CommitMerkleTree;
use crate::model::merkle_tree::node::{
    EMerkleTreeNode, FileNode, MerkleTreeNode, StagedMerkleTreeNode,
};

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

pub fn add(repo: &LocalRepository, path: impl AsRef<Path>) -> Result<(), OxenError> {
    // Collect paths that match the glob pattern either:
    // 1. In the repo working directory (untracked or modified files)
    // 2. In the commit entry db (removed files)

    let path = path.as_ref();
    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            log::debug!("glob path: {}", path_str);
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                paths.insert(entry?);
            }

            if let Some(commit) = repositories::commits::head_commit_maybe(repo)? {
                let pattern_entries =
                    repositories::commits::search_entries(repo, &commit, path_str)?;
                log::debug!("pattern entries: {:?}", pattern_entries);
                paths.extend(pattern_entries);
            }
        } else {
            // Non-glob path
            paths.insert(path.to_owned());
        }
    }

    // Get the version store from the repository
    let version_store = repo.version_store()?;

    // Open the staged db once at the beginning and reuse the connection
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
    let _stats = add_files(repo, &paths, &staged_db, &version_store)?;

    Ok(())
}

pub fn add_files(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    version_store: &Arc<dyn VersionStore>,
) -> Result<CumulativeStats, OxenError> {
    log::debug!("add files: {:?}", paths);

    // Start a timer
    let start = std::time::Instant::now();

    // Lookup the head commit
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };
    for path in paths {
        log::debug!("path is {path:?}");

        if path.is_dir() {
            total += add_dir_inner(
                repo,
                &maybe_head_commit,
                path.clone(),
                staged_db,
                version_store,
            )?;
        } else if path.is_file() {
            let entry = add_file_inner(repo, &maybe_head_commit, path, staged_db, version_store)?;
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
        } else {
            // TODO: Should there be a way to add nonexistent dirs? I think it's safer to just require rm for those?
            log::debug!(
                "Found nonexistent path {path:?}. Staging for removal. Recursive flag not set"
            );
            let mut opts = RmOpts::from_path(path);
            opts.recursive = true;
            core::v_latest::rm::rm_with_staged_db(paths, repo, &opts, staged_db)?;

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

fn add_dir_inner(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: PathBuf,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    version_store: &Arc<dyn VersionStore>,
) -> Result<CumulativeStats, OxenError> {
    process_add_dir(repo, maybe_head_commit, version_store, staged_db, path)
}

pub fn add_dir(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: PathBuf,
) -> Result<CumulativeStats, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    // Get the version store from the repository
    let version_store = repo.version_store()?;

    add_dir_inner(repo, maybe_head_commit, path, &staged_db, &version_store)
}

pub fn process_add_dir(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    version_store: &Arc<dyn VersionStore>,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    path: PathBuf,
) -> Result<CumulativeStats, OxenError> {
    let start = std::time::Instant::now();

    let progress_1 = Arc::new(ProgressBar::new_spinner());
    progress_1.set_style(ProgressStyle::default_spinner());
    progress_1.enable_steady_tick(Duration::from_millis(100));

    let path = path.clone();
    let repo = repo.clone();
    let maybe_head_commit = maybe_head_commit.clone();
    let repo_path = &repo.path.clone();

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    let byte_counter = Arc::new(AtomicU64::new(0));
    let added_file_counter = Arc::new(AtomicU64::new(0));
    let unchanged_file_counter = Arc::new(AtomicU64::new(0));
    let progress_1_clone = Arc::clone(&progress_1);

    let mut cumulative_stats = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };

    let walker = WalkDir::new(&path).into_iter();
    walker
        .filter_entry(|e| e.file_type().is_dir() && e.file_name() != OXEN_HIDDEN_DIR)
        .par_bridge()
        .try_for_each(|entry| -> Result<(), OxenError> {
            let entry = entry.unwrap();
            let dir = entry.path();

            log::debug!("Entry is: {dir:?}");

            let byte_counter_clone = Arc::clone(&byte_counter);
            let added_file_counter_clone = Arc::clone(&added_file_counter);
            let unchanged_file_counter_clone = Arc::clone(&unchanged_file_counter);

            let dir_path = util::fs::path_relative_to_dir(dir, repo_path).unwrap();
            log::debug!("path now: {dir_path:?}");

            let dir_node = maybe_load_directory(&repo, &maybe_head_commit, &dir_path).unwrap();
            let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

            // Change the closure to return a Result
            add_dir_to_staged_db(staged_db, &dir_path, &seen_dirs)?;

            let entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;

            entries.par_iter().for_each(|dir_entry| {
                log::debug!("Dir Entry is: {dir_entry:?}");
                let total_bytes = byte_counter_clone.load(Ordering::Relaxed);
                let path = dir_entry.path();
                let duration = start.elapsed().as_secs_f32();
                let mbps = (total_bytes as f32 / duration) / 1_000_000.0;

                progress_1.set_message(format!(
                    "🐂 add {} files, {} unchanged ({}) {:.2} MB/s",
                    added_file_counter_clone.load(Ordering::Relaxed),
                    unchanged_file_counter_clone.load(Ordering::Relaxed),
                    bytesize::ByteSize::b(total_bytes),
                    mbps
                ));

                let seen_dirs_clone = Arc::clone(&seen_dirs);
                match process_add_file(
                    &repo,
                    repo_path,
                    version_store,
                    staged_db,
                    &dir_node,
                    &path,
                    &seen_dirs_clone,
                ) {
                    Ok(Some(node)) => {
                        if let EMerkleTreeNode::File(file_node) = &node.node.node {
                            byte_counter_clone.fetch_add(file_node.num_bytes(), Ordering::Relaxed);
                            added_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(None) => {
                        unchanged_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        log::error!("Error adding file: {:?}", e);
                    }
                }
            });
            Ok(())
        })?;

    progress_1_clone.finish_and_clear();
    cumulative_stats.total_files = added_file_counter.load(Ordering::Relaxed) as usize;
    cumulative_stats.total_bytes = byte_counter.load(Ordering::Relaxed);
    Ok(cumulative_stats)
}

fn maybe_load_directory(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    if let Some(head_commit) = maybe_head_commit {
        let dir_node = CommitMerkleTree::dir_with_children(repo, head_commit, path)?;
        Ok(dir_node)
    } else {
        Ok(None)
    }
}

fn get_file_node(
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

fn add_file_inner(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    version_store: &Arc<dyn VersionStore>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let repo_path = &repo.path.clone();
    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, repo_path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node = CommitMerkleTree::dir_with_children(repo, head_commit, parent_path)?;
    }

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    process_add_file(
        repo,
        repo_path,
        version_store,
        staged_db,
        &maybe_dir_node,
        path,
        &seen_dirs,
    )
}

pub fn add_file(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    // Get the version store from the repository
    let version_store = repo.version_store()?;

    add_file_inner(repo, maybe_head_commit, path, &staged_db, &version_store)
}

pub fn process_add_file(
    repo: &LocalRepository,
    repo_path: &Path,
    version_store: &Arc<dyn VersionStore>,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    maybe_dir_node: &Option<MerkleTreeNode>,
    path: &Path,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    log::debug!("process_add_file {:?}", path);
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
    let full_path = repo_path.join(&relative_path);

    if !full_path.is_file() {
        // If it's not a file - no need to add it
        // We handle directories by traversing the parents of files below
        log::debug!("file is not a file - skipping add on {:?}", full_path);
        return Ok(Some(StagedMerkleTreeNode {
            status: StagedEntryStatus::Added,
            node: MerkleTreeNode::default_dir(),
        }));
    }

    // Check if the file is already in the head commit
    let file_path = relative_path.file_name().unwrap_or_default();
    let maybe_file_node = get_file_node(maybe_dir_node, file_path)?;
    let mut previous_oxen_metadata: Option<GenericMetadata> = None;
    // This is ugly - but makes sure we don't have to rehash the file if it hasn't changed
    let (mut status, hash, num_bytes, mtime) = if let Some(file_node) = &maybe_file_node {
        log::debug!("got existing file_node: {}", file_node);
        // first check if the file timestamp is different
        let metadata = std::fs::metadata(path)?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        previous_oxen_metadata = file_node.metadata();
        if has_different_modification_time(file_node, &mtime) {
            log::debug!("has_different_modification_time true {}", file_node);
            let hash = util::hasher::get_hash_given_metadata(&full_path, &metadata)?;
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
            let hash = util::hasher::get_hash_given_metadata(&full_path, &metadata)?;

            if file_node.hash().to_u128() != hash {
                log::debug!("hash is different true {}", file_node);
                (
                    StagedEntryStatus::Modified,
                    MerkleHash::new(hash),
                    file_node.num_bytes(),
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
        }
    } else {
        let metadata = std::fs::metadata(path)?;
        let mtime = FileTime::from_last_modification_time(&metadata);
        let hash = util::hasher::get_hash_given_metadata(&full_path, &metadata)?;
        (
            StagedEntryStatus::Added,
            MerkleHash::new(hash),
            metadata.len(),
            mtime,
        )
    };
    log::debug!("status {status:?} hash {hash:?} num_bytes {num_bytes:?} mtime {mtime:?} file_node {maybe_file_node:?}");

    // TODO: Move this out of this function so we don't check for conflicts on every file
    if let Some(_file_node) = &maybe_file_node {
        let conflicts = repositories::merge::list_conflicts(repo)?;
        for conflict in conflicts {
            let conflict_path = repo.path.join(&conflict.merge_entry.path);
            log::debug!("comparing conflict_path {:?} to {:?}", conflict_path, path);
            let relative_conflict_path = util::fs::path_relative_to_dir(&conflict_path, repo_path)?;
            if relative_conflict_path == relative_path {
                status = StagedEntryStatus::Modified; // Mark as modified if there's a conflict
                repositories::merge::mark_conflict_as_resolved(repo, &conflict.merge_entry.path)?;
            }
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
    let metadata = match &previous_oxen_metadata {
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

    // Store the file in the version store using the hash as the key
    let hash_str = hash.to_string();
    version_store.store_version_from_path(&hash_str, &full_path)?;

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

/// Used to add a file node to the staged db in a workspace
pub fn add_file_node_to_staged_db(
    staged_db: &DBWithThreadMode<MultiThreaded>,
    relative_path: impl AsRef<Path>,
    status: StagedEntryStatus,
    file_node: &FileNode,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    p_add_file_node_to_staged_db(staged_db, relative_path, status, file_node, &seen_dirs)
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
    let mut seen_dirs = seen_dirs.lock().unwrap();
    if !seen_dirs.insert(relative_path.to_path_buf()) {
        return Ok(());
    }

    let dir_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Added,
        node: MerkleTreeNode::default_dir_from_path(relative_path),
    };

    log::debug!("writing dir to staged db: {}", dir_entry);
    let mut buf = Vec::new();
    dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
    staged_db.put(relative_path_str, &buf).unwrap();
    Ok(())
}

pub fn has_different_modification_time(node: &FileNode, time: &FileTime) -> bool {
    node.last_modified_nanoseconds() != time.nanoseconds()
        || node.last_modified_seconds() != time.unix_seconds()
}
