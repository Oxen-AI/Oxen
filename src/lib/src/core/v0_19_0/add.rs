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


use crate::constants::{FILES_DIR, OXEN_HIDDEN_DIR, STAGED_DIR, VERSIONS_DIR};
use crate::core::db;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::model::{Commit, EntryDataType, MerkleHash, MerkleTreeNodeType, StagedEntryStatus};
use crate::{error::OxenError, model::LocalRepository};
use crate::{repositories, util};
use std::ops::AddAssign;


use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};


#[derive(Clone, Debug, Default)]
pub struct CumulativeStats {
    total_files: usize,
    total_bytes: u64,
    data_type_counts: HashMap<EntryDataType, usize>,
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


    // Start a timer
    let start = std::time::Instant::now();
    let path = path.as_ref();
    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                paths.insert(entry?);
            }
        } else {
            // Non-glob path
            paths.insert(path.to_owned());
        }
    }


    let stats = add_files(repo, &paths)?;


    // Stop the timer, and round the duration to the nearest second
    let duration = Duration::from_millis(start.elapsed().as_millis() as u64);
    log::debug!("---END--- oxen add: {:?} duration: {:?}", path, duration);


    // oxen staged?
    println!(
        "🐂 oxen added {} files ({}) in {}",
        stats.total_files,
        bytesize::ByteSize::b(stats.total_bytes),
        humantime::format_duration(duration)
    );


    Ok(())
}


fn add_files(
    repo: &LocalRepository,
    paths: &HashSet<PathBuf>,
) -> Result<CumulativeStats, OxenError> {
    // To start, let's see how fast we can simply loop through all the paths
    // and and copy them into an index.

    println!("Add files");

    // Create the versions dir if it doesn't exist
    let versions_path = util::fs::oxen_hidden_dir(&repo.path).join(VERSIONS_DIR);
    if !versions_path.exists() {
        util::fs::create_dir_all(versions_path)?;
    }

    // Lookup the head commit
    let maybe_head_commit = repositories::commits::head_commit_maybe(repo)?;

    let mut total = CumulativeStats {
        total_files: 0,
        total_bytes: 0,
        data_type_counts: HashMap::new(),
    };
    for path in paths {
        println!("path is {path:?} in container {paths:?}");
        if path.is_dir() {
            total += add_dir(repo, &maybe_head_commit, path.clone())?;
         
        } else {
         
            println!("found is_file()");
            let entry = add_file(repo, &maybe_head_commit, path)?;
            if let Some(entry) = entry {
                if let EMerkleTreeNode::File(file_node) = &entry.node.node {
                    let data_type = file_node.data_type.clone();
                    total.total_files += 1;
                    total.total_bytes += file_node.num_bytes;
                    total
                        .data_type_counts
                        .entry(data_type)
                        .and_modify(|count| *count += 1)
                        .or_insert(1);
                }
            }
        } 
    }


    Ok(total)
}

pub fn add_dir(    
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: PathBuf,
) -> Result<CumulativeStats, OxenError> {

    let versions_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    process_add_dir(repo, maybe_head_commit, staged_db, path)
}


fn process_add_dir(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
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
    let repo_path = repo.path.clone();

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
    for entry in walker.filter_entry(|e| e.file_type().is_dir() && e.file_name() != OXEN_HIDDEN_DIR)
    {
        let entry = entry.unwrap();
        let dir = entry.path();


        let byte_counter_clone = Arc::clone(&byte_counter);
        let added_file_counter_clone = Arc::clone(&added_file_counter);
        let unchanged_file_counter_clone = Arc::clone(&unchanged_file_counter);


        let dir_path = util::fs::path_relative_to_dir(dir, &repo_path).unwrap();
        let dir_node = maybe_load_directory(&repo, &maybe_head_commit, &dir_path).unwrap();
        let seen_dirs = Arc::new(Mutex::new(HashSet::new()));


        // Curious why this is only < 300% CPU usage
        std::fs::read_dir(dir)?.for_each(|dir_entry_result| {
            if let Ok(dir_entry) = dir_entry_result {
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
                    &repo_path,
                    &versions_path,
                    &staged_db,
                    &dir_node,
                    &path,
                    &seen_dirs_clone,
                ) {
                    Ok(Some(node)) => {
                        if let EMerkleTreeNode::File(file_node) = &node.node.node {
                            byte_counter_clone.fetch_add(file_node.num_bytes, Ordering::Relaxed);
                            added_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                            cumulative_stats.total_bytes += file_node.num_bytes;
                            cumulative_stats
                                .data_type_counts
                                .entry(file_node.data_type.clone())
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                            if node.status != StagedEntryStatus::Unmodified {
                                cumulative_stats.total_files += 1;
                            }
                        }
                    }
                    Ok(None) => {
                        unchanged_file_counter_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        log::error!("Error adding file: {:?}", e);
                    }
                }
            }
        });
    }


    progress_1_clone.finish_and_clear();


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


pub fn add_file(
    repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    println!("Made it to add_file");
    let repo_path = repo.path.clone();
    let versions_path = util::fs::oxen_hidden_dir(&repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;


    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, &repo_path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node = CommitMerkleTree::dir_with_children(repo, head_commit, parent_path)?;
    }


    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    process_add_file(
        &repo_path,
        &versions_path,
        &staged_db,
        &maybe_dir_node,
        path,
        &seen_dirs,
    )
}

// Need to ensure this function never gets called with a non-existant path unless that path has intentionally been removed
fn process_add_file(
    repo_path: &Path,
    versions_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    maybe_dir_node: &Option<MerkleTreeNode>,
    path: &Path,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let relative_path = util::fs::path_relative_to_dir(path, repo_path)?;
    let full_path = repo_path.join(&relative_path);

    println!("Check if file");
    if !full_path.is_file() {

        // Handle removed files
        if !full_path.exists() {
            println!("Added file did not exist. Staging removed entry");

            // Find node to remove
            let file_path = relative_path.file_name().unwrap();

            // TODO: This might be buggy. What if we add a dir but also a file within the dir? will this throw an error then?
            let node: MerkleTreeNode = if let Some(file_node) = get_file_node(maybe_dir_node, file_path)? {
                MerkleTreeNode::from_file(file_node)
            } else {
                let error = format!("File {relative_path:?} must be committed to use `oxen rm`");
                return Err(OxenError::basic_str(error));
            };
            
            let entry = StagedMerkleTreeNode {
                status: StagedEntryStatus::Removed,
                node: node.clone(),
            };
   
            // Remove the file from the versions db
            // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
            let dir_prefix_len = 2;
            let dir_name = node.hash.to_string();
            let dir_prefix = dir_name.chars().take(dir_prefix_len).collect::<String>();
            let dir_suffix = dir_name.chars().skip(dir_prefix_len).collect::<String>();
            let dst_dir = versions_path.join(dir_prefix).join(dir_suffix);

            let dst = dst_dir.join("data");
            util::fs::remove_dir_all(&dst);

            // Write removed node to staged db
            log::debug!("writing file to staged db: {}", entry);
            write_file_with_parents(entry.clone(), &relative_path, repo_path, staged_db, seen_dirs);
            return Ok(Some(entry));
        }

        // If it's not a file - no need to add it
        // We handle directories by traversing the parents of files below
        return Ok(Some(StagedMerkleTreeNode {
            status: StagedEntryStatus::Added,
            node: MerkleTreeNode::default_dir(),
        }));
    }

    // Check if the file is already in the head commit
        let file_path = relative_path.file_name().unwrap();
        let maybe_file_node = get_file_node(maybe_dir_node, file_path)?;


        // This is ugly - but makes sure we don't have to rehash the file if it hasn't changed
        let (status, hash, num_bytes, mtime) = if let Some(file_node) = maybe_file_node {
            // first check if the file timestamp is different
            let metadata = std::fs::metadata(path)?;
            let mtime = FileTime::from_last_modification_time(&metadata);
            log::debug!("path: {:?}", path);
            log::debug!(
                "file_node.last_modified_seconds: {}",
                file_node.last_modified_seconds
            );
            log::debug!(
                "file_node.last_modified_nanoseconds: {}",
                file_node.last_modified_nanoseconds
            );
            log::debug!("mtime.unix_seconds(): {}", mtime.unix_seconds());
            log::debug!("mtime.nanoseconds(): {}", mtime.nanoseconds());
            log::debug!(
                "has_different_modification_time: {}",
                has_different_modification_time(&file_node, &mtime)
            );
            log::debug!("-----------------------------------");
            if has_different_modification_time(&file_node, &mtime) {
                let hash = util::hasher::get_hash_given_metadata(&full_path, &metadata)?;
                if file_node.hash.to_u128() != hash {
                    (
                        StagedEntryStatus::Modified,
                        MerkleHash::new(hash),
                        file_node.num_bytes,
                        mtime,
                    )
                } else {
                    (
                        StagedEntryStatus::Unmodified,
                        MerkleHash::new(hash),
                        file_node.num_bytes,
                        mtime,
                    )
                }
            } else {
                (
                    StagedEntryStatus::Unmodified,
                    file_node.hash,
                    file_node.num_bytes,
                    mtime,
                )
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


        // Don't have to add the file to the staged db if it hasn't changed
        if status == StagedEntryStatus::Unmodified {
            return Ok(None);
        }


        // Get the data type of the file
        let mime_type = util::fs::file_mime_type(path);
        let data_type = util::fs::datatype_from_mimetype(path, &mime_type);
        let metadata = repositories::metadata::get_file_metadata(&full_path, &data_type)?;


        // Add the file to the versions db
        // Take first 2 chars of hash as dir prefix and last N chars as the dir suffix
        let dir_prefix_len = 2;
        let dir_name = hash.to_string();
        let dir_prefix = dir_name.chars().take(dir_prefix_len).collect::<String>();
        let dir_suffix = dir_name.chars().skip(dir_prefix_len).collect::<String>();
        let dst_dir = versions_path.join(dir_prefix).join(dir_suffix);


        if !dst_dir.exists() {
            util::fs::create_dir_all(&dst_dir).unwrap();
        }


        let dst = dst_dir.join("data");
        util::fs::copy(&full_path, &dst).unwrap();


        let file_extension = relative_path
            .extension()
            .unwrap_or_default()
            .to_string_lossy();
        let relative_path_str = relative_path.to_str().unwrap();
        let entry = StagedMerkleTreeNode {
            status,
            node: MerkleTreeNode::from_file(FileNode {
                hash,
                name: relative_path_str.to_string(),
                data_type,
                num_bytes,
                last_modified_seconds: mtime.unix_seconds(),
                last_modified_nanoseconds: mtime.nanoseconds(),
                metadata,
                extension: file_extension.to_string(),
                mime_type: mime_type.clone(),
                ..Default::default()
            }),
        };


    log::debug!("writing file to staged db: {}", entry);
    write_file_with_parents(entry.clone(), &relative_path, repo_path, staged_db, seen_dirs);
   
    Ok(Some(entry))
}

fn write_file_with_parents(entry: StagedMerkleTreeNode,
    relative_path: &Path,
    repo_path: &Path,
    staged_db: &DBWithThreadMode<MultiThreaded>,
    seen_dirs: &Arc<Mutex<HashSet<PathBuf>>>,
) -> () {
    let mut buf = Vec::new();
    entry.serialize(&mut Serializer::new(&mut buf)).unwrap();


    let relative_path_str = relative_path.to_str().unwrap();
    staged_db.put(relative_path_str, &buf).unwrap();


    // Add all the parent dirs to the staged db
    let mut parent_path = relative_path.to_path_buf();
    let mut seen_dirs = seen_dirs.lock().unwrap();
    while let Some(parent) = parent_path.parent() {
        let relative_path = util::fs::path_relative_to_dir(parent, repo_path).unwrap();
        parent_path = parent.to_path_buf();


        let relative_path_str = relative_path.to_str().unwrap();
        if !seen_dirs.insert(relative_path.to_owned()) {
            // Don't write the same dir twice
            continue;
        }


        let dir_entry = StagedMerkleTreeNode {
            status: StagedEntryStatus::Added,
            node: MerkleTreeNode::default_dir_from_path(&relative_path),
        };


        log::debug!("writing dir to staged db: {}", dir_entry);
        let mut buf = Vec::new();
        dir_entry.serialize(&mut Serializer::new(&mut buf)).unwrap();
        staged_db.put(relative_path_str, &buf).unwrap();


        if relative_path == Path::new("") {
            break;
        }
    }
}

pub fn has_different_modification_time(node: &FileNode, time: &FileTime) -> bool {
    node.last_modified_nanoseconds != time.nanoseconds()
        || node.last_modified_seconds != time.unix_seconds()
}


