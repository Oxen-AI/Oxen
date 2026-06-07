use rocksdb::{DBWithThreadMode, SingleThreaded, WriteBatch};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::constants::STAGED_DIR;
use crate::core::db::{self};
use crate::error::{OxenError, PathBufError};
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::{Commit, LocalRepository, MerkleHash, PartialNode};
use crate::opts::RestoreOpts;
use crate::repositories;
use crate::storage::version_store::VersionStore;
use crate::util;

#[derive(Debug)]
pub struct FileToRestore {
    pub file_node: FileNode,
    pub path: PathBuf,
}

pub async fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore: start");
    if opts.staged {
        return restore_staged(repo, opts);
    }

    // Get the version store from the repository
    let version_store = repo.version_store();

    let paths = opts.paths;
    log::debug!("restore::restore got {:?} paths", paths.len());

    let commit: Commit = repositories::commits::get_commit_or_head(repo, opts.source_ref)?;
    log::debug!("restore::restore: got commit {:?}", commit.id);

    let repo_path = repo.path.clone();

    // Accumulate per-file failures so the caller can recover everything that did succeed
    // and we can still surface a single aggregated error at the end.
    let mut failures: Vec<(PathBufError, Box<OxenError>)> = Vec::new();

    for path in paths {
        let path = util::fs::path_relative_to_dir(&path, &repo_path)?;
        let Some(node) = repositories::tree::get_node_by_path_with_children(repo, &commit, &path)?
        else {
            log::error!(
                "path {:?} not found in tree for commit {:?}",
                path,
                commit.id
            );
            continue;
        };

        match &node.node {
            EMerkleTreeNode::Directory(_dir_node) => {
                log::debug!("restore::restore: restoring directory");
                let dir_failures = restore_dir(repo, node, &path, &version_store).await?;
                failures.extend(dir_failures);
            }
            EMerkleTreeNode::File(file_node) => {
                if let Err(e) = restore_file(repo, file_node, &path, &version_store).await {
                    log::error!("restore::restore_file failed for file {path:?} with error {e:?}");
                    failures.push((path.clone().into(), Box::new(e)));
                }
            }
            _ => {
                return Err(OxenError::basic_str("Error: Unexpected node type"));
            }
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(OxenError::RestoreFailed { failures })
    }
}

fn restore_staged(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore_staged: start");
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let repo_path = repo.path.clone();
    if let Some(db) = open_staged_db(&db_path)? {
        for path in &opts.paths {
            let path = util::fs::path_relative_to_dir(path, &repo_path)?;
            let mut batch = WriteBatch::default();

            // Remove specific staged entry or entries under a directory
            let prefix = path.to_string_lossy().into_owned();
            for result in db.iterator(rocksdb::IteratorMode::From(
                prefix.as_bytes(),
                rocksdb::Direction::Forward,
            )) {
                match result {
                    Ok((key, _)) => {
                        let key_str = String::from_utf8_lossy(&key);
                        // if prefix is a file, it will also return true on starts_with
                        if key_str.starts_with(&prefix) {
                            batch.delete(&key);
                            log::debug!(
                                "restore::restore_staged: prepared to remove staged entry for path {key_str:?}"
                            );
                        } else {
                            break; // Stop when we've passed all entries with the given prefix
                        }
                    }
                    Err(e) => return Err(OxenError::basic_str(&e)),
                }
            }

            db.write(batch)?;
            let mut parent_batch = WriteBatch::default();

            let mut parent_path = PathBuf::from(&prefix);
            while let Some(parent) = parent_path.parent() {
                let parent_str = parent.to_string_lossy().into_owned();
                let mut has_children = false;

                for result in db.iterator(rocksdb::IteratorMode::From(
                    parent_str.as_bytes(),
                    rocksdb::Direction::Forward,
                )) {
                    match result {
                        Ok((key, _)) => {
                            let key_str = String::from_utf8_lossy(&key);
                            if key_str.starts_with(&parent_str) && key_str != parent_str {
                                has_children = true;
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }

                if !has_children {
                    parent_batch.delete(parent_str.as_bytes());
                    log::debug!(
                        "restore::restore_staged: removed parent directory with no children: {parent_str:?}"
                    );
                } else {
                    break;
                }

                parent_path = parent.to_path_buf();
            }
            db.write(parent_batch)?;
            log::debug!("restore::restore_staged: changes committed to the database");
        }
    } else {
        log::debug!("restore::restore_staged: no staged database found");
    }

    log::debug!("restore::restore_staged: end");
    Ok(())
}

fn open_staged_db(db_path: &Path) -> Result<Option<DBWithThreadMode<SingleThreaded>>, OxenError> {
    if db_path.join("CURRENT").exists() {
        let opts = db::key_val::opts::default();
        let db = DBWithThreadMode::open(&opts, dunce::simplified(db_path))?;
        Ok(Some(db))
    } else {
        Ok(None)
    }
}

/// Restore every file in `dir` (recursive). Returns the list of files that failed to
/// restore — empty when every entry succeeded. Restoration of one file failing does not
/// stop the loop; the caller wraps any non-empty result in [`OxenError::RestoreFailed`].
/// An outer error means some other error occurred other than a failed file restore.
async fn restore_dir(
    repo: &LocalRepository,
    dir: MerkleTreeNode,
    path: &Path,
    version_store: &Arc<dyn VersionStore>,
) -> Result<Vec<(PathBufError, Box<OxenError>)>, OxenError> {
    log::debug!("restore::restore_dir: start");
    let entries = repositories::tree::dir_entries_with_paths(&dir, path);
    log::debug!(
        "restore::restore_dir: got {} file entries, {} directory entries",
        entries.files.len(),
        entries.dirs.len()
    );

    let mut failures: Vec<(PathBufError, Box<OxenError>)> = Vec::new();

    // `path` is "" when the user ran `oxen restore .` (path_relative_to_dir collapses "."
    // to ""). Render it as "." instead of blank.
    let label = if path.as_os_str().is_empty() {
        ".".to_string()
    } else {
        path.display().to_string()
    };
    let msg = format!("Restoring {label}");
    let total = (entries.dirs.len() + entries.files.len()) as u64;
    let bar = util::progress_bar::oxen_progress_bar_with_msg(total, &msg);

    // Materialize every tracked directory in the subtree first. Files inside non-empty
    // dirs would otherwise rely on `restore_file`'s `create_dir_all(parent)` side-effect,
    // but empty dirs (first-class in Oxen — see CLAUDE.md "How Oxen Differs from Git")
    // have no files to drive that, and would be silently skipped (ENG-1003).
    for dir_path in &entries.dirs {
        let working_dir_path = repo.path.join(dir_path);
        if let Err(e) = tokio::fs::create_dir_all(&working_dir_path).await {
            log::error!(
                "restore::restore_dir: error creating directory {working_dir_path:?}: {e:?}"
            );
            failures.push((dir_path.clone().into(), Box::new(OxenError::IO(e))));
        }
        bar.inc(1);
    }

    for (file_node, file_path) in entries.files.iter() {
        match restore_file(repo, file_node, file_path, version_store).await {
            Ok(_) => log::debug!("restore::restore_dir: entry restored successfully"),
            Err(e) => {
                log::error!("restore::restore_dir: error restoring file {file_path:?}: {e:?}");
                failures.push((file_path.clone().into(), Box::new(e)));
            }
        }
        bar.inc(1);
    }

    bar.finish_and_clear();
    log::debug!("restore::restore_dir: end ({} failures)", failures.len());

    Ok(failures)
}

pub async fn should_restore_partial_node(
    repo: &LocalRepository,
    base_node: Option<PartialNode>,
    file_node: &FileNode,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let working_path = repo.path.join(path);

    // Check to see if the file has been modified if it exists
    if working_path.exists() {
        // Check metadata for changes first
        let meta = util::fs::metadata(&working_path)?;
        let file_last_modified = filetime::FileTime::from_last_modification_time(&meta);
        let file_size = meta.len();

        // If there are modifications compared to the base node, we should not restore the file
        if let Some(base_node) = base_node {
            let node_last_modified = base_node.last_modified;
            let node_size = base_node.size;

            if repo
                .mtime_matches(file_last_modified, node_last_modified)
                .await
                && file_size == node_size
            {
                return Ok(true);
            }

            // If modified times are different, check hashes
            let hash = MerkleHash::new(util::hasher::u128_hash_file_contents(&working_path)?);

            // File already matches the merge target — no-op.
            if hash == *file_node.hash() {
                return Ok(true);
            }

            let base_node_hash = base_node.hash;
            if hash != base_node_hash {
                return Ok(false);
            }
        } else {
            // Untracked file, check if we are overwriting it
            let node_modified_nanoseconds = std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_secs(file_node.last_modified_seconds() as u64)
                + std::time::Duration::from_nanos(file_node.last_modified_nanoseconds() as u64);

            let node_last_modified =
                filetime::FileTime::from_system_time(node_modified_nanoseconds);

            if repo
                .mtime_matches(file_last_modified, node_last_modified)
                .await
                && file_size == file_node.num_bytes()
            {
                return Ok(true);
            }

            // If modified times are different, check hashes
            let hash = MerkleHash::new(util::hasher::u128_hash_file_contents(&working_path)?);
            if hash != *file_node.hash() {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

pub async fn should_restore_file(
    repo: &LocalRepository,
    base_node: Option<FileNode>,
    file_node: &FileNode,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let working_path = repo.path.join(path);
    // Check to see if the file has been modified if it exists
    if working_path.exists() {
        // Check metadata for changes first
        let meta = util::fs::metadata(&working_path)?;
        let file_last_modified = filetime::FileTime::from_last_modification_time(&meta);
        let file_size = meta.len();

        // If there are modifications compared to the base node, we should not restore the file
        if let Some(base_node) = base_node {
            // First, check the modified times
            let node_modified_nanoseconds = std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_secs(base_node.last_modified_seconds() as u64)
                + std::time::Duration::from_nanos(base_node.last_modified_nanoseconds() as u64);

            let node_last_modified =
                filetime::FileTime::from_system_time(node_modified_nanoseconds);

            if repo
                .mtime_matches(file_last_modified, node_last_modified)
                .await
                && file_size == base_node.num_bytes()
            {
                return Ok(true);
            }

            // Second, check the combined hashes
            let file_hash = util::hasher::u128_hash_file_contents(&working_path)?;
            let file_combined_hash = {
                let mime_type = util::fs::file_mime_type(path);
                let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());

                let file_metadata = repositories::metadata::get_file_metadata(path, &data_type)?;
                let file_metadata_hash = util::hasher::maybe_get_metadata_hash(&file_metadata)?;

                let combined_hash = util::hasher::get_combined_hash(file_metadata_hash, file_hash)?;
                MerkleHash::new(combined_hash)
            };

            let node_combined_hash = file_node.combined_hash();
            // File already matches the merge target — no-op. Lets an interrupted
            // pull resume when the prior pull had time to fully rewrite the file
            // before being killed.
            if file_combined_hash == *node_combined_hash {
                return Ok(true);
            }

            let base_hash = base_node.combined_hash();

            if file_combined_hash != *base_hash {
                return Ok(false);
            }
        } else {
            // Untracked file, check if we are overwriting it
            let node_modified_nanoseconds = std::time::SystemTime::UNIX_EPOCH
                + std::time::Duration::from_secs(file_node.last_modified_seconds() as u64)
                + std::time::Duration::from_nanos(file_node.last_modified_nanoseconds() as u64);

            let node_last_modified =
                filetime::FileTime::from_system_time(node_modified_nanoseconds);

            if repo
                .mtime_matches(file_last_modified, node_last_modified)
                .await
                && file_size == file_node.num_bytes()
            {
                return Ok(true);
            }

            // Second, check the combined hashes
            let file_hash = util::hasher::u128_hash_file_contents(&working_path)?;
            let file_combined_hash = {
                let mime_type = util::fs::file_mime_type(path);
                let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());

                let file_metadata = repositories::metadata::get_file_metadata(path, &data_type)?;
                let file_metadata_hash = util::hasher::maybe_get_metadata_hash(&file_metadata)?;

                let combined_hash = util::hasher::get_combined_hash(file_metadata_hash, file_hash)?;
                MerkleHash::new(combined_hash)
            };

            let node_combined_hash = file_node.combined_hash();
            if file_combined_hash != *node_combined_hash {
                return Ok(false);
            }
        }
    }

    Ok(true)
}

fn abs_duration_diff(a: SystemTime, b: SystemTime) -> Duration {
    if a >= b {
        a.duration_since(b).unwrap_or_default()
    } else {
        b.duration_since(a).unwrap_or_default()
    }
}

pub async fn restore_file(
    repo: &LocalRepository,
    file_node: &FileNode,
    path: impl AsRef<Path>,
    version_store: &Arc<dyn VersionStore>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    let file_hash = file_node.hash();
    let last_modified_seconds = file_node.last_modified_seconds();
    let last_modified_nanoseconds = file_node.last_modified_nanoseconds();

    let working_path = repo.path.join(path);
    let expected_mtime = SystemTime::UNIX_EPOCH
        + Duration::from_secs(last_modified_seconds as u64)
        + Duration::from_nanos(last_modified_nanoseconds as u64);

    // Fast path: if the on-disk file already matches the target by size + mtime (within
    // the filesystem's measured rounding), skip the copy entirely. Mirrors git's "stat
    // index" heuristic. Tolerance comes from the version store's one-time-per-repo probe
    // so we don't hard-code per-platform values and we catch cross-cutting cases like a
    // FAT USB stick mounted on Linux.
    if let Ok(meta) = tokio::fs::metadata(&working_path).await
        && meta.len() == file_node.num_bytes()
        && let Ok(actual_mtime) = meta.modified()
        && abs_duration_diff(actual_mtime, expected_mtime) <= repo.mtime_tolerance().await
    {
        return Ok(());
    }

    let parent = working_path.parent().unwrap();
    util::fs::create_dir_all(parent)?;

    // Use the version store to atomically copy the file to the working path with the correct mtime
    let hash_str = file_hash.to_string();
    version_store
        .copy_version_to_path(&hash_str, &working_path, expected_mtime)
        .await?;

    Ok(())
}
