use rocksdb::{DBWithThreadMode, SingleThreaded, WriteBatch};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::constants::STAGED_DIR;
use crate::core::db::{self};
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::{Commit, LocalRepository, MerkleHash};
use crate::opts::RestoreOpts;
use crate::repositories;
use crate::storage::version_store::VersionStore;
use crate::util;
use std::sync::Arc;

pub struct FileToRestore {
    pub file_node: FileNode,
    pub path: PathBuf,
}

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore: start");
    if opts.staged {
        log::debug!("restore::restore: handling staged restore");
        return restore_staged(repo, opts);
    }

    // Get the version store from the repository
    let version_store = repo.version_store()?;

    let path = opts.path;
    let commit: Commit = repositories::commits::get_commit_or_head(repo, opts.source_ref)?;
    log::debug!("restore::restore: got commit {:?}", commit.id);
    log::debug!("restore::restore: got path {:?}", path);

    let dir = CommitMerkleTree::dir_with_children_recursive(repo, &commit, &path)?;

    match dir {
        Some(dir) => {
            log::debug!("restore::restore: restoring directory");
            restore_dir(repo, dir, &path, &version_store)
        }
        None => {
            log::debug!("restore::restore: restoring file");
            match CommitMerkleTree::from_path(repo, &commit, &path, false) {
                Ok(merkle_tree) => {
                    log::debug!(
                        "restore::restore: got merkle tree {:?}",
                        merkle_tree.root.node
                    );
                    if !matches!(&merkle_tree.root.node, EMerkleTreeNode::File(_)) {
                        return Err(OxenError::basic_str("Path is not a file"));
                    }

                    let child_file = merkle_tree.root.file().unwrap();

                    restore_file(repo, &child_file, &path, &version_store)
                }
                Err(OxenError::Basic(msg))
                    if msg.to_string().contains("Merkle tree hash not found") =>
                {
                    log::warn!("restore::restore: No file found at path {:?}", path);
                    println!("No file found at the specified path: {:?}", path);
                    Err(OxenError::basic_str("No file found at the specified path"))
                }
                Err(e) => Err(e),
            }
        }
    }
}

fn restore_staged(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore_staged: start");
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    if let Some(db) = open_staged_db(&db_path)? {
        let mut batch = WriteBatch::default();

        if opts.path == PathBuf::from(".") {
            // If path is ".", remove all staged entries
            for result in db.iterator(rocksdb::IteratorMode::Start) {
                match result {
                    Ok((key, _)) => batch.delete(key),
                    Err(e) => return Err(OxenError::basic_str(&e)),
                }
            }
            log::debug!("restore::restore_staged: prepared to clear all staged entries");
        } else {
            // Remove specific staged entry or entries under a directory
            let prefix = opts.path.to_string_lossy().into_owned();
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
                                "restore::restore_staged: prepared to remove staged entry for path {:?}",
                                key_str
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
                        "restore::restore_staged: removed parent directory with no children: {:?}",
                        parent_str
                    );
                } else {
                    break;
                }

                parent_path = parent.to_path_buf();
            }
            db.write(parent_batch)?;
        }

        log::debug!("restore::restore_staged: changes committed to the database");
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

fn restore_dir(
    repo: &LocalRepository,
    dir: MerkleTreeNode,
    path: &PathBuf,
    version_store: &Arc<dyn VersionStore>,
) -> Result<(), OxenError> {
    log::debug!("restore::restore_dir: start");
    // Change the return type to include both FileNode and PathBuf
    let file_nodes_with_paths = repositories::tree::dir_entries_with_paths(&dir, path)?;
    log::debug!(
        "restore::restore_dir: got {} entries",
        file_nodes_with_paths.len()
    );

    let msg = format!("Restoring Directory: {:?}", dir);
    let bar =
        util::progress_bar::oxen_progress_bar_with_msg(file_nodes_with_paths.len() as u64, &msg);

    let mut existing_files = HashSet::new();
    if let Ok(entries) = fs::read_dir(path) {
        existing_files.extend(
            entries
                .filter_map(Result::ok)
                .filter(|e| e.path().is_file())
                .map(|e| e.path()),
        );
    }

    file_nodes_with_paths
        .iter()
        .for_each(|(file_node, file_path)| {
            existing_files.remove(file_path);

            match restore_file(repo, file_node, file_path, version_store) {
                Ok(_) => log::debug!("restore::restore_dir: entry restored successfully"),
                Err(e) => {
                    log::error!(
                        "restore::restore_dir: error restoring file {:?}: {:?}",
                        file_path,
                        e
                    );
                }
            }
            bar.inc(1);
        });

    for file_to_remove in existing_files {
        fs::remove_file(file_to_remove)?;
    }

    bar.finish_and_clear();
    log::debug!("restore::restore_dir: end");

    Ok(())
}

pub fn should_restore_file(
    repo: &LocalRepository,
    base_node: Option<FileNode>,
    file_node: &FileNode,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let working_path = repo.path.join(path);

    // Check to see if the file has been modified if it exists
    if working_path.exists() {
        let hash = MerkleHash::new(util::hasher::u128_hash_file_contents(&working_path)?);
        // If there are modifications compared to the base node, we should not restore the file
        if let Some(base_node) = base_node {
            let base_node_hash = base_node.hash();
            if hash != *base_node_hash {
                return Ok(false);
            }
        } else {
            // Untracked file, check if we are overwriting it
            if hash != *file_node.hash() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

pub fn restore_file(
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
    let parent = working_path.parent().unwrap();
    if !parent.exists() {
        util::fs::create_dir_all(parent)?;
    }

    // Use the version store to copy the file to the working path
    let hash_str = file_hash.to_string();
    version_store.copy_version_to_path(&hash_str, &working_path)?;

    let last_modified = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(last_modified_seconds as u64)
        + std::time::Duration::from_nanos(last_modified_nanoseconds as u64);
    filetime::set_file_mtime(
        &working_path,
        filetime::FileTime::from_system_time(last_modified),
    )?;
    Ok(())
}
