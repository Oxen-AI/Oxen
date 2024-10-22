use rocksdb::{DBWithThreadMode, SingleThreaded, WriteBatch};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::constants::STAGED_DIR;
use crate::core::db::{self};
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::{Commit, LocalRepository};
use crate::opts::RestoreOpts;
use crate::repositories;
use crate::util;

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    log::debug!("restore::restore: start");
    if opts.staged {
        log::debug!("restore::restore: handling staged restore");
        return restore_staged(repo, opts);
    }

    let path = opts.path;
    let commit: Commit = repositories::commits::get_commit_or_head(repo, opts.source_ref)?;
    log::debug!("restore::restore: got commit {:?}", commit.id);
    log::debug!("restore::restore: got path {:?}", path);

    let dir = CommitMerkleTree::dir_with_children_recursive(repo, &commit, &path)?;

    match dir {
        Some(dir) => {
            log::debug!("restore::restore: restoring directory");
            restore_dir(repo, dir, &path)
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

                    restore_file(repo, &child_file, &path)
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
) -> Result<(), OxenError> {
    log::debug!("restore::restore_dir: start");
    // Change the return type to include both FileNode and PathBuf
    let file_nodes_with_paths = CommitMerkleTree::dir_entries_with_paths(&dir, path)?;
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

            match restore_file(repo, file_node, file_path) {
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

pub fn restore_file(
    repo: &LocalRepository,
    file_node: &FileNode,
    path: &PathBuf,
) -> Result<(), OxenError> {
    log::debug!("restore::restore_regular: start for {:?}", file_node.name);
    log::debug!("restore::restore_regular: got entry resource");

    let file_hash = file_node.hash;
    let last_modified_seconds = file_node.last_modified_seconds;
    let last_modified_nanoseconds = file_node.last_modified_nanoseconds;
    log::debug!("restore::restore_regular: got file hash {:?}", file_hash);

    let version_path = util::fs::version_path_from_node(repo, file_hash.to_string(), path);
    log::debug!("restore::restore_regular: calculated version path");

    let working_path = repo.path.join(path);
    let parent = working_path.parent().unwrap();
    if !parent.exists() {
        log::debug!("restore::restore_regular: creating parent directory");
        util::fs::create_dir_all(parent)?;
    }

    log::debug!("restore::restore_regular: copying file");
    log::debug!("restore::restore_regular: version_path {:?}", version_path);
    log::debug!("restore::restore_regular: working_path {:?}", working_path);
    util::fs::copy(version_path, working_path.clone())?;
    let last_modified = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(last_modified_seconds as u64)
        + std::time::Duration::from_nanos(last_modified_nanoseconds as u64);
    filetime::set_file_mtime(
        &working_path,
        filetime::FileTime::from_system_time(last_modified),
    )?;

    log::debug!("restore::restore_regular: set updated time from tree");
    log::debug!("restore::restore_regular: end");
    Ok(())
}
