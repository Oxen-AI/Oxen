//! Caches the size of the repo to disk at the time of the commit, so that we can quickly query it

use fs_extra::dir::get_size;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::constants::{CACHE_DIR, DIRS_DIR, HISTORY_DIR};
use crate::core;
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::{
    CommitDirEntryReader, CommitEntryReader, CommitReader, ObjectDBReader,
};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use crate::util;

pub fn repo_size_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("repo_size.txt")
}

pub fn dir_size_path(repo: &LocalRepository, commit: &Commit, dir: &Path) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(DIRS_DIR)
        .join(dir)
        .join("size.txt")
}

pub fn dir_latest_commit_path(repo: &LocalRepository, commit: &Commit, dir: &Path) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(DIRS_DIR)
        .join(dir)
        .join("latest_commit.txt")
}

// TODO: Very aware this is ugly and not DRY. Struggles of shipping in startup mode.
// Refactor each one of these computes into a configurable cache
// 1) Compute the size of the repo at the time of the commit
// 2) Compute the size of each directory at time of commit
// 3) Compute the latest commit that modified each directory
pub fn compute(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::debug!(
        "REPO_SIZE {commit} Running compute_repo_size on {:?} for commit {}",
        repo.path,
        commit.id
    );

    // A few ways to optimize...
    // 1) *Let's do this sans block level dedup, but with same schema of file* Compute on commit of client...seems like the best but might require migration
    // 2) Read whole merkle tree into memory from object reader to make the rest of the logic faster
    // 3) Can we only look at the parent commit and not full history? Would have to make sure they are processed in order.

    // List directories in the repo, and cache all of their entry sizes
    let reader = CommitEntryReader::new(repo, commit)?;
    let commit_reader = CommitReader::new(repo)?;

    let commits = commit_reader.list_all_sorted_by_timestamp()?;

    let dirs = reader.list_dirs()?;
    log::debug!("REPO_SIZE {commit} Computing size of {} dirs", dirs.len());

    let object_reader = get_object_reader(repo, &commit.id)?;

    let mut object_readers: Vec<Arc<ObjectDBReader>> = Vec::new();
    for commit in &commits {
        object_readers.push(get_object_reader(repo, &commit.id)?);
    }

    for dir in dirs {
        log::debug!("REPO_SIZE {commit} PROCESSING DIR {dir:?}");

        // Start with the size of all the entries in this dir
        let entries = {
            // let dir_reader go out of scope
            let dir_reader =
                CommitDirEntryReader::new(repo, &commit.id, &dir, object_reader.clone())?;

            dir_reader.list_entries()?
        };
        let mut total_size = repositories::entries::compute_entries_size(&entries)?;
        log::debug!(
            "REPO_SIZE {commit} PROCESSING DIR {dir:?} total_size: {}",
            total_size
        );

        let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
        for (i, c) in commits.iter().enumerate() {
            let reader = CommitDirEntryReader::new(repo, &c.id, &dir, object_readers[i].clone())?;
            commit_entry_readers.push((c.clone(), reader));
        }

        // For each dir, find the latest commit that modified it
        let mut latest_commit: Option<Commit> = None;
        log::debug!(
            "REPO_SIZE {commit} PROCESSING DIR {dir:?} computing latest commit with {} readers and {} entries",
            commit_entry_readers.len(),
            entries.len()
        );

        // TODO: do not copy pasta this code
        for entry in entries {
            let Some(entry_commit) =
                core::v0_10_0::entries::get_latest_commit_for_entry(&commit_entry_readers, &entry)?
            else {
                log::debug!(
                    "No commit found for entry {:?} in dir {:?}",
                    entry.path,
                    dir
                );
                continue;
            };

            if latest_commit.is_none() {
                // log::debug!(
                //     "FOUND LATEST COMMIT PARENT EMPTY {:?} -> {:?}",
                //     entry.path,
                //     commit
                // );
                latest_commit = Some(entry_commit.clone());
            } else {
                // log::debug!(
                //     "CONSIDERING COMMIT PARENT TIMESTAMP {:?} {:?} < {:?}",
                //     entry.path,
                //     latest_commit.as_ref().unwrap().timestamp,
                //     commit.as_ref().unwrap().timestamp
                // );
                if latest_commit.as_ref().unwrap().timestamp < commit.timestamp {
                    // log::debug!(
                    //     "FOUND LATEST COMMIT PARENT TIMESTAMP {:?} -> {:?}",
                    //     entry.path,
                    //     commit
                    // );
                    latest_commit = Some(entry_commit.clone());
                }
            }
        }

        // Recursively compute the size of the directory children
        let children = reader.list_dir_children(&dir)?;

        for child in children {
            log::debug!("REPO_SIZE {commit} PROCESSING CHILD {child:?}");

            let entries = {
                let dir_reader =
                    CommitDirEntryReader::new(repo, &commit.id, &child, object_reader.clone())?;

                dir_reader.list_entries()?
            };

            let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
            for (i, c) in commits.iter().enumerate() {
                let reader =
                    CommitDirEntryReader::new(repo, &c.id, &child, object_readers[i].clone())?;
                commit_entry_readers.push((c.clone(), reader));
            }

            let size = repositories::entries::compute_entries_size(&entries)?;
            log::debug!("REPO_SIZE {commit} CHILD {child:?} size: {}", size);

            total_size += size;

            log::debug!(
                "REPO_SIZE {commit} CHILD {child:?} computing latest commit for {} entries",
                entries.len()
            );

            for entry in entries {
                let Some(entry_commit) = core::v0_10_0::entries::get_latest_commit_for_entry(
                    &commit_entry_readers,
                    &entry,
                )?
                else {
                    log::debug!(
                        "No commit found for entry {:?} in child {:?}",
                        entry.path,
                        child
                    );
                    continue;
                };

                if latest_commit.is_none() {
                    // log::debug!("FOUND LATEST COMMIT CHILD EMPTY {:?} -> {:?}", entry.path, commit);
                    latest_commit = Some(entry_commit.clone());
                } else {
                    // log::debug!("CONSIDERING COMMIT PARENT TIMESTAMP {:?} {:?} < {:?}", entry.path, latest_commit.as_ref().unwrap().timestamp, commit.as_ref().unwrap().timestamp);
                    if latest_commit.as_ref().unwrap().timestamp < entry_commit.timestamp {
                        // log::debug!("FOUND LATEST COMMIT PARENT TIMESTAMP {:?} -> {:?}", entry.path, commit);
                        latest_commit = Some(entry_commit.clone());
                    }
                }
            }
        }

        let size_str = total_size.to_string();
        let size_path = dir_size_path(repo, commit, &dir);
        log::debug!(
            "REPO_SIZE {commit} Writing dir size {} to {:?}",
            size_str,
            size_path
        );
        // create parent directory if not exists
        if let Some(parent) = size_path.parent() {
            util::fs::create_dir_all(parent)?;
        }
        util::fs::write_to_path(&size_path, &size_str)?;

        let latest_commit_path = dir_latest_commit_path(repo, commit, &dir);
        if let Some(latest_commit) = latest_commit {
            log::debug!(
                "REPO_SIZE {commit} Writing latest commit {} to {:?}",
                latest_commit.id,
                latest_commit_path
            );
            // create parent directory if not exists
            if let Some(parent) = latest_commit_path.parent() {
                util::fs::create_dir_all(parent)?;
            }
            util::fs::write_to_path(&latest_commit_path, &latest_commit.id)?;
        }
    }

    // Cache the full size of the repo
    log::debug!("REPO_SIZE {commit} Computing size of repo {:?}", repo.path);
    match get_size(&repo.path) {
        Ok(size) => {
            log::debug!(
                "REPO_SIZE {commit} Repo size for {:?} is {}",
                repo.path,
                size
            );
            write_repo_size(repo, commit, &size.to_string())?;
        }
        Err(e) => {
            // If we can't get the size, we'll just write an error message to the file
            // When we try to deserialize the file as a u64, we'll get an error and be able to return it
            let error_str = format!("Failed to get repo size: {}", e);
            write_repo_size(repo, commit, &error_str)?;
        }
    }

    Ok(())
}

fn write_repo_size(repo: &LocalRepository, commit: &Commit, val: &str) -> Result<(), OxenError> {
    let hash_file_path = repo_size_path(repo, commit);
    log::debug!(
        "REPO_SIZE {commit} Writing repo size {} to {:?}",
        val,
        hash_file_path
    );
    util::fs::write_to_path(&hash_file_path, val)?;
    Ok(())
}
