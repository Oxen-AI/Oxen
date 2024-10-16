use crate::constants;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};

use std::path::PathBuf;

pub fn commit_is_synced(repo: &LocalRepository, commit: &Commit) -> bool {
    let is_synced_path = commit_is_synced_file_path(repo, commit);
    log::debug!("Checking if commit is synced: {is_synced_path:?}");
    match std::fs::read_to_string(&is_synced_path) {
        Ok(value) => {
            log::debug!("Is synced value: {value}");
            "true" == value
        }
        Err(err) => {
            log::debug!("Could not read is_synced file {is_synced_path:?}: {}", err);
            false
        }
    }
}

pub fn mark_commit_as_synced(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    let is_synced_path = commit_is_synced_file_path(repo, commit);
    if let Some(parent) = is_synced_path.parent() {
        if !parent.exists() {
            log::debug!("Creating parent directory: {parent:?}");
            std::fs::create_dir_all(parent)?;
        }
    }

    log::debug!("Writing is synced: {is_synced_path:?}");

    match std::fs::write(&is_synced_path, "true") {
        Ok(_) => {
            log::debug!("Wrote is synced file: {is_synced_path:?}");
            Ok(())
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "Could not write is_synced file: {}",
            err
        ))),
    }
}

fn commit_is_synced_file_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    repo.path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(constants::SYNC_STATUS_DIR)
        .join(constants::COMMITS_DIR)
        .join(&commit.id)
        .join(constants::IS_SYNCED)
}
