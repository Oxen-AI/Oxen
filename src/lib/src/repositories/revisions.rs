//! Revisions can either be commits by id or head commits on branches by name

use std::path::{Path, PathBuf};

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;

/// Get a commit object from a commit id or branch name
/// Returns Ok(None) if the revision does not exist
pub fn get(repo: &LocalRepository, revision: impl AsRef<str>) -> Result<Option<Commit>, OxenError> {
    let revision = revision.as_ref();
    if repositories::branches::exists(repo, revision)? {
        log::debug!("revision is a branch: {}", revision);
        let branch = repositories::branches::get_by_name(repo, revision)?;
        let branch = branch.ok_or(OxenError::local_branch_not_found(revision))?;
        let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?;
        Ok(commit)
    } else {
        log::debug!("revision is a commit id: {}", revision);
        let commit = repositories::commits::get_by_id(repo, revision)?;
        Ok(commit)
    }
}

/// Get the version file path from a commit id
pub fn get_version_file_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::revisions::get_version_file_from_commit_id(repo, commit_id, path),
    }
}
