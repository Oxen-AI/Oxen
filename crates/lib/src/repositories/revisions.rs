//! Revisions can either be commits by id or head commits on branches by name

use std::path::Path;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use crate::storage::LocalFilePath;

/// Get a commit object from a commit id or branch name
/// Returns Ok(None) if the revision does not exist
pub fn get(repo: &LocalRepository, revision: impl AsRef<str>) -> Result<Option<Commit>, OxenError> {
    let revision = revision.as_ref();
    if revision == "HEAD" {
        let commit = repositories::commits::head_commit(repo)?;
        return Ok(Some(commit));
    }

    let commit_id = repositories::branches::get_commit_id(repo, revision)?
        .unwrap_or_else(|| revision.to_string());
    let commit = repositories::commits::get_by_id(repo, &commit_id)?;
    Ok(commit)
}

/// Get the version file path from a commit id
pub async fn get_version_file_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<LocalFilePath, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::revisions::get_version_file_from_commit_id(repo, commit_id, path).await
        }
    }
}
