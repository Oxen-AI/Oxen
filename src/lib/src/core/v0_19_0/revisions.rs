use crate::error::OxenError;
use crate::util;
use crate::{model::LocalRepository, repositories};
use std::path::{Path, PathBuf};

/// Get the version file path from a commit id
pub fn get_version_file_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let commit_id = commit_id.as_ref();
    let path = path.as_ref();
    let commit = repositories::commits::get_by_id(repo, commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(commit_id))?;

    let file_node = repositories::tree::get_file_by_path(repo, &commit, path)?
        .ok_or(OxenError::entry_does_not_exist_in_commit(path, commit_id))?;

    let version_path = util::fs::version_path_from_hash(repo, file_node.hash.to_string());
    Ok(version_path)
}
