use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util;
use std::path::{Path, PathBuf};

use super::index::{object_db_reader::get_object_reader, CommitDirEntryReader};

/// Get the version file path from a commit id
pub fn get_version_file_from_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let commit_id = commit_id.as_ref();
    let path = path.as_ref();
    let parent = match path.parent() {
        Some(parent) => parent,
        None => return Err(OxenError::file_has_no_parent(path)),
    };

    let object_reader = get_object_reader(repo, commit_id)?;

    // Instantiate CommitDirEntryReader to fetch entry
    let relative_parent = util::fs::path_relative_to_dir(parent, &repo.path)?;
    let commit_entry_reader =
        CommitDirEntryReader::new(repo, commit_id, &relative_parent, object_reader)?;
    let file_name = match path.file_name() {
        Some(file_name) => file_name,
        None => return Err(OxenError::file_has_no_name(path)),
    };

    let entry = match commit_entry_reader.get_entry(file_name) {
        Ok(Some(entry)) => entry,
        _ => return Err(OxenError::entry_does_not_exist_in_commit(path, commit_id)),
    };

    Ok(util::fs::version_path(repo, &entry))
}
