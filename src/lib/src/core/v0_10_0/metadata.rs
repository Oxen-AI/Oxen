//! Helper functions to get metadata from the local filesystem.
//!

use crate::core::v0_10_0::index::CommitEntryReader;
use crate::core::v0_10_0::index::CommitReader;
use crate::error::OxenError;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use crate::util;

use std::path::Path;

/// Returns metadata with latest commit information. Less efficient than get().
pub fn get_cli(
    repo: &LocalRepository,
    entry_path: impl AsRef<Path>,
    data_path: impl AsRef<Path>,
) -> Result<CLIMetadataEntry, OxenError> {
    let path = data_path.as_ref();
    let entry_path = entry_path.as_ref();
    let base_name = entry_path
        .file_name()
        .ok_or(OxenError::file_has_no_name(path))?;
    let size = repositories::metadata::get_file_size(path)?;
    let hash = util::hasher::hash_file_contents(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);

    let commit_reader = CommitReader::new(repo)?;

    // Not the most efficient, if there are a ton of commits, but it's the easiest way to get the last updated commit
    let mut last_updated: Option<Commit> = None;
    // Sort commits by timestamp
    let commits = commit_reader.list_all_sorted_by_timestamp()?;

    // Now that we know the commits are sorted, we can iterate through them and find when the file was last updated
    for commit in commits {
        log::debug!("looking for entry in commit {commit}");
        let commit_entry_reader = CommitEntryReader::new(repo, &commit)?;
        match commit_entry_reader.get_entry(entry_path) {
            Ok(Some(entry)) => {
                log::debug!(
                    "considering commit {} for file {} and entry.hash {} current hash {}",
                    commit,
                    entry_path.display(),
                    entry.hash,
                    hash
                );
                if last_updated.is_none() {
                    last_updated = Some(commit.clone());
                }

                let latest = last_updated.as_ref().unwrap();

                // make sure the commit is newer than the last one
                // and that the hash is the same as the current version
                // if the hash is the same as the current data, this is the latest commit given that file
                if commit.timestamp >= latest.timestamp && entry.hash == hash {
                    last_updated = Some(commit);
                    break;
                }
            }
            Ok(None) => {
                continue;
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    Ok(CLIMetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        last_updated,
        hash,
        size,
        data_type,
        mime_type,
        extension,
    })
}
