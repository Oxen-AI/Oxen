//! Marker file written at the start of a client-side merge and removed after
//! HEAD advances. Its presence on a subsequent merge tells the resumed merge
//! that a prior attempt left the working tree in a half-applied state for the
//! same target commit and that it should force-restore target files rather
//! than flag them as conflicts.

use std::path::PathBuf;

use crate::constants;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util;

fn marker_path(repo: &LocalRepository) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path).join(constants::MERGE_IN_PROGRESS_FILE)
}

/// Write the marker containing the target commit id.
pub async fn write(repo: &LocalRepository, target_commit_id: &str) -> Result<(), OxenError> {
    let path = marker_path(repo);
    tokio::fs::write(&path, target_commit_id.as_bytes()).await?;
    Ok(())
}

/// Read the target commit id from the marker, or `None` if no marker exists.
pub async fn read(repo: &LocalRepository) -> Result<Option<String>, OxenError> {
    let path = marker_path(repo);
    match tokio::fs::read_to_string(&path).await {
        Ok(contents) => Ok(Some(contents.trim().to_string())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Remove the marker if present. No-op if it does not exist.
pub async fn clear(repo: &LocalRepository) -> Result<(), OxenError> {
    let path = marker_path(repo);
    match tokio::fs::remove_file(&path).await {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

/// Return true if the marker file exists.
pub async fn exists(repo: &LocalRepository) -> Result<bool, OxenError> {
    Ok(tokio::fs::try_exists(marker_path(repo)).await?)
}
