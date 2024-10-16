use crate::core::v0_10_0::index::{CommitEntryReader, Stager};
use crate::error::OxenError;
use crate::model::{LocalRepository, StagedData};
use std::path::Path;

pub fn status(repository: &LocalRepository) -> Result<StagedData, OxenError> {
    let reader = CommitEntryReader::new_from_head(repository)?;
    let stager = Stager::new(repository)?;
    let status = stager.status(&reader)?;
    Ok(status)
}

pub fn status_from_dir(
    repo: &LocalRepository,
    dir: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    let reader = CommitEntryReader::new_from_head(repo)?;
    let stager = Stager::new(repo)?;
    let status = stager.status_from_dir(&reader, dir)?;
    Ok(status)
}

pub fn status_without_untracked(repository: &LocalRepository) -> Result<StagedData, OxenError> {
    let reader = CommitEntryReader::new_from_head(repository)?;
    let stager = Stager::new(repository)?;
    let status = stager.status_without_untracked(&reader)?;
    Ok(status)
}
