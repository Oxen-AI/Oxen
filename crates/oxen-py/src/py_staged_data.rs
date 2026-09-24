use pyo3::prelude::*;

use liboxen::model::StagedData as OxenStagedData;
use liboxen::model::{StagedEntry, StagedEntryStatus};
use liboxen::view::{PaginatedDirEntries, RemoteStagedStatus};
use std::path::PathBuf;

#[pyclass]
pub struct PyStagedData {
    pub data: OxenStagedData,
}

impl PyStagedData {
    fn staged_paths(&self, status: StagedEntryStatus) -> Vec<String> {
        self.data
            .paths_with_status(status)
            .map(|path| path.to_string_lossy().to_string())
            .collect()
    }
}

#[pymethods]
impl PyStagedData {
    fn __repr__(&self) -> String {
        format!(
            "PyStagedData(added={}, removed={}, modified={})",
            self.data
                .paths_with_status(StagedEntryStatus::Added)
                .count(),
            self.data
                .paths_with_status(StagedEntryStatus::Removed)
                .count(),
            self.data
                .paths_with_status(StagedEntryStatus::Modified)
                .count()
        )
    }

    fn __str__(&self) -> String {
        self.data.to_string()
    }

    pub fn is_dirty(&self) -> bool {
        !self.data.is_clean()
    }

    pub fn is_clean(&self) -> bool {
        self.data.is_clean()
    }

    /// Paths staged as added for the next commit.
    pub fn added_files(&self) -> PyResult<Vec<String>> {
        Ok(self.staged_paths(StagedEntryStatus::Added))
    }

    /// Paths staged as removed for the next commit.
    pub fn removed_files(&self) -> PyResult<Vec<String>> {
        Ok(self.staged_paths(StagedEntryStatus::Removed))
    }

    /// Paths staged as modified for the next commit.
    pub fn modified_files(&self) -> PyResult<Vec<String>> {
        Ok(self.staged_paths(StagedEntryStatus::Modified))
    }

    /// Tracked paths missing from disk with nothing staged for them.
    pub fn unstaged_removed_files(&self) -> PyResult<Vec<String>> {
        Ok(self
            .data
            .removed_files
            .iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect())
    }

    /// Tracked paths edited on disk with nothing staged for them.
    pub fn unstaged_modified_files(&self) -> PyResult<Vec<String>> {
        Ok(self
            .data
            .modified_files
            .iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect())
    }
}

impl From<RemoteStagedStatus> for PyStagedData {
    fn from(remote_status: RemoteStagedStatus) -> PyStagedData {
        let mut status = OxenStagedData::empty();
        status.staged_dirs = remote_status.added_dirs;
        status.staged_files = staged_entries(remote_status.added_files, StagedEntryStatus::Added)
            .chain(staged_entries(
                remote_status.modified_files,
                StagedEntryStatus::Modified,
            ))
            .chain(staged_entries(
                remote_status.removed_files,
                StagedEntryStatus::Removed,
            ))
            .collect();
        PyStagedData { data: status }
    }
}

fn staged_entries(
    entries: PaginatedDirEntries,
    status: StagedEntryStatus,
) -> impl Iterator<Item = (PathBuf, StagedEntry)> {
    entries.entries.into_iter().map(move |entry| {
        (
            PathBuf::from(entry.filename()),
            StagedEntry::empty_status(status.clone()),
        )
    })
}
