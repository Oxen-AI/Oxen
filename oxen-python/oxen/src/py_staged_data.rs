use pyo3::prelude::*;

use liboxen::model::StagedData as OxenStagedData;
use liboxen::model::{StagedEntry, StagedEntryStatus};
use liboxen::view::RemoteStagedStatus;
use std::collections::HashMap;
use std::path::PathBuf;

#[pyclass]
pub struct PyStagedData {
    pub data: OxenStagedData,
}

#[pymethods]
impl PyStagedData {
    fn __repr__(&self) -> String {
        format!(
            "PyStagedData(added={}, removed={}, modified={})",
            self.data.staged_files.len(),
            self.data.removed_files.len(),
            self.data.modified_files.len()
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

    pub fn added_files(&self) -> PyResult<Vec<String>> {
        Ok(self
            .data
            .staged_files
            .iter()
            .map(|f| String::from(f.0.to_string_lossy()))
            .collect())
    }

    pub fn removed_files(&self) -> PyResult<Vec<String>> {
        Ok(self
            .data
            .removed_files
            .iter()
            .map(|f| String::from(f.to_string_lossy()))
            .collect())
    }

    pub fn modified_files(&self) -> PyResult<Vec<String>> {
        Ok(self
            .data
            .modified_files
            .iter()
            .map(|f| String::from(f.to_string_lossy()))
            .collect())
    }
}

impl From<RemoteStagedStatus> for PyStagedData {
    fn from(remote_status: RemoteStagedStatus) -> PyStagedData {
        let mut status = OxenStagedData::empty();
        status.staged_dirs = remote_status.added_dirs;
        let added_files: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.added_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Added),
                )
            }));
        let added_mods: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.modified_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Modified),
                )
            }));
        status.staged_files = added_files.into_iter().chain(added_mods).collect();
        PyStagedData { data: status }
    }
}
