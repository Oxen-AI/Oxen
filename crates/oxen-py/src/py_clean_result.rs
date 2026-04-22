use std::path::PathBuf;

use liboxen::repositories::clean::CleanResult;
use pyo3::prelude::*;

/// Python-visible outcome of `Repo.clean(...)`.
///
/// Mirrors `liboxen::repositories::clean::CleanResult` — in dry-run mode (`applied == False`)
/// the `files` and `dirs` lists describe what *would* be removed; in apply mode (`applied == True`)
/// they describe what actually was removed.
#[pyclass]
pub struct PyCleanResult {
    inner: CleanResult,
}

#[pymethods]
impl PyCleanResult {
    #[getter]
    pub fn files(&self) -> Vec<PathBuf> {
        self.inner.files.clone()
    }

    #[getter]
    pub fn dirs(&self) -> Vec<PathBuf> {
        self.inner.dirs.clone()
    }

    #[getter]
    pub fn total_bytes(&self) -> u64 {
        self.inner.total_bytes
    }

    #[getter]
    pub fn applied(&self) -> bool {
        self.inner.applied
    }

    fn __repr__(&self) -> String {
        format!(
            "CleanResult(files={}, dirs={}, total_bytes={}, applied={})",
            self.inner.files.len(),
            self.inner.dirs.len(),
            self.inner.total_bytes,
            self.inner.applied,
        )
    }
}

impl From<CleanResult> for PyCleanResult {
    fn from(inner: CleanResult) -> Self {
        Self { inner }
    }
}
