use liboxen::error::OxenError;
use liboxen::model::diff::generic_diff::GenericDiff;
use liboxen::model::DiffEntry;
use pyo3::prelude::*;

use liboxen::model::diff::DiffResult;

use crate::diff::{PyTabularDiff, PyTextDiff};
use crate::error::PyOxenError;

#[pyclass]
pub struct PyDiff {
    pub diff: DiffResult,
}

#[pymethods]
impl PyDiff {
    fn __repr__(&self) -> String {
        format!("PyDiff(format={:?})", self.format())
    }

    #[getter]
    pub fn format(&self) -> String {
        match &self.diff {
            DiffResult::Tabular(_diff) => "tabular".to_string(),
            DiffResult::Text(_diff) => "text".to_string(),
        }
    }

    #[getter]
    pub fn tabular(&self) -> Result<PyTabularDiff, PyOxenError> {
        match &self.diff {
            DiffResult::Tabular(diff) => Ok(PyTabularDiff::from(diff)),
            _ => Err(OxenError::basic_str("Diff is not tabular").into()),
        }
    }

    #[getter]
    pub fn text(&self) -> Result<PyTextDiff, PyOxenError> {
        match &self.diff {
            DiffResult::Text(diff) => Ok(PyTextDiff::from(diff)),
            _ => Err(OxenError::basic_str("Diff is not text").into()),
        }
    }
}

#[pyclass]
pub struct PyDiffEntry {
    pub _diff: DiffEntry,
}

#[pymethods]
impl PyDiffEntry {
    fn __repr__(&self) -> String {
        format!("PyDiffEntry(diff={:?})", self._diff)
    }

    #[getter]
    fn format(&self) -> String {
        match &self._diff.diff {
            Some(GenericDiff::TabularDiff(_diff)) => "tabular".to_string(),
            Some(GenericDiff::TextDiff(_diff)) => "text".to_string(),
            Some(GenericDiff::DirDiff(_diff)) => "dir".to_string(),
            None => "unknown".to_string(),
        }
    }

    #[getter]
    fn text(&self) -> Result<PyTextDiff, PyOxenError> {
        match &self._diff.diff {
            Some(GenericDiff::TextDiff(diff)) => Ok(PyTextDiff::from(diff)),
            _ => Err(OxenError::basic_str("Diff is not text").into()),
        }
    }
}

impl From<DiffEntry> for PyDiffEntry {
    fn from(diff: DiffEntry) -> PyDiffEntry {
        PyDiffEntry { _diff: diff }
    }
}
