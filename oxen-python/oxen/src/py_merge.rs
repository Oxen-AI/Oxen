use pyo3::prelude::*;

use liboxen::view::merge::Mergeable;

use crate::py_commit::PyCommit;

#[pyclass]
pub struct PyMergeable {
    _mergeable: Mergeable,
}

impl From<Mergeable> for PyMergeable {
    fn from(mergeable: Mergeable) -> PyMergeable {
        PyMergeable {
            _mergeable: mergeable,
        }
    }
}

#[pymethods]
impl PyMergeable {
    fn __repr__(&self) -> String {
        format!("{:?}", self._mergeable)
    }

    #[getter]
    pub fn is_mergeable(&self) -> bool {
        self._mergeable.is_mergeable
    }

    #[getter]
    pub fn conflict_files(&self) -> Vec<String> {
        self._mergeable
            .conflicts
            .iter()
            .map(|c| c.path.clone())
            .collect()
    }

    #[getter]
    pub fn commits(&self) -> Vec<PyCommit> {
        self._mergeable
            .commits
            .iter()
            .map(|c| PyCommit::from(c.to_owned()))
            .collect()
    }
}
