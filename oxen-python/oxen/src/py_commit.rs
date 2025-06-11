use pyo3::prelude::*;

use liboxen::model::Commit as OxenCommit;
use liboxen::view::PaginatedCommits as OxenPaginatedCommits;

use crate::py_pagination::PyPagination;
// use crate::error::PyOxenError;

#[pyclass]
pub struct PyCommit {
    pub commit: OxenCommit,
}

#[pymethods]
impl PyCommit {
    fn __repr__(&self) -> String {
        format!(
            "PyCommit(id={}, message={}, author={}, email={}, timestamp={}, parent_ids=[{}])",
            self.commit.id,
            self.commit.message,
            self.commit.author,
            self.commit.email,
            self.commit.timestamp,
            self.commit.parent_ids.join(", ")
        )
    }

    fn __str__(&self) -> String {
        self.commit.id.to_owned()
    }

    #[getter]
    pub fn id(&self) -> String {
        self.commit.id.to_string()
    }

    #[getter]
    pub fn commit_id(&self) -> String {
        self.commit.id.to_string()
    }

    #[getter]
    pub fn message(&self) -> String {
        self.commit.message.to_string()
    }

    #[getter]
    pub fn author(&self) -> String {
        self.commit.author.to_string()
    }

    #[getter]
    pub fn email(&self) -> String {
        self.commit.email.to_string()
    }

    #[getter]
    pub fn timestamp(&self) -> String {
        self.commit.timestamp.to_string()
    }

    #[getter]
    pub fn parent_ids(&self) -> Vec<String> {
        self.commit.parent_ids.to_owned()
    }
}

impl From<OxenCommit> for PyCommit {
    fn from(commit: OxenCommit) -> PyCommit {
        PyCommit { commit }
    }
}

#[pyclass]
pub struct PyPaginatedCommits {
    pub _commits: OxenPaginatedCommits,
}

#[pymethods]
impl PyPaginatedCommits {
    fn __repr__(&self) -> String {
        let commits_str = self
            ._commits
            .commits
            .iter()
            .map(|c| PyCommit::from(c.clone()).__repr__())
            .collect::<Vec<String>>()
            .join(",\n");
        format!("[{}]", commits_str)
    }

    fn __str__(&self) -> String {
        let commits_str = self
            ._commits
            .commits
            .iter()
            .map(|c| PyCommit::from(c.clone()).__str__())
            .collect::<Vec<String>>()
            .join(", ");
        format!("[{}]", commits_str)
    }

    #[getter]
    pub fn commits(&self) -> Vec<PyCommit> {
        self._commits
            .commits
            .iter()
            .map(|c| PyCommit::from(c.to_owned()))
            .collect()
    }

    #[getter]
    pub fn pagination(&self) -> PyPagination {
        self._commits.pagination.clone().into()
    }

    fn __len__(&self) -> usize {
        self._commits.commits.len()
    }

    fn __getitem__(&self, idx: isize) -> PyResult<PyCommit> {
        let len = self._commits.commits.len() as isize;
        let idx = if idx < 0 { len + idx } else { idx };

        if idx < 0 || idx >= len {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "Index out of bounds",
            ));
        }

        Ok(PyCommit::from(self._commits.commits[idx as usize].clone()))
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyResult<Py<PyCommitIterator>> {
        let iter = PyCommitIterator {
            commits: slf._commits.commits.clone(),
            index: 0,
        };
        Py::new(slf.py(), iter)
    }

    fn __contains__(&self, commit: &PyCommit) -> bool {
        self._commits
            .commits
            .iter()
            .any(|c| c.id == commit.commit.id)
    }
}

#[pyclass]
struct PyCommitIterator {
    commits: Vec<OxenCommit>,
    index: usize,
}

#[pymethods]
impl PyCommitIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyCommit> {
        if slf.index >= slf.commits.len() {
            None
        } else {
            let commit = PyCommit::from(slf.commits[slf.index].clone());
            slf.index += 1;
            Some(commit)
        }
    }
}

impl From<OxenPaginatedCommits> for PyPaginatedCommits {
    fn from(commits: OxenPaginatedCommits) -> PyPaginatedCommits {
        PyPaginatedCommits { _commits: commits }
    }
}
