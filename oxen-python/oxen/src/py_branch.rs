use liboxen::model::Branch;
use pyo3::prelude::*;

#[pyclass]
pub struct PyBranch {
    _branch: Branch,
}

#[pymethods]
impl PyBranch {
    #[new]
    #[pyo3(signature = (name, commit_id))]
    pub fn new(name: String, commit_id: String) -> Self {
        Self {
            _branch: Branch { name, commit_id },
        }
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self._branch.name
    }

    #[getter]
    pub fn commit_id(&self) -> &str {
        &self._branch.commit_id
    }

    fn __repr__(&self) -> String {
        format!(
            "Branch(name={}, commit_id={})",
            self._branch.name, self._branch.commit_id
        )
    }

    fn __str__(&self) -> String {
        self._branch.name.to_string()
    }
}

impl From<Branch> for PyBranch {
    fn from(branch: Branch) -> PyBranch {
        PyBranch { _branch: branch }
    }
}
