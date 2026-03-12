use liboxen::view::ErrorFileInfo;
use pyo3::prelude::*;

#[pyclass]
#[derive(Clone, Debug)]
pub struct PyErrorFileInfo {
    pub hash: String,
    pub path: Option<String>,
    pub error: String,
}

#[pymethods]
impl PyErrorFileInfo {
    #[getter]
    fn hash(&self) -> &str {
        &self.hash
    }

    #[getter]
    fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    #[getter]
    fn error(&self) -> &str {
        &self.error
    }

    fn __repr__(&self) -> String {
        format!(
            "ErrorFileInfo(hash='{}', path={}, error='{}')",
            self.hash,
            self.path
                .as_deref()
                .map(|p| format!("'{}'", p))
                .unwrap_or_else(|| "None".to_string()),
            self.error
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

impl From<ErrorFileInfo> for PyErrorFileInfo {
    fn from(e: ErrorFileInfo) -> Self {
        PyErrorFileInfo {
            hash: e.hash,
            path: e.path.map(|p| p.to_string_lossy().to_string()),
            error: e.error,
        }
    }
}
