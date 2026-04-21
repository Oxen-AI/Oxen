use liboxen::error::OxenError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use thiserror::Error;

/// A [PyOxenError] is a wrapper for an [OxenError] that can be converted into a pyO3 [PyErr].
#[derive(Debug, Error)]
#[error("{0}")]
pub struct PyOxenError(OxenError);

/// Automatically convert a [PyOxenError] into a pyO3 [PyErr]. Specifically, a Python `ValueError`.
impl From<PyOxenError> for PyErr {
    fn from(error: PyOxenError) -> Self {
        PyValueError::new_err(error.to_string())
    }
}

/// We can convert anything that can convert into an [OxenError] into a [PyOxenError].
impl<T: Into<OxenError>> From<T> for PyOxenError {
    fn from(error: T) -> Self {
        let as_oxen: OxenError = error.into();
        Self(as_oxen)
    }
}
