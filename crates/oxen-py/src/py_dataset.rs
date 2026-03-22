//! Python bindings for the `Repo` struct.
//!

use liboxen::opts::DFOpts;
use pyo3::prelude::*;

use pyo3_polars::PyDataFrame;

use crate::error::PyOxenError;

use std::path::PathBuf;

#[pyclass]
pub struct PyDataset {}

#[pymethods]
impl PyDataset {
    #[staticmethod]
    fn df(path: PathBuf) -> Result<PyDataFrame, PyOxenError> {
        let opts = DFOpts::empty();
        let df = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { liboxen::core::df::tabular::read_df(path, opts).await })?;
        Ok(PyDataFrame(df))
    }
}
