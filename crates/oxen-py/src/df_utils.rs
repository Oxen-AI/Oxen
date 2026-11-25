use pyo3::prelude::*;
use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_data_frame::PyDataFrame;
use liboxen::{core::df::tabular, opts::DFOpts};

#[pyfunction]
pub fn load(path: PathBuf) -> Result<PyDataFrame, PyOxenError> {
    let opts = DFOpts::empty();
    let df = pyo3_async_runtimes::tokio::get_runtime()
        .block_on(async { tabular::read_df(path, opts).await })?;
    Ok(PyDataFrame(df))
}

#[pyfunction]
pub fn save(df: PyDataFrame, path: PathBuf) -> Result<(), PyOxenError> {
    let PyDataFrame(mut df) = df;
    tabular::write_df(&mut df, path)?;
    Ok(())
}
