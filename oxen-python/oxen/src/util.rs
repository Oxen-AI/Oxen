//! Oxen Utils
//!

use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use std::path::PathBuf;

use crate::error::PyOxenError;

use liboxen::util::fs::oxen_config_dir;
use liboxen::{opts::DFOpts, util};

/// Get the default home directory for exen
#[pyfunction]
pub fn get_oxen_config_dir() -> Result<PathBuf, PyOxenError> {
    let path = oxen_config_dir()?;
    Ok(path)
}

/// Checks if a path is tabular
#[pyfunction]
pub fn is_tabular(path: PathBuf) -> PyResult<bool> {
    Ok(util::fs::is_tabular(&path))
}

/// Checks if a path is tabular
#[pyfunction]
pub fn read_df(path: PathBuf) -> Result<PyDataFrame, PyOxenError> {
    let opts = DFOpts::empty();
    let df = liboxen::core::df::tabular::read_df(path, opts)?;
    Ok(PyDataFrame(df))
}
