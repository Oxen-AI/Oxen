use pyo3::prelude::*;
use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_diff::PyDiff;
use liboxen::repositories;

pub mod py_tabular_diff;
pub mod py_text_diff;

pub use py_tabular_diff::{PyTabularDiff, PyTabularDiffMods, PyTabularDiffSummary};
pub use py_text_diff::PyTextDiff;

#[pyfunction]
#[pyo3(signature = (path_1, keys, compares, path_2=None, repo_dir=None, revision_1=None, revision_2=None))]
pub fn diff_paths(
    path_1: PathBuf,
    keys: Vec<String>,
    compares: Vec<String>,
    path_2: Option<PathBuf>,
    repo_dir: Option<PathBuf>,
    revision_1: Option<String>,
    revision_2: Option<String>,
) -> Result<PyDiff, PyOxenError> {
    let result = repositories::diffs::diff(
        path_1, path_2, keys, compares, repo_dir, revision_1, revision_2,
    )?;

    Ok(PyDiff { diff: result })
}
