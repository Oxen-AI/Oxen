use pyo3::prelude::*;
use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_diff::PyDiff;
use liboxen::{opts::DiffOpts, repositories};

pub mod py_tabular_diff;
pub mod py_text_diff;

pub use py_tabular_diff::{PyTabularDiff, PyTabularDiffMods, PyTabularDiffSummary};
pub use py_text_diff::PyTextDiff;

#[pyfunction]
#[pyo3(signature = (path_1, keys, path_2=None, repo_dir=None, revision_1=None, revision_2=None))]
pub fn diff_paths(
    path_1: PathBuf,
    keys: Vec<String>,
    path_2: Option<PathBuf>,
    repo_dir: Option<PathBuf>,
    revision_1: Option<String>,
    revision_2: Option<String>,
) -> Result<PyDiff, PyOxenError> {
    let opts = DiffOpts {
        path_1,
        path_2,
        keys,
        repo_dir,
        revision_1,
        revision_2,
        ..Default::default()
    };

    let diff = pyo3_async_runtimes::tokio::get_runtime()
        .block_on(async { repositories::diffs::diff(opts).await })?;

    Ok(PyDiff { diff })
}
