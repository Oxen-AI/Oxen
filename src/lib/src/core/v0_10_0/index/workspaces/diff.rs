use std::path::Path;

use crate::core;
use crate::error::OxenError;
use crate::model::diff::DiffResult;
use crate::model::LocalRepository;
use crate::model::Workspace;
use crate::util;

pub fn diff(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<DiffResult, OxenError> {
    if util::fs::is_tabular(path.as_ref()) {
        core::v0_10_0::index::workspaces::data_frames::diff(workspace, path)
    } else {
        Err(OxenError::basic_str("Unsupported file type"))
    }
}
