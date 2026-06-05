//! # oxen workspace diff
//!
//! Compare files and versions in workspaces
//!

use std::path::Path;

use crate::core;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::Workspace;
use crate::model::diff::DiffResult;

pub fn diff(
    _repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<DiffResult, OxenError> {
    core::v_latest::workspaces::diff::diff(workspace, path)
}
