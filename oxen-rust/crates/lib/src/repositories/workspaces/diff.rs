//! # oxen workspace diff
//!
//! Compare files and versions in workspaces
//!

use std::path::Path;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::Workspace;
use crate::model::diff::DiffResult;

pub fn diff(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<DiffResult, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::diff::diff(workspace, path),
    }
}
