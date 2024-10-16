//! # oxen workspace diff
//!
//! Compare files and versions in workspaces
//!

use std::path::Path;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::diff::DiffResult;
use crate::model::LocalRepository;
use crate::model::Workspace;

pub fn diff(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<DiffResult, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => core::v0_19_0::workspaces::diff::diff(workspace, path),
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::data_frames::diff(workspace, path)
        }
    }
}
