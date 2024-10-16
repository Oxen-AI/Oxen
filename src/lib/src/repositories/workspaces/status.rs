//!
//! Get the status of a workspace
//!
//! What files are staged for commit within a directory
//!

use std::path::Path;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{StagedData, Workspace};

pub fn status(workspace: &Workspace) -> Result<StagedData, OxenError> {
    status_from_dir(workspace, Path::new(""))
}

pub fn status_from_dir(
    workspace: &Workspace,
    directory: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::stager::status(workspace, directory)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::workspaces::status::status(workspace, directory),
    }
}
