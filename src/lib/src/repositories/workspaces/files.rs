use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::Workspace;

use std::path::{Path, PathBuf};

pub fn exists(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::files::has_file(workspace, path)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::workspaces::files::exists(workspace, path),
    }
}

pub fn add(workspace: &Workspace, path: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::index::workspaces::files::add(workspace, path),
        MinOxenVersion::V0_19_0 => core::v0_19_0::workspaces::files::add(workspace, path),
    }
}

pub fn delete(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::index::workspaces::files::delete_file(workspace, path)
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::workspaces::files::delete(workspace, path),
    }
}
