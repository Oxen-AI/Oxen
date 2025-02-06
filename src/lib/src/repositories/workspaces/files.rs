use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::Workspace;

use std::path::{Path, PathBuf};

pub fn exists(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::exists(workspace, path),
    }
}

pub fn add(workspace: &Workspace, path: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::add(workspace, path),
    }
}

pub fn delete(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::delete(workspace, path),
    }
}

pub async fn import(
    url: &str,
    auth: &str,
    directory: PathBuf,
    filename: String,
    workspace: &Workspace,
) -> Result<(), OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::workspaces::files::import(url, auth, directory, filename, workspace)
                .await?;
            Ok(())
        }
    }
}
