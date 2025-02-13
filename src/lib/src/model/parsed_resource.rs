use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::{workspace::WorkspaceView, Branch, Commit, Workspace};

/// Internal model
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ParsedResource {
    pub commit: Option<Commit>,
    pub branch: Option<Branch>,
    pub workspace: Option<Workspace>,
    pub path: PathBuf,
    pub version: PathBuf,
    pub resource: PathBuf,
}

impl std::fmt::Display for ParsedResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}",
            self.version.to_string_lossy(),
            self.path.to_string_lossy()
        )
    }
}

/// External (view) model that is returned to the client with fewer fields.
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ParsedResourceView {
    pub workspace: Option<WorkspaceView>,
    pub commit: Option<Commit>,
    pub branch: Option<Branch>,
    pub path: PathBuf,
    pub version: PathBuf,
    pub resource: PathBuf,
}

/// Conversion from the internal `ParsedResource` to the external `ParsedResourceView`.
impl From<ParsedResource> for ParsedResourceView {
    fn from(pr: ParsedResource) -> Self {
        Self {
            workspace: pr.workspace.map(WorkspaceView::from),
            commit: pr.commit,
            branch: pr.branch,
            path: pr.path,
            version: pr.version,
            resource: pr.resource,
        }
    }
}
