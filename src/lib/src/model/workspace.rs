use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACES_DIR};
use crate::model::{Commit, LocalRepository};
use crate::util;

// Define a struct for the workspace config to make it easier to serialize
#[derive(Serialize, Deserialize)]
pub struct WorkspaceConfig {
    pub workspace_commit_id: String,
    pub is_editable: bool,
    pub workspace_name: Option<String>,
    pub workspace_id: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Workspace {
    pub id: String,
    pub name: Option<String>,
    // Workspaces have a base repository that they are created in .oxen/
    pub base_repo: LocalRepository,
    // And a sub repository that is just to make changes in
    // .oxen/workspaces/<workspace_id>/.oxen/
    pub workspace_repo: LocalRepository,
    // .oxen/workspaces/<workspace_ id>/.oxen/WORKSPACE_CONFIG
    pub is_editable: bool,
    pub commit: Commit,
}

impl Workspace {
    pub fn workspaces_dir(repo: &LocalRepository) -> PathBuf {
        repo.path.join(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR)
    }

    pub fn workspace_dir(repo: &LocalRepository, workspace_id_hash: &str) -> PathBuf {
        Self::workspaces_dir(repo).join(workspace_id_hash)
    }

    /// Returns the path to the workspace directory
    pub fn dir(&self) -> PathBuf {
        let workspace_id_hash = util::hasher::hash_str_sha256(&self.id);
        Self::workspace_dir(&self.base_repo, &workspace_id_hash)
    }
}

/// Conversion from the internal `Workspace` to a `WorkspaceView`
impl From<Workspace> for WorkspaceView {
    fn from(workspace: Workspace) -> Self {
        Self {
            name: workspace.name,
            id: workspace.id,
            commit: workspace.commit,
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WorkspaceView {
    pub name: Option<String>,
    pub id: String,
    pub commit: Commit,
}
