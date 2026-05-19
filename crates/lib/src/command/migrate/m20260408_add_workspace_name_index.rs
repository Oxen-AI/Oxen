use super::Migrate;

use crate::core::workspaces::workspace_name_index;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::Workspace;
use crate::util;

pub struct AddWorkspaceNameIndexMigration;

impl Migrate for AddWorkspaceNameIndexMigration {
    fn name(&self) -> &'static str {
        "add_workspace_name_index"
    }

    fn description(&self) -> &'static str {
        "Creates a RocksDB index mapping workspace names to IDs for O(1) lookup"
    }

    fn up(&self, repo: LocalRepository) -> Result<(), OxenError> {
        let workspaces_dir = Workspace::workspaces_dir(&repo);
        if !workspaces_dir.exists() {
            return Ok(());
        }

        log::info!("Creating workspace name index for repo: {:?}", repo.path);

        let idx = workspace_name_index::get_index(&repo)?;
        idx.rebuild_from_disk(&repo)?;
        Ok(())
    }

    fn down(&self, repo: LocalRepository) -> Result<(), OxenError> {
        let index_dir = workspace_name_index::index_dir(&repo);
        workspace_name_index::remove_from_cache(&repo);
        if index_dir.exists() {
            util::fs::remove_dir_all(index_dir)?;
        }
        Ok(())
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        // Needed if workspace directory exists but index does not
        let workspaces_dir = Workspace::workspaces_dir(repo);
        Ok(workspaces_dir.exists() && !workspace_name_index::index_exists(repo))
    }
}
