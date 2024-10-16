use std::path::{Path, PathBuf};

use crate::core::v0_10_0::index::commit_entry_reader::CommitEntryReader;
use crate::core::v0_10_0::index::schema_reader::SchemaReader;
use crate::core::v0_10_0::index::stager::Stager;
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::repositories;
use crate::util;

// Stages a file in a specified directory
pub fn add(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let repo = &workspace.base_repo;
    let workspace_repo = &workspace.workspace_repo;
    let commit = &workspace.commit;
    // Stager will be in the new repo workspace
    let stager = Stager::new(workspace_repo)?;
    // But we will read from the commit in the main repo
    let commit = repositories::commits::get_by_id(repo, &commit.id)?.unwrap();
    let reader = CommitEntryReader::new(repo, &commit)?;
    log::debug!("core::v0_10_0::index::workspaces::files::add adding file {filepath:?}");
    // Add a schema_reader to stager.add_file for?

    let schema_reader = SchemaReader::new_from_workspace(workspace_repo, &commit.id, workspace)?;

    stager.add_file(filepath.as_ref(), &reader, &schema_reader)?;
    log::debug!("done adding file in the stager");

    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

pub fn has_file(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<bool, OxenError> {
    let filepath = filepath.as_ref();
    // Stager will be in the new repo workspace
    let stager = Stager::new(&workspace.workspace_repo)?;
    stager.has_staged_file(filepath)
}

pub fn delete_file(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<(), OxenError> {
    // Stager will be in the repo workspace
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let stager = Stager::new(workspace_repo)?;
    stager.remove_staged_file(filepath)?;
    let full_path = workspace_repo.path.join(filepath);
    match util::fs::remove_file(&full_path) {
        Ok(_) => Ok(()),
        Err(e) => {
            log::error!("Error deleting file {full_path:?} -> {e:?}");
            Err(OxenError::entry_does_not_exist(full_path))
        }
    }
}
