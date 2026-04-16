use std::path::Path;

use super::Migrate;

use crate::core::workspaces::workspace_name_index;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::Workspace;
use crate::repositories;
use crate::util;
use crate::util::progress_bar::{ProgressBarType, oxen_progress_bar};

pub struct AddWorkspaceNameIndexMigration;

impl Migrate for AddWorkspaceNameIndexMigration {
    fn name(&self) -> &'static str {
        "add_workspace_name_index"
    }

    fn description(&self) -> &'static str {
        "Creates a RocksDB index mapping workspace names to IDs for O(1) lookup"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            run_on_all_repos(path)?;
        } else {
            let repo = LocalRepository::from_dir(path)?;
            run_on_one_repo(&repo)?;
        }
        Ok(())
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            revert_all_repos(path)?;
        } else {
            let repo = LocalRepository::from_dir(path)?;
            revert_one_repo(&repo)?;
        }
        Ok(())
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        // Needed if workspace directory exists but index does not
        let workspaces_dir = Workspace::workspaces_dir(repo);
        Ok(workspaces_dir.exists() && !workspace_name_index::index_exists(repo))
    }
}

fn run_on_all_repos(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match run_on_one_repo(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not create workspace name index for repo {:?}\nErr: {}",
                        util::fs::canonicalize(repo.path),
                        err
                    );
                }
            }
        }
        bar.inc(1);
    }
    Ok(())
}

fn run_on_one_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    let workspaces_dir = Workspace::workspaces_dir(repo);
    if !workspaces_dir.exists() {
        return Ok(());
    }

    log::info!("Creating workspace name index for repo: {:?}", repo.path);

    let idx = workspace_name_index::get_index(repo)?;
    idx.rebuild_from_disk(repo)
}

fn revert_all_repos(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to revert...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Reverting {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match revert_one_repo(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not revert workspace name index for repo {:?}\nErr: {}",
                        repo.path.canonicalize(),
                        err
                    );
                }
            }
        }
        bar.inc(1);
    }
    Ok(())
}

fn revert_one_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    let index_dir = workspace_name_index::index_dir(repo);
    workspace_name_index::remove_from_cache(&repo.path)?;
    if index_dir.exists() {
        util::fs::remove_dir_all(index_dir)?;
    }
    Ok(())
}
