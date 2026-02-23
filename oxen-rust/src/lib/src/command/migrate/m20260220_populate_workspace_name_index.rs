use std::path::Path;

use super::Migrate;

use crate::constants::WORKSPACE_NAMES_DIR;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{repositories, repositories::workspaces};

pub struct PopulateWorkspaceNameIndexMigration;

impl Migrate for PopulateWorkspaceNameIndexMigration {
    fn name(&self) -> &'static str {
        "populate_workspace_name_index"
    }

    fn description(&self) -> &'static str {
        "Populates the workspace name → ID index for O(1) name lookups"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            run_up_on_all_repos(path)
        } else {
            let repo = LocalRepository::from_dir(path)?;
            let count = workspaces::populate_workspace_name_index(&repo)?;
            println!("Indexed {count} named workspace(s).");
            Ok(())
        }
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            run_down_on_all_repos(path)
        } else {
            let repo = LocalRepository::from_dir(path)?;
            remove_workspace_name_index(&repo)
        }
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        let names_dir = util::fs::oxen_hidden_dir(&repo.path).join(WORKSPACE_NAMES_DIR);
        let has_index = names_dir.exists() && names_dir.is_dir();

        // If the index already exists, no migration needed
        if has_index {
            return Ok(false);
        }

        // Only needed if there are workspaces to index
        let workspaces = workspaces::list(repo)?;
        Ok(workspaces.iter().any(|ws| ws.name.is_some()))
    }
}

fn run_up_on_all_repos(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match workspaces::populate_workspace_name_index(&repo) {
                Ok(count) => {
                    if count > 0 {
                        log::info!(
                            "Indexed {count} named workspace(s) for repo {:?}",
                            repo.path
                        );
                    }
                }
                Err(err) => {
                    log::error!(
                        "Could not populate workspace name index for repo {:?}\nErr: {}",
                        repo.path,
                        err
                    );
                }
            }
        }
        bar.inc(1);
    }
    Ok(())
}

fn run_down_on_all_repos(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to revert...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Reverting {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            if let Err(err) = remove_workspace_name_index(&repo) {
                log::error!(
                    "Could not remove workspace name index for repo {:?}\nErr: {}",
                    repo.path,
                    err
                );
            }
        }
        bar.inc(1);
    }
    Ok(())
}

fn remove_workspace_name_index(repo: &LocalRepository) -> Result<(), OxenError> {
    // Clear the DB from the cache first so we don't hold a stale handle
    crate::core::workspaces::remove_from_cache(&repo.path)?;
    let names_dir = util::fs::oxen_hidden_dir(&repo.path).join(WORKSPACE_NAMES_DIR);
    if names_dir.exists() {
        util::fs::remove_dir_all(&names_dir)?;
    }
    Ok(())
}
