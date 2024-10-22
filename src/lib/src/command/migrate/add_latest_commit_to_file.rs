use std::path::Path;

use super::Migrate;

use crate::core::cache::cachers;
use crate::core::v0_10_0::index::{CommitEntryReader, CommitReader, ObjectDBReader};
use crate::error::OxenError;
use crate::model::LocalRepository;

use crate::api;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

pub struct AddLatestCommitToFileMigration;
impl AddLatestCommitToFileMigration {}

impl Migrate for AddLatestCommitToFileMigration {
    fn name(&self) -> &'static str {
        "add_latest_commit_to_file"
    }
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            add_latest_commit_to_file_for_all_repos_up(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            add_latest_commit_to_file_up(&repo)?;
        }
        Ok(())
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            add_latest_commit_to_file_for_all_repos_down(path)?;
        } else {
            println!("Running down migration");
            let repo = LocalRepository::new(path)?;
            add_latest_commit_to_file_down(&repo)?;
        }
        Ok(())
    }

    fn is_needed(&self, _repo: &LocalRepository) -> Result<bool, OxenError> {
        // Server-side migration, not necessary for autodetection on client
        Ok(false)
    }
}

pub fn add_latest_commit_to_file_for_all_repos_up(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = api::local::repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        // Show the canonical namespace path
        log::debug!(
            "This is the namespace path we're walking: {:?}",
            namespace_path.canonicalize()?
        );
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match add_latest_commit_to_file_up(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate version files for repo {:?}\nErr: {}",
                        repo.path.canonicalize(),
                        err
                    )
                }
            }
        }
        bar.inc(1);
    }

    Ok(())
}

pub fn add_latest_commit_to_file_up(repo: &LocalRepository) -> Result<(), OxenError> {
    // Traverses commits from BASE to HEAD and write all schemas for all history leading up to HEAD.
    let mut lock_file = api::local::repositories::get_lock_file(repo)?;
    let _mutex = api::local::repositories::get_exclusive_lock(&mut lock_file)?;

    let reader = CommitReader::new(repo)?;
    let mut all_commits = reader.list_all()?;
    // Sort by timestamp from oldest to newest
    all_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    for current_commit in &all_commits {
        let entries = {
            // Need the readers to go out of scope
            let entry_reader = CommitEntryReader::new(repo, current_commit)?;
            entry_reader.list_entries()?
        };

        log::debug!("Updating {} entries", entries.len());
    }

    Ok(())
}

pub fn add_latest_commit_to_file_down(_repo: &LocalRepository) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}
pub fn add_latest_commit_to_file_for_all_repos_down(_path: &Path) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}
