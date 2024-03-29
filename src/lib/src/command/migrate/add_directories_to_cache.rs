use std::path::Path;

use crate::api;
use crate::core::cache;
use crate::core::index::CommitReader;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

use super::Migrate;

pub struct AddDirectoriesToCacheMigration;
impl AddDirectoriesToCacheMigration {}

impl Migrate for AddDirectoriesToCacheMigration {
    fn name(&self) -> &'static str {
        "add_directories_to_cache"
    }
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            add_directories_to_cache_for_all_repos_up(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            add_directories_to_cache_up(&repo)?;
        }
        Ok(())
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            add_directories_to_cache_for_all_repos_down(path)?;
        } else {
            println!("Running down migration");
            let repo = LocalRepository::new(path)?;
            add_directories_to_cache_down(&repo)?;
        }
        Ok(())
    }

    fn is_needed(&self, _repo: &LocalRepository) -> Result<bool, OxenError> {
        // Server migration only, no client-side migration needed
        Ok(false)
    }
}

pub fn add_directories_to_cache_up(repo: &LocalRepository) -> Result<(), OxenError> {
    // Lock repo, releases when goes out of scope at the end of this
    let mut lock_file = api::local::repositories::get_lock_file(repo)?;
    let _mutex = api::local::repositories::get_exclusive_lock(&mut lock_file)?;

    let reader = CommitReader::new(repo)?;

    let mut all_commits = reader.list_all()?;

    all_commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    for commit in all_commits {
        cache::cachers::content_stats::compute(repo, &commit)?;
    }

    Ok(())
}

pub fn add_directories_to_cache_down(_repo: &LocalRepository) -> Result<(), OxenError> {
    println!("Nothing to do here.");
    Ok(())
}

pub fn add_directories_to_cache_for_all_repos_down(_path: &Path) -> Result<(), OxenError> {
    println!("Nothing to do here.");
    Ok(())
}

pub fn add_directories_to_cache_for_all_repos_up(path: &Path) -> Result<(), OxenError> {
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
        log::debug!("🐂 Migrating {} repos", repos.len());
        for repo in repos {
            log::debug!("Migrating repo {:?}", repo.path);
            match add_directories_to_cache_up(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate directory cache for repo {:?}\nErr: {}",
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
