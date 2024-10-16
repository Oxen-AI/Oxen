use fs_extra::dir::get_size;
use rayon::prelude::*;
use std::path::Path;

use crate::core;
use crate::error::OxenError;
use crate::model::{LocalRepository, Namespace};
use crate::repositories;
use crate::util;

pub fn list(path: &Path) -> Vec<String> {
    log::debug!("repositories::namespaces::list",);
    let mut results: Vec<String> = vec![];

    if let Ok(dir) = std::fs::read_dir(path) {
        for entry in dir.into_iter().filter_map(|e| e.ok()) {
            // if the directory has a .oxen dir, let's add it, otherwise ignore
            let path = entry.path();

            log::debug!("repositories::namespaces::list checking path {:?}", path);

            if path.is_dir() && !util::fs::is_in_oxen_hidden_dir(&path) {
                results.push(path.file_name().unwrap().to_str().unwrap().to_string())
            }
        }
    }

    results
}

// return the named namespace object
pub fn get(data_dir: &Path, name: &str) -> Result<Option<Namespace>, OxenError> {
    log::debug!("repositories::namespaces::get {}", name);
    let namespace_path = data_dir.join(name);

    if !namespace_path.is_dir() {
        return Ok(None);
    }

    let mut namespace = Namespace {
        name: name.to_string(),
        storage_usage_gb: 0.0,
    };

    let repos = repositories::list_repos_in_namespace(&namespace_path);
    // Get storage per repo in parallel and sum up
    namespace.storage_usage_gb = repos
        .par_iter()
        .map(get_storage_for_repo)
        .sum::<Result<u64, OxenError>>()? as f64
        / bytesize::GB as f64;

    Ok(Some(namespace))
}

fn get_storage_for_repo(repo: &LocalRepository) -> Result<u64, OxenError> {
    let latest_commit = match repositories::commits::latest_commit(repo) {
        Ok(commit) => commit,
        Err(e) => {
            log::warn!(
                "repositories::namespaces::get_storage_for_repo no latest commit for repo {:?}: {}",
                repo.path,
                e
            );
            return Ok(0);
        }
    };

    let cache_path = core::v0_10_0::cache::cachers::repo_size::repo_size_path(repo, &latest_commit);
    log::debug!(
        "repositories::namespaces::get_storage_for_repo cache path {:?}",
        cache_path
    );
    match util::fs::read_from_path(&cache_path) {
        Ok(size) => {
            log::debug!(
                "repositories::namespaces::get_storage_for_repo got repo size {:?}",
                cache_path
            );
            match size.parse::<u64>() {
                Ok(size) => Ok(size),
                Err(e) => {
                    log::error!(
                        "repositories::namespaces::get_storage_for_repo error parsing size: {}",
                        e
                    );
                    Err(OxenError::basic_str(e.to_string()))
                }
            }
        }
        Err(_) => {
            log::warn!("repositories::namespaces::get_storage_for_repo cache file not found, calculating size");
            match get_size(&repo.path) {
                Ok(size) => Ok(size),
                Err(e) => {
                    log::error!(
                        "repositories::namespaces::get_storage_for_repo error getting size: {}",
                        e
                    );
                    Err(OxenError::basic_str(e.to_string()))
                }
            }
        }
    }
}
