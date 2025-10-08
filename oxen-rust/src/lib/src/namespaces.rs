use rayon::prelude::*;
use std::path::Path;

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

            log::debug!("repositories::namespaces::list checking path {path:?}");

            if path.is_dir() && !util::fs::is_in_oxen_hidden_dir(&path) {
                results.push(path.file_name().unwrap().to_str().unwrap().to_string())
            }
        }
    }

    results
}

// return the named namespace object
pub fn get(data_dir: &Path, name: &str) -> Result<Option<Namespace>, OxenError> {
    log::debug!("repositories::namespaces::get {name}");
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
    log::debug!(
        "repositories::namespaces::get_storage_for_repo for repo {:?}",
        repo.path
    );

    match repositories::size::get_size(repo) {
        Ok(size_file) => match size_file.status {
            repositories::size::SizeStatus::Done => {
                log::debug!("Got repo size: {} bytes", size_file.size);
                Ok(size_file.size)
            }
            repositories::size::SizeStatus::Pending => {
                log::info!("Size calculation is still pending, returning previous size");
                Ok(size_file.size)
            }
            repositories::size::SizeStatus::Error => {
                log::warn!("Size calculation failed, returning 0");
                Err(OxenError::basic_str("Size calculation failed"))
            }
        },
        Err(e) => {
            log::error!(
                "repositories::namespaces::get_storage_for_repo error getting size: {e}"
            );
            Err(e)
        }
    }
}
