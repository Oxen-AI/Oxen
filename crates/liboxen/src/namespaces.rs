use rayon::prelude::*;
use std::path::Path;

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

/// The named namespace, or `None` when it has no directory on disk. A repository whose recorded
/// size cannot be read contributes nothing to the storage total rather than failing it.
pub fn get(data_dir: &Path, name: &str) -> Option<Namespace> {
    log::debug!("repositories::namespaces::get {name}");
    let namespace_path = data_dir.join(name);

    if !namespace_path.is_dir() {
        return None;
    }

    let mut namespace = Namespace {
        name: name.to_string(),
        storage_usage_gb: 0.0,
    };

    let repos: Vec<LocalRepository> =
        repositories::list_repos_in_namespace(&namespace_path).collect();
    // Get storage per repo in parallel and sum up
    namespace.storage_usage_gb =
        repos.par_iter().map(get_storage_for_repo).sum::<u64>() as f64 / bytesize::GB as f64;

    Some(namespace)
}

/// The figure recorded for `repo`, or zero when none can be read.
fn get_storage_for_repo(repo: &LocalRepository) -> u64 {
    log::debug!(
        "repositories::namespaces::get_storage_for_repo for repo {:?}",
        repo.path
    );

    match repositories::size::get_size(repo) {
        Ok(size_file) => match size_file.status {
            repositories::size::SizeStatus::Done => {
                log::debug!("Got repo size: {} bytes", size_file.size);
                size_file.size
            }
            repositories::size::SizeStatus::Pending => {
                log::info!("Size calculation is still pending, returning previous size");
                size_file.size
            }
            repositories::size::SizeStatus::Error => {
                tracing::warn!(
                    repo = ?repo.path,
                    "Using the size recorded for a repository whose recalculation failed"
                );
                size_file.size
            }
        },
        Err(e) => {
            tracing::error!(
                repo = ?repo.path,
                cause = ?e,
                "Could not read a repository's recorded size, leaving it out of the total"
            );
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::repositories::size::{RepoSizeFile, SizeStatus, repo_size_path};
    use crate::test;
    use crate::util::fs::AtomicFile;

    /// Record `size` for a repo at `path` without going through a commit, since what is under test
    /// is the summation rather than how a figure gets computed.
    fn repo_recording(path: &Path, size: u64) -> Result<LocalRepository, OxenError> {
        let repo = repositories::init(path)?;
        let recorded = RepoSizeFile {
            status: SizeStatus::Done,
            size,
        };
        AtomicFile::new(repo_size_path(&repo)).write(recorded.to_string().as_bytes())?;
        Ok(repo)
    }

    #[test]
    fn test_get_sums_the_recorded_size_of_every_repo() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            assert!(
                get(dir, "ox").is_none(),
                "a namespace with no directory on disk is reported as absent"
            );

            let namespace_path = dir.join("ox");
            repo_recording(&namespace_path.join("first"), 1500)?;
            let second = repo_recording(&namespace_path.join("second"), 2500)?;

            let namespace = get(dir, "ox").expect("namespace exists");
            assert_eq!(namespace.storage_usage_gb, 4000.0 / bytesize::GB as f64);

            let failed = RepoSizeFile {
                status: SizeStatus::Error,
                size: 2500,
            };
            AtomicFile::new(repo_size_path(&second)).write(failed.to_string().as_bytes())?;
            let namespace = get(dir, "ox").expect("namespace exists");
            assert_eq!(
                namespace.storage_usage_gb,
                4000.0 / bytesize::GB as f64,
                "a repository whose recalculation failed counts at the figure from before it"
            );

            util::fs::write_to_path(repo_size_path(&second), "not a size record")?;
            let namespace = get(dir, "ox").expect("namespace exists");
            assert_eq!(
                namespace.storage_usage_gb,
                1500.0 / bytesize::GB as f64,
                "a record that will not parse leaves the rest of the total intact"
            );
            Ok(())
        })
    }
}
