use rayon::prelude::*;
use std::path::Path;

use crate::model::{LocalRepository, Namespace};
use crate::repositories;
use crate::repositories::name_table::NAME_TABLE_DIR;
use crate::repositories::size::{self, RepoSizeFile, SizeStatus};
use crate::util;

/// Entries at the top of the sync dir holding the server's own state rather than a namespace's
/// repositories. A directory named for one of these is not reported as a namespace, so a namespace
/// could not be seen under that name either.
const SERVER_OWNED_DIRS: &[&str] = &[NAME_TABLE_DIR];

/// Whether the entry named `name` at the top of the sync dir is the server's own state.
fn is_server_owned(name: &str) -> bool {
    SERVER_OWNED_DIRS.contains(&name)
}

pub fn list(path: &Path) -> Vec<String> {
    log::debug!("repositories::namespaces::list",);
    let mut results: Vec<String> = vec![];

    if let Ok(dir) = std::fs::read_dir(path) {
        for entry in dir.into_iter().filter_map(|e| e.ok()) {
            // if the directory has a .oxen dir, let's add it, otherwise ignore
            let path = entry.path();

            log::debug!("repositories::namespaces::list checking path {path:?}");

            let server_owned = util::fs::is_in_oxen_hidden_dir(&path)
                || path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(is_server_owned);
            if path.is_dir() && !server_owned {
                results.push(path.file_name().unwrap().to_str().unwrap().to_string())
            }
        }
    }

    results
}

/// The named namespace, or `None` when it has no directory on disk. Starts a size recalculation for
/// every repository that has no figure to count, so the total is a lower bound that later reads
/// converge on, and a warning names how many repositories are counted at an unfinished figure.
pub fn get(data_dir: &Path, name: &str) -> Option<Namespace> {
    log::debug!("repositories::namespaces::get {name}");
    let namespace_path = data_dir.join(name);

    if is_server_owned(name) || !namespace_path.is_dir() {
        return None;
    }

    let repos: Vec<LocalRepository> =
        repositories::list_repos_in_namespace(&namespace_path).collect();
    // Get storage per repo in parallel and sum up
    let figures: Vec<RepoSizeFile> = repos.par_iter().map(size::get_size).collect();

    // A read reports a failed pass rather than starting another, so a repository left with no
    // figure would count as nothing on every later read. Start one here for those.
    for (repo, figure) in repos.iter().zip(&figures) {
        if matches!(figure.status, SizeStatus::Error)
            && figure.size == 0
            && let Err(cause) = size::update_size(repo)
        {
            tracing::warn!(
                repo = ?repo.path,
                ?cause,
                "Could not start a size recalculation for a repository counted as nothing"
            );
        }
    }

    let outstanding = figures
        .iter()
        .filter(|figure| !matches!(figure.status, SizeStatus::Done))
        .count();
    if outstanding > 0 {
        tracing::warn!(
            namespace = name,
            outstanding,
            repositories = figures.len(),
            "Reporting a storage total that counts some repositories at a figure no pass completed"
        );
    }

    Some(Namespace {
        name: name.to_string(),
        storage_usage_gb: figures.iter().map(|figure| figure.size).sum::<u64>() as f64
            / bytesize::GB as f64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::repositories::size::repo_size_path;
    use crate::test;
    use crate::util::fs::AtomicFile;

    /// Leave `record` as the size a repo at `path` has recorded, without going through a commit,
    /// since what is under test is the summation rather than how a figure gets computed.
    fn repo_recording(path: &Path, record: &str) -> Result<LocalRepository, OxenError> {
        let repo = repositories::init(path)?;
        AtomicFile::new(repo_size_path(&repo)).write(record.as_bytes())?;
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
            repo_recording(&namespace_path.join("first"), "1500")?;
            let second = repo_recording(&namespace_path.join("second"), "2500")?;

            let namespace = get(dir, "ox").expect("namespace exists");
            assert_eq!(namespace.storage_usage_gb, 4000.0 / bytesize::GB as f64);

            util::fs::write_to_path(repo_size_path(&second), "not a size record")?;
            let namespace = get(dir, "ox").expect("namespace exists");
            assert_eq!(
                namespace.storage_usage_gb,
                1500.0 / bytesize::GB as f64,
                "a repository with no readable figure leaves the rest of the total intact"
            );
            assert_eq!(
                size::wait_for_recorded_size(&second)?,
                second.version_bytes()?,
                "reading a namespace starts a recalculation for a repository with no figure"
            );
            Ok(())
        })
    }
}
