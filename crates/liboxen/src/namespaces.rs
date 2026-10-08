use rayon::prelude::*;
use std::collections::HashSet;
use std::path::Path;

use crate::error::OxenError;
use crate::model::{LocalRepository, Namespace};
use crate::repositories;
use crate::repositories::name_table::NameTable;
use crate::repositories::size::{self, RepoSizeFile, SizeStatus};
use crate::sync_dir::{is_server_owned, namespace_dirs};

/// The namespaces under `path`, in name order: its namespace directories, and the namespaces the
/// name table records a repository under that no directory's name matches ignoring case. A
/// namespace only the table records is listed lowercased.
pub fn list(path: &Path) -> Result<Vec<String>, OxenError> {
    log::debug!("repositories::namespaces::list",);
    let mut namespaces: Vec<String> = namespace_dirs(path)?
        .iter()
        .filter_map(|dir| dir.file_name()?.to_str().map(str::to_string))
        .collect();
    let in_directories: HashSet<String> = namespaces
        .iter()
        .map(|namespace| namespace.to_ascii_lowercase())
        .collect();
    namespaces.extend(
        NameTable::new(path)
            .namespaces()?
            .into_iter()
            .filter(|namespace| !in_directories.contains(namespace)),
    );
    namespaces.sort();
    Ok(namespaces)
}

/// The namespace called `name`, whose total counts the repositories in the directory
/// `legacy_directory` (`name` when `None`) and the repositories placed by UUID that the name table
/// records under `name`. `None` when it has neither.
///
/// Starts a size recalculation for every repository that has no figure to count, so the total is a
/// lower bound that later reads converge on, and a warning names how many repositories are counted
/// at an unfinished figure.
pub fn get(
    data_dir: &Path,
    name: &str,
    legacy_directory: Option<&str>,
) -> Result<Option<Namespace>, OxenError> {
    log::debug!("repositories::namespaces::get {name} (legacy directory {legacy_directory:?})");
    let legacy_directory = legacy_directory.unwrap_or(name);
    let namespace_path = repositories::namespace_dir(data_dir, legacy_directory)?;
    let legacy = (!is_server_owned(legacy_directory) && namespace_path.is_dir())
        .then(|| repositories::list_repos_in_namespace(&namespace_path))
        .transpose()?;
    let placed = repositories::list_placed_repos_in_namespace(data_dir, name)?;
    if legacy.is_none() && placed.is_empty() {
        return Ok(None);
    }

    let repos: Vec<LocalRepository> = legacy.into_iter().flatten().chain(placed).collect();
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

    Ok(Some(Namespace {
        name: name.to_string(),
        storage_usage_gb: figures.iter().map(|figure| figure.size).sum::<u64>() as f64
            / bytesize::GB as f64,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repositories::size::repo_size_path;
    use crate::sync_dir::placed_repo_dir;
    use crate::test;
    use crate::util;
    use crate::util::fs::AtomicFile;
    use uuid::Uuid;

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
                get(dir, "ox", None)?.is_none(),
                "a namespace with no directory on disk and nothing placed under it is absent"
            );

            let namespace_path = dir.join("ox");
            repo_recording(&namespace_path.join("first"), "1500")?;
            let second = repo_recording(&namespace_path.join("second"), "2500")?;

            let namespace = get(dir, "ox", None)?.expect("namespace exists");
            assert_eq!(namespace.storage_usage_gb, 4000.0 / bytesize::GB as f64);

            util::fs::write_to_path(repo_size_path(&second), "not a size record")?;
            let namespace = get(dir, "ox", None)?.expect("namespace exists");
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

            let gb = |bytes: u64| bytes as f64 / bytesize::GB as f64;
            let total = |name: &str, legacy_directory| {
                Ok::<_, OxenError>(
                    get(dir, name, legacy_directory)?
                        .expect("namespace exists")
                        .storage_usage_gb,
                )
            };
            let legacy_bytes = 1500 + second.version_bytes()?;
            let placed_uuid = Uuid::new_v4();
            repo_recording(&placed_repo_dir(dir, placed_uuid), "500")?;
            let table = NameTable::new(dir);
            table.claim("ox", "third", placed_uuid)?;
            table.claim("ox", "first", Uuid::new_v4())?;
            assert_eq!(
                total("ox", None)?,
                gb(legacy_bytes + 500),
                "the total adds the repositories placed by UUID that the table records under the \
                 namespace, and a table entry with no placed directory is counted once"
            );

            table.move_to_namespace("ox", "third", "zoo", placed_uuid)?;
            assert_eq!(
                total("zoo", Some("ox"))?,
                gb(legacy_bytes + 500),
                "a legacy directory named apart from the namespace counts alongside it"
            );
            assert_eq!(
                total("zoo", None)?,
                gb(500),
                "a namespace with no directory on disk answers for the repositories placed under it"
            );
            Ok(())
        })
    }
}
