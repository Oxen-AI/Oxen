use std::ffi::OsStr;
use std::io;
use std::path::Path;

use crate::{
    constants::{LAST_MIGRATION_FILE, OXEN_HIDDEN_DIR},
    error::OxenError,
    model::{LocalRepository, RepoIdentity},
    repositories, sync_dir,
    view::repository::RepositoryListView,
};

/// The repositories under `data_dir` whose last migration predates `migration_tstamp`, each listed
/// under the namespace and name a request addresses it by.
///
/// A repository placed by UUID is listed under its recorded namespace and name when
/// `names_in_positions` and it records both. Otherwise it is listed with its UUID in the name
/// position, under its recorded namespace or, where it records none, its UUID again.
pub fn list_unmigrated(
    data_dir: &Path,
    migration_tstamp: String,
    names_in_positions: bool,
) -> Result<Vec<RepositoryListView>, OxenError> {
    let global_last_migration = data_dir.join(crate::constants::LAST_MIGRATION_FILE);

    if !global_last_migration.exists() {
        return Err(OxenError::basic_str(
            "No global migration file found on server.",
        ));
    }

    let global_last_migration = std::fs::read_to_string(&global_last_migration)?;

    if global_last_migration >= migration_tstamp {
        log::debug!(
            "Global last migration file indicates all files successfully migrated up to {migration_tstamp}"
        );
        return Ok(vec![]);
    }

    let mut legacy = vec![];
    for namespace_dir in sync_dir::namespace_dirs(data_dir)? {
        let Some(namespace) = namespace_dir.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        legacy.extend(
            repositories::list_repos_in_namespace(&namespace_dir)?.filter_map(|repo| {
                let name = repo.path.file_name().and_then(OsStr::to_str)?.to_string();
                Some((repo, namespace.to_string(), name))
            }),
        );
    }
    let placed = sync_dir::placed_repo_dirs(data_dir)?
        .into_iter()
        .filter_map(|repo_dir| {
            let repo = LocalRepository::from_dir(&repo_dir).ok()?;
            let repo_uuid = repo_dir.file_name().and_then(OsStr::to_str)?.to_string();
            let (namespace, name) = match repo.identity.clone() {
                Some(RepoIdentity {
                    namespace: Some(namespace),
                    name: Some(name),
                    ..
                }) if names_in_positions => (namespace, name),
                identity => (
                    identity
                        .and_then(|identity| identity.namespace)
                        .unwrap_or_else(|| repo_uuid.clone()),
                    repo_uuid,
                ),
            };
            Some((repo, namespace, name))
        });

    Ok(legacy
        .into_iter()
        .chain(placed)
        .filter(|(repo, ..)| {
            // A repository recording no migration of its own is at the global one, which is
            // already out of date.
            let repo_last_migration = repo.path.join(OXEN_HIDDEN_DIR).join(LAST_MIGRATION_FILE);
            match std::fs::read_to_string(&repo_last_migration) {
                Ok(repo_last_migration) => repo_last_migration <= migration_tstamp,
                Err(err) => err.kind() == io::ErrorKind::NotFound,
            }
        })
        .map(|(_, namespace, name)| RepositoryListView {
            namespace,
            name,
            min_version: None,
        })
        .collect())
}
