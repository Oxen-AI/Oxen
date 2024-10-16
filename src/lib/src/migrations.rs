use std::path::Path;

use crate::{
    constants::{LAST_MIGRATION_FILE, OXEN_HIDDEN_DIR},
    error::OxenError,
    namespaces, repositories,
    view::repository::RepositoryListView,
};

pub fn list_unmigrated(
    data_dir: &Path,
    migration_tstamp: String,
) -> Result<Vec<RepositoryListView>, OxenError> {
    let mut result: Vec<RepositoryListView> = vec![];

    let global_last_migration = data_dir.join(crate::constants::LAST_MIGRATION_FILE);

    if !global_last_migration.exists() {
        return Err(OxenError::basic_str(
            "No global migration file found on server.",
        ));
    }

    let global_last_migration = std::fs::read_to_string(&global_last_migration)?;

    if global_last_migration >= migration_tstamp {
        log::debug!(
            "Global last migration file indicates all files successfully migrated up to {}",
            migration_tstamp
        );
        return Ok(result);
    }

    let namespaces = namespaces::list(data_dir);
    for namespace in namespaces {
        let namespace_path = data_dir.join(&namespace);
        for repo in repositories::list_repos_in_namespace(&namespace_path) {
            let repo_name = repo.path.file_name().unwrap().to_str().unwrap();

            let repo_last_migration = repo.path.join(OXEN_HIDDEN_DIR).join(LAST_MIGRATION_FILE);

            // If no repo-level migration file, the repo is migrated up to the global migration.
            // If we're here, that's out of date.
            if !repo_last_migration.exists() {
                log::debug!(
                    "No repo migration file found for {} looking in {}",
                    repo_name,
                    repo_last_migration.to_string_lossy()
                );
                result.push(RepositoryListView {
                    namespace: namespace.clone(),
                    name: repo_name.to_string(),
                    min_version: None,
                });
            } else if let Ok(repo_last_migration) = std::fs::read_to_string(&repo_last_migration) {
                if repo_last_migration <= migration_tstamp {
                    log::debug!("Repo migration file found for {} is out of date", repo_name);
                    result.push(RepositoryListView {
                        namespace: namespace.clone(),
                        name: repo_name.to_string(),
                        min_version: None,
                    });
                }
            }
        }
    }

    Ok(result)
}
