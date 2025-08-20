//! Repositories
//!
//! This module is all the domain logic for repositories, and it's sub-modules.
//!

use crate::constants;
use crate::core;
use crate::core::refs::with_ref_manager;
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::error::NO_REPO_FOUND;
use crate::model::file::FileContents;
use crate::model::merkle_tree;
use crate::model::repository::local_repository::LocalRepositoryWithEntries;
use crate::model::Commit;
use crate::model::MetadataEntry;
use crate::model::{LocalRepository, RepoNew};
use crate::repositories;
use crate::repositories::fork::FORK_STATUS_FILE;
use crate::util;
use fd_lock::RwLock;
use jwalk::WalkDir;
use std::fs::File;
use std::path::Path;

pub mod add;
pub mod branches;
pub mod checkout;
pub mod clone;
pub mod commits;
pub mod data_frames;
pub mod diffs;
pub mod download;
pub mod entries;
pub mod fetch;
pub mod fork;
pub mod init;
pub mod load;
pub mod merge;
pub mod metadata;
pub mod pull;
pub mod push;
pub mod restore;
pub mod revisions;
pub mod rm;
pub mod save;
pub mod size;
pub mod stats;
pub mod status;
pub mod tree;
pub mod workspaces;

pub use add::add;
pub use checkout::checkout;
pub use clone::{clone, clone_url, deep_clone_url};
pub use commits::commit;
pub use download::{download, download_with_bearer_token};
pub use fetch::{fetch_all, fetch_branch};
pub use init::init;
pub use load::load;
pub use pull::{pull, pull_all, pull_remote_branch};
pub use push::push;
pub use restore::restore;
pub use rm::rm;
pub use save::save;
pub use status::status;
pub use status::status_from_dir;

pub fn get_by_namespace_and_name(
    sync_dir: &Path,
    namespace: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<Option<LocalRepository>, OxenError> {
    let namespace = namespace.as_ref();
    let name = name.as_ref();
    let repo_dir = sync_dir.join(namespace).join(name);

    if !repo_dir.exists() {
        log::debug!("Repo does not exist: {:?}", repo_dir);
        return Ok(None);
    }

    let repo = LocalRepository::from_dir(&repo_dir);
    match repo {
        Ok(repo) => Ok(Some(repo)),
        Err(OxenError::LocalRepoNotFound(_)) => is_repo_forked(&repo_dir),
        Err(err) => {
            log::error!("Error getting repo from dir: {:?}", err);
            Err(err)
        }
    }
}

fn is_repo_forked(repo_dir: &Path) -> Result<Option<LocalRepository>, OxenError> {
    let status_path = repo_dir.join(FORK_STATUS_FILE);

    if status_path.exists() {
        Ok(Some(LocalRepository::from_dir(repo_dir)?))
    } else {
        Err(OxenError::basic_str(NO_REPO_FOUND))
    }
}

pub fn is_empty(repo: &LocalRepository) -> Result<bool, OxenError> {
    match branches::list(repo) {
        Ok(branches) => Ok(branches.is_empty()),
        Err(err) => Err(err),
    }
}

pub fn list_namespaces(sync_dir: &Path) -> Result<Vec<String>, OxenError> {
    log::debug!(
        "repositories::entries::list_namespaces repositories for sync dir: {:?}",
        sync_dir
    );
    let mut namespaces: Vec<String> = vec![];
    for path in std::fs::read_dir(sync_dir)? {
        let path = path.unwrap().path();
        if is_namespace_dir(&path) {
            let name = path.file_name().unwrap().to_str().unwrap();
            namespaces.push(String::from(name));
        }
    }

    Ok(namespaces)
}

fn is_namespace_dir(path: &Path) -> bool {
    if let Some(name) = path.to_str() {
        // Make sure it is a directory, that doesn't start with .oxen and has repositories in it
        return path.is_dir()
            && !name.starts_with(constants::OXEN_HIDDEN_DIR)
            && !list_repos_in_namespace(path).is_empty();
    }
    false
}

pub fn list_repos_in_namespace(namespace_path: &Path) -> Vec<LocalRepository> {
    log::debug!(
        "repositories::entries::list_repos_in_namespace repositories for dir: {:?}",
        namespace_path
    );
    let mut repos: Vec<LocalRepository> = vec![];
    for entry in WalkDir::new(namespace_path)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        // if the directory has a .oxen dir, let's add it, otherwise ignore
        let local_dir = entry.path();
        let oxen_dir = util::fs::oxen_hidden_dir(&local_dir);
        // log::debug!(
        //     "repositories::entries::list_repos_in_namespace got local dir {:?}",
        //     local_dir
        // );

        if oxen_dir.exists() {
            if let Ok(repository) = LocalRepository::from_dir(&local_dir) {
                repos.push(repository);
            }
        }
    }

    repos
}

pub fn transfer_namespace(
    sync_dir: &Path,
    repo_name: &str,
    from_namespace: &str,
    to_namespace: &str,
) -> Result<LocalRepository, OxenError> {
    log::debug!(
        "transfer_namespace from: {} to: {}",
        from_namespace,
        to_namespace
    );

    let repo_dir = sync_dir.join(from_namespace).join(repo_name);
    let new_repo_dir = sync_dir.join(to_namespace).join(repo_name);

    if !repo_dir.exists() {
        log::debug!(
            "Error while transferring repo: repo does not exist: {:?}",
            repo_dir
        );
        return Err(OxenError::repo_not_found(RepoNew::from_namespace_name(
            from_namespace,
            repo_name,
        )));
    }

    // ensure DB instance is closed before we move the repo
    merkle_tree::merkle_tree_node_cache::remove_from_cache(&repo_dir)?;
    core::staged::remove_from_cache_with_children(&repo_dir)?;
    core::refs::remove_from_cache(&repo_dir)?;

    util::fs::create_dir_all(&new_repo_dir)?;
    util::fs::rename(&repo_dir, &new_repo_dir)?;

    // Update path in config
    let repo = LocalRepository::from_dir(&new_repo_dir)?;
    repo.save()?;

    let updated_repo = get_by_namespace_and_name(sync_dir, to_namespace, repo_name)?;

    match updated_repo {
        Some(new_repo) => Ok(new_repo),
        None => Err(OxenError::basic_str(
            "Repository not found after attempted transfer",
        )),
    }
}

pub async fn create(
    root_dir: &Path,
    new_repo: RepoNew,
) -> Result<LocalRepositoryWithEntries, OxenError> {
    let repo_dir = root_dir
        .join(&new_repo.namespace)
        .join(Path::new(&new_repo.name));
    if repo_dir.exists() {
        log::error!("Repository already exists {repo_dir:?}");
        return Err(OxenError::repo_already_exists(new_repo));
    }

    // Create the repo dir
    log::debug!("repositories::create repo dir: {:?}", repo_dir);
    util::fs::create_dir_all(&repo_dir)?;

    // Create oxen hidden dir
    let hidden_dir = util::fs::oxen_hidden_dir(&repo_dir);
    log::debug!("repositories::create hidden dir: {:?}", hidden_dir);
    util::fs::create_dir_all(&hidden_dir)?;

    // Create config file
    let local_repo = LocalRepository::new(&repo_dir)?;
    local_repo.save()?;

    // Create history dir
    let history_dir = util::fs::oxen_hidden_dir(&repo_dir).join(constants::HISTORY_DIR);
    util::fs::create_dir_all(history_dir)?;

    // Create HEAD file and point it to DEFAULT_BRANCH_NAME
    with_ref_manager(&local_repo, |manager| {
        manager.set_head(constants::DEFAULT_BRANCH_NAME);
        Ok(())
    })?;

    // If the user supplied files, add and commit them
    let mut commit: Option<Commit> = None;
    if let Some(files) = &new_repo.files {
        let user = &files[0].user;
        // Add the files
        log::debug!("repositories::create files: {:?}", files.len());
        for file in files {
            let path = &file.path;
            let contents = &file.contents;
            // write the data to the path
            // if the path does not exist within the repo, make it
            let full_path = repo_dir.join(path);
            let parent_dir = full_path.parent().unwrap();
            if !parent_dir.exists() {
                util::fs::create_dir_all(parent_dir)?;
            }
            match contents {
                FileContents::Text(text) => {
                    util::fs::write(&full_path, text.as_bytes())?;
                }
                FileContents::Binary(bytes) => {
                    util::fs::write(&full_path, bytes)?;
                }
            }
            add(&local_repo, &full_path).await?;
        }

        commit = Some(core::v_latest::commits::commit_with_user(
            &local_repo,
            "Initial commit",
            user,
        )?);
        branches::create(
            &local_repo,
            constants::DEFAULT_BRANCH_NAME,
            &commit.as_ref().unwrap().id,
        )?;
    }

    let metadata_entries: Option<Vec<MetadataEntry>> = if let Some(files) = &new_repo.files {
        let entries: Vec<MetadataEntry> = files
            .iter()
            .filter_map(|file| {
                repositories::entries::get_meta_entry(
                    &local_repo,
                    commit.as_ref().unwrap(),
                    &file.path,
                )
                .ok()
            })
            .collect();

        if entries.is_empty() {
            None
        } else {
            Some(entries)
        }
    } else {
        None
    };

    Ok(LocalRepositoryWithEntries {
        local_repo,
        entries: metadata_entries,
    })
}

pub fn delete(repo: &LocalRepository) -> Result<&LocalRepository, OxenError> {
    if !repo.path.exists() {
        let err = format!("Repository does not exist {:?}", repo.path);
        return Err(OxenError::basic_str(err));
    }

    // Close DB instances before trying to delete the directory
    merkle_tree::merkle_tree_node_cache::remove_from_cache(&repo.path)?;
    core::staged::remove_from_cache_with_children(&repo.path)?;
    core::refs::ref_manager::remove_from_cache(&repo.path)?;

    log::debug!("Deleting repo directory: {:?}", repo);
    util::fs::remove_dir_all(&repo.path)?;
    Ok(repo)
}

// Creates an OS level lock on the file. The lock is immediately
// released when the return value is dropped or process is killed.
pub fn get_exclusive_lock(
    lock_file: &mut fd_lock::RwLock<File>,
) -> Result<fd_lock::RwLockWriteGuard<'_, File>, std::io::Error> {
    lock_file.write()
}

pub fn is_locked(repo: &LocalRepository) -> bool {
    match get_lock_file(repo) {
        Err(_) => true,
        Ok(mut lock_file) => lock_file.try_write().is_err(),
    }
}

// Returns an instance of a lockfile. The lockfile is an empty file
pub fn get_lock_file(repo: &LocalRepository) -> Result<fd_lock::RwLock<File>, std::io::Error> {
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let lock_file_path = hidden_dir.join(constants::REPOSITORY_LOCK_FILE);

    let lock_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(lock_file_path)?;

    Ok(RwLock::new(lock_file))
}

#[cfg(test)]
mod tests {
    use crate::config::UserConfig;
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::file::{FileContents, FileNew};
    use crate::model::{Commit, LocalRepository, RepoNew};
    use crate::repositories;
    use crate::test;
    use crate::util;
    use std::path::{Path, PathBuf};
    use time::OffsetDateTime;

    #[tokio::test]
    async fn test_local_repository_api_create_empty_with_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";
            let initial_commit_id = format!("{}", uuid::Uuid::new_v4());
            let timestamp = OffsetDateTime::now_utc();
            let root_commit = Commit {
                id: initial_commit_id,
                parent_ids: vec![],
                message: String::from(constants::INITIAL_COMMIT_MSG),
                author: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
                timestamp,
            };
            let repo_new = RepoNew::from_root_commit(namespace, name, root_commit);
            let _repo = repositories::create(&sync_dir, repo_new).await?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_create_empty_with_files() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";

            let user = UserConfig::get()?.to_user();
            let files: Vec<FileNew> = vec![FileNew {
                path: PathBuf::from("README"),
                contents: FileContents::Text(String::from("Hello world!")),
                user,
            }];
            let repo_new = RepoNew::from_files(namespace, name, files);
            let _repo = repositories::create(&sync_dir, repo_new).await?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_create_empty_no_commit() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let namespace: &str = "test-namespace";
            let name: &str = "test-repo-name";
            let repo_new = RepoNew::from_namespace_name(namespace, name);
            let _repo = repositories::create(&sync_dir, repo_new).await?;

            let repo_path = Path::new(&sync_dir)
                .join(Path::new(namespace))
                .join(Path::new(name));
            assert!(repo_path.exists());

            // Test that we can successful load a repository from that dir
            let _repo = LocalRepository::from_dir(&repo_path)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_local_repository_api_list_namespaces_one() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace: &str = "test-namespace";
            let name: &str = "cool-repo";

            let namespace_dir = sync_dir.join(namespace);
            util::fs::create_dir_all(&namespace_dir)?;
            let repo_dir = namespace_dir.join(name);
            repositories::init(&repo_dir)?;

            let namespaces = repositories::list_namespaces(sync_dir)?;
            assert_eq!(namespaces.len(), 1);
            assert_eq!(namespaces[0], namespace);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_local_repository_api_list_multiple_namespaces() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace_1 = "my-namespace-1";
            let namespace_1_dir = sync_dir.join(namespace_1);

            let namespace_2 = "my-namespace-2";
            let namespace_2_dir = sync_dir.join(namespace_2);

            // We will not create any repos in the last namespace, to test that it gets filtered out
            let namespace_3 = "my-namespace-3";
            let _ = sync_dir.join(namespace_3);

            let _ = repositories::init(namespace_1_dir.join("testing1"))?;
            let _ = repositories::init(namespace_1_dir.join("testing2"))?;
            let _ = repositories::init(namespace_2_dir.join("testing3"))?;

            let repos = repositories::list_namespaces(sync_dir)?;
            assert_eq!(repos.len(), 2);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_local_repository_api_list_multiple_within_namespace() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace = "my-namespace";
            let namespace_dir = sync_dir.join(namespace);

            let _ = repositories::init(namespace_dir.join("testing1"))?;
            let _ = repositories::init(namespace_dir.join("testing2"))?;
            let _ = repositories::init(namespace_dir.join("testing3"))?;

            let repos = repositories::list_repos_in_namespace(&namespace_dir);
            assert_eq!(repos.len(), 3);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_local_repository_api_get_by_name() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let namespace = "my-namespace";
            let name = "my-repo";
            let repo_dir = sync_dir.join(namespace).join(name);
            util::fs::create_dir_all(&repo_dir)?;

            let _ = repositories::init(&repo_dir)?;
            let _repo =
                repositories::get_by_namespace_and_name(sync_dir, namespace, name)?.unwrap();
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_local_repository_transfer_namespace() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let old_namespace: &str = "test-namespace-old";
            let new_namespace: &str = "test-namespace-new";

            let old_namespace_dir = sync_dir.join(old_namespace);
            let new_namespace_dir = sync_dir.join(new_namespace);

            let name = "moving-repo";

            let initial_commit_id = format!("{}", uuid::Uuid::new_v4());
            let timestamp = OffsetDateTime::now_utc();
            // Create new namespace
            util::fs::create_dir_all(&new_namespace_dir)?;

            let root_commit = Commit {
                id: initial_commit_id,
                parent_ids: vec![],
                message: String::from(constants::INITIAL_COMMIT_MSG),
                author: String::from("Ox"),
                email: String::from("ox@oxen.ai"),
                timestamp,
            };
            let repo_new = RepoNew::from_root_commit(old_namespace, name, root_commit);
            let _repo = repositories::create(&sync_dir, repo_new).await?;

            let old_namespace_repos = repositories::list_repos_in_namespace(&old_namespace_dir);
            let new_namespace_repos = repositories::list_repos_in_namespace(&new_namespace_dir);

            assert_eq!(old_namespace_repos.len(), 1);
            assert_eq!(new_namespace_repos.len(), 0);

            // Transfer to new namespace
            let updated_repo =
                repositories::transfer_namespace(&sync_dir, name, old_namespace, new_namespace)?;

            // Log out updated_repo
            log::debug!("updated_repo: {:?}", updated_repo);

            let new_repo_path = sync_dir.join(new_namespace).join(name);
            assert_eq!(updated_repo.path, new_repo_path);

            // Check that the old namespace is empty
            let old_namespace_repos = repositories::list_repos_in_namespace(&old_namespace_dir);
            let new_namespace_repos = repositories::list_repos_in_namespace(&new_namespace_dir);

            assert_eq!(old_namespace_repos.len(), 0);
            assert_eq!(new_namespace_repos.len(), 1);

            Ok(())
        })
        .await
    }
}
