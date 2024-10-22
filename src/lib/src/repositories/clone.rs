//! # oxen clone
//!
//! Clone data from a remote repository
//!

use std::path::Path;

use crate::api;
use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote, RemoteRepository};
use crate::opts::CloneOpts;

pub async fn clone(opts: &CloneOpts) -> Result<LocalRepository, OxenError> {
    match clone_remote(opts).await {
        Ok(Some(repo)) => Ok(repo),
        Ok(None) => Err(OxenError::remote_repo_not_found(&opts.url)),
        Err(err) => Err(err),
    }
}

pub async fn clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = false;
    let all = false;
    _clone(url, dst, shallow, all).await
}

pub async fn shallow_clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = true;
    let all = false;
    _clone(url, dst, shallow, all).await
}

pub async fn deep_clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let shallow = false;
    let all = true;
    _clone(url, dst, shallow, all).await
}

async fn _clone(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
    shallow: bool,
    all: bool,
) -> Result<LocalRepository, OxenError> {
    let opts = CloneOpts {
        url: url.as_ref().to_string(),
        dst: dst.as_ref().to_owned(),
        shallow,
        all,
        branch: DEFAULT_BRANCH_NAME.to_string(),
    };
    clone(&opts).await
}

async fn clone_remote(opts: &CloneOpts) -> Result<Option<LocalRepository>, OxenError> {
    log::debug!(
        "clone_remote {} -> {:?} -> shallow? {} -> all? {}",
        opts.url,
        opts.dst,
        opts.shallow,
        opts.all
    );

    let remote = Remote {
        name: String::from(DEFAULT_REMOTE_NAME),
        url: opts.url.to_owned(),
    };
    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or_else(|| OxenError::remote_repo_not_found(&opts.url))?;
    let repo = clone_repo(remote_repo, opts).await?;
    Ok(Some(repo))
}

async fn clone_repo(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    println!("🐂 cloning repo {}", remote_repo.url());
    match remote_repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::clone::clone_repo(remote_repo, opts).await,
        MinOxenVersion::V0_19_0 => core::v0_19_0::clone::clone_repo(remote_repo, opts).await,
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::model::RepoNew;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use super::*;

    #[tokio::test]
    async fn test_clone_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            // Create remote repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            log::debug!("created the remote repo");

            test::run_empty_dir_test_async(|dir| async move {
                let opts = CloneOpts::new(remote_repo.remote.url.to_owned(), dir.join("new_repo"));

                log::debug!("about to clone the remote");
                let local_repo = clone_remote(&opts).await?.unwrap();
                log::debug!("succeeded");
                let cfg_fname = ".oxen/config.toml".to_string();
                let config_path = local_repo.path.join(&cfg_fname);
                assert!(config_path.exists());

                let repository = LocalRepository::from_dir(&local_repo.path);
                assert!(repository.is_ok());

                let repository = repository.unwrap();
                let status = repositories::status(&repository)?;
                assert!(status.is_clean());

                // Cleanup
                api::client::repositories::delete(&remote_repo).await?;

                Ok(dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_move_local_repo_path_valid() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            // Create remote repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            test::run_empty_dir_test_async(|dir| async move {
                let opts = CloneOpts::new(remote_repo.remote.url.to_owned(), dir.join("new_repo"));
                let local_repo = clone_remote(&opts).await?.unwrap();

                api::client::repositories::delete(&remote_repo).await?;

                repositories::status(&local_repo)?;

                let new_path = dir.join("new_path");

                util::fs::rename(&local_repo.path, &new_path)?;

                let new_repo = LocalRepository::from_dir(&new_path)?;
                repositories::status(&new_repo)?;
                assert_eq!(new_repo.path, new_path);

                Ok(dir)
            })
            .await
        })
        .await
    }

    // Test for clone --all that checks to make sure we have all commits, all deleted files, etc
    #[tokio::test]
    async fn test_clone_dash_all() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Create additional branch on remote repo before clone
            let branch_name = "test-branch";
            api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let cloned_remote = remote_repo.clone();
            let og_commits = repositories::commits::list_all(&local_repo)?;

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::deep_clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;

                // Make sure we have all the commit objects
                let cloned_commits = repositories::commits::list_all(&cloned_repo)?;
                assert_eq!(og_commits.len(), cloned_commits.len());

                // Make sure we set the HEAD file
                let head_commit = repositories::commits::head_commit(&cloned_repo);
                assert!(head_commit.is_ok());

                // We remove the test/ directory in one of the commits, so make sure we can go
                // back in the history to that commit
                let test_dir_path = cloned_repo.path.join("test");
                println!("test_clone_dash_all test_dir_path: {:?}", test_dir_path);
                let commit = repositories::commits::first_by_message(&cloned_repo, "Adding test/")?;
                assert!(commit.is_some());
                assert!(!test_dir_path.exists());

                // checkout the commit
                repositories::checkout(&cloned_repo, &commit.unwrap().id).await?;
                // Make sure we restored the directory
                assert!(test_dir_path.exists());

                // list files in test_dir_path
                let test_dir_files = util::fs::list_files_in_dir(&test_dir_path);
                println!("test_dir_files: {:?}", test_dir_files.len());
                for file in test_dir_files.iter() {
                    println!("file: {:?}", file);
                }
                assert_eq!(test_dir_files.len(), 2);

                assert!(test_dir_path.join("1.jpg").exists());
                assert!(test_dir_path.join("2.jpg").exists());

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    // Test for clone --all that checks to make sure we have all commits, all deleted files, etc
    #[tokio::test]
    async fn test_clone_all_push_all_full_commit_history() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let mut cloned_repo =
                    repositories::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

                let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
                let remote_url = test::repo_remote_url_from(&repo_name);
                let remote_name = "different";

                // Create a different repo
                let repo_new = RepoNew::from_namespace_name_host(
                    constants::DEFAULT_NAMESPACE,
                    repo_name,
                    test::test_host(),
                );
                api::client::repositories::create_from_local(&cloned_repo, repo_new).await?;

                command::config::set_remote(&mut cloned_repo, remote_name, &remote_url)?;

                // Should be able to push all data successfully
                repositories::push::push_remote_branch(&cloned_repo, remote_name, "main").await?;

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_all_push_all_modified_deleted_files() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Create a new text file
            let filename = "file_to_modify.txt";
            let filepath = local_repo.path.join(filename);
            test::write_txt_file_to_path(&filepath, "Content before modification")?;
            repositories::add(&local_repo, &filepath)?;
            repositories::commit(&local_repo, "Adding file_to_modify.txt")?;

            // Change the file's contents - different hash
            test::write_txt_file_to_path(&filepath, "A whole new hash now!")?;
            repositories::add(&local_repo, &filepath)?;
            repositories::commit(&local_repo, "Modifying file_to_modify.txt")?;

            test::write_txt_file_to_path(&filepath, "Changing againnnnn")?;
            repositories::add(&local_repo, &filepath)?;
            repositories::commit(&local_repo, "Modifying file_to_modify.txt")?;

            // Delete file
            std::fs::remove_file(&filepath)?;
            repositories::add(&local_repo, &filepath)?;
            repositories::commit(&local_repo, "Deleting file_to_modify.txt")?;

            // Add back new
            test::write_txt_file_to_path(&filepath, "Adding back new")?;
            repositories::add(&local_repo, &filepath)?;
            repositories::commit(&local_repo, "Adding back file_to_modify.txt")?;

            repositories::push::push_remote_branch(
                &local_repo,
                DEFAULT_REMOTE_NAME,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let mut cloned_repo =
                    repositories::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

                let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
                let remote_url = test::repo_remote_url_from(&repo_name);
                let remote_name = "different";

                // Create a different repo
                let repo_new = RepoNew::from_namespace_name_host(
                    constants::DEFAULT_NAMESPACE,
                    repo_name,
                    test::test_host(),
                );
                api::client::repositories::create_empty(repo_new).await?;

                command::config::set_remote(&mut cloned_repo, remote_name, &remote_url)?;

                // Should be able to push all data successfully
                repositories::push::push_remote_branch(&cloned_repo, remote_name, "main").await?;

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_shallow_cannot_push_all() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let mut cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

                let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
                let remote_url = test::repo_remote_url_from(&repo_name);
                let remote_name = "different";

                // Create a different repo
                let repo_new = RepoNew::from_namespace_name_host(
                    constants::DEFAULT_NAMESPACE,
                    repo_name,
                    test::test_host(),
                );
                api::client::repositories::create_from_local(&cloned_repo, repo_new).await?;

                command::config::set_remote(&mut cloned_repo, remote_name, &remote_url)?;

                // Should fail
                let push_res =
                    repositories::push::push_remote_branch(&cloned_repo, remote_name, "main").await;
                assert!(push_res.is_err());

                Ok(new_repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // Track a file
            let filename = "labels.txt";
            let file_path = repo.path.join(filename);
            repositories::add(&repo, &file_path)?;
            repositories::commit(&repo, "Adding labels file")?;

            let train_path = repo.path.join("train");
            repositories::add(&repo, &train_path)?;
            repositories::commit(&repo, "Adding train dir")?;

            let test_path = repo.path.join("test");
            repositories::add(&repo, &test_path)?;
            repositories::commit(&repo, "Adding test dir")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                // 2 test, 5 train, 1 labels
                assert_eq!(8, cloned_num_files);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_oxen_clone_empty_repo() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Create a new repo to clone to, then clean it up
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;

                let status = repositories::status(&cloned_repo);
                assert!(status.is_ok());

                Ok(new_repo_dir)
            })
            .await?;

            Ok(ret_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_oxen_clone_empty_repo_then_push() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Create a new repo to clone to, then clean it up
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;

                let status = repositories::status(&cloned_repo);
                assert!(status.is_ok());

                // Add a file to the cloned repo
                let new_file = "new_file.txt";
                let new_file_path = cloned_repo.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                repositories::add(&cloned_repo, &new_file_path)?;
                repositories::commit(&cloned_repo, "Adding new file path.")?;

                repositories::push(&cloned_repo).await?;

                Ok(new_repo_dir)
            })
            .await?;

            Ok(ret_repo)
        })
        .await
    }
}
