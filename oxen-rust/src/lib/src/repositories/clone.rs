//! # oxen clone
//!
//! Clone data from a remote repository
//!

use std::path::Path;

use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{LocalRepository, Remote, RemoteRepository};
use crate::opts::fetch_opts::FetchOpts;
use crate::opts::CloneOpts;
use crate::{api, util};

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
    let fetch_opts = FetchOpts::new();
    _clone(url, dst, fetch_opts).await
}

pub async fn deep_clone_url(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let fetch_opts = FetchOpts {
        all: true,
        ..FetchOpts::new()
    };

    _clone(url, dst, fetch_opts).await
}

async fn _clone(
    url: impl AsRef<str>,
    dst: impl AsRef<Path>,
    fetch_opts: FetchOpts,
) -> Result<LocalRepository, OxenError> {
    let opts = CloneOpts {
        url: url.as_ref().to_string(),
        dst: dst.as_ref().to_owned(),
        fetch_opts,
        is_remote: false,
    };
    clone(&opts).await
}

async fn clone_remote(opts: &CloneOpts) -> Result<Option<LocalRepository>, OxenError> {
    log::debug!(
        "clone_remote {} -> {:?} -> subtree? {:?} -> depth? {:?} -> all? {} -> is remote? {}",
        opts.url,
        opts.dst,
        opts.fetch_opts.subtree_paths,
        opts.fetch_opts.depth,
        opts.fetch_opts.all,
        opts.is_remote,
    );

    let remote = Remote {
        name: String::from(DEFAULT_REMOTE_NAME),
        url: opts.url.to_owned(),
    };
    let remote_repo = api::client::repositories::get_by_remote(&remote)
        .await?
        .ok_or_else(|| OxenError::remote_repo_not_found(&opts.url))?;

    // Check if the destination directory exists before cloning so we know
    // whether to clean it up or not in the case of an error.
    let dst_exists_before_clone = opts.dst.exists();

    if opts.is_remote {
        match clone_repo_remote_mode(remote_repo, opts).await {
            Ok(repo) => Ok(Some(repo)),
            Err(err) => {
                if !dst_exists_before_clone && opts.dst.exists() {
                    // Cleanup the destination directory if it wasn't there before cloning
                    // Close DB instances before we delete it.
                    core::staged::remove_from_cache_with_children(&opts.dst)?;
                    core::refs::remove_from_cache(&opts.dst)?;
                    util::fs::remove_dir_all(&opts.dst)?;
                }
                Err(err)
            }
        }
    } else {
        match clone_repo(remote_repo, opts).await {
            Ok(repo) => Ok(Some(repo)),
            Err(err) => {
                if !dst_exists_before_clone && opts.dst.exists() {
                    // Cleanup the destination directory if it wasn't there before cloning
                    // Close DB instances before we delete it.
                    core::staged::remove_from_cache_with_children(&opts.dst)?;
                    core::refs::remove_from_cache(&opts.dst)?;
                    util::fs::remove_dir_all(&opts.dst)?;
                }
                Err(err)
            }
        }
    }
}

async fn clone_repo(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    println!("🐂 cloning repo {}", remote_repo.url());
    match remote_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::clone::clone_repo(remote_repo, opts).await,
    }
}

async fn clone_repo_remote_mode(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    println!("🐂 cloning repo {}", remote_repo.url());
    match remote_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::clone::clone_repo_remote_mode(remote_repo, opts).await,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

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
                let opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));

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

                Ok(())
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
                let opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                let local_repo = clone_remote(&opts).await?.unwrap();

                api::client::repositories::delete(&remote_repo).await?;

                repositories::status(&local_repo)?;

                let new_path = dir.join("new_path");

                core::staged::remove_from_cache_with_children(&local_repo.path)?;
                core::refs::remove_from_cache(&local_repo.path)?;
                util::fs::rename(&local_repo.path, &new_path)?;

                let new_repo = LocalRepository::from_dir(&new_path)?;
                repositories::status(&new_repo)?;
                assert_eq!(new_repo.path, new_path);

                Ok(())
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_root_subtree_depth_1() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from(".")]);
                opts.fetch_opts.depth = Some(1);
                let local_repo = clone_remote(&opts).await?.unwrap();

                // Make sure we set the depth and subtree paths
                assert_eq!(local_repo.depth(), Some(1));
                assert_eq!(local_repo.subtree_paths(), Some(vec![PathBuf::from("")]));

                // All the files should be here
                assert!(local_repo.path.join("README.md").exists());
                assert!(local_repo.path.join("labels.txt").exists());
                assert!(local_repo.path.join("prompts.jsonl").exists());
                assert!(local_repo.path.join("LICENSE").exists());

                // But none of the subdirectories should be here
                assert!(!local_repo.path.join("nlp").exists());
                assert!(!local_repo.path.join("annotations").exists());
                assert!(!local_repo.path.join("train").exists());

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_annotations_subtree() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from("annotations")]);
                let local_repo = clone_remote(&opts).await?.unwrap();

                // Make sure we set the depth and subtree paths
                assert_eq!(
                    local_repo.subtree_paths(),
                    Some(vec![PathBuf::from("annotations")])
                );

                assert!(local_repo.path.join("annotations").exists());
                assert!(local_repo.path.join("annotations").join("train").exists());
                assert!(local_repo
                    .path
                    .join("annotations")
                    .join("train")
                    .join("one_shot.csv")
                    .exists());
                assert!(local_repo
                    .path
                    .join("annotations")
                    .join("train")
                    .join("two_shot.csv")
                    .exists());
                assert!(local_repo.path.join("annotations").join("test").exists());
                assert!(local_repo
                    .path
                    .join("annotations")
                    .join("test")
                    .join("annotations.csv")
                    .exists());
                assert!(!local_repo.path.join("train").exists());

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_annotations_test_subtree() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("annotations").join("test")]);
                let local_repo = clone_remote(&opts).await?.unwrap();

                assert!(local_repo.path.join("annotations").join("test").exists());
                assert!(local_repo
                    .path
                    .join("annotations")
                    .join("test")
                    .join("annotations.csv")
                    .exists());
                assert!(!local_repo.path.join("annotations").join("train").exists());
                assert!(!local_repo.path.join("train").exists());

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_multiple_subtrees() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths = Some(vec![
                    PathBuf::from("annotations").join("test"),
                    PathBuf::from("nlp"),
                ]);
                let local_repo = clone_remote(&opts).await?.unwrap();

                assert!(local_repo.path.join("annotations").join("test").exists());
                assert!(local_repo.path.join("nlp").exists());
                assert!(local_repo
                    .path
                    .join("annotations")
                    .join("test")
                    .join("annotations.csv")
                    .exists());
                assert!(!local_repo.path.join("annotations").join("train").exists());
                assert!(!local_repo.path.join("train").exists());

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
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

                Ok(())
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

                Ok(())
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
            repositories::add(&local_repo, &filepath).await?;
            repositories::commit(&local_repo, "Adding file_to_modify.txt")?;

            // Change the file's contents - different hash
            test::write_txt_file_to_path(&filepath, "A whole new hash now!")?;
            repositories::add(&local_repo, &filepath).await?;
            repositories::commit(&local_repo, "Modifying file_to_modify.txt")?;

            test::write_txt_file_to_path(&filepath, "Changing againnnnn")?;
            repositories::add(&local_repo, &filepath).await?;
            repositories::commit(&local_repo, "Modifying file_to_modify.txt")?;

            // Delete file
            std::fs::remove_file(&filepath)?;
            repositories::add(&local_repo, &filepath).await?;
            repositories::commit(&local_repo, "Deleting file_to_modify.txt")?;

            // Add back new
            test::write_txt_file_to_path(&filepath, "Adding back new")?;
            repositories::add(&local_repo, &filepath).await?;
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

                Ok(())
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
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding labels file")?;

            let train_path = repo.path.join("train");
            repositories::add(&repo, &train_path).await?;
            repositories::commit(&repo, "Adding train dir")?;

            let test_path = repo.path.join("test");
            repositories::add(&repo, &test_path).await?;
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

                Ok(())
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

                Ok(())
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
                repositories::add(&cloned_repo, &new_file_path).await?;
                repositories::commit(&cloned_repo, "Adding new file path.")?;

                repositories::push(&cloned_repo).await?;

                Ok(())
            })
            .await?;

            Ok(ret_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_error_cleanup() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("README.md", |_, remote_repo| async move {
            // Clone a non-existent repo, destination directory should be cleaned up
            let remote_url = remote_repo.remote.url.clone();
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let dst_dir = new_repo_dir.join("new_repo_1");
                // cloning with a valid remote url but a non-existent branch
                // will fail later in the clone process
                let clone_opts = CloneOpts::from_branch(&remote_url, &dst_dir, "fake-branch");
                let cloned_repo = repositories::clone(&clone_opts).await;
                assert!(cloned_repo.is_err());
                assert!(!dst_dir.exists());

                Ok(())
            })
            .await?;

            // Clone a non-existent repo, destination directory should *NOT* be
            // cleaned up because it already existed before cloning
            let remote_url = remote_repo.remote.url.clone();
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let dst_dir = new_repo_dir.join("new_repo_2");
                // Create the destination directory before cloning
                util::fs::create_dir_all(&dst_dir)?;
                let clone_opts = CloneOpts::from_branch(&remote_url, &dst_dir, "fake-branch");
                let cloned_repo = repositories::clone(&clone_opts).await;
                assert!(cloned_repo.is_err());
                assert!(dst_dir.exists());

                Ok(())
            })
            .await?;

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_clone_subtree_and_checkout_branch() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            let feature_branch_name = "feature/subtree-checkout-test";
            repositories::branches::create_checkout(&local_repo, feature_branch_name)?;

            let new_file_relative_path = PathBuf::from("annotations")
                .join("test")
                .join("new_on_feature.txt");
            let new_file_full_path = local_repo.path.join(&new_file_relative_path);
            util::fs::create_dir_all(new_file_full_path.parent().unwrap())?;
            util::fs::write_to_path(
                &new_file_full_path,
                "This file is new on the feature branch.",
            )?;
            repositories::add(&local_repo, &new_file_full_path).await?;
            repositories::commit(&local_repo, "Added new_on_feature.txt to annotations/test")?;
            repositories::push::push_remote_branch(
                &local_repo,
                DEFAULT_REMOTE_NAME,
                feature_branch_name,
            )
            .await?;

            // Switch original_local_repo back to main for good measure, though not strictly necessary for this test's focus
            repositories::checkout(&local_repo, DEFAULT_BRANCH_NAME).await?;

            // 2. Clone ONLY the subtree "annotations/test" from the new feature branch.
            let remote_for_inner_closure = remote_repo_copy.clone();
            test::run_empty_dir_test_async(|dir_a| async move {
                let path_a = dir_a.join("repo_A_subtree");
                let subtree_to_clone = PathBuf::from("annotations").join("test");

                let mut clone_opts_subtree =
                    CloneOpts::new(remote_for_inner_closure.url(), &path_a);
                clone_opts_subtree.fetch_opts.branch = feature_branch_name.to_string();
                clone_opts_subtree.fetch_opts.subtree_paths = Some(vec![subtree_to_clone.clone()]);

                let cloned_repo_a = repositories::clone(&clone_opts_subtree).await?;

                // Verify initial clone state (feature branch, subtree only)
                let expected_new_file_in_a = cloned_repo_a.path.join(&new_file_relative_path);
                let expected_orig_file_in_a = cloned_repo_a
                    .path
                    .join("annotations")
                    .join("test")
                    .join("annotations.csv");

                assert!(
                    expected_new_file_in_a.exists(),
                    "new_on_feature.txt should exist in feature branch clone"
                );
                assert!(
                    expected_orig_file_in_a.exists(),
                    "annotations.csv should exist in feature branch clone"
                );

                // Verify other files/dirs are NOT present
                assert!(!cloned_repo_a.path.join("README.md").exists());
                assert!(!cloned_repo_a.path.join("train").exists());
                assert!(!cloned_repo_a
                    .path
                    .join("annotations")
                    .join("train")
                    .exists()); // only annotations/test

                let fetch_opts = FetchOpts {
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                    ..FetchOpts::new()
                };
                core::v_latest::fetch::fetch_remote_branch(
                    &cloned_repo_a,
                    &remote_repo,
                    &fetch_opts,
                )
                .await?;
                repositories::checkout(&cloned_repo_a, DEFAULT_BRANCH_NAME).await?;

                assert!(
                    !expected_new_file_in_a.exists(),
                    "new_on_feature.txt should NOT exist on main branch after checkout"
                );

                assert!(
                    expected_orig_file_in_a.exists(),
                    "annotations.csv should still exist on main branch after checkout"
                );

                assert!(!cloned_repo_a.path.join("README.md").exists());
                assert!(!cloned_repo_a.path.join("train").exists());
                assert!(!cloned_repo_a
                    .path
                    .join("annotations")
                    .join("train")
                    .exists());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_cloned_repos_not_set_as_remote_mode_by_default() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|repo_dir| async move {
                let new_repo_dir = repo_dir.join(PathBuf::from("new"));
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                assert!(!cloned_repo.is_remote_mode());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
