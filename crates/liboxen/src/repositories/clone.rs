//! # oxen clone
//!
//! Clone data from a remote repository
//!

use std::path::Path;

use crate::api;
use crate::core;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteRepository};
use crate::opts::CloneOpts;
use crate::opts::FetchOpts;

pub async fn clone(opts: &CloneOpts) -> Result<LocalRepository, OxenError> {
    clone_remote(opts).await
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
        fetch_opts,
        ..CloneOpts::new(url, dst)
    };
    clone(&opts).await
}

async fn clone_remote(opts: &CloneOpts) -> Result<LocalRepository, OxenError> {
    log::debug!(
        "clone_remote {} -> {:?} -> subtree? {:?} -> depth? {:?} -> all? {} -> is vfs? {} -> is remote? {}",
        opts.url,
        opts.dst,
        opts.fetch_opts.subtree_paths,
        opts.fetch_opts.depth,
        opts.fetch_opts.all,
        opts.is_vfs,
        opts.is_remote,
    );

    let remote_repo = api::client::repositories::get_by_url(&opts.url).await?;

    if opts.is_remote {
        clone_repo_remote_mode(remote_repo, opts).await
    } else {
        clone_repo(remote_repo, opts).await
    }
}

async fn clone_repo(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    println!("🐂 cloning repo {}", remote_repo.url());
    core::v_latest::clone::clone_repo(remote_repo, opts).await
}

async fn clone_repo_remote_mode(
    remote_repo: RemoteRepository,
    opts: &CloneOpts,
) -> Result<LocalRepository, OxenError> {
    println!("🐂 cloning repo {}", remote_repo.url());
    core::v_latest::clone::clone_repo_remote_mode(remote_repo, opts).await
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::api;
    use crate::api::requests::RepoNew;

    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::opts::PushOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use super::*;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_clone_empty_remote_then_push_and_move() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();
            let expected_uuid = remote_repo
                .remote
                .repo_uuid
                .expect("the server reports a UUID for a repo it created");

            test::run_empty_dir_test_async(|dir| async move {
                let opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                let cloned = clone_remote(&opts).await?;
                assert!(cloned.path.join(".oxen").join("config.toml").exists());

                // Read the remote back off disk rather than from the returned struct, since a clone
                // is the main path that first attaches a repository to a server.
                let reopened = LocalRepository::from_dir(&cloned.path)?;
                let remote = reopened
                    .get_remote(DEFAULT_REMOTE_NAME)
                    .expect("a clone records a remote under the default name");
                assert_eq!(
                    remote.repo_uuid,
                    Some(expected_uuid),
                    "the clone persists the UUID the server reports"
                );
                assert!(repositories::status(&reopened).await?.is_clean());

                let new_file_path =
                    test::write_txt_file_to_path(cloned.path.join("new_file.txt"), "new file")?;
                repositories::add(&cloned, &new_file_path).await?;
                repositories::commit(&cloned, "Adding new file path.")?;
                repositories::push(&cloned).await?;

                let old_path = cloned.path.clone();
                let new_path = dir.join("new_path");
                core::staged::remove_from_cache_with_children(&old_path)?;
                core::refs::remove_from_cache(&old_path)?;
                // Drop the repo to release its LMDB env before the rename: on Windows
                // `util::fs::rename` copies then removes the source, and a mapped env file can't be
                // removed while the repo holds it open.
                drop(cloned);
                drop(reopened);
                util::fs::rename(&old_path, &new_path)?;

                let moved = LocalRepository::from_dir(&new_path)?;
                assert_eq!(moved.path, new_path);
                assert!(
                    repositories::status(&moved).await?.is_clean(),
                    "a repo moved to a new path still reads its own history"
                );

                Ok(())
            })
            .await?;

            Ok(ret_repo)
        })
        .await
    }

    /// Check that `cloned_repo` holds every commit, then check out the training data's "Adding
    /// test/" commit and check that it restores the four images a later commit removes.
    async fn assert_checkout_restores_removed_test_dir(
        cloned_repo: &LocalRepository,
        expected_commit_count: usize,
    ) -> Result<(), OxenError> {
        assert_eq!(
            repositories::commits::list_all(cloned_repo)?.len(),
            expected_commit_count,
            "the clone holds every commit object"
        );
        assert!(repositories::commits::head_commit(cloned_repo).is_ok());

        let test_dir_path = cloned_repo.path.join("test");
        let commit = repositories::commits::first_by_message(cloned_repo, "Adding test/")?
            .expect("the training data adds test/ in its own commit");
        assert!(!test_dir_path.exists());

        repositories::checkout(cloned_repo, &commit.id).await?;
        assert_eq!(util::fs::list_files_in_dir(&test_dir_path).await?.len(), 4);
        for name in ["1.jpg", "2.jpg", "3.jpg", "4.jpg"] {
            assert!(
                test_dir_path.join(name).exists(),
                "checkout restores test/{name}"
            );
        }
        Ok(())
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_clone_subtrees_and_history() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("root_depth_1"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from(".")]);
                opts.fetch_opts.depth = Some(1);
                let root_repo = clone_remote(&opts).await?;
                assert_eq!(root_repo.depth(), Some(1));
                assert_eq!(root_repo.subtree_paths(), Some(vec![PathBuf::from("")]));
                for name in ["README.md", "labels.txt", "prompts.jsonl", "LICENSE"] {
                    assert!(
                        root_repo.path.join(name).exists(),
                        "a root clone carries {name}"
                    );
                }
                for name in ["nlp", "annotations", "train"] {
                    assert!(
                        !root_repo.path.join(name).exists(),
                        "a depth-1 root clone leaves out the {name} subdirectory"
                    );
                }

                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("annotations"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from("annotations")]);
                let annotations_repo = clone_remote(&opts).await?;
                assert_eq!(
                    annotations_repo.subtree_paths(),
                    Some(vec![PathBuf::from("annotations")])
                );
                let annotations = annotations_repo.path.join("annotations");
                assert!(annotations.join("train").join("one_shot.csv").exists());
                assert!(annotations.join("train").join("two_shot.csv").exists());
                assert!(annotations.join("test").join("annotations.csv").exists());
                assert!(!annotations_repo.path.join("train").exists());

                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("multiple"));
                opts.fetch_opts.subtree_paths = Some(vec![
                    PathBuf::from("annotations").join("test"),
                    PathBuf::from("nlp"),
                ]);
                let multiple_repo = clone_remote(&opts).await?;
                assert!(multiple_repo.path.join("nlp").exists());
                assert!(
                    multiple_repo
                        .path
                        .join("annotations")
                        .join("test")
                        .join("annotations.csv")
                        .exists()
                );
                assert!(
                    !multiple_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .exists()
                );
                assert!(!multiple_repo.path.join("train").exists());

                // A default clone fetches the files a checkout needs that it did not download, and
                // a clone with --all holds them from the start.
                let og_commit_count = repositories::commits::list_all(&local_repo)?.len();
                let default_repo =
                    repositories::clone_url(&remote_repo.remote.url, dir.join("default")).await?;
                assert_checkout_restores_removed_test_dir(&default_repo, og_commit_count).await?;

                api::client::branches::create_from_branch(
                    &remote_repo,
                    "test-branch",
                    DEFAULT_BRANCH_NAME,
                )
                .await?;
                let deep_repo =
                    repositories::deep_clone_url(&remote_repo.remote.url, dir.join("deep")).await?;
                assert_checkout_restores_removed_test_dir(&deep_repo, og_commit_count).await?;

                // A subtree clone of a branch, then a checkout of main, keeps to the subtree.
                let feature_branch_name = "feature/subtree-checkout-test";
                repositories::branches::create_checkout(&local_repo, feature_branch_name)?;
                let new_file_relative_path = PathBuf::from("annotations")
                    .join("test")
                    .join("new_on_feature.txt");
                let new_file_full_path = local_repo.path.join(&new_file_relative_path);
                util::fs::write_to_path(
                    &new_file_full_path,
                    "This file is new on the feature branch.",
                )?;
                repositories::add(&local_repo, &new_file_full_path).await?;
                repositories::commit(&local_repo, "Added new_on_feature.txt to annotations/test")?;
                let push_opts = PushOpts {
                    remote: DEFAULT_REMOTE_NAME.to_string(),
                    branch: feature_branch_name.to_string(),
                    ..Default::default()
                };
                repositories::push::push_remote_branch(&local_repo, &push_opts, None).await?;

                let mut clone_opts = CloneOpts::new(remote_repo.url(), dir.join("feature_subtree"));
                clone_opts.fetch_opts.branch = feature_branch_name.to_string();
                clone_opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("annotations").join("test")]);
                let feature_repo = repositories::clone(&clone_opts).await?;

                let new_file_in_clone = feature_repo.path.join(&new_file_relative_path);
                let orig_file_in_clone = feature_repo
                    .path
                    .join("annotations")
                    .join("test")
                    .join("annotations.csv");
                assert!(
                    new_file_in_clone.exists(),
                    "new_on_feature.txt should exist in feature branch clone"
                );
                assert!(
                    orig_file_in_clone.exists(),
                    "annotations.csv should exist in feature branch clone"
                );
                assert!(!feature_repo.path.join("README.md").exists());
                assert!(!feature_repo.path.join("train").exists());
                assert!(!feature_repo.path.join("annotations").join("train").exists());

                let fetch_opts = FetchOpts {
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                    ..FetchOpts::new()
                };
                core::v_latest::fetch::fetch_remote_branch(
                    &feature_repo,
                    &remote_repo,
                    &fetch_opts,
                )
                .await?;
                repositories::checkout(&feature_repo, DEFAULT_BRANCH_NAME).await?;

                assert!(
                    !new_file_in_clone.exists(),
                    "new_on_feature.txt should NOT exist on main branch after checkout"
                );
                assert!(
                    orig_file_in_clone.exists(),
                    "annotations.csv should still exist on main branch after checkout"
                );
                assert!(!feature_repo.path.join("README.md").exists());
                assert!(!feature_repo.path.join("train").exists());
                assert!(!feature_repo.path.join("annotations").join("train").exists());

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_clone_all_push_all_modified_deleted_files() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
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
            let opts = PushOpts {
                remote: DEFAULT_REMOTE_NAME.to_string(),
                branch: DEFAULT_BRANCH_NAME.to_string(),
                ..Default::default()
            };

            repositories::push::push_remote_branch(&local_repo, &opts, None).await?;

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let mut cloned_repo =
                    repositories::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

                let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
                let _remote_url = test::repo_remote_url_from(&repo_name);
                let remote_name = "different";

                // Create a different repo
                let repo_new = RepoNew::from_namespace_name_host(
                    constants::DEFAULT_NAMESPACE,
                    repo_name,
                    test::test_host(),
                    None,
                );
                let different_repo = api::client::repositories::create_empty(repo_new).await?;

                cloned_repo.set_remote_repo(remote_name, &different_repo);
                cloned_repo.save()?;

                let opts = PushOpts {
                    remote: remote_name.to_string(),
                    branch: "main".to_string(),
                    ..Default::default()
                };

                // Should be able to push all data successfully
                repositories::push::push_remote_branch(&cloned_repo, &opts, None).await?;

                Ok(())
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_clone_full() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // First commit: a single inline file
            let file_path = repo.path.join("labels.txt");
            test::write_txt_file_to_path(&file_path, "label_a\nlabel_b")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding labels file")?;

            // Second commit: inline `train` dir
            let train_path = repo.path.join("train");
            util::fs::create_dir_all(&train_path)?;
            for i in 0..2 {
                test::write_txt_file_to_path(
                    train_path.join(format!("train_{i}.txt")),
                    format!("train {i}"),
                )?;
            }
            repositories::add(&repo, &train_path).await?;
            repositories::commit(&repo, "Adding train dir")?;

            // Third commit: inline `test` dir
            let test_path = repo.path.join("test");
            util::fs::create_dir_all(&test_path)?;
            for i in 0..2 {
                test::write_txt_file_to_path(
                    test_path.join(format!("test_{i}.txt")),
                    format!("test {i}"),
                )?;
            }
            repositories::add(&repo, &test_path).await?;
            repositories::commit(&repo, "Adding test dir")?;

            // Create the remote repo and point the local one at it
            let remote_repo = test::connect_remote_repo(&mut repo).await?;
            repositories::push(&repo).await?;

            let expected_files = util::fs::rcount_files_in_dir(&repo.path);

            // Clone elsewhere and confirm the cloned tree has every committed file
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                assert_eq!(expected_files, cloned_num_files);
                assert!(
                    !cloned_repo.is_remote_mode(),
                    "a clone is not in remote mode unless asked"
                );

                api::client::repositories::delete(&remote_repo).await?;

                Ok(())
            })
            .await
        })
        .await
    }
}
