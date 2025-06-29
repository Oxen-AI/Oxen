//! # oxen push
//!
//! Push data from your local machine to a remote.
//!

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};

/// # Get a log of all the commits
///
/// ```
/// # use liboxen::api;
/// # use liboxen::test;
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # #[tokio::main]
/// # async fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_push");
/// let mut repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// repositories::add(&repo, &hello_file)?;
///
/// // Commit staged
/// repositories::commit(&repo, "My commit message")?;
///
/// // Set the remote server
/// command::config::set_remote(&mut repo, "origin", "http://localhost:3000/repositories/hello");
///
/// let remote_repo = api::client::repositories::create(&repo, "repositories", "hello", "localhost:3000").await?;
///
/// // Push the file
/// repositories::push(&repo).await;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # api::client::repositories::delete(&remote_repo).await?;
/// # Ok(())
/// # }
/// ```
pub async fn push(repo: &LocalRepository) -> Result<Branch, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 is deprecated"),
        _ => core::v_latest::push::push(repo).await,
    }
}

/// Push to a specific remote branch on the default remote repository
pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch_name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 is deprecated"),
        _ => core::v_latest::push::push_remote_branch(repo, remote, branch_name).await,
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::{AVG_CHUNK_SIZE, DEFAULT_BRANCH_NAME};
    use crate::core::progress::push_progress::PushProgress;
    use crate::error::OxenError;
    use crate::model::merkle_tree::node::MerkleTreeNode;
    use crate::opts::CloneOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::view::entries::EMetadataEntry;
    use futures::future;
    use rand::Rng;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_command_push_one_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async {
            let mut repo = repo;

            // Track the file
            let train_dir = repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&train_dir);
            repositories::add(&repo, &train_dir)?;

            // Write a README.md file
            let readme_path = repo.path.join("README.md");
            let readme_path = test::write_txt_file_to_path(readme_path, "Ready to train 🏋️‍♂️")?;
            repositories::add(&repo, &readme_path)?;

            // Commit the train dir
            let commit = repositories::commit(&repo, "Adding training data")?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push it real good
            repositories::push(&repo).await?;

            let page_num = 1;
            let page_size = num_files + 10;
            let entries =
                api::client::dir::list(&remote_repo, &commit.id, "train", page_num, page_size)
                    .await?;
            assert_eq!(entries.total_entries, num_files);
            assert_eq!(entries.entries.len(), num_files);

            // Make sure we can download the file
            let readme_path = repo.path.join("README.md");
            let download_path = repo.path.join("README_2.md");
            api::client::entries::download_entry(&remote_repo, "README.md", &download_path, "main")
                .await?;

            // Make sure the file is the same
            let readme_1_contents = util::fs::read_from_path(&download_path)?;
            let readme_2_contents = util::fs::read_from_path(&readme_path)?;
            assert_eq!(readme_1_contents, readme_2_contents);

            api::client::repositories::delete(&remote_repo).await?;
            future::ok::<(), OxenError>(()).await
        })
        .await
    }

    #[tokio::test]
    async fn test_command_push_inbetween_two_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async {
            let mut repo = repo;
            // Track the train dir
            let train_dir = repo.path.join("train");
            let num_train_files = util::fs::rcount_files_in_dir(&train_dir);
            repositories::add(&repo, &train_dir)?;
            // Commit the train dur
            repositories::commit(&repo, "Adding training data")?;

            // Create the remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push the files
            repositories::push(&repo).await?;

            // Track the test dir
            let test_dir = repo.path.join("test");
            let num_test_files = util::fs::count_files_in_dir(&test_dir);
            repositories::add(&repo, &test_dir)?;
            let commit = repositories::commit(&repo, "Adding test data")?;

            // Push the files
            repositories::push(&repo).await?;

            let page_num = 1;
            let page_size = num_train_files + num_test_files + 5;
            let train_entries =
                api::client::dir::list(&remote_repo, &commit.id, "/train", page_num, page_size)
                    .await?;
            let test_entries =
                api::client::dir::list(&remote_repo, &commit.id, "/test", page_num, page_size)
                    .await?;
            assert_eq!(
                train_entries.total_entries + test_entries.total_entries,
                num_train_files + num_test_files
            );
            assert_eq!(
                train_entries.entries.len() + test_entries.entries.len(),
                num_train_files + num_test_files
            );

            api::client::repositories::delete(&remote_repo).await?;

            future::ok::<(), OxenError>(()).await
        })
        .await
    }

    #[tokio::test]
    async fn test_command_push_after_two_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async {
            // Make mutable copy so we can set remote
            let mut repo = repo;

            // Track the train dir
            let train_dir = repo.path.join("train");

            repositories::add(&repo, &train_dir)?;
            // Commit the train dur
            repositories::commit(&repo, "Adding training data")?;

            // Track the test dir
            let test_dir = repo.path.join("test");
            let num_test_files = util::fs::rcount_files_in_dir(&test_dir);
            repositories::add(&repo, &test_dir)?;
            let commit = repositories::commit(&repo, "Adding test data")?;

            // Create the remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push the files
            repositories::push(&repo).await?;

            let page_num = 1;
            let entries =
                api::client::dir::list(&remote_repo, &commit.id, ".", page_num, 10).await?;
            assert_eq!(entries.total_entries, 2);
            assert_eq!(entries.entries.len(), 2);

            let page_size = num_test_files + 10;
            let entries =
                api::client::dir::list(&remote_repo, &commit.id, "test", page_num, page_size)
                    .await?;
            assert_eq!(entries.total_entries, num_test_files);
            assert_eq!(entries.entries.len(), num_test_files);

            api::client::repositories::delete(&remote_repo).await?;

            future::ok::<(), OxenError>(()).await
        })
        .await
    }

    #[tokio::test]
    async fn test_latest_commit_is_computed_properly() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async {
            // Make mutable copy so we can set remote
            let mut repo = repo;

            /*
            Create a directory structure with one file per nested dir
              (This was really slow in post commit actions, want to optimize)

            README.md
            data/
              file.txt
              1/
                file.txt
              2/
                file.txt
              3/
                file.txt
              4/
                file.txt
              5/
                file.txt
            */

            // Create README
            let readme_path = repo.path.join("README.md");
            let readme_path = test::write_txt_file_to_path(readme_path, "README")?;
            repositories::add(&repo, &readme_path)?;
            let first_commit_id = repositories::commit(&repo, "Adding README")?;

            // Create the data dir
            let data_dir = repo.path.join("data");
            util::fs::create_dir_all(&data_dir)?;

            // Create subdirs with files
            let num_dirs = 5;
            for i in 0..num_dirs {
                let dir_path = data_dir.join(format!("{}", i));
                util::fs::create_dir_all(&dir_path)?;
                let file_path = dir_path.join("file.txt");
                let file_path = test::write_txt_file_to_path(file_path, format!("file -> {}", i))?;
                repositories::add(&repo, &file_path)?;
                repositories::commit(&repo, &format!("Adding file -> data/{}/file.txt", i))?;
            }

            // modify the 3rd file
            let file_path = data_dir.join("2").join("file.txt");
            let file_path = test::write_txt_file_to_path(file_path, "modified file")?;
            repositories::add(&repo, &file_path)?;
            let last_commit = repositories::commit(&repo, "Modifying file again")?;

            // Create the remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push the files
            repositories::push(&repo).await?;

            // Make sure we get the correct latest commit messages
            let page_num = 1;
            let entries =
                api::client::dir::list(&remote_repo, &last_commit.id, ".", page_num, 10).await?;
            assert_eq!(entries.total_entries, 2);
            assert_eq!(entries.entries.len(), 2);

            // find the data entry, and make sure the latest_commit matches the last commit
            let data_entry = entries.entries.iter().find(|e| e.filename() == "data");
            assert!(data_entry.is_some());
            let data_entry = data_entry.unwrap();
            assert_eq!(data_entry.filename(), "data");
            assert_eq!(
                data_entry.latest_commit().as_ref().unwrap().id,
                last_commit.id
            );

            // find the README entry, and make sure latest_commit matches the first commit
            let readme_entry = entries.entries.iter().find(|e| e.filename() == "README.md");
            assert!(readme_entry.is_some());
            let readme_entry = readme_entry.unwrap();
            assert_eq!(readme_entry.filename(), "README.md");
            assert_eq!(
                readme_entry.latest_commit().as_ref().unwrap().id,
                first_commit_id.id
            );

            // Check the latest commit in a subdir
            let page_num = 1;
            let entries =
                api::client::dir::list(&remote_repo, &last_commit.id, "data/3", page_num, 10)
                    .await?;
            assert_eq!(entries.total_entries, 1);
            assert_eq!(entries.entries.len(), 1);

            let entry = entries.entries.first().unwrap();
            assert_eq!(entry.filename(), "file.txt");
            assert_eq!(
                entry.latest_commit().as_ref().unwrap().message,
                "Adding file -> data/3/file.txt"
            );

            api::client::repositories::delete(&remote_repo).await?;

            future::ok::<(), OxenError>(()).await
        })
        .await
    }

    // This broke when you tried to add the "." directory to add everything, after already committing the train directory.
    #[tokio::test]
    async fn test_command_push_after_two_commits_adding_dot() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async {
            // Make mutable copy so we can set remote
            let mut repo = repo;

            // Track the train dir
            let train_dir = repo.path.join("train");

            repositories::add(&repo, &train_dir)?;
            // Commit the train dur
            repositories::commit(&repo, "Adding training data")?;

            // Track the rest of the files
            let full_dir = &repo.path;
            let num_files = util::fs::count_items_in_dir(full_dir);
            repositories::add(&repo, full_dir)?;
            let commit = repositories::commit(&repo, "Adding rest of data")?;

            // Create the remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push the files
            repositories::push(&repo).await?;

            let page_num = 1;
            let page_size = num_files + 10;
            let entries =
                api::client::dir::list(&remote_repo, &commit.id, ".", page_num, page_size).await?;
            assert_eq!(entries.total_entries, num_files);
            assert_eq!(entries.entries.len(), num_files);

            api::client::repositories::delete(&remote_repo).await?;

            future::ok::<(), OxenError>(()).await
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_push_if_remote_not_set() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Track the file
            let train_dirname = "train";
            let train_dir = repo.path.join(train_dirname);
            repositories::add(&repo, &train_dir)?;
            // Commit the train dir
            repositories::commit(&repo, "Adding training data")?;

            // Should not be able to push
            let result = repositories::push(&repo).await;
            assert!(result.is_err());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_push_branch_with_with_no_new_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // Track a dir
            let train_path = repo.path.join("train");
            repositories::add(&repo, &train_path)?;
            repositories::commit(&repo, "Adding train dir")?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push it
            repositories::push(&repo).await?;

            let new_branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, new_branch_name)?;

            // Push new branch, without any new commits, should still create the branch
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                new_branch_name,
            )
            .await?;

            let remote_branches = api::client::branches::list(&remote_repo).await?;
            assert_eq!(2, remote_branches.len());

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_push_two_separate_empty_roots() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Clone the first repo
            test::run_empty_dir_test_async(|first_repo_dir| async move {
                println!("test_cannot_push_two_separate_empty_roots clone first repo");
                let first_cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &first_repo_dir.join("first_repo"),
                )
                .await?;

                // Clone the second repo
                test::run_empty_dir_test_async(|second_repo_dir| async move {
                    println!("test_cannot_push_two_separate_empty_roots clone second repo");
                    let second_cloned_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &second_repo_dir.join("second_repo"),
                    )
                    .await?;

                    // Add to the first repo, after we have the second repo cloned
                    let new_file = "new_file.txt";
                    let new_file_path = first_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    repositories::add(&first_cloned_repo, &new_file_path)?;
                    repositories::commit(&first_cloned_repo, "Adding first file path.")?;
                    repositories::push(&first_cloned_repo).await?;

                    // The push to the second version of the same repo should fail
                    // Adding two commits to have a longer history that also should fail
                    let new_file = "new_file_2.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                    repositories::add(&second_cloned_repo, &new_file_path)?;
                    repositories::commit(&second_cloned_repo, "Adding second file path.")?;

                    let new_file = "new_file_3.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                    repositories::add(&second_cloned_repo, &new_file_path)?;
                    repositories::commit(&second_cloned_repo, "Adding third file path.")?;

                    // Push should FAIL
                    let result = repositories::push(&second_cloned_repo).await;
                    assert!(result.is_err());

                    Ok(second_repo_dir)
                })
                .await?;

                Ok(first_repo_dir)
            })
            .await?;

            Ok(ret_repo)
        })
        .await
    }

    // Test that we cannot push two completely separate local repos to the same history
    // 1) Create repo A with data
    // 2) Create repo B with data
    // 3) Push Repo A
    // 4) Push repo B to repo A and fail
    #[tokio::test]
    async fn test_cannot_push_two_separate_repos() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo_1| async move {
            test::run_training_data_repo_test_fully_committed_async(|mut repo_2| async move {
                // Add to the first repo
                let new_file = "new_file.txt";
                let new_file_path = repo_1.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                repositories::add(&repo_1, &new_file_path)?;
                repositories::commit(&repo_1, "Adding first file path.")?;
                // Set/create the proper remote
                let remote = test::repo_remote_url_from(&repo_1.dirname());
                command::config::set_remote(&mut repo_1, constants::DEFAULT_REMOTE_NAME, &remote)?;
                test::create_remote_repo(&repo_1).await?;
                repositories::push(&repo_1).await?;

                // Adding two commits to have a longer history that also should fail
                let new_file = "new_file_2.txt";
                let new_file_path = repo_2.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                repositories::add(&repo_2, &new_file_path)?;
                repositories::commit(&repo_2, "Adding second file path.")?;

                let new_file = "new_file_3.txt";
                let new_file_path = repo_2.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                repositories::add(&repo_2, &new_file_path)?;
                repositories::commit(&repo_2, "Adding third file path.")?;

                // Set remote to the same as the first repo
                command::config::set_remote(&mut repo_2, constants::DEFAULT_REMOTE_NAME, &remote)?;

                // Push should FAIL
                let result = repositories::push(&repo_2).await;
                assert!(result.is_err());

                Ok(())
            })
            .await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_push_many_commits_default_branch() -> Result<(), OxenError> {
        test::run_many_local_commits_empty_sync_remote_test(|local_repo, remote_repo| async move {
            // Nothing should be synced on remote and no commit objects created
            let history =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 0);

            // Push all to remote
            repositories::push(&local_repo).await?;

            // Should now have 25 commits on remote
            let history =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 25);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_many_commits_new_branch() -> Result<(), OxenError> {
        test::run_many_local_commits_empty_sync_remote_test(|local_repo, remote_repo| async move {
            // Nothing should be synced on remote and no commit objects created
            let history =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 0);

            // Create new local branch
            let new_branch_name = "my-branch";
            repositories::branches::create_checkout(&local_repo, new_branch_name)?;

            // New commit
            let new_file = "new_file.txt";
            let new_file_path = local_repo.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
            repositories::add(&local_repo, &new_file_path)?;
            repositories::commit(&local_repo, "Adding first file path.")?;

            // Push new branch to remote without first syncing main
            repositories::push::push_remote_branch(
                &local_repo,
                constants::DEFAULT_REMOTE_NAME,
                new_branch_name,
            )
            .await?;

            // Should now have 26 commits on remote on new branch
            let history_new =
                api::client::commits::list_commit_history(&remote_repo, new_branch_name).await?;
            assert_eq!(history_new.len(), 26);

            // TODO: v0_10_0 logic should have 1 commit on main
            // Should still have no commits on main
            let history_main =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME).await;
            log::debug!("history_main: {:?}", history_main);
            // assert_eq!(history_main.len(), 1);
            assert!(history_main.is_err());

            // Back to main
            repositories::checkout(&local_repo, DEFAULT_BRANCH_NAME).await?;

            // Push to remote
            repositories::push(&local_repo).await?;

            // 25 on main
            let history_main =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history_main.len(), 25);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_push_while_another_user_is_pushing() -> Result<(), OxenError> {
        test::run_no_commit_remote_repo_test(|remote_repo| async move {
            let ret_repo = remote_repo.clone();

            // Clone the first repo
            test::run_empty_dir_test_async(|first_repo_dir| async move {
                let first_cloned_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &first_repo_dir.join("first_repo"),
                )
                .await?;

                // Clone the second repo
                test::run_empty_dir_test_async(|second_repo_dir| async move {
                    let second_cloned_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &second_repo_dir.join("second_repo"),
                    )
                    .await?;

                    // Add to the first repo, after we have the second repo cloned
                    let new_file = "new_file.txt";
                    let new_file_path = first_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    repositories::add(&first_cloned_repo, &new_file_path)?;
                    repositories::commit(&first_cloned_repo, "Adding first file path.")?;
                    repositories::push(&first_cloned_repo).await?;

                    // The push to the second version of the same repo should fail
                    // Adding two commits to have a longer history that also should fail
                    let new_file = "new_file_2.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                    repositories::add(&second_cloned_repo, &new_file_path)?;
                    repositories::commit(&second_cloned_repo, "Adding second file path.")?;

                    let new_file = "new_file_3.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                    repositories::add(&second_cloned_repo, &new_file_path)?;
                    repositories::commit(&second_cloned_repo, "Adding third file path.")?;

                    // Push should FAIL
                    let result = repositories::push(&second_cloned_repo).await;
                    assert!(result.is_err());

                    Ok(second_repo_dir)
                })
                .await?;

                Ok(first_repo_dir)
            })
            .await?;

            Ok(ret_repo)
        })
        .await
    }

    // Test that we cannot clone separate repos with separate histories, then push to the same history
    // 1) Clone repo A with data
    // 2) Clone repo B with data
    // 3) Push Repo A
    // 4) Push repo B to repo A and fail
    #[tokio::test]
    async fn test_tree_cannot_push_two_separate_cloned_repos() -> Result<(), OxenError> {
        // Push the first repo with data
        test::run_training_data_fully_sync_remote(|_, remote_repo_1| async move {
            let remote_repo_1_copy = remote_repo_1.clone();

            // Push the second repo with data
            test::run_training_data_fully_sync_remote(|_, remote_repo_2| async move {
                let remote_repo_2_copy = remote_repo_2.clone();
                // Clone the first repo
                test::run_empty_dir_test_async(|first_repo_dir| async move {
                    let first_cloned_repo = repositories::clone_url(
                        &remote_repo_1.remote.url,
                        &first_repo_dir.join("first_repo_dir"),
                    )
                    .await?;

                    // Clone the second repo
                    test::run_empty_dir_test_async(|second_repo_dir| async move {
                        let mut second_cloned_repo = repositories::clone_url(
                            &remote_repo_2.remote.url,
                            &second_repo_dir.join("second_repo_dir"),
                        )
                        .await?;

                        // Add to the first repo, after we have the second repo cloned
                        let new_file = "new_file.txt";
                        let new_file_path = first_cloned_repo.path.join(new_file);
                        let new_file_path =
                            test::write_txt_file_to_path(new_file_path, "new file")?;
                        repositories::add(&first_cloned_repo, &new_file_path)?;
                        repositories::commit(&first_cloned_repo, "Adding first file path.")?;
                        repositories::push(&first_cloned_repo).await?;

                        // Reset the remote on the second repo to the first repo
                        let first_remote = test::repo_remote_url_from(&first_cloned_repo.dirname());
                        command::config::set_remote(
                            &mut second_cloned_repo,
                            constants::DEFAULT_REMOTE_NAME,
                            &first_remote,
                        )?;

                        // Adding two commits to have a longer history that also should fail
                        let new_file = "new_file_2.txt";
                        let new_file_path = second_cloned_repo.path.join(new_file);
                        let new_file_path =
                            test::write_txt_file_to_path(new_file_path, "new file 2")?;
                        repositories::add(&second_cloned_repo, &new_file_path)?;
                        repositories::commit(&second_cloned_repo, "Adding second file path.")?;

                        let new_file = "new_file_3.txt";
                        let new_file_path = second_cloned_repo.path.join(new_file);
                        let new_file_path =
                            test::write_txt_file_to_path(new_file_path, "new file 3")?;
                        repositories::add(&second_cloned_repo, &new_file_path)?;
                        repositories::commit(&second_cloned_repo, "Adding third file path.")?;

                        // Push should FAIL
                        let result = repositories::push(&second_cloned_repo).await;
                        assert!(result.is_err());

                        Ok(second_repo_dir)
                    })
                    .await?;

                    Ok(first_repo_dir)
                })
                .await?;
                Ok(remote_repo_2_copy)
            })
            .await?;

            Ok(remote_repo_1_copy)
        })
        .await
    }

    // Test that we cannot push when the remote repo is ahead
    // * Clone repo to user A
    // * Clone repo to user B
    // * User A makes commit modifying `README.md` and pushes
    // * User B makes commit modifying `README.md` pushes and fails
    // * User B pulls user A's changes and there is a conflict
    // * User B fixes the conflict and pushes and succeeds
    #[tokio::test]
    async fn test_tree_cannot_push_when_remote_repo_is_ahead_same_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir_copy.join("new_repo"),
                )
                .await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");

                    let user_b_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir_copy.join("New_repo"),
                    )
                    .await?;

                    // User A modifies the README.md and pushes
                    let mod_file = "README.md";
                    let a_mod_file_path = user_a_repo.path.join(mod_file);
                    let a_mod_file_path =
                        test::write_txt_file_to_path(a_mod_file_path, "I am the README now")?;
                    repositories::add(&user_a_repo, &a_mod_file_path)?;
                    let commit_a =
                        repositories::commit(&user_a_repo, "User A modifying the README.")?;
                    log::debug!("commit_a: {}", commit_a);
                    repositories::push(&user_a_repo).await?;

                    // User B tries to modify the same README.md and push
                    let b_mod_file_path = user_b_repo.path.join(mod_file);
                    let b_mod_file_path =
                        test::write_txt_file_to_path(b_mod_file_path, "I be the README now.")?;
                    repositories::add(&user_b_repo, &b_mod_file_path)?;
                    let commit_b =
                        repositories::commit(&user_b_repo, "User B modifying the README.")?;
                    log::debug!("commit_b: {}", commit_b);

                    // Push should fail! Remote is ahead
                    let first_push_result = repositories::push(&user_b_repo).await;
                    log::debug!("first_push_result: {:?}", first_push_result);
                    assert!(first_push_result.is_err());

                    // Pull should error because there are conflicts
                    let result = repositories::pull(&user_b_repo).await;
                    assert!(result.is_err());

                    // There should be conflicts
                    let status = repositories::status(&user_b_repo)?;
                    assert!(status.has_merge_conflicts());
                    println!("passed has_merge_conflicts");
                    status.print();

                    // User B resolves conflicts
                    let b_mod_file_path = user_b_repo.path.join(mod_file);
                    let b_mod_file_path = test::write_txt_file_to_path(
                        b_mod_file_path,
                        "No for real. I be the README now.",
                    )?;
                    println!("passed write_txt_file_to_path");
                    repositories::add(&user_b_repo, &b_mod_file_path)?;
                    println!("passed add");
                    repositories::commit(&user_b_repo, "User B resolving conflicts.")?;
                    println!("passed commit");

                    // Push should now succeed
                    let third_push_result = repositories::push(&user_b_repo).await;
                    assert!(third_push_result.is_ok());

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_tree_cannot_push_when_remote_is_many_commits_ahead_tree_conflicts(
    ) -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir.join("new_repo"),
                )
                .await?;

                // Log out all files in this directory with fs
                let files = util::fs::rlist_paths_in_dir(&user_a_repo_dir);
                for item in files {
                    log::debug!("\nfile or dir: {:?}\n", item)
                }

                // User A: add files in `nlp`
                // User B: add files in `annotations`

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // User A adds a file and pushes
                    let modify_path_a = user_a_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");
                    let modify_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");
                    test::write_txt_file_to_path(&modify_path_a, "new file")?;
                    repositories::add(&user_a_repo, &modify_path_a)?;
                    repositories::commit(&user_a_repo, "Adding first file path.")?;

                    repositories::push(&user_a_repo).await?;

                    // User B adds a different file and pushe
                    test::write_txt_file_to_path(&modify_path_b, "newer file")?;
                    repositories::add(&user_b_repo, &modify_path_b)?;
                    repositories::commit(&user_b_repo, "User B adding second file path.")?;

                    // Push should fail - this creates a merge conflict.
                    let res = repositories::push(&user_b_repo).await;
                    assert!(res.is_err());

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_tree_cannot_push_tree_conflict_deleted_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir.join("new_repo"),
                )
                .await?;

                // Log out all files in this directory with fs
                let files = util::fs::rlist_paths_in_dir(&user_a_repo_dir);
                for item in files {
                    log::debug!("\nfile or dir: {:?}\n", item)
                }

                // User A: add files in `nlp`
                // User B: add files in `annotations`

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.clone();

                    let user_b_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // User A deletes the file and commits
                    let modify_path_a = user_a_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");
                    let modify_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt");

                    let _add_path_b = user_b_repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("averynewfile.txt");

                    // print all files in annotations/train
                    let files = util::fs::rlist_paths_in_dir(
                        &user_b_repo.path.join("annotations").join("train"),
                    );
                    for item in files {
                        log::debug!("\npre file or dir: {:?}\n", item)
                    }
                    // User A modifies
                    test::write_txt_file_to_path(&modify_path_a, "fancy new file contents")?;
                    repositories::add(&user_a_repo, &modify_path_a)?;
                    let commit_a =
                        repositories::commit(&user_a_repo, "modifying first file path.")?;
                    repositories::push(&user_a_repo).await?;

                    // User B deletes at user a path A modified, causing conflicts.
                    util::fs::remove_file(&modify_path_b)?;
                    let files = util::fs::rlist_paths_in_dir(
                        &user_b_repo.path.join("annotations").join("train"),
                    );
                    for item in files {
                        log::debug!("\npost file or dir: {:?}\n", item)
                    }
                    repositories::add(&user_b_repo, &modify_path_b)?;
                    // also add a file
                    // test::write_txt_file_to_path(&add_path_b, "new file")?;
                    // repositories::add(&user_b_repo, &add_path_b)?;

                    // Before this commit, init a reader at b's head
                    let head = repositories::commits::head_commit(&user_b_repo)?;
                    let pre_b =
                        repositories::tree::get_root_with_children(&user_b_repo, &head)?.unwrap();
                    log::debug!("b head before is {:?}", head);

                    let maybe_b_entry = pre_b.get_by_path(
                        PathBuf::from("annotations")
                            .join("train")
                            .join("annotations.txt"),
                    )?;

                    log::debug!("maybe_b_entry before commit is {:?}", maybe_b_entry);

                    let commit_b =
                        repositories::commit(&user_b_repo, "user B deleting file path.")?;

                    let head = repositories::commits::head_commit(&user_b_repo)?;
                    let post_b =
                        repositories::tree::get_root_with_children(&user_b_repo, &head)?.unwrap();
                    let maybe_b_entry = post_b.get_by_path(
                        PathBuf::from("annotations")
                            .join("train")
                            .join("annotations.txt"),
                    )?;

                    log::debug!("maybe_b_entry after commitis {:?}", maybe_b_entry);

                    log::debug!("commit_a is {:?}", commit_a);
                    log::debug!("commit_b is {:?}", commit_b);

                    let commit_a =
                        repositories::commits::get_by_id(&user_a_repo, &commit_a.id)?.unwrap();
                    let commit_b =
                        repositories::commits::get_by_id(&user_b_repo, &commit_b.id)?.unwrap();

                    log::debug!("commit_a pre is {:?}", commit_a);
                    log::debug!("commit_b pre is {:?}", commit_b);

                    // Push should fail
                    let res = repositories::push(&user_b_repo).await;

                    log::debug!("here's the result and why it failed: {:?}", res);

                    assert!(res.is_err());

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_move_entire_directory() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Move the README to a new file name
            let train_images = local_repo.path.join("train");
            let new_path = local_repo.path.join("images").join("train");
            util::fs::create_dir_all(local_repo.path.join("images"))?;
            util::fs::rename(&train_images, &new_path)?;

            repositories::add(&local_repo, new_path)?;
            let mut rm_opts = RmOpts::from_path("train");
            rm_opts.recursive = true;
            repositories::rm(&local_repo, &rm_opts)?;
            let commit =
                repositories::commit(&local_repo, "Moved all the train image files to images/")?;
            repositories::push(&local_repo).await?;

            let path = PathBuf::from("");
            let page = 1;
            let page_size = 100;
            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &path, page, page_size).await?;
            // check to make sure we only have the images directory and not the train directory
            assert!(!dir_entries
                .entries
                .iter()
                .any(|entry| entry.filename() == "train"));
            assert!(dir_entries
                .entries
                .iter()
                .any(|entry| entry.filename() == "images"));

            // Add a single new file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file)?;
            let commit = repositories::commit(&local_repo, "Added a new file")?;
            repositories::push(&local_repo).await?;

            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &path, page, page_size).await?;
            // make sure we have the new file
            assert!(dir_entries
                .entries
                .iter()
                .any(|entry| entry.filename() == "new_file.txt"));

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_only_one_modified_file() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Move the README to a new file name
            let readme_path = local_repo.path.join("README.md");
            let new_path = local_repo.path.join("README2.md");
            util::fs::rename(&readme_path, &new_path)?;

            repositories::add(&local_repo, new_path)?;
            let rm_opts = RmOpts::from_path("README.md");
            repositories::rm(&local_repo, &rm_opts)?;
            let commit = repositories::commit(&local_repo, "Moved the readme")?;
            repositories::push(&local_repo).await?;

            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                    .await?;
            // make sure we have the new file
            assert!(dir_entries
                .entries
                .iter()
                .any(|entry| entry.filename() == "README2.md"));
            // make sure we don't have the old file
            assert!(!dir_entries
                .entries
                .iter()
                .any(|entry| entry.filename() == "README.md"));

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_root_subtree_depth_1() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths = Some(vec![PathBuf::from(".")]);
                opts.fetch_opts.depth = Some(1);
                let local_repo = repositories::clone::clone(&opts).await?;

                // Add a new file
                let readme_file = local_repo.path.join("ANOTHER_FILE.md");
                util::fs::write_to_path(
                    &readme_file,
                    r"
Q: How can I version a giant dataset of images?
A: Oxen.ai is a great tool for this! It can handle any size dataset, and is optimized for speed.
",
                )?;
                repositories::add(&local_repo, &readme_file)?;
                let commit = repositories::commit(&local_repo, "Added another file")?;

                let result = repositories::push(&local_repo).await;
                println!("push result: {:?}", result);

                assert!(result.is_ok());

                // List the files in the remote repo and confirm the new file is there
                let dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "ANOTHER_FILE.md"));

                Ok(dir)
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_annotations_test_subtree() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("annotations").join("test")]);
                let local_repo = repositories::clone::clone(&opts).await?;

                let annotations_test_dir = local_repo.path.join("annotations").join("test");

                // Add a new file
                let readme_file = annotations_test_dir.join("README.md");
                util::fs::write_to_path(
                    &readme_file,
                    r"
Q: What is a faster alternative to DVC?
A: Checkout Oxen.ai
",
                )?;
                repositories::add(&local_repo, &readme_file)?;
                let commit = repositories::commit(&local_repo, "adding README.md to the test dir")?;

                let result = repositories::push(&local_repo).await;
                println!("push result: {:?}", result);

                assert!(result.is_ok());

                // Get the file from the remote repo
                let dir_entries = api::client::dir::list(
                    &remote_repo,
                    &commit.id,
                    &PathBuf::from("annotations").join("test"),
                    1,
                    100,
                )
                .await?;
                println!("dir_entries: {:?}", dir_entries);

                // Make sure we have the new file
                assert!(dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "README.md"));

                Ok(dir)
            })
            .await?;
            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_subtree_nlp_classification() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo
            test::run_empty_dir_test_async(|repos_base_dir| async move {
                let repos_base_dir_copy = repos_base_dir.clone();
                let user_a_repo_dir = repos_base_dir.join("user_a_repo");

                // Make sure to clone a subtree to test subtree merge conflicts
                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &user_a_repo_dir);
                clone_opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("nlp").join("classification")]);
                clone_opts.fetch_opts.depth = Some(2);
                let user_a_repo = repositories::clone(&clone_opts).await?;

                // User adds a file and pushes
                let new_file = PathBuf::from("nlp")
                    .join("classification")
                    .join("new_data.tsv");
                let new_file_path = user_a_repo.path.join(&new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "image\tlabel")?;
                repositories::add(&user_a_repo, &new_file_path)?;
                let commit =
                    repositories::commit(&user_a_repo, "Adding nlp/classification/new_data.tsv")?;
                repositories::push(&user_a_repo).await?;

                // Make sure the file is in the remote repo
                let dir_entries = api::client::dir::list(
                    &remote_repo,
                    &commit.id,
                    &PathBuf::from("nlp").join("classification"),
                    1,
                    100,
                )
                .await?;

                assert!(dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "new_data.tsv"));

                // Make sure the root directory is in tact
                let root_dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(root_dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "README.md"));

                Ok(repos_base_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_partial_clone_nlp_classification() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo
            test::run_empty_dir_test_async(|repos_base_dir| async move {
                let repos_base_dir_copy = repos_base_dir.clone();
                let user_a_repo_dir = repos_base_dir.join("user_a_repo");

                // Make sure to clone a subtree to test subtree merge conflicts
                let mut clone_opts = CloneOpts::new(&remote_repo.remote.url, &user_a_repo_dir);
                clone_opts.fetch_opts.subtree_paths =
                    Some(vec![PathBuf::from("nlp").join("classification")]);
                clone_opts.fetch_opts.depth = Some(2);
                let user_a_repo = repositories::clone(&clone_opts).await?;

                // User adds multiple files and modifies an existing file
                let new_file_1 = PathBuf::from("nlp")
                    .join("classification")
                    .join("new_partial_data_1.tsv");
                let new_file_path_1 = user_a_repo.path.join(&new_file_1);
                let new_file_path_1 =
                    test::write_txt_file_to_path(new_file_path_1, "image\tlabel1")?;
                repositories::add(&user_a_repo, &new_file_path_1)?;

                let new_file_2 = PathBuf::from("nlp")
                    .join("classification")
                    .join("new_partial_data_2.tsv");
                let new_file_path_2 = user_a_repo.path.join(&new_file_2);
                let new_file_path_2 =
                    test::write_txt_file_to_path(new_file_path_2, "image\tlabel2")?;
                repositories::add(&user_a_repo, &new_file_path_2)?;

                // Modify an existing file
                let existing_file_path = user_a_repo
                    .path
                    .join("nlp/classification/existing_file.tsv");
                let modified_file_path =
                    test::write_txt_file_to_path(existing_file_path, "image\tmodified_label")?;
                repositories::add(&user_a_repo, &modified_file_path)?;

                // Commit changes
                let commit = repositories::commit(
                    &user_a_repo,
                    "Adding new partial data files and modifying existing file",
                )?;
                repositories::push(&user_a_repo).await?;

                // Verify that the new files are in the remote repo
                let dir_entries = api::client::dir::list(
                    &remote_repo,
                    &commit.id,
                    &PathBuf::from("nlp").join("classification"),
                    1,
                    100,
                )
                .await?;

                assert!(dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "new_partial_data_1.tsv"));
                assert!(dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "new_partial_data_2.tsv"));
                assert!(dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "existing_file.tsv"));

                let metadata_entry = dir_entries
                    .entries
                    .iter()
                    .find(|entry| entry.filename() == "existing_file.tsv")
                    .unwrap();

                let metadata_entry = match metadata_entry {
                    EMetadataEntry::MetadataEntry(entry) => entry,
                    _ => panic!("Expected a metadata entry"),
                };

                // Verify the content of the modified existing file
                api::client::entries::download_file(
                    &remote_repo,
                    metadata_entry,
                    &PathBuf::from("nlp/classification/existing_file.tsv"),
                    &user_a_repo
                        .path
                        .join("nlp/classification/existing_file.tsv"),
                    &commit.id,
                )
                .await?;
                let modified_file_content = std::fs::read_to_string(
                    user_a_repo
                        .path
                        .join("nlp/classification/existing_file.tsv"),
                )?;
                assert_eq!(modified_file_content, "image\tmodified_label");

                // Verify that the root directory is intact
                let root_dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(root_dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "README.md"));

                // Verify that the original repo structure is intact
                let classification_dir_entries = api::client::dir::list(
                    &remote_repo,
                    &commit.id,
                    &PathBuf::from("nlp").join("classification"),
                    1,
                    100,
                )
                .await?;

                assert!(!classification_dir_entries.entries.is_empty()); // Ensure there are entries in the classification directory

                let root_dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(root_dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "README.md"));

                Ok(repos_base_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_file_with_exact_avg_chunk_size() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let local_repo = local_repo.clone();
            let remote_repo = remote_repo.clone();

            // Create a file with exactly AVG_CHUNK_SIZE bytes of random data
            let file_path = local_repo.path.join("exact_chunk_size_file.bin");

            // Generate random data of exactly AVG_CHUNK_SIZE bytes
            let mut rng = rand::thread_rng();
            let random_data: Vec<u8> = (0..AVG_CHUNK_SIZE).map(|_| rng.gen::<u8>()).collect();

            // Write the data to the file
            util::fs::write_data(&file_path, &random_data)?;

            // Verify the file size is exactly AVG_CHUNK_SIZE
            let metadata = util::fs::metadata(&file_path)?;
            assert_eq!(
                metadata.len(),
                AVG_CHUNK_SIZE,
                "File size should be exactly AVG_CHUNK_SIZE"
            );

            // Add and commit the file
            repositories::add(&local_repo, &file_path)?;
            let commit_msg = "Add file with exactly AVG_CHUNK_SIZE bytes";
            let commit = repositories::commit(&local_repo, commit_msg)?;

            // Push the commit to the remote repository
            let branch = repositories::push(&local_repo).await?;

            // Verify the push was successful by checking the remote repository
            let remote_commit_opt =
                api::client::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit_opt.is_some(), "Remote commit should exist");

            let remote_repo_clone = remote_repo.clone();
            // Create a temporary directory to download the file to
            test::run_empty_dir_test_async(|temp_dir| async move {
                let download_path = temp_dir.join("downloaded_file.bin");

                // Download the file from the remote repository
                repositories::download(
                    &remote_repo_clone,
                    "exact_chunk_size_file.bin",
                    &download_path,
                    &branch.name,
                )
                .await?;

                // Verify the file was downloaded successfully
                assert!(download_path.exists(), "Downloaded file should exist");

                // Verify the file size is exactly AVG_CHUNK_SIZE
                let downloaded_metadata = util::fs::metadata(&download_path)?;
                assert_eq!(
                    downloaded_metadata.len(),
                    metadata.len(),
                    "Downloaded file size should match the original file size"
                );

                // Verify the file contents match the original data
                let downloaded_data = util::fs::read_bytes_from_path(&download_path)?;
                assert_eq!(
                    downloaded_data, random_data,
                    "Downloaded file contents should match the original data"
                );

                Ok(temp_dir)
            })
            .await?;

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_create_nodes_before_starting_push() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            // Add a single new file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, &new_file)?;
            let commit = repositories::commit(&local_repo, "Added a new file")?;

            // Collect all nodes in the local tree
            let progress = Arc::new(PushProgress::new());
            progress.set_message("Collecting missing nodes...");

            let mut candidate_nodes: HashSet<MerkleTreeNode> = HashSet::new();
            let Some(commit_node) =
                repositories::tree::get_root_with_children(&local_repo, &commit)?
            else {
                return Err(OxenError::basic_str("Err: Root not found"));
            };
            candidate_nodes.insert(commit_node.clone());
            commit_node.walk_tree_without_leaves(|node| {
                candidate_nodes.insert(node.clone());
                progress.set_message(format!(
                    "Collecting missing nodes... {}",
                    candidate_nodes.len()
                ));
            });

            // Create nodes on server
            progress.set_message(format!("Pushing {} nodes...", candidate_nodes.len()));
            api::client::tree::create_nodes(
                &local_repo,
                &remote_repo,
                candidate_nodes.clone(),
                &progress,
            )
            .await?;

            // Attempt push with non-leaf nodes already created on server
            repositories::push(&local_repo).await?;

            let new_path = PathBuf::from("new_file.txt");
            // Check for new file node on server
            let _found_node =
                api::client::tree::get_node_hash_by_path(&remote_repo, &commit.id, new_path)
                    .await?;
            Ok(remote_repo)
        })
        .await
    }
}
