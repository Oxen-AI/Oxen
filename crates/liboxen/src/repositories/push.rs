//! # oxen push
//!
//! Push data from your local machine to a remote.
//!

/// # Push committed data to a remote
///
/// ```ignore
/// use liboxen::command;
/// use liboxen::repositories;
/// use liboxen::util;
///
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_push");
/// let mut repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// repositories::add(&repo, &hello_file).await?;
///
/// // Commit staged
/// repositories::commit(&repo, "My commit message")?;
///
/// // Set the remote server
/// command::config::set_remote(&mut repo, "origin", "http://localhost:3000/repositories/hello");
///
/// // Push the file
/// repositories::push(&repo).await?;
/// ```
pub use crate::core::v_latest::push::push;

/// Push to a specific remote branch on the default remote repository
pub use crate::core::v_latest::push::push_remote_branch;

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME, stream_segment_size};
    use crate::core::progress::push_progress::PushProgress;
    use crate::error::OxenError;
    use crate::model::merkle_tree::node::MerkleTreeNode;
    use crate::opts::RmOpts;
    use crate::opts::{CloneOpts, PushOpts};
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::view::entries::EMetadataEntry;
    use futures::future;
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_command_push_one_commit() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            // Create a small directory of inline files
            let data_dir = repo.path.join("data");
            util::fs::create_dir_all(&data_dir)?;
            for i in 0..3 {
                test::write_txt_file_to_path(
                    data_dir.join(format!("file_{i}.txt")),
                    format!("contents {i}"),
                )?;
            }
            let num_files = util::fs::rcount_files_in_dir(&data_dir);
            repositories::add(&repo, &data_dir).await?;

            // Write a README.md file
            let readme_path = repo.path.join("README.md");
            let readme_path = test::write_txt_file_to_path(readme_path, "Ready to train 🏋️‍♂️")?;
            repositories::add(&repo, &readme_path).await?;

            let commit = repositories::commit(&repo, "Adding initial data")?;

            // Create the remote
            let remote_repo = test::create_remote_repo(&repo).await?;
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push it real good
            repositories::push(&repo).await?;

            let page_num = 1;
            let page_size = num_files + 10;
            let entries =
                api::client::dir::list(&remote_repo, &commit.id, "data", page_num, page_size)
                    .await?;
            assert_eq!(entries.total_entries, num_files);
            assert_eq!(entries.entries.len(), num_files);

            // Make sure we can download the README back and it matches
            let download_path = repo.path.join("README_2.md");
            api::client::entries::download_entry(&remote_repo, "README.md", &download_path, "main")
                .await?;
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
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            // Create two small inline directories
            let train_dir = repo.path.join("train");
            util::fs::create_dir_all(&train_dir)?;
            for i in 0..2 {
                test::write_txt_file_to_path(
                    train_dir.join(format!("train_{i}.txt")),
                    format!("train {i}"),
                )?;
            }
            let num_train_files = util::fs::rcount_files_in_dir(&train_dir);
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "Adding train data")?;

            // Set up remote and push the first commit
            let remote_repo = test::create_remote_repo(&repo).await?;
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;
            repositories::push(&repo).await?;

            // Add a second inline directory, commit, push between commits
            let test_dir = repo.path.join("test");
            util::fs::create_dir_all(&test_dir)?;
            for i in 0..2 {
                test::write_txt_file_to_path(
                    test_dir.join(format!("test_{i}.txt")),
                    format!("test {i}"),
                )?;
            }
            let num_test_files = util::fs::count_files_in_dir(&test_dir);
            repositories::add(&repo, &test_dir).await?;
            let commit = repositories::commit(&repo, "Adding test data")?;
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
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            // First commit: inline `train` dir
            let train_dir = repo.path.join("train");
            util::fs::create_dir_all(&train_dir)?;
            for i in 0..2 {
                test::write_txt_file_to_path(
                    train_dir.join(format!("train_{i}.txt")),
                    format!("train {i}"),
                )?;
            }
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "Adding train data")?;

            // Second commit: inline `test` dir
            let test_dir = repo.path.join("test");
            util::fs::create_dir_all(&test_dir)?;
            for i in 0..2 {
                test::write_txt_file_to_path(
                    test_dir.join(format!("test_{i}.txt")),
                    format!("test {i}"),
                )?;
            }
            let num_test_files = util::fs::rcount_files_in_dir(&test_dir);
            repositories::add(&repo, &test_dir).await?;
            let commit = repositories::commit(&repo, "Adding test data")?;

            // Set up remote and push (only at the end — both commits in one push)
            let remote_repo = test::create_remote_repo(&repo).await?;
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;
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
            repositories::add(&repo, &readme_path).await?;
            let first_commit_id = repositories::commit(&repo, "Adding README")?;

            // Create the data dir
            let data_dir = repo.path.join("data");
            util::fs::create_dir_all(&data_dir)?;

            // Create subdirs with files
            let num_dirs = 5;
            for i in 0..num_dirs {
                let dir_path = data_dir.join(format!("{i}"));
                util::fs::create_dir_all(&dir_path)?;
                let file_path = dir_path.join("file.txt");
                let file_path = test::write_txt_file_to_path(file_path, format!("file -> {i}"))?;
                repositories::add(&repo, &file_path).await?;
                repositories::commit(&repo, &format!("Adding file -> data/{i}/file.txt"))?;
            }

            // modify the 3rd file
            let file_path = data_dir.join("2").join("file.txt");
            let file_path = test::write_txt_file_to_path(file_path, "modified file")?;
            repositories::add(&repo, &file_path).await?;
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
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            // First commit: just the train dir
            let train_dir = repo.path.join("train");
            util::fs::create_dir_all(&train_dir)?;
            for i in 0..2 {
                test::write_txt_file_to_path(
                    train_dir.join(format!("train_{i}.txt")),
                    format!("train {i}"),
                )?;
            }
            repositories::add(&repo, &train_dir).await?;
            repositories::commit(&repo, "Adding train data")?;

            // Add loose files at root that the second `add(.)` should pick up
            test::write_txt_file_to_path(repo.path.join("README.md"), "README")?;
            test::write_txt_file_to_path(repo.path.join("LICENSE.md"), "license")?;

            // Second commit: `add(.)` catches the loose root files
            let full_dir = &repo.path;
            let num_files = util::fs::count_items_in_dir(full_dir);
            repositories::add(&repo, full_dir).await?;
            let commit = repositories::commit(&repo, "Adding rest of data")?;

            // Set up remote and push
            let remote_repo = test::create_remote_repo(&repo).await?;
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;
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
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Should not be able to push when no remote is configured
            let result = repositories::push(&repo).await;
            assert!(result.is_err());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_push_branch_with_with_no_new_commits() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|repo, remote_repo| async move {
            // The helper has already pushed the main branch with one commit.
            // Create a new branch with no new commits and push it.
            let new_branch_name = "my-branch";
            repositories::branches::create_checkout(&repo, new_branch_name)?;

            let opts = PushOpts {
                remote: DEFAULT_REMOTE_NAME.to_string(),
                branch: new_branch_name.to_string(),
                ..Default::default()
            };
            repositories::push::push_remote_branch(&repo, &opts).await?;

            let remote_branches = api::client::branches::list(&remote_repo).await?;
            assert_eq!(2, remote_branches.len());

            Ok(remote_repo)
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
                    repositories::add(&first_cloned_repo, &new_file_path).await?;
                    repositories::commit(&first_cloned_repo, "Adding first file path.")?;
                    repositories::push(&first_cloned_repo).await?;

                    // The push to the second version of the same repo should fail
                    // Adding two commits to have a longer history that also should fail
                    let new_file = "new_file_2.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                    repositories::add(&second_cloned_repo, &new_file_path).await?;
                    repositories::commit(&second_cloned_repo, "Adding second file path.")?;

                    let new_file = "new_file_3.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                    repositories::add(&second_cloned_repo, &new_file_path).await?;
                    repositories::commit(&second_cloned_repo, "Adding third file path.")?;

                    // Push should FAIL
                    let result = repositories::push(&second_cloned_repo).await;
                    assert!(result.is_err());

                    Ok(())
                })
                .await?;

                Ok(())
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
        test::run_one_commit_local_repo_test_async(|mut repo_1| async move {
            test::run_one_commit_local_repo_test_async(|mut repo_2| async move {
                // Add to the first repo
                let new_file = "new_file.txt";
                let new_file_path = repo_1.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                repositories::add(&repo_1, &new_file_path).await?;
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
                repositories::add(&repo_2, &new_file_path).await?;
                repositories::commit(&repo_2, "Adding second file path.")?;

                let new_file = "new_file_3.txt";
                let new_file_path = repo_2.path.join(new_file);
                let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                repositories::add(&repo_2, &new_file_path).await?;
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

            // Should now have 5 commits on remote
            let history =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history.len(), 5);

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
            repositories::add(&local_repo, &new_file_path).await?;
            repositories::commit(&local_repo, "Adding first file path.")?;

            // Push new branch to remote without first syncing main
            let opts = PushOpts {
                remote: DEFAULT_REMOTE_NAME.to_string(),
                branch: new_branch_name.to_string(),
                ..Default::default()
            };
            repositories::push::push_remote_branch(&local_repo, &opts).await?;

            // Should now have 6 commits on remote on new branch
            let history_new =
                api::client::commits::list_commit_history(&remote_repo, new_branch_name).await?;
            assert_eq!(history_new.len(), 6);

            // Should still have no commits on main
            let history_main =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME).await;
            log::debug!("history_main: {history_main:?}");
            // assert_eq!(history_main.len(), 1);
            assert!(history_main.is_err());

            // Back to main
            repositories::checkout(&local_repo, DEFAULT_BRANCH_NAME).await?;

            // Push to remote
            repositories::push(&local_repo).await?;

            // 5 on main
            let history_main =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(history_main.len(), 5);

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
                    repositories::add(&first_cloned_repo, &new_file_path).await?;
                    repositories::commit(&first_cloned_repo, "Adding first file path.")?;
                    repositories::push(&first_cloned_repo).await?;

                    // The push to the second version of the same repo should fail
                    // Adding two commits to have a longer history that also should fail
                    let new_file = "new_file_2.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 2")?;
                    repositories::add(&second_cloned_repo, &new_file_path).await?;
                    repositories::commit(&second_cloned_repo, "Adding second file path.")?;

                    let new_file = "new_file_3.txt";
                    let new_file_path = second_cloned_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file 3")?;
                    repositories::add(&second_cloned_repo, &new_file_path).await?;
                    repositories::commit(&second_cloned_repo, "Adding third file path.")?;

                    // Push should FAIL
                    let result = repositories::push(&second_cloned_repo).await;
                    assert!(result.is_err());

                    Ok(())
                })
                .await?;

                Ok(())
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
        test::run_readme_remote_repo_test(|_, remote_repo_1| async move {
            let remote_repo_1_copy = remote_repo_1.clone();

            // Push the second repo with data
            test::run_readme_remote_repo_test(|_, remote_repo_2| async move {
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
                        repositories::add(&first_cloned_repo, &new_file_path).await?;
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
                        repositories::add(&second_cloned_repo, &new_file_path).await?;
                        repositories::commit(&second_cloned_repo, "Adding second file path.")?;

                        let new_file = "new_file_3.txt";
                        let new_file_path = second_cloned_repo.path.join(new_file);
                        let new_file_path =
                            test::write_txt_file_to_path(new_file_path, "new file 3")?;
                        repositories::add(&second_cloned_repo, &new_file_path).await?;
                        repositories::commit(&second_cloned_repo, "Adding third file path.")?;

                        // Push should FAIL
                        let result = repositories::push(&second_cloned_repo).await;
                        assert!(result.is_err());

                        Ok(())
                    })
                    .await?;

                    Ok(())
                })
                .await?;
                Ok(remote_repo_2_copy)
            })
            .await?;

            Ok(remote_repo_1_copy)
        })
        .await
    }

    // Test that force push succeeds when the remote is ahead (non-fast-forward)
    // * Clone repo to user A and user B
    // * User A makes a commit and pushes
    // * User B makes a different commit — normal push fails
    // * User B force pushes — succeeds and remote matches user B's history
    #[tokio::test]
    async fn test_force_push_when_remote_is_ahead() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone to user A
            test::run_empty_dir_test_async(|user_a_dir| async move {
                let user_a_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &user_a_dir.join("user_a_repo"),
                )
                .await?;

                // Clone to user B
                test::run_empty_dir_test_async(|user_b_dir| async move {
                    let user_b_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &user_b_dir.join("user_b_repo"),
                    )
                    .await?;

                    // User A modifies README.md and pushes
                    let a_file = user_a_repo.path.join("README.md");
                    test::write_txt_file_to_path(a_file, "User A's changes")?;
                    repositories::add(&user_a_repo, &user_a_repo.path).await?;
                    repositories::commit(&user_a_repo, "User A commit")?;
                    repositories::push(&user_a_repo).await?;

                    // User B modifies README.md and tries to push — should fail
                    let b_file = user_b_repo.path.join("README.md");
                    test::write_txt_file_to_path(b_file, "User B's changes")?;
                    repositories::add(&user_b_repo, &user_b_repo.path).await?;
                    let user_b_commit = repositories::commit(&user_b_repo, "User B commit")?;
                    let normal_push = repositories::push(&user_b_repo).await;
                    assert!(normal_push.is_err());

                    // User B force pushes — should succeed
                    let opts = PushOpts {
                        remote: DEFAULT_REMOTE_NAME.to_string(),
                        branch: DEFAULT_BRANCH_NAME.to_string(),
                        force: true,
                        ..Default::default()
                    };
                    let force_push =
                        repositories::push::push_remote_branch(&user_b_repo, &opts).await;
                    assert!(force_push.is_ok());

                    // Verify remote branch now points to user B's commit
                    let remote_branch =
                        api::client::branches::get_by_name(&remote_repo, DEFAULT_BRANCH_NAME)
                            .await?
                            .unwrap();
                    assert_eq!(remote_branch.commit_id, user_b_commit.id);

                    Ok(())
                })
                .await?;

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
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
        test::run_readme_remote_repo_test(|_, remote_repo| async move {
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
                    repositories::add(&user_a_repo, &a_mod_file_path).await?;
                    let commit_a =
                        repositories::commit(&user_a_repo, "User A modifying the README.")?;
                    log::debug!("commit_a: {commit_a}");
                    repositories::push(&user_a_repo).await?;

                    // User B tries to modify the same README.md and push
                    let b_mod_file_path = user_b_repo.path.join(mod_file);
                    let b_mod_file_path =
                        test::write_txt_file_to_path(b_mod_file_path, "I be the README now.")?;
                    repositories::add(&user_b_repo, &b_mod_file_path).await?;
                    let commit_b =
                        repositories::commit(&user_b_repo, "User B modifying the README.")?;
                    log::debug!("commit_b: {commit_b}");

                    // Push should fail! Remote is ahead
                    let first_push_result = repositories::push(&user_b_repo).await;
                    log::debug!("first_push_result: {first_push_result:?}");
                    assert!(first_push_result.is_err());

                    // Pull should error because there are conflicts
                    let result = repositories::pull(&user_b_repo).await;
                    assert!(result.is_err());

                    // There should be conflicts
                    let status = repositories::status(&user_b_repo).await?;
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
                    repositories::add(&user_b_repo, &b_mod_file_path).await?;
                    println!("passed add");
                    repositories::commit(&user_b_repo, "User B resolving conflicts.")?;
                    println!("passed commit");

                    // Push should now succeed
                    let third_push_result = repositories::push(&user_b_repo).await;
                    assert!(third_push_result.is_ok());

                    Ok(())
                })
                .await?;

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_tree_cannot_push_tree_conflict_deleted_file() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_readme_remote_repo_test(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo = repositories::clone_url(
                    &remote_repo.remote.url,
                    &user_a_repo_dir.join("new_repo"),
                )
                .await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo = repositories::clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir.join("new_repo"),
                    )
                    .await?;

                    // Both users target README.md (which both clones got from
                    // run_readme_remote_repo_test). User A modifies it; User B
                    // deletes it — causing a modify/delete conflict.
                    let modify_path_a = user_a_repo.path.join("README.md");
                    let modify_path_b = user_b_repo.path.join("README.md");

                    // User A modifies
                    test::write_txt_file_to_path(&modify_path_a, "fancy new file contents")?;
                    repositories::add(&user_a_repo, &modify_path_a).await?;
                    repositories::commit(&user_a_repo, "modifying first file path.")?;
                    repositories::push(&user_a_repo).await?;

                    // User B deletes at user a path A modified, causing conflicts.
                    util::fs::remove_file(&modify_path_b)?;
                    repositories::add(&user_b_repo, &modify_path_b).await?;
                    repositories::commit(&user_b_repo, "user B deleting file path.")?;

                    // Push should fail
                    let res = repositories::push(&user_b_repo).await;
                    assert!(res.is_err());

                    Ok(())
                })
                .await?;

                Ok(())
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

            repositories::add(&local_repo, new_path).await?;
            let rm_opts = RmOpts {
                path: PathBuf::from("train"),
                staged: false,
                recursive: true,
            };

            repositories::rm(&local_repo, &rm_opts).await?;
            let commit =
                repositories::commit(&local_repo, "Moved all the train image files to images/")?;
            repositories::push(&local_repo).await?;

            let path = PathBuf::from("");
            let page = 1;
            let page_size = 100;
            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &path, page, page_size).await?;
            // check to make sure we only have the images directory and not the train directory
            assert!(
                !dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "train")
            );
            assert!(
                dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "images")
            );

            // Add a single new file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, new_file).await?;
            let commit = repositories::commit(&local_repo, "Added a new file")?;
            repositories::push(&local_repo).await?;

            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &path, page, page_size).await?;
            // make sure we have the new file
            assert!(
                dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "new_file.txt")
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_only_one_modified_file() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            // Move the README to a new file name
            let readme_path = local_repo.path.join("README.md");
            let new_path = local_repo.path.join("README2.md");
            util::fs::rename(&readme_path, &new_path)?;

            repositories::add(&local_repo, new_path).await?;
            let rm_opts = RmOpts::from_path("README.md");
            repositories::rm(&local_repo, &rm_opts).await?;
            let commit = repositories::commit(&local_repo, "Moved the readme")?;
            repositories::push(&local_repo).await?;

            let dir_entries =
                api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                    .await?;
            // make sure we have the new file
            assert!(
                dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "README2.md")
            );
            // make sure we don't have the old file
            assert!(
                !dir_entries
                    .entries
                    .iter()
                    .any(|entry| entry.filename() == "README.md")
            );

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
                repositories::add(&local_repo, &readme_file).await?;
                let commit = repositories::commit(&local_repo, "Added another file")?;

                let result = repositories::push(&local_repo).await;
                println!("push result: {result:?}");

                assert!(result.is_ok());

                // List the files in the remote repo and confirm the new file is there
                let dir_entries =
                    api::client::dir::list(&remote_repo, &commit.id, &PathBuf::from(""), 1, 100)
                        .await?;

                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "ANOTHER_FILE.md")
                );

                Ok(())
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
                repositories::add(&local_repo, &readme_file).await?;
                let commit = repositories::commit(&local_repo, "adding README.md to the test dir")?;

                let result = repositories::push(&local_repo).await;
                println!("push result: {result:?}");

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
                println!("dir_entries: {dir_entries:?}");

                // Make sure we have the new file
                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README.md")
                );

                Ok(())
            })
            .await?;
            Ok(cloned_remote)
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
                repositories::add(&user_a_repo, &new_file_path_1).await?;

                let new_file_2 = PathBuf::from("nlp")
                    .join("classification")
                    .join("new_partial_data_2.tsv");
                let new_file_path_2 = user_a_repo.path.join(&new_file_2);
                let new_file_path_2 =
                    test::write_txt_file_to_path(new_file_path_2, "image\tlabel2")?;
                repositories::add(&user_a_repo, &new_file_path_2).await?;

                // Modify an existing file
                let existing_file_path = user_a_repo
                    .path
                    .join("nlp/classification/existing_file.tsv");
                let modified_file_path =
                    test::write_txt_file_to_path(existing_file_path, "image\tmodified_label")?;
                repositories::add(&user_a_repo, &modified_file_path).await?;

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

                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "new_partial_data_1.tsv")
                );
                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "new_partial_data_2.tsv")
                );
                assert!(
                    dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "existing_file.tsv")
                );

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

                assert!(
                    root_dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README.md")
                );

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

                assert!(
                    root_dir_entries
                        .entries
                        .iter()
                        .any(|entry| entry.filename() == "README.md")
                );

                Ok(())
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

            // Create a file with exactly stream_segment_size() bytes so it sits at the
            // boundary between the batched-zip and parallel-segment transfer paths.
            let segment_size = stream_segment_size();
            let file_path = local_repo.path.join("exact_chunk_size_file.bin");
            let file_data: Vec<u8> = vec![42; segment_size as usize];

            // Write the data to the file
            util::fs::write_data(&file_path, &file_data)?;

            // Verify the file size equals the segment size exactly.
            let metadata = util::fs::metadata(&file_path)?;
            assert_eq!(
                metadata.len(),
                segment_size,
                "File size should be exactly stream_segment_size()"
            );

            // Add and commit the file
            repositories::add(&local_repo, &file_path).await?;
            let commit_msg = "Add file with exactly stream_segment_size() bytes";
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

                // Verify the file size matches the original.
                let downloaded_metadata = util::fs::metadata(&download_path)?;
                assert_eq!(
                    downloaded_metadata.len(),
                    metadata.len(),
                    "Downloaded file size should match the original file size"
                );

                // Verify the file contents match the original data
                let downloaded_data = util::fs::read_bytes_from_path(&download_path)?;
                assert_eq!(
                    downloaded_data, file_data,
                    "Downloaded file contents should match the original data"
                );

                Ok(())
            })
            .await?;

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_conflict_push_failure() -> Result<(), OxenError> {
        // Test that after a merge conflict, pushing again should fail
        // 1) Create repo, push
        // 2) Clone to different location
        // 3) Modify dir1/file1.txt in clone, add, commit, push
        // 4) Modify dir1/file1.txt in original, add, commit, pull (expect merge conflict)
        // 5) After merge conflict, pushing should fail

        test::run_readme_remote_repo_test(|original_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Create dir1/file1.txt in the original repo
            let dir1_path = original_repo.path.join("dir1");
            util::fs::create_dir_all(&dir1_path)?;
            let file1_path = dir1_path.join("file1.txt");
            util::fs::write(&file1_path, "Original content")?;
            repositories::add(&original_repo, &file1_path).await?;
            repositories::commit(&original_repo, "Add dir1/file1.txt")?;
            repositories::push(&original_repo).await?;

            // Clone to a different location
            test::run_empty_dir_test_async(|clone_dir| async move {
                let clone_repo_path = clone_dir.join("cloned_repo");
                let clone_repo =
                    repositories::clone_url(&remote_repo.remote.url, &clone_repo_path).await?;

                // Modify dir1/file1.txt in the clone
                let clone_file1_path = clone_repo.path.join("dir1").join("file1.txt");
                util::fs::write(&clone_file1_path, "Clone modified content")?;
                repositories::add(&clone_repo, &clone_file1_path).await?;
                repositories::commit(&clone_repo, "Modify file1.txt in clone")?;
                repositories::push(&clone_repo).await?;

                // Modify dir1/file1.txt in the original repo (different content)
                let original_file1_path = original_repo.path.join("dir1").join("file1.txt");
                util::fs::write(&original_file1_path, "Original repo modified content")?;
                repositories::add(&original_repo, &original_file1_path).await?;
                repositories::commit(&original_repo, "Modify file1.txt in original")?;

                // Capture the head commit before pull to verify it doesn't change
                let head_before_pull = repositories::commits::head_commit(&original_repo)?;

                // Pull should create a merge conflict
                let pull_result = repositories::pull(&original_repo).await;
                assert!(
                    pull_result.is_err(),
                    "Pull should fail due to merge conflict"
                );

                // Verify the head commit is unchanged after failed pull
                let head_after_pull = repositories::commits::head_commit(&original_repo)?;
                assert_eq!(
                    head_before_pull.id, head_after_pull.id,
                    "Head commit should not change when pull fails due to merge conflict"
                );

                // Verify we have merge conflicts
                let status = repositories::status(&original_repo).await?;
                assert!(status.has_merge_conflicts(), "Should have merge conflicts");

                // Verify we can't push
                let push_result = repositories::push(&original_repo).await;
                assert!(
                    push_result.is_err(),
                    "Push should fail due to merge conflict"
                );

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_create_nodes_before_starting_push() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            // Add a single new file
            let new_file = local_repo.path.join("new_file.txt");
            util::fs::write(&new_file, "I am a new file")?;
            repositories::add(&local_repo, &new_file).await?;
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
                candidate_nodes
                    .clone()
                    .into_iter()
                    .map(|node| node.hash)
                    .collect(),
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

    #[tokio::test]
    async fn test_push_large_file_and_clone_verify() -> Result<(), OxenError> {
        // Push a file just over the streamed-transfer threshold so the chunked path is
        // exercised, then clone it back and verify size and contents match.
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let file_size = (stream_segment_size() + 1024 * 1024) as usize;
            let file_path = local_repo.path.join("large_file.bin");
            let file_data: Vec<u8> = vec![42; file_size];
            util::fs::write_data(&file_path, &file_data)?;

            repositories::add(&local_repo, &file_path).await?;
            let commit = repositories::commit(&local_repo, "Add large file")?;

            let remote_repo = test::create_remote_repo(&local_repo).await?;
            let mut local_repo_mut = local_repo.clone();
            command::config::set_remote(
                &mut local_repo_mut,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;
            repositories::push(&local_repo_mut).await?;

            let remote_commit_opt =
                api::client::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit_opt.is_some(), "Remote commit should exist");

            let remote_repo_clone = remote_repo.clone();
            let file_data_clone = file_data.clone();

            test::run_empty_dir_test_async(|clone_dir| async move {
                let clone_repo_path = clone_dir.join("cloned_repo");
                let clone_repo =
                    repositories::clone_url(&remote_repo_clone.remote.url, &clone_repo_path)
                        .await?;

                let cloned_file_path = clone_repo.path.join("large_file.bin");
                assert!(
                    cloned_file_path.exists(),
                    "Cloned file should exist at {cloned_file_path:?}"
                );

                let cloned_metadata = util::fs::metadata(&cloned_file_path)?;
                assert_eq!(
                    cloned_metadata.len(),
                    file_size as u64,
                    "Cloned file size should match original"
                );

                let cloned_data = util::fs::read_bytes_from_path(&cloned_file_path)?;
                assert_eq!(
                    cloned_data, file_data_clone,
                    "Cloned file contents should match the original data"
                );

                Ok(())
            })
            .await?;

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_push_large_file_in_subdir_and_clone_verify() -> Result<(), OxenError> {
        // Test pushing a >10MB file inside a subdirectory, then cloning.
        // This exercises the chunk download path where entry.path must include
        // the directory prefix for the server to find the file.
        test::run_empty_local_repo_test_async(|local_repo| async move {
            // Create a subdirectory and write a >10MB file into it
            let sub_dir = local_repo.path.join("data").join("models");
            util::fs::create_dir_all(&sub_dir)?;

            let file_size = (stream_segment_size() + 1024 * 1024) as usize;
            let file_path = sub_dir.join("weights.bin");
            let file_data: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();
            util::fs::write_data(&file_path, &file_data)?;

            // Add and commit
            repositories::add(&local_repo, &local_repo.path).await?;
            let commit = repositories::commit(&local_repo, "Add large file in subdir")?;

            // Set up remote and push
            let remote_repo = test::create_remote_repo(&local_repo).await?;
            let mut local_repo_mut = local_repo.clone();
            command::config::set_remote(
                &mut local_repo_mut,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;
            repositories::push(&local_repo_mut).await?;

            // Verify push succeeded
            let remote_commit = api::client::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit.is_some(), "Remote commit should exist");

            let remote_repo_clone = remote_repo.clone();
            let file_data_clone = file_data.clone();

            // Clone to a different directory and verify the file
            test::run_empty_dir_test_async(|clone_dir| async move {
                let clone_repo_path = clone_dir.join("cloned_repo");
                let clone_repo =
                    repositories::clone_url(&remote_repo_clone.remote.url, &clone_repo_path)
                        .await?;

                let cloned_file = clone_repo
                    .path
                    .join("data")
                    .join("models")
                    .join("weights.bin");
                assert!(
                    cloned_file.exists(),
                    "Cloned file should exist at {cloned_file:?}"
                );

                let cloned_metadata = util::fs::metadata(&cloned_file)?;
                assert_eq!(
                    cloned_metadata.len(),
                    file_size as u64,
                    "Cloned file size should match original"
                );

                let cloned_data = util::fs::read_bytes_from_path(&cloned_file)?;
                assert_eq!(
                    cloned_data, file_data_clone,
                    "Cloned file contents should match the original data"
                );

                Ok(())
            })
            .await?;

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    // `oxen push --revalidate` must re-push a blob the remote is simply MISSING, not only one that
    // is corrupt-but-present. `versions::clean` reports zero corrupted for an absent blob, so the
    // repair used to no-op on the common case.
    #[tokio::test]
    async fn test_revalidate_repushes_a_missing_blob() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async {
            let mut repo = repo;

            let path = repo.path.join("a.txt");
            test::write_txt_file_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "add a.txt")?;

            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, DEFAULT_REMOTE_NAME, &remote)?;
            let remote_repo = test::create_remote_repo(&repo).await?;
            repositories::push(&repo).await?;

            // The server loses a.txt's blob — absent, not corrupt.
            let hash = repositories::tree::get_node_by_path(&repo, &commit, "a.txt")?
                .expect("a.txt in commit")
                .file()?
                .hash()
                .to_string();
            let sync_dir =
                PathBuf::from(std::env::var("SYNC_DIR").expect("SYNC_DIR set by bin/test-rust"));
            let server = repositories::get_by_namespace_and_name(
                &sync_dir,
                constants::DEFAULT_NAMESPACE,
                repo.dirname(),
                None,
            )?
            .expect("server repo exists");
            server.version_store().delete_version(&hash).await?;
            assert!(
                !server.version_store().version_exists(&hash).await?,
                "precondition: the blob should be gone from the server"
            );

            // clean finds nothing corrupt (the blob is absent), but revalidate must still re-push it.
            let opts = PushOpts {
                remote: DEFAULT_REMOTE_NAME.to_string(),
                branch: DEFAULT_BRANCH_NAME.to_string(),
                revalidate: true,
                ..Default::default()
            };
            repositories::push::push_remote_branch(&repo, &opts).await?;

            assert!(
                server.version_store().version_exists(&hash).await?,
                "revalidate must re-push the missing blob"
            );

            api::client::repositories::delete(&remote_repo).await?;
            Ok(())
        })
        .await
    }
}
