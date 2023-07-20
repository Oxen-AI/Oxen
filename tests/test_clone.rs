use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::test;
use liboxen::util;

// Test for clone --all that checks to make sure we have all commits, all deleted files, etc
#[tokio::test]
async fn test_clone_all() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
        let cloned_remote = remote_repo.clone();
        let og_commits = api::local::commits::list_all(&local_repo)?;

        // Clone with the --all flag
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo =
                command::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

            // Make sure we have all the commit objects
            let cloned_commits = api::local::commits::list_all(&cloned_repo)?;
            assert_eq!(og_commits.len(), cloned_commits.len());

            // Make sure we set the HEAD file
            let head_commit = api::local::commits::head_commit(&cloned_repo);
            assert!(head_commit.is_ok());

            // We remove the test/ directory in one of the commits, so make sure we can go
            // back in the history to that commit
            let test_dir_path = cloned_repo.path.join("test");
            let commit = api::local::commits::first_by_message(&cloned_repo, "Adding test/")?;
            assert!(commit.is_some());
            assert!(!test_dir_path.exists());

            // checkout the commit
            command::checkout(&cloned_repo, &commit.unwrap().id).await?;
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
async fn test_clone_all_push_all() -> Result<(), OxenError> {
    test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
        let cloned_remote = remote_repo.clone();

        // Clone with the --all flag
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let mut cloned_repo =
                command::deep_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

            let repo_name = format!("new_remote_repo_name_{}", uuid::Uuid::new_v4());
            let remote_url = test::repo_remote_url_from(&repo_name);
            let remote_name = "different";

            // Create a different repo
            api::remote::repositories::create(
                &cloned_repo,
                constants::DEFAULT_NAMESPACE,
                &repo_name,
                test::test_host(),
            )
            .await?;

            command::config::set_remote(&mut cloned_repo, remote_name, &remote_url)?;

            // Should be able to push all data successfully
            command::push_remote_branch(&cloned_repo, remote_name, "main").await?;

            // TODO: figure out how to repro why the Pets Dataset we could not clone --all and push to staging

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
        command::add(&repo, &file_path)?;
        command::commit(&repo, "Adding labels file")?;

        let train_path = repo.path.join("train");
        command::add(&repo, &train_path)?;
        command::commit(&repo, "Adding train dir")?;

        let test_path = repo.path.join("test");
        command::add(&repo, &test_path)?;
        command::commit(&repo, "Adding test dir")?;

        // Set the proper remote
        let remote = test::repo_remote_url_from(&repo.dirname());
        command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

        // Create Remote
        let remote_repo = test::create_remote_repo(&repo).await?;

        // Push it
        command::push(&repo).await?;

        // run another test with a new repo dir that we are going to sync to
        test::run_empty_dir_test_async(|new_repo_dir| async move {
            let cloned_repo = command::clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
            let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
            // 2 test, 5 train, 1 labels
            assert_eq!(8, cloned_num_files);

            api::remote::repositories::delete(&remote_repo).await?;

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
            let cloned_repo = command::clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

            let status = command::status(&cloned_repo);
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
            let cloned_repo = command::clone_url(&remote_repo.remote.url, &new_repo_dir).await?;

            let status = command::status(&cloned_repo);
            assert!(status.is_ok());

            // Add a file to the cloned repo
            let new_file = "new_file.txt";
            let new_file_path = cloned_repo.path.join(new_file);
            let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
            command::add(&cloned_repo, &new_file_path)?;
            command::commit(&cloned_repo, "Adding new file path.")?;

            command::push(&cloned_repo).await?;

            Ok(new_repo_dir)
        })
        .await?;

        Ok(ret_repo)
    })
    .await
}