//! # oxen pull
//!
//! Pull data from a remote branch
//!

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;

/// Pull a repository's data from default branches origin/main
/// Defaults defined in
/// `constants::DEFAULT_REMOTE_NAME` and `constants::DEFAULT_BRANCH_NAME`
pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::pull::pull(repo).await,
        MinOxenVersion::V0_19_0 => core::v0_19_0::pull::pull(repo).await,
    }
}

pub async fn pull_remote_branch_shallow(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch: impl AsRef<str>,
) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::pull::pull_shallow(repo).await,
        MinOxenVersion::V0_19_0 => core::v0_19_0::pull::pull_shallow(repo, remote, branch).await,
    }
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::pull::pull_all(repo).await,
        MinOxenVersion::V0_19_0 => core::v0_19_0::pull::pull_all(repo).await,
    }
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch: impl AsRef<str>,
    all: bool,
) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::pull::pull_remote_branch(repo, remote.as_ref(), branch.as_ref(), all)
                .await
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::pull::pull_remote_branch(repo, remote, branch, all).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::constants::OXEN_HIDDEN_DIR;
    use crate::core;
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::opts::CloneOpts;
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_command_push_clone_pull_push() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // Track the file
            let train_dirname = "train";
            let train_dir = repo.path.join(train_dirname);
            let og_num_files = util::fs::rcount_files_in_dir(&train_dir);
            repositories::add(&repo, &train_dir)?;
            // Commit the train dir
            repositories::commit(&repo, "Adding training data")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create the remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Add a new file
            let party_ppl_filename = "party_ppl.txt";
            let party_ppl_contents = String::from("Wassup Party Ppl");
            let party_ppl_file_path = repo.path.join(party_ppl_filename);
            util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents)?;

            // Add and commit and push
            repositories::add(&repo, &party_ppl_file_path)?;
            let latest_commit = repositories::commit(&repo, "Adding party_ppl.txt")?;
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("new_repo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                let oxen_dir = cloned_repo.path.join(OXEN_HIDDEN_DIR);
                assert!(oxen_dir.exists());
                repositories::pull(&cloned_repo).await?;

                // Make sure we pulled all of the train dir
                let cloned_train_dir = cloned_repo.path.join(train_dirname);
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_train_dir);
                assert_eq!(og_num_files, cloned_num_files);

                // Make sure we have the party ppl file from the next commit
                let cloned_party_ppl_path = cloned_repo.path.join(party_ppl_filename);
                assert!(cloned_party_ppl_path.exists());
                let cloned_contents = util::fs::read_from_path(&cloned_party_ppl_path)?;
                assert_eq!(cloned_contents, party_ppl_contents);

                // Make sure that pull updates local HEAD to be correct
                let head = repositories::commits::head_commit(&cloned_repo)?;
                assert_eq!(head.id, latest_commit.id);

                // Make sure we synced all the commits
                let repo_commits = repositories::commits::list(&repo)?;
                let cloned_commits = repositories::commits::list(&cloned_repo)?;
                assert_eq!(repo_commits.len(), cloned_commits.len());

                // Make sure we updated the dbs properly
                let status = repositories::status(&cloned_repo)?;
                assert!(status.is_clean());

                // Have this side add a file, and send it back over
                let send_it_back_filename = "send_it_back.txt";
                let send_it_back_contents = String::from("Hello from the other side");
                let send_it_back_file_path = cloned_repo.path.join(send_it_back_filename);
                util::fs::write_to_path(&send_it_back_file_path, &send_it_back_contents)?;

                // Add and commit and push
                repositories::add(&cloned_repo, &send_it_back_file_path)?;
                repositories::commit(&cloned_repo, "Adding send_it_back.txt")?;
                repositories::push(&cloned_repo).await?;

                // Pull back from the OG Repo
                repositories::pull(&repo).await?;
                let old_repo_status = repositories::status(&repo)?;
                old_repo_status.print();
                // Make sure we don't modify the timestamps or anything of the OG data
                assert!(!old_repo_status.has_modified_entries());

                let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
                assert!(pulled_send_it_back_path.exists());
                let pulled_contents = util::fs::read_from_path(&pulled_send_it_back_path)?;
                assert_eq!(pulled_contents, send_it_back_contents);

                // Modify the party ppl contents
                let party_ppl_contents = String::from("Late to the party");
                util::fs::write_to_path(&party_ppl_file_path, &party_ppl_contents)?;
                repositories::add(&repo, &party_ppl_file_path)?;
                repositories::commit(&repo, "Modified party ppl contents")?;
                repositories::push(&repo).await?;

                // Pull the modifications
                repositories::pull(&cloned_repo).await?;
                let pulled_contents = util::fs::read_from_path(&cloned_party_ppl_path)?;
                assert_eq!(pulled_contents, party_ppl_contents);

                println!("----BEFORE-----");
                // Remove a file, add, commit, push the change
                util::fs::remove_file(&send_it_back_file_path)?;
                repositories::add(&cloned_repo, &send_it_back_file_path)?;
                repositories::commit(&cloned_repo, "Removing the send it back file")?;
                repositories::push(&cloned_repo).await?;
                println!("----AFTER-----");

                // Pull down the changes and make sure the file is removed
                repositories::pull(&repo).await?;
                let pulled_send_it_back_path = repo.path.join(send_it_back_filename);
                assert!(!pulled_send_it_back_path.exists());

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    // This specific flow broke during a demo
    //   Because there ends up being no files in the root dir
    //   after we remove the labels.txt file, push, and pull

    // * add file *
    // push
    // pull
    // * modify file *
    // push
    // pull
    // * remove file *
    // push
    // pull
    #[tokio::test]
    async fn test_command_add_modify_remove_push_pull() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // Track a file
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            test::write_txt_file_to_path(&filepath, "I am the labels")?;
            repositories::add(&repo, &filepath)?;
            repositories::commit(&repo, "Adding labels file")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("new_repo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull(&cloned_repo).await?;

                // Modify the file in the cloned dir
                let cloned_filepath = cloned_repo.path.join(filename);
                let changed_content = "messing up the labels";
                util::fs::write_to_path(&cloned_filepath, changed_content)?;
                repositories::add(&cloned_repo, &cloned_filepath)?;
                repositories::commit(&cloned_repo, "I messed with the label file")?;

                // Push back to server
                repositories::push(&cloned_repo).await?;

                // Pull back to original guy
                repositories::pull(&repo).await?;

                // Make sure content changed
                let pulled_content = util::fs::read_from_path(&filepath)?;
                assert_eq!(pulled_content, changed_content);

                // Delete the file in the og filepath
                util::fs::remove_file(&filepath)?;

                // Stage & Commit & Push the removal
                repositories::add(&repo, &filepath)?;
                repositories::commit(&repo, "You mess with it, I remove it")?;
                repositories::push(&repo).await?;

                // This pull should still work even with no files in the root dir
                repositories::pull(&cloned_repo).await?;
                assert!(!cloned_filepath.exists());

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    // Make sure we can push again after pulling on the other side, then pull again
    #[tokio::test]
    async fn test_push_pull_push_pull_on_branch() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // Track a dir
            let train_path = repo.path.join("train");
            repositories::add(&repo, &train_path)?;
            repositories::commit(&repo, "Adding train dir")?;

            // Track larger files
            let larger_dir = repo.path.join("large_files");
            repositories::add(&repo, &larger_dir)?;
            repositories::commit(&repo, "Adding larger files")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("new_repo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull_all(&cloned_repo).await?;
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                assert_eq!(6, cloned_num_files);
                let og_commits = repositories::commits::list(&repo)?;
                let cloned_commits = repositories::commits::list(&cloned_repo)?;
                assert_eq!(og_commits.len(), cloned_commits.len());

                // Create a branch to collab on
                let branch_name = "adding-training-data";
                repositories::branches::create_checkout(&cloned_repo, branch_name)?;

                // Track some more data in the cloned repo
                let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
                let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
                util::fs::copy(hotdog_path, &new_file_path)?;
                repositories::add(&cloned_repo, &new_file_path)?;
                repositories::commit(&cloned_repo, "Adding one file to train dir")?;

                // Push it back
                repositories::push::push_remote_branch(
                    &cloned_repo,
                    constants::DEFAULT_REMOTE_NAME,
                    branch_name,
                )
                .await?;

                // Pull it on the OG side
                repositories::pull_remote_branch(
                    &repo,
                    constants::DEFAULT_REMOTE_NAME,
                    branch_name,
                    true,
                )
                .await?;
                let num_new_files = util::fs::rcount_files_in_dir(&repo.path);
                // Now there should be a new hotdog file
                assert_eq!(og_num_files + 1, num_new_files);

                // Add another file on the OG side, and push it back
                let hotdog_path = Path::new("data/test/images/hotdog_2.jpg");
                let new_file_path = train_path.join("hotdog_2.jpg");
                util::fs::copy(hotdog_path, &new_file_path)?;
                repositories::add(&repo, &train_path)?;
                repositories::commit(&repo, "Adding next file to train dir")?;
                repositories::push::push_remote_branch(
                    &repo,
                    constants::DEFAULT_REMOTE_NAME,
                    branch_name,
                )
                .await?;

                // Pull it on the second side again
                repositories::pull_remote_branch(
                    &cloned_repo,
                    constants::DEFAULT_REMOTE_NAME,
                    branch_name,
                    false,
                )
                .await?;
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                // Now there should be 7 train/ files and 1 in large_files/
                assert_eq!(8, cloned_num_files);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    // Make sure we can push again after pulling on the other side, then pull again
    #[tokio::test]
    async fn test_push_pull_push_pull_on_other_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // Track a dir
            let train_dir = repo.path.join("train");
            let train_paths = [
                Path::new("data/test/images/cat_1.jpg"),
                Path::new("data/test/images/cat_2.jpg"),
                Path::new("data/test/images/cat_3.jpg"),
                Path::new("data/test/images/dog_1.jpg"),
                Path::new("data/test/images/dog_2.jpg"),
            ];
            std::fs::create_dir_all(&train_dir)?;
            for path in train_paths.iter() {
                util::fs::copy(path, train_dir.join(path.file_name().unwrap()))?;
            }

            repositories::add(&repo, &train_dir)?;
            repositories::commit(&repo, "Adding train dir")?;

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("new_repo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull_all(&cloned_repo).await?;
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                // the original training files
                assert_eq!(train_paths.len(), cloned_num_files);

                // Create a branch to collaborate on
                let branch_name = "adding-training-data";
                repositories::branches::create_checkout(&cloned_repo, branch_name)?;

                // Track some more data in the cloned repo
                let hotdog_path = Path::new("data/test/images/hotdog_1.jpg");
                let new_file_path = cloned_repo.path.join("train").join("hotdog_1.jpg");
                util::fs::copy(hotdog_path, &new_file_path)?;
                repositories::add(&cloned_repo, &new_file_path)?;
                repositories::commit(&cloned_repo, "Adding one file to train dir")?;

                // Push it back
                repositories::push::push_remote_branch(
                    &cloned_repo,
                    constants::DEFAULT_REMOTE_NAME,
                    branch_name,
                )
                .await?;

                // Pull it on the OG side
                repositories::pull_remote_branch(
                    &repo,
                    constants::DEFAULT_REMOTE_NAME,
                    &og_branch.name,
                    true,
                )
                .await?;
                let og_num_files = util::fs::rcount_files_in_dir(&repo.path);
                // Now there should be still be the original train files, not the new file
                assert_eq!(train_paths.len(), og_num_files);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_push_pull_file_without_extension() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            let filename = "LICENSE";
            let filepath = repo.path.join(filename);

            let og_content = "I am the License.";
            test::write_txt_file_to_path(&filepath, og_content)?;

            repositories::add(&repo, filepath)?;
            let commit = repositories::commit(&repo, "Adding file without extension");

            assert!(commit.is_ok());

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("new_repo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull(&cloned_repo).await?;
                let filepath = cloned_repo.path.join(filename);
                let content = util::fs::read_from_path(&filepath)?;
                assert_eq!(og_content, content);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    /*
    Test this workflow:

    User 1: adds data and creates a branch with more data
        oxen init
        oxen add data/1.txt
        oxen add data/2.txt
        oxen commit -m "Adding initial data"
        oxen push
        oxen checkout -b feature/add-mooooore-data
        oxen add data/3.txt
        oxen add data/4.txt
        oxen add data/5.txt
        oxen push

    User 2: clones just the branch with more data, then switches to main branch and pulls
        oxen clone remote.url -b feature/add-mooooore-data
        oxen fetch
        oxen checkout main
        # should only have the data on main

    */
    #[tokio::test]
    async fn test_push_pull_separate_branch_less_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // create 5 text files in the repo.path
            for i in 1..6 {
                let filename = format!("{}.txt", i);
                let filepath = repo.path.join(&filename);
                test::write_txt_file_to_path(&filepath, &filename)?;
            }

            // add file 1.txt and 2.txt
            let filepath = repo.path.join("1.txt");
            repositories::add(&repo, &filepath)?;
            let filepath = repo.path.join("2.txt");
            repositories::add(&repo, &filepath)?;

            // Commit the files
            repositories::commit(&repo, "Adding initial data")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // Create a branch to collab on
            let branch_name = "feature/add-mooooore-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add the rest of the files
            for i in 3..6 {
                let filename = format!("{}.txt", i);
                let filepath = repo.path.join(&filename);
                repositories::add(&repo, &filepath)?;
            }

            // Commit the files
            repositories::commit(&repo, "Adding mooooore data")?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                // Clone the branch
                let opts = CloneOpts {
                    url: remote_repo.url().to_string(),
                    dst: new_repo_dir.join("new_repo"),
                    branch: branch_name.to_owned(),
                    shallow: false,
                    all: false,
                };
                let cloned_repo = repositories::clone(&opts).await?;

                // Make sure we have all the files from the branch
                let cloned_files = util::fs::rlist_files_in_dir(&cloned_repo.path);
                for file in cloned_files.iter() {
                    println!("Cloned file: {}", file.display());
                }
                let cloned_num_files = cloned_files.len();
                assert_eq!(cloned_num_files, 5);

                // Switch to main branch and pull
                repositories::fetch(&cloned_repo, false).await?;
                repositories::checkout(&cloned_repo, "main").await?;

                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                assert_eq!(cloned_num_files, 2);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_push_pull_separate_branch_more_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // create 5 text files in the repo.path
            for i in 1..6 {
                let filename = format!("{}.txt", i);
                let filepath = repo.path.join(&filename);
                test::write_txt_file_to_path(&filepath, &filename)?;
            }

            // add file 1.txt and 2.txt
            let filepath = repo.path.join("1.txt");
            repositories::add(&repo, &filepath)?;
            let filepath = repo.path.join("2.txt");
            repositories::add(&repo, &filepath)?;

            // Commit the files
            repositories::commit(&repo, "Adding initial data")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // Create a branch to collab on
            let branch_name = "feature/add-mooooore-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add the rest of the files
            for i in 3..6 {
                let filename = format!("{}.txt", i);
                let filepath = repo.path.join(&filename);
                repositories::add(&repo, &filepath)?;
            }

            // Commit the files
            repositories::commit(&repo, "Adding mooooore data")?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                // Clone the branch
                let opts = CloneOpts {
                    url: remote_repo.url().to_string(),
                    dst: new_repo_dir.join("new_repo"),
                    branch: DEFAULT_BRANCH_NAME.to_string(),
                    shallow: false,
                    all: false,
                };
                let cloned_repo = repositories::clone(&opts).await?;

                // Make sure we have all the files from the branch
                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                assert_eq!(cloned_num_files, 2);

                // Switch to main branch and pull
                repositories::fetch(&cloned_repo, false).await?;

                repositories::checkout(&cloned_repo, branch_name).await?;

                let cloned_num_files = util::fs::rcount_files_in_dir(&cloned_repo.path);
                assert_eq!(cloned_num_files, 5);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_push_pull_moved_files() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let contents = "this is the file";
            let path = &local_repo.path.join("a.txt");
            test::write_txt_file_to_path(path, contents)?;
            println!("Writing file to {}", path.display());
            repositories::add(&local_repo, path)?;
            println!("adding file to index at path {}", path.display());
            println!("First commit");
            repositories::commit(&local_repo, "Adding file for first time")?;
            println!("Commit successfull");
            // Write the same file to newfolder/a.txt

            let new_path = &local_repo.path.join("newfolder").join("a.txt");

            util::fs::create_dir_all(local_repo.path.join("newfolder"))?;
            test::write_txt_file_to_path(new_path, contents)?;
            repositories::add(&local_repo, new_path)?;

            // Write the same file to newfolder/b.txt
            let new_path = &local_repo.path.join("newfolder").join("b.txt");

            test::write_txt_file_to_path(new_path, contents)?;
            repositories::add(&local_repo, new_path)?;

            // Delete the original file at a.txt
            let path = "a.txt";
            let new_path = local_repo.path.join(path);
            util::fs::remove_file(&new_path)?;
            repositories::add(&local_repo, &new_path)?;
            println!("Second commit");
            repositories::commit(
                &local_repo,
                "Moved file to 2 new places and deleted original",
            )?;
            repositories::push(&local_repo).await?;

            test::run_empty_dir_test_async(|repo_dir| async move {
                // Pull down this removal
                let repo_dir = repo_dir.join("repoo");
                let _cloned_repo =
                    repositories::deep_clone_url(&remote_repo.remote.url, &repo_dir).await?;
                Ok(repo_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_push_new_branch_default_clone() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|repo_dir| async move {
                // Clone the remote repo
                let repo_dir = repo_dir.join("repoo");
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &repo_dir).await?;

                // Create-checkout a new branch
                let branch_name = "new-branch";
                repositories::branches::create_checkout(&cloned_repo, branch_name)?;

                // Add a file
                let contents = "this is the file";
                let path = &cloned_repo.path.join("a.txt");
                test::write_txt_file_to_path(path, contents)?;

                repositories::add(&cloned_repo, path)?;
                let commit = repositories::commit(&cloned_repo, "Adding file for first time")?;

                // Try to push upstream branch
                let push_result = repositories::push::push_remote_branch(
                    &cloned_repo,
                    constants::DEFAULT_REMOTE_NAME,
                    branch_name,
                )
                .await;

                log::debug!("Push result: {:?}", push_result);

                assert!(push_result.is_ok());

                // Get the remote branch
                let remote_branch = api::client::branches::get_by_name(&remote_repo, branch_name)
                    .await?
                    .unwrap();

                assert_eq!(remote_branch.commit_id, commit.id);

                Ok(repo_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // Deal with merge conflicts on pull
    // 1) Clone repo to user A
    // 2) Clone repo to user B
    // 3) User A changes file commit and pushes
    // 4) User B changes same file, commites, and pushes and fails
    // 5) User B pulls user A's changes, there is a merge conflict
    // 6) User B cannot push until merge conflict is resolved
    #[tokio::test]
    async fn test_flags_merge_conflict_on_pull() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo =
                    repositories::clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy).await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");

                    let user_b_repo =
                        repositories::clone_url(&remote_repo.remote.url, &user_b_repo_dir_copy)
                            .await?;

                    // User A adds a file and pushes
                    let new_file = "new_file.txt";
                    let new_file_path = user_a_repo.path.join(new_file);
                    let new_file_path = test::write_txt_file_to_path(new_file_path, "new file")?;
                    repositories::add(&user_a_repo, &new_file_path)?;
                    repositories::commit(&user_a_repo, "User A changing file.")?;
                    repositories::push(&user_a_repo).await?;

                    // User B changes the same file and pushes
                    let new_file_path = user_b_repo.path.join(new_file);
                    let new_file_path =
                        test::write_txt_file_to_path(new_file_path, "I am user B, try to stop me")?;
                    repositories::add(&user_b_repo, &new_file_path)?;
                    repositories::commit(&user_b_repo, "User B changing file.")?;

                    // Push should fail
                    let result = repositories::push(&user_b_repo).await;
                    assert!(result.is_err());

                    // Pull
                    repositories::pull(&user_b_repo).await?;

                    // Check for merge conflict
                    let status = repositories::status(&user_b_repo)?;
                    assert!(!status.merge_conflicts.is_empty());
                    status.print();

                    // Checkout your version and add the changes
                    repositories::checkout::checkout_ours(&user_b_repo, new_file)?;
                    repositories::add(&user_b_repo, &new_file_path)?;
                    // Commit the changes
                    repositories::commit(&user_b_repo, "Taking my changes")?;

                    // Push should succeed
                    repositories::push(&user_b_repo).await?;

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
    async fn test_pull_does_not_remove_local_files() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_one_commit_sync_repo_test(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo =
                    repositories::clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy).await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");
                    let user_b_repo =
                        repositories::clone_url(&remote_repo.remote.url, &user_b_repo_dir_copy)
                            .await?;

                    // Add file_1 and file_2 to user A repo
                    let file_1 = "file_1.txt";
                    test::write_txt_file_to_path(user_a_repo.path.join(file_1), "File 1")?;
                    let file_2 = "file_2.txt";
                    test::write_txt_file_to_path(user_a_repo.path.join(file_2), "File 2")?;

                    repositories::add(&user_a_repo, user_a_repo.path.join(file_1))?;
                    repositories::add(&user_a_repo, user_a_repo.path.join(file_2))?;

                    repositories::commit(&user_a_repo, "Adding file_1 and file_2")?;

                    // Push
                    repositories::push(&user_a_repo).await?;

                    // Add file_3 to user B repo
                    let file_3 = "file_3.txt";
                    test::write_txt_file_to_path(user_b_repo.path.join(file_3), "File 3")?;

                    repositories::add(&user_b_repo, user_b_repo.path.join(file_3))?;
                    repositories::commit(&user_b_repo, "Adding file_3")?;

                    // Pull changes without pushing first - fine since no conflict
                    repositories::pull(&user_b_repo).await?;

                    // Get new  head commit of the pulled repo
                    repositories::commits::head_commit(&user_b_repo)?;

                    // Make sure we now have all three files
                    assert!(user_b_repo.path.join(file_1).exists());
                    assert!(user_b_repo.path.join(file_2).exists());
                    assert!(user_b_repo.path.join(file_3).exists());

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
    async fn test_pull_does_not_remove_untracked_files() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_one_commit_sync_repo_test(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo =
                    repositories::clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy).await?;

                // Clone Repo to User B
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("user_b_repo");
                    let user_b_repo =
                        repositories::clone_url(&remote_repo.remote.url, &user_b_repo_dir_copy)
                            .await?;

                    // Add file_1 and file_2 to user A repo
                    let file_1 = "file_1.txt";
                    test::write_txt_file_to_path(user_a_repo.path.join(file_1), "File 1")?;
                    let file_2 = "file_2.txt";
                    test::write_txt_file_to_path(user_a_repo.path.join(file_2), "File 2")?;

                    repositories::add(&user_a_repo, user_a_repo.path.join(file_1))?;
                    repositories::add(&user_a_repo, user_a_repo.path.join(file_2))?;

                    repositories::commit(&user_a_repo, "Adding file_1 and file_2")?;

                    // Push
                    repositories::push(&user_a_repo).await?;

                    let local_file_2 = "file_2.txt";
                    test::write_txt_file_to_path(
                        user_b_repo.path.join(local_file_2),
                        "wrong not correct content",
                    )?;

                    // Add file_3 to user B repo
                    let file_3 = "file_3.txt";
                    test::write_txt_file_to_path(user_b_repo.path.join(file_3), "File 3")?;

                    // Make a dir
                    let dir_1 = "dir_1";
                    std::fs::create_dir(user_b_repo.path.join(dir_1))?;

                    // Make another dir
                    let dir_2 = "dir_2";
                    std::fs::create_dir(user_b_repo.path.join(dir_2))?;

                    // Add files in dir_2
                    let file_4 = "file_4.txt";
                    test::write_txt_file_to_path(
                        user_b_repo.path.join(dir_2).join(file_4),
                        "File 4",
                    )?;
                    let file_5 = "file_5.txt";
                    test::write_txt_file_to_path(
                        user_b_repo.path.join(dir_2).join(file_5),
                        "File 5",
                    )?;

                    let dir_3 = "dir_3";
                    let subdir = "subdir";
                    std::fs::create_dir_all(user_b_repo.path.join(dir_3).join(subdir))?;

                    let subfile = "subfile.txt";
                    test::write_txt_file_to_path(
                        user_b_repo.path.join(dir_3).join(subdir).join(subfile),
                        "Subfile",
                    )?;

                    // Pull changes
                    repositories::pull(&user_b_repo).await?;

                    // Files from the other commit successfully pulled
                    assert!(user_b_repo.path.join(file_1).exists());
                    assert!(user_b_repo.path.join(file_2).exists());

                    // Bad local data successfully overwritten on pull (should we flag conflict here?)
                    let local_file_2_contents =
                        std::fs::read_to_string(user_b_repo.path.join(local_file_2))?;
                    assert_eq!(local_file_2_contents, "File 2");

                    // Untracked files not removed
                    assert!(user_b_repo.path.join(file_3).exists());
                    assert!(user_b_repo.path.join(dir_1).exists());
                    assert!(user_b_repo.path.join(dir_2).exists());
                    assert!(user_b_repo.path.join(dir_2).join(file_4).exists());
                    assert!(user_b_repo.path.join(dir_2).join(file_5).exists());
                    assert!(user_b_repo.path.join(dir_3).exists());
                    assert!(user_b_repo.path.join(dir_3).join(subdir).exists());
                    assert!(user_b_repo
                        .path
                        .join(dir_3)
                        .join(subdir)
                        .join(subfile)
                        .exists());

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
    async fn test_pull_multiple_commits() -> Result<(), OxenError> {
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
                let new_repo_dir = new_repo_dir.join("repoo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull(&cloned_repo).await?;
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
    async fn test_pull_data_frame() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |mut repo| async move {
            // Track a file
            let filename = "annotations/train/bounding_box.csv";
            let file_path = repo.path.join(filename);
            let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
            let og_contents = util::fs::read_from_path(&file_path)?;

            repositories::add(&repo, &file_path)?;
            repositories::commit(&repo, "Adding bounding box file")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull(&cloned_repo).await?;
                let file_path = cloned_repo.path.join(filename);

                let cloned_df = tabular::read_df(&file_path, DFOpts::empty())?;
                let cloned_contents = util::fs::read_from_path(&file_path)?;
                assert_eq!(og_df.height(), cloned_df.height());
                assert_eq!(og_df.width(), cloned_df.width());
                assert_eq!(cloned_contents, og_contents);

                // Status should be empty too
                let status = repositories::status(&cloned_repo)?;
                status.print();
                assert!(status.is_clean());

                // Make sure that the schema gets pulled
                let commit = repositories::commits::head_commit(&cloned_repo)?;
                let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
                assert!(!schemas.is_empty());

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    // Test that we pull down the proper data frames
    #[tokio::test]
    async fn test_pull_multiple_data_frames_multiple_schemas() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            let filename = Path::new("nlp")
                .join("classification")
                .join("annotations")
                .join("train.tsv");
            let file_path = repo.path.join(filename);
            let og_df = tabular::read_df(&file_path, DFOpts::empty())?;
            let og_sentiment_contents = util::fs::read_from_path(&file_path)?;

            let commit = repositories::commits::head_commit(&repo)?;
            let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
            let num_schemas = schemas.len();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull(&cloned_repo).await?;

                let filename = Path::new("nlp")
                    .join("classification")
                    .join("annotations")
                    .join("train.tsv");
                let file_path = cloned_repo.path.join(&filename);
                let cloned_df = tabular::read_df(&file_path, DFOpts::empty())?;
                let cloned_contents = util::fs::read_from_path(&file_path)?;
                assert_eq!(og_df.height(), cloned_df.height());
                assert_eq!(og_df.width(), cloned_df.width());
                assert_eq!(cloned_contents, og_sentiment_contents);
                println!("Cloned {filename:?} {cloned_df}");

                // Status should be empty too
                let status = repositories::status(&cloned_repo)?;
                status.print();
                assert!(status.is_clean());

                // Make sure we grab the same amount of schemas
                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                let pulled_schemas = repositories::data_frames::schemas::list(&repo, &head_commit)?;
                assert_eq!(pulled_schemas.len(), num_schemas);

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_pull_full_commit_history() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // First commit
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            repositories::add(&repo, &filepath)?;
            repositories::commit(&repo, "Adding labels file")?;

            // Second commit
            let new_filename = "new.txt";
            let new_filepath = repo.path.join(new_filename);
            util::fs::write_to_path(&new_filepath, "hallo")?;
            repositories::add(&repo, &new_filepath)?;
            repositories::commit(&repo, "Adding a new file")?;

            // Third commit
            let train_path = repo.path.join("train");
            repositories::add(&repo, &train_path)?;
            repositories::commit(&repo, "Adding train dir")?;

            // Fourth commit
            let test_path = repo.path.join("test");
            repositories::add(&repo, &test_path)?;
            repositories::commit(&repo, "Adding test dir")?;

            // Get local history
            let local_history = repositories::commits::list(&repo)?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            // run another test with a new repo dir that we are going to sync to
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("repoo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &new_repo_dir).await?;
                repositories::pull_all(&cloned_repo).await?;

                // Get cloned history, which should fall back to API if not found locally
                let cloned_history = repositories::commits::list(&cloned_repo)?;

                // Make sure the histories match
                assert_eq!(local_history.len(), cloned_history.len());

                api::client::repositories::delete(&remote_repo).await?;

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_pull_shallow_local_status_is_err() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let repo_dir = repo_dir.join("repoo");
                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

                let result = repositories::status(&cloned_repo);
                assert!(result.is_err());

                Ok(repo_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_pull_shallow_local_add_is_err() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let repo_dir = repo_dir.join("repoo");

                let cloned_repo =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &repo_dir).await?;

                let path = cloned_repo.path.join("README.md");
                util::fs::write_to_path(&path, "# Can't add this")?;

                let result = repositories::add(&cloned_repo, path);
                assert!(result.is_err());

                Ok(repo_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_pull_shallow_clone_only_pulls_head() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.clone();
                let user_a_repo_dir_copy = user_a_repo_dir_copy.join("repoo");
                let user_a_shallow =
                    repositories::shallow_clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy)
                        .await?;
                let remote_repo_inner_copy = remote_repo.clone();

                // Deep clone pushes two new commits to advance the remote
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("repoo");

                    let user_b_repo = repositories::deep_clone_url(
                        &remote_repo_inner_copy.remote.url,
                        &user_b_repo_dir_copy,
                    )
                    .await?;

                    let new_file = "new_file.txt";
                    let new_file_path = user_b_repo.path.join(new_file);
                    test::write_txt_file_to_path(&new_file_path, "hello from a file")?;
                    repositories::add(&user_b_repo, &new_file_path)?;
                    repositories::commit(&user_b_repo, "Adding new file")?;

                    let new_file = "new_file_2.txt";
                    let new_file_path = user_b_repo.path.join(new_file);
                    test::write_txt_file_to_path(&new_file_path, "hello from a different")?;
                    repositories::add(&user_b_repo, &new_file_path)?;
                    repositories::commit(&user_b_repo, "Adding new file 2")?;
                    repositories::push(&user_b_repo).await?;

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                // Pull on the shallow copy
                repositories::pull_remote_branch_shallow(
                    &user_a_shallow,
                    DEFAULT_REMOTE_NAME,
                    DEFAULT_BRANCH_NAME,
                )
                .await?;

                // Get all commits on the remote
                let remote_commits = api::client::commits::list_all(&remote_repo).await?;

                let mut synced_commits = 0;
                log::debug!("total n remote commits {}", remote_commits.len());
                for commit in remote_commits {
                    if repositories::commits::get_by_id(&user_a_shallow, &commit.id)?.is_some() {
                        log::debug!("synced commit {}", commit);
                        synced_commits += 1;
                    }
                }

                // Two commits should be exist locally - the one we just downloaded,
                // and the original clone
                assert_eq!(synced_commits, 2);

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_pull_standard_clone_only_pulls_head() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("repo_a");
                let user_a_repo =
                    repositories::clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy).await?;

                // Deep copy pushes two new commits to advance the remote
                test::run_empty_dir_test_async(|user_b_repo_dir| async move {
                    let user_b_repo_dir_copy = user_b_repo_dir.join("repo_b");

                    let user_b_repo = repositories::deep_clone_url(
                        &remote_repo.remote.url,
                        &user_b_repo_dir_copy,
                    )
                    .await?;

                    let new_file = "new_file.txt";
                    let new_file_path = user_b_repo.path.join(new_file);
                    test::write_txt_file_to_path(&new_file_path, "hello from a file")?;
                    repositories::add(&user_b_repo, &new_file_path)?;
                    repositories::commit(&user_b_repo, "Adding new file")?;

                    let new_file = "new_file_2.txt";
                    let new_file_path = user_b_repo.path.join(new_file);
                    test::write_txt_file_to_path(&new_file_path, "hello from a different")?;
                    repositories::add(&user_b_repo, &new_file_path)?;
                    repositories::commit(&user_b_repo, "Adding new file 2")?;
                    repositories::push(&user_b_repo).await?;

                    Ok(user_b_repo_dir_copy)
                })
                .await?;

                // Pull again
                let all = false;
                repositories::pull_remote_branch(
                    &user_a_repo,
                    DEFAULT_REMOTE_NAME,
                    DEFAULT_BRANCH_NAME,
                    all,
                )
                .await?;

                // Get all commits on the remote
                let remote_commits = repositories::commits::list(&user_a_repo)?;

                let mut synced_commits = 0;
                log::debug!("total n remote commits {}", remote_commits.len());
                for commit in remote_commits {
                    if core::commit_sync_status::commit_is_synced(&user_a_repo, &commit) {
                        synced_commits += 1;
                    }
                }

                // Two fully synced commits: the original clone, and the one we just grabbed.
                assert_eq!(synced_commits, 2);

                Ok(user_a_repo_dir_copy)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_pull_full_commit_history_after_shallow_clone() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Get the commits from the local repo to compare against later
            let og_commits = repositories::commits::list_all(&repo)?;

            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it
            repositories::push(&repo).await?;

            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let mut opts = CloneOpts::new(
                    remote_repo.remote.url.to_owned(),
                    new_repo_dir.join("new_repo"),
                );
                opts.shallow = true;

                // Clone in shallow mode
                let cloned_repo = repositories::clone(&opts).await?;

                // Pull all the commits
                repositories::pull_all(&cloned_repo).await?;

                let pulled_commits = repositories::commits::list_all(&cloned_repo)?;
                assert_eq!(pulled_commits.len(), og_commits.len());

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }
}
