//! # oxen checkout
//!
//! Checkout a branch or commit
//!

use std::path::Path;

use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};
use crate::opts::{DFOpts, RestoreOpts};
use crate::{repositories, util};

/// # Checkout a branch or commit id
/// This switches HEAD to point to the branch name or commit id,
/// it also updates all the local files to be from the commit that this branch references
pub async fn checkout(repo: &LocalRepository, value: &str) -> Result<Option<Branch>, OxenError> {
    log::debug!("--- CHECKOUT START {value} ----");
    if repositories::branches::exists(repo, value)? {
        if repositories::branches::is_checked_out(repo, value) {
            println!("Already on branch {value}");
            return repositories::branches::get_by_name(repo, value);
        }

        println!("Checkout branch: {value}");
        let commit = repositories::revisions::get(repo, value)?
            .ok_or(OxenError::revision_not_found(value.into()))?;
        let subtree_paths = match repo.subtree_paths() {
            Some(paths_vec) => paths_vec, // If Some(vec), take the inner vector
            None => vec![Path::new("").to_path_buf()],
        };
        let depth = repo.depth().unwrap_or(i32::MAX); //TODO: make repo depth not an option so that we use depth from the repo consistently.
        repositories::branches::checkout_subtrees_to_commit(repo, &commit, &subtree_paths, depth)
            .await?;
        repositories::branches::set_head(repo, value)?;
        repositories::branches::get_by_name(repo, value)
    } else {
        // If we are already on the commit, do nothing
        if repositories::branches::is_checked_out(repo, value) {
            eprintln!("Commit already checked out {value}");
            return Ok(None);
        }

        let commit = repositories::revisions::get(repo, value)?
            .ok_or(OxenError::revision_not_found(value.into()))?;

        let previous_head_commit = repositories::commits::head_commit_maybe(repo)?;
        repositories::branches::checkout_commit_from_commit(repo, &commit, &previous_head_commit)
            .await?;
        repositories::branches::update(repo, value, &commit.id)?;
        repositories::branches::set_head(repo, value)?;

        if repo.is_remote_mode() {
            // Set workspace_name to new branch name
            let mut mut_repo = repo.clone();
            mut_repo.set_workspace(value)?;
            mut_repo.save()?;
        }

        Ok(None)
    }
}

/// # Checkout a file and take their changes
/// This overwrites the current file with the changes in the branch we are merging in
pub async fn checkout_theirs(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let conflicts = repositories::merge::list_conflicts(repo)?;
    log::debug!(
        "checkout_theirs {:?} conflicts.len() {}",
        path,
        conflicts.len()
    );

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts.iter().find(|c| c.merge_entry.path == path) {
        // Lookup the file for the merge commit entry and copy it over
        repositories::restore::restore(
            repo,
            RestoreOpts::from_path_ref(path, &conflict.merge_entry.commit_id.clone()),
        )
        .await
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Checkout a file and take our changes
/// This overwrites the current file with the changes we had in our current branch
pub async fn checkout_ours(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let conflicts = repositories::merge::list_conflicts(repo)?;
    log::debug!(
        "checkout_ours {:?} conflicts.len() {}",
        path,
        conflicts.len()
    );

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts.iter().find(|c| c.merge_entry.path == path) {
        // Lookup the file for the base commit entry and copy it over
        repositories::restore(
            repo,
            RestoreOpts::from_path_ref(path, &conflict.base_entry.commit_id.clone()),
        )
        .await
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Combine Conflicting Tabular Data Files
/// This overwrites the current file with the changes in their file
pub async fn checkout_combine(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let conflicts = repositories::merge::list_conflicts(repo)?;

    log::debug!(
        "checkout_combine checking path {:?} -> [{}] conflicts",
        path,
        conflicts.len()
    );
    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts.iter().find(|c| c.merge_entry.path == path) {
        if util::fs::is_tabular(&conflict.base_entry.path) {
            let version_store = repo.version_store()?;
            let df_base_path = version_store
                .get_version_path(&conflict.base_entry.hash)
                .await?;
            let df_base = tabular::maybe_read_df_with_extension(
                repo,
                &df_base_path,
                &conflict.base_entry.path,
                &conflict.base_entry.commit_id,
                &DFOpts::empty(),
            )
            .await?;
            let df_merge_path = version_store
                .get_version_path(&conflict.merge_entry.hash)
                .await?;
            let df_merge = tabular::maybe_read_df_with_extension(
                repo,
                &df_merge_path,
                &conflict.merge_entry.path,
                &conflict.merge_entry.commit_id,
                &DFOpts::empty(),
            )
            .await?;

            log::debug!("GOT DF HEAD {df_base}");
            log::debug!("GOT DF MERGE {df_merge}");

            match df_base.vstack(&df_merge) {
                Ok(result) => {
                    log::debug!("GOT DF COMBINED {result}");
                    match result.unique_stable(None, polars::frame::UniqueKeepStrategy::First, None)
                    {
                        Ok(mut uniq) => {
                            log::debug!("GOT DF COMBINED UNIQUE {uniq}");
                            let output_path = repo.path.join(&conflict.base_entry.path);
                            tabular::write_df(&mut uniq, &output_path)
                        }
                        _ => Err(OxenError::basic_str("Could not uniq data")),
                    }
                }
                _ => Err(OxenError::basic_str(
                    "Could not combine data, make sure schema's match",
                )),
            }
        } else {
            Err(OxenError::basic_str(
                "Cannot use --combine on non-tabular data file.",
            ))
        }
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::opts::FetchOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_command_checkout_non_existant_commit_id() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // This shouldn't work
            let checkout_result = repositories::checkout(&repo, "non-existent").await;
            assert!(checkout_result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_commit_id() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write a hello file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Stage a hello file
            repositories::add(&repo, &hello_file).await?;
            // Commit the hello file
            let first_commit = repositories::commit(&repo, "Adding hello")?;

            // Write a world
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            // Stage a world file
            repositories::add(&repo, &world_file).await?;

            // Commit the world file
            repositories::commit(&repo, "Adding world")?;

            // We have the world file
            assert!(world_file.exists());

            // We checkout the previous commit
            repositories::checkout(&repo, &first_commit.id).await?;

            // // Then we do not have the world file anymore
            assert!(!world_file.exists());

            // // Check status
            let status = repositories::status(&repo)?;
            assert!(status.is_clean());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_commit_then_merge_main() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write a hello file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            let first_commit = repositories::commit(&repo, "Added hello.txt")?;

            // Write a world
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            // Stage a world file
            repositories::add(&repo, &world_file).await?;

            // Commit the world file
            let _ = repositories::commit(&repo, "Adding world")?;

            // Create and checkout branch
            let branch_name = "feature/my-branch";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add a third file
            let branch_file = repo.path.join("branch.txt");
            util::fs::write_to_path(&branch_file, "Branch file")?;

            // Stage a world file
            repositories::add(&repo, &branch_file).await?;

            // Commit the world file
            repositories::commit(&repo, "Adding branch file")?;

            // We have the branch file
            assert!(branch_file.exists());

            // Checkout the previous commit
            repositories::checkout(&repo, &first_commit.id).await?;

            // Then we do not have the branch file anymore
            assert!(!branch_file.exists());
            assert!(!world_file.exists());

            // Check status
            let status = repositories::status(&repo)?;
            assert!(status.is_clean());

            // Checkout branch again
            repositories::checkout(&repo, branch_name).await?;

            // // Merge to main again
            // let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            // // Checkout the branch
            // repositories::checkout(&repo, second_commit.id).await?;

            let has_merges = repositories::merge::merge(&repo, DEFAULT_BRANCH_NAME)
                .await
                .is_ok();

            // We should not have a merge because the current branch has all the old commits
            assert!(!has_merges);
            assert!(world_file.exists());
            assert!(hello_file.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_current_branch_name_does_nothing() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write the first file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Added hello.txt")?;

            // Create and checkout branch
            let branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, branch_name)?;
            repositories::checkout(&repo, branch_name).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_checkout_branch_with_dots_in_name() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write the first file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Added hello.txt")?;

            // Create and checkout branch
            let branch_name = "test..ing";
            let result = repositories::branches::create_checkout(&repo, branch_name);
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_added_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write the first file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Added hello.txt")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create and checkout branch
            let branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Write a second file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            // Track & commit the second file in the branch
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Added world.txt")?;

            // Make sure we have both commits
            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            let branches = repositories::branches::list(&repo)?;
            assert_eq!(branches.len(), 2);

            // Make sure we have both files on disk in our repo dir
            assert!(hello_file.exists());
            assert!(world_file.exists());

            // Go back to the main branch
            repositories::checkout(&repo, &orig_branch.name).await?;

            // The world file should no longer be there
            assert!(hello_file.exists());
            assert!(!world_file.exists());

            // Go back to the world branch
            repositories::checkout(&repo, branch_name).await?;
            assert!(hello_file.exists());
            assert!(world_file.exists());

            Ok(())
        })
        .await
    }

    /*
     * Modify the file on a branch
     * Commit file on branch
     * Checkout main
     * Modify the file on main
     * Merge branch into main
     * Assert that the file on main is not overwritten
     */
    #[tokio::test]
    async fn test_command_merge_does_not_overwrite_modified_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write the first file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Added hello.txt")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create and checkout branch
            let branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Write a second file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            // Track & commit the second file in the branch
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Added world.txt")?;

            // Modify the hello file on the branch
            let hello_file = test::modify_txt_file(&hello_file, "Hello from branch")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Changed hello.txt on branch")?;

            // Checkout the main branch
            repositories::checkout(&repo, &orig_branch.name).await?;

            // Modify the hello file on the main branch
            let hello_file = test::modify_txt_file(&hello_file, "Hello from main")?;

            // Merge the branch into main while there are conflicts
            let result = repositories::merge::merge(&repo, branch_name).await;
            assert!(result.is_err());

            // Assert that the hello file on main is not overwritten
            assert_eq!(util::fs::read_from_path(&hello_file)?, "Hello from main");

            Ok(())
        })
        .await
    }

    /*
     * Modify the file on a branch
     * Commit file on branch
     * Checkout main
     * Add a new file on main
     * Merge branch into main
     * Assert that the new file on main is not overwritten
     */
    #[tokio::test]
    async fn test_command_merge_does_not_overwrite_new_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write the first file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Added hello.txt")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create and checkout branch
            let branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Write a second file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            // Track & commit the second file in the branch
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Added world.txt")?;

            // Modify the hello file on the branch
            let hello_file = test::modify_txt_file(&hello_file, "Hello from branch")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Changed hello.txt on branch")?;

            // Checkout the main branch
            repositories::checkout(&repo, &orig_branch.name).await?;

            // Add a new file on main
            let new_file = repo.path.join("new_file.txt");
            util::fs::write_to_path(&new_file, "New file")?;

            // Merge the branch should not have conflicts
            repositories::merge::merge(&repo, branch_name).await?;

            // Assert that the new file on main is not overwritten
            assert_eq!(util::fs::read_from_path(&new_file)?, "New file");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_added_file_keep_untracked() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write the first file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Have another file lying around we will not remove
            let keep_file = repo.path.join("keep_me.txt");
            util::fs::write_to_path(&keep_file, "I am untracked, don't remove me")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Added hello.txt")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create and checkout branch
            let branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Write a second file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            // Track & commit the second file in the branch
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Added world.txt")?;

            // Make sure we have both commits
            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            let branches = repositories::branches::list(&repo)?;
            assert_eq!(branches.len(), 2);

            // Make sure we have all files on disk in our repo dir
            assert!(hello_file.exists());
            assert!(world_file.exists());
            assert!(keep_file.exists());

            // Go back to the main branch
            repositories::checkout(&repo, &orig_branch.name).await?;

            // The world file should no longer be there
            assert!(hello_file.exists());
            assert!(!world_file.exists());
            assert!(keep_file.exists());

            // Go back to the world branch
            repositories::checkout(&repo, branch_name).await?;
            assert!(hello_file.exists());
            assert!(world_file.exists());
            assert!(keep_file.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_modified_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write the first file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            // Track & commit the file
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Added hello.txt")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create and checkout branch
            let branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify the file
            let hello_file = test::modify_txt_file(&hello_file, "World")?;

            // Track & commit the change in the branch
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Changed file to world")?;

            // It should say World at this point
            assert_eq!(util::fs::read_from_path(&hello_file)?, "World");

            // Go back to the main branch
            repositories::checkout(&repo, &orig_branch.name).await?;

            // The file contents should be Hello, not World
            log::debug!("HELLO FILE NAME: {hello_file:?}");
            assert!(hello_file.exists());

            // It should be reverted back to Hello
            assert_eq!(util::fs::read_from_path(&hello_file)?, "Hello");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_modified_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Track & commit the file
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            repositories::add(&repo, &one_shot_path).await?;
            repositories::commit(&repo, "Adding one shot")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Get OG file contents
            let og_content = util::fs::read_from_path(&one_shot_path)?;

            let branch_name = "feature/change-the-shot";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
            let one_shot_path = test::modify_txt_file(&one_shot_path, file_contents)?;
            let status = repositories::status(&repo)?;
            assert_eq!(status.modified_files.len(), 1);
            status.print();
            repositories::add(&repo, &one_shot_path).await?;
            let status = repositories::status(&repo)?;
            status.print();
            repositories::commit(&repo, "Changing one shot")?;

            // checkout OG and make sure it reverts
            repositories::checkout(&repo, &orig_branch.name).await?;
            let updated_content = util::fs::read_from_path(&one_shot_path)?;
            assert_eq!(og_content, updated_content);

            // checkout branch again and make sure it reverts
            repositories::checkout(&repo, branch_name).await?;
            let updated_content = util::fs::read_from_path(&one_shot_path)?;
            assert_eq!(file_contents, updated_content);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_checkout_modified_file_from_fully_committed_repo() -> Result<(), OxenError>
    {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Track & commit all the data
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Adding one shot")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Get OG file contents
            let og_content = util::fs::read_from_path(&one_shot_path)?;

            let branch_name = "feature/modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
            let one_shot_path = test::modify_txt_file(&one_shot_path, file_contents)?;
            let status = repositories::status(&repo)?;
            assert_eq!(status.modified_files.len(), 1);
            repositories::add(&repo, &one_shot_path).await?;
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.modified_files.len(), 0);
            assert_eq!(status.staged_files.len(), 1);

            let status = repositories::status(&repo)?;
            status.print();
            repositories::commit(&repo, "Changing one shot")?;

            // checkout OG and make sure it reverts
            repositories::checkout(&repo, &orig_branch.name).await?;
            let updated_content = util::fs::read_from_path(&one_shot_path)?;
            assert_eq!(og_content, updated_content);

            // checkout branch again and make sure it reverts
            repositories::checkout(&repo, branch_name).await?;
            let updated_content = util::fs::read_from_path(&one_shot_path)?;
            assert_eq!(file_contents, updated_content);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_remove_dir_then_revert() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // (dir already created in helper)
            let dir_to_remove = repo.path.join("train");
            let og_num_files = util::fs::rcount_files_in_dir(&dir_to_remove);

            // track the dir
            repositories::add(&repo, &dir_to_remove).await?;
            repositories::commit(&repo, "Adding train dir")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a new branch to make the changes
            let branch_name = "feature/removing-train";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Delete the directory from disk
            util::fs::remove_dir_all(&dir_to_remove)?;

            // Track the deletion
            repositories::add(&repo, &dir_to_remove).await?;
            repositories::commit(&repo, "Removing train dir")?;

            // checkout OG and make sure it restores the train dir
            repositories::checkout(&repo, &orig_branch.name).await?;
            assert!(dir_to_remove.exists());
            assert_eq!(util::fs::rcount_files_in_dir(&dir_to_remove), og_num_files);

            // checkout branch again and make sure it reverts
            repositories::checkout(&repo, branch_name).await?;
            assert!(!dir_to_remove.exists());

            Ok(())
        })
        .await
    }

    // Test the default clone (not --all or --shallow) can revert to files that are not local
    #[tokio::test]
    async fn test_checkout_deleted_after_clone() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            let og_commits = repositories::commits::list_all(&local_repo)?;

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::clone_url(
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
                    println!("file: {file:?}");
                }
                assert_eq!(test_dir_files.len(), 4);

                assert!(test_dir_path.join("1.jpg").exists());
                assert!(test_dir_path.join("2.jpg").exists());
                assert!(test_dir_path.join("3.jpg").exists());
                assert!(test_dir_path.join("4.jpg").exists());

                Ok(())
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    /*
    Checks workflow:

    $ oxen clone <URL>

    $ oxen checkout f412d166be1bead8 # earlier commit
    $ oxen checkout 55a4df7cd5d00eee # later commit

    Checkout commit: 55a4df7cd5d00eee
    Setting working directory to 55a4df7cd5d00eee
    IO(Os { code: 2, kind: NotFound, message: "No such file or directory" })

    */
    #[tokio::test]
    async fn test_clone_checkout_old_commit_checkout_new_commit() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &repo_dir.join("new_repo"))
                        .await?;

                let commits = repositories::commits::list(&cloned_repo)?;
                // iterate over commits in reverse order and checkout each one
                for commit in commits.iter().rev() {
                    println!(
                        "TEST checking out commit: {} -> '{}'",
                        commit.id, commit.message
                    );
                    repositories::checkout(&cloned_repo, &commit.id).await?;
                }

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_checkout_local_does_not_remove_untracked_files() -> Result<(), OxenError> {
        // Push the Remote Repo
        test::run_one_commit_sync_repo_test(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Clone Repo to User A
            test::run_empty_dir_test_async(|user_a_repo_dir| async move {
                let user_a_repo_dir_copy = user_a_repo_dir.join("user_a_repo");
                let user_a_repo =
                    repositories::clone_url(&remote_repo.remote.url, &user_a_repo_dir_copy).await?;

                // Create a new branch
                let branch_name = "test-branch";
                repositories::branches::create_checkout(&user_a_repo, branch_name)?;

                // Back to main
                repositories::checkout(&user_a_repo, DEFAULT_BRANCH_NAME).await?;

                // Create some untracked files...
                let file_1 = user_a_repo.path.join("file_1.txt");
                let dir_1 = user_a_repo.path.join("dir_1");
                let file_in_dir_1 = dir_1.join("file_in_dir_1.txt");
                let dir_2 = user_a_repo.path.join("dir_2");
                let subdir_2 = dir_2.join("subdir_2");
                let file_in_dir_2 = subdir_2.join("file_in_dir_2.txt");
                let file_in_subdir_2 = subdir_2.join("file_in_subdir_2.txt");

                // Create the files and dirs
                std::fs::create_dir(&dir_1)?;
                std::fs::create_dir(&dir_2)?;
                std::fs::create_dir(&subdir_2)?;

                test::write_txt_file_to_path(&file_1, "this is file 1")?;
                test::write_txt_file_to_path(&file_in_dir_1, "this is file in dir 1")?;
                test::write_txt_file_to_path(&file_in_dir_2, "this is file in dir 2")?;
                test::write_txt_file_to_path(&file_in_subdir_2, "this is file in subdir 2")?;

                // Switch back over to the other branch
                repositories::checkout(&user_a_repo, branch_name).await?;

                // Files should exist
                assert!(file_1.exists());
                assert!(file_in_dir_1.exists());
                assert!(file_in_dir_2.exists());
                assert!(file_in_subdir_2.exists());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_checkout_remote_does_not_remove_untracked_files() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            // Create additional branch on remote repo before clone

            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::deep_clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;

                // Create untracked files
                let file_1 = cloned_repo.path.join("file_1.txt");
                let dir_1 = cloned_repo.path.join("dir_1");
                let file_in_dir_1 = dir_1.join("file_in_dir_1.txt");
                let dir_2 = cloned_repo.path.join("dir_2");
                let subdir_2 = dir_2.join("subdir_2");
                let file_in_dir_2 = subdir_2.join("file_in_dir_2.txt");

                // Create the files and dirs
                std::fs::create_dir(&dir_1)?;
                std::fs::create_dir(&dir_2)?;
                std::fs::create_dir(&subdir_2)?;

                test::write_txt_file_to_path(&file_1, "this is file 1")?;
                test::write_txt_file_to_path(&file_in_dir_1, "this is file in dir 1")?;
                test::write_txt_file_to_path(&file_in_dir_2, "this is file in dir 2")?;

                // Create a new branch after cloning (so we have to fetch the new commit from the remote)

                let branch_name = "test-branch";
                api::client::branches::create_from_branch(
                    &remote_repo,
                    branch_name,
                    DEFAULT_BRANCH_NAME,
                )
                .await?;

                repositories::fetch_all(&cloned_repo, &FetchOpts::new()).await?;

                // Checkout the new branch
                repositories::checkout(&cloned_repo, branch_name).await?;

                // Files should exist
                assert!(file_1.exists());
                assert!(file_in_dir_1.exists());
                assert!(file_in_dir_2.exists());

                Ok(())
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_checkout_old_commit_does_not_overwrite_untracked_files() -> Result<(), OxenError>
    {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            // Create additional branch on remote repo before clone
            let branch_name = "test-branch";
            api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let cloned_remote = remote_repo.clone();

            // Clone with the --all flag
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = repositories::deep_clone_url(
                    &remote_repo.remote.url,
                    &new_repo_dir.join("new_repo"),
                )
                .await?;

                let test_dir_path = cloned_repo.path.join("test");
                let commit = repositories::commits::first_by_message(&cloned_repo, "Adding test/")?;

                // Create untracked files
                let file_1 = cloned_repo.path.join("file_1.txt");
                let dir_1 = cloned_repo.path.join("dir_1");
                let file_in_dir_1 = dir_1.join("file_in_dir_1.txt");
                let dir_2 = cloned_repo.path.join("dir_2");
                let subdir_2 = dir_2.join("subdir_2");
                let file_in_dir_2 = subdir_2.join("file_in_dir_2.txt");

                // Create the files and dirs
                std::fs::create_dir(&dir_1)?;
                std::fs::create_dir(&dir_2)?;
                std::fs::create_dir(&subdir_2)?;

                test::write_txt_file_to_path(&file_1, "this is file 1")?;
                test::write_txt_file_to_path(&file_in_dir_1, "this is file in dir 1")?;
                test::write_txt_file_to_path(&file_in_dir_2, "this is file in dir 2")?;

                assert!(commit.is_some());
                assert!(!test_dir_path.exists());

                // checkout the commit
                repositories::checkout(&cloned_repo, &commit.unwrap().id).await?;
                // Make sure we restored the directory
                assert!(test_dir_path.exists());
                // Make sure the untracked files are still there
                assert!(file_1.exists());
                assert!(file_in_dir_1.exists());
                assert!(file_in_dir_2.exists());

                Ok(())
            })
            .await?;

            Ok(cloned_remote)
        })
        .await
    }

    #[tokio::test]
    async fn test_checkout_preserves_uncommitted_file_deletion() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write and commit two files
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Adding files")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a feature branch with modifications to world.txt and a new file
            let branch_name = "feature/new-stuff";
            repositories::branches::create_checkout(&repo, branch_name)?;
            let world_file = test::modify_txt_file(&world_file, "World modified")?;
            let new_file = repo.path.join("new.txt");
            util::fs::write_to_path(&new_file, "New")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Modified world.txt and added new.txt")?;

            // Go back to main
            repositories::checkout(&repo, &orig_branch.name).await?;

            // Delete hello.txt without committing
            std::fs::remove_file(&hello_file)?;
            assert!(!hello_file.exists());

            // Checkout the feature branch
            repositories::checkout(&repo, branch_name).await?;

            // hello.txt should still be deleted (uncommitted deletion preserved)
            assert!(!hello_file.exists());
            // world.txt should have the feature branch content
            assert!(world_file.exists());
            // new.txt should be restored from the feature branch
            assert!(new_file.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_checkout_conflicts_on_uncommitted_deletion_of_modified_file()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write and commit a file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;

            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Adding hello.txt")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a feature branch that modifies hello.txt
            let branch_name = "feature/modify-hello";
            repositories::branches::create_checkout(&repo, branch_name)?;
            test::modify_txt_file(&hello_file.clone(), "Hello from feature")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Modified hello.txt")?;

            // Go back to main
            repositories::checkout(&repo, &orig_branch.name).await?;

            // Delete hello.txt without committing
            std::fs::remove_file(&hello_file)?;
            assert!(!hello_file.exists());

            // Checkout the feature branch should fail because hello.txt was deleted
            // locally but modified on the target branch
            let result = repositories::checkout(&repo, branch_name).await;
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    // a file that only exists on branch B should be removed
    // when checking out branch A, even if the user deleted a different file.
    #[tokio::test]
    async fn test_checkout_removes_branch_specific_file_when_switching() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a file on main
            let common_file = repo.path.join("common.txt");
            util::fs::write_to_path(&common_file, "common content")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Initial commit with common.txt")?;

            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create feature branch and add a new file
            let branch_name = "feature/add-new";
            repositories::branches::create_checkout(&repo, branch_name)?;
            let new_file = repo.path.join("feature_only.txt");
            util::fs::write_to_path(&new_file, "only on feature branch")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add feature_only.txt")?;

            // feature_only.txt should exist on feature branch
            assert!(new_file.exists());

            // Delete a different (untracked) file before switching branches
            let untracked_file = repo.path.join("untracked.txt");
            util::fs::write_to_path(&untracked_file, "I will be deleted")?;
            assert!(untracked_file.exists());
            std::fs::remove_file(&untracked_file)?;
            assert!(!untracked_file.exists());

            // Checkout main — feature_only.txt should be removed
            repositories::checkout(&repo, &main_branch.name).await?;
            assert!(
                !new_file.exists(),
                "feature_only.txt should be removed when checking out main"
            );
            assert!(
                common_file.exists(),
                "common.txt should exist when checking out"
            );

            Ok(())
        })
        .await
    }

    // when a directory exists on branch B but not branch A,
    // checking out branch B should restore the directory and its files.
    #[tokio::test]
    async fn test_checkout_restores_directory_from_target_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit files in a directory on main
            let dir = repo.path.join("data");
            std::fs::create_dir_all(&dir)?;
            let file1 = dir.join("file1.txt");
            let file2 = dir.join("file2.txt");
            util::fs::write_to_path(&file1, "data file 1")?;
            util::fs::write_to_path(&file2, "data file 2")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add data directory")?;

            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a branch that removes the directory
            let branch_name = "feature/no-data";
            repositories::branches::create_checkout(&repo, branch_name)?;
            std::fs::remove_dir_all(&dir)?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Remove data directory")?;

            // Verify dir is gone on feature branch
            assert!(!dir.exists());

            // Checkout main — directory should be restored
            repositories::checkout(&repo, &main_branch.name).await?;
            assert!(
                dir.exists(),
                "data/ directory should be restored when checking out main"
            );
            assert!(
                file1.exists(),
                "data/file1.txt should be restored when checking out main"
            );
            assert!(
                file2.exists(),
                "data/file2.txt should be restored when checking out main"
            );

            Ok(())
        })
        .await
    }

    // round-trip checkout should give a clean working directory
    // matching the target branch each time.
    #[tokio::test]
    async fn test_checkout_roundtrip_restores_all_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Main branch: common.txt
            let common_file = repo.path.join("common.txt");
            util::fs::write_to_path(&common_file, "common")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Initial commit")?;

            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Feature branch: common.txt + extra.txt + subdir/nested.txt + datadir/a.txt + datadir/b.txt
            let branch_name = "feature/extras";
            repositories::branches::create_checkout(&repo, branch_name)?;
            let extra_file = repo.path.join("extra.txt");
            util::fs::write_to_path(&extra_file, "extra content")?;
            let subdir = repo.path.join("subdir");
            std::fs::create_dir_all(&subdir)?;
            let nested_file = subdir.join("nested.txt");
            util::fs::write_to_path(&nested_file, "nested content")?;
            let datadir = repo.path.join("datadir");
            std::fs::create_dir_all(&datadir)?;
            let data_a = datadir.join("a.txt");
            let data_b = datadir.join("b.txt");
            util::fs::write_to_path(&data_a, "data a")?;
            util::fs::write_to_path(&data_b, "data b")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add extra files")?;

            // Checkout main — extra files and directories should be removed
            repositories::checkout(&repo, &main_branch.name).await?;
            assert!(common_file.exists(), "common.txt should exist on main");
            assert!(!extra_file.exists(), "extra.txt should NOT exist on main");
            assert!(
                !nested_file.exists(),
                "subdir/nested.txt should NOT exist on main"
            );
            assert!(!datadir.exists(), "datadir/ should NOT exist on main");

            // Checkout feature again — all files and directories should come back
            repositories::checkout(&repo, branch_name).await?;
            assert!(common_file.exists(), "common.txt should exist on feature");
            assert!(
                extra_file.exists(),
                "extra.txt should be restored on feature"
            );
            assert!(
                nested_file.exists(),
                "subdir/nested.txt should be restored on feature"
            );
            assert!(
                datadir.is_dir(),
                "datadir/ should be restored as a directory on feature"
            );
            assert!(
                data_a.exists(),
                "datadir/a.txt should be restored on feature"
            );
            assert!(
                data_b.exists(),
                "datadir/b.txt should be restored on feature"
            );

            // And back to main once more
            repositories::checkout(&repo, &main_branch.name).await?;
            assert!(
                !extra_file.exists(),
                "extra.txt should be removed again on main"
            );
            assert!(
                !nested_file.exists(),
                "subdir/nested.txt should be removed again on main"
            );
            assert!(
                !datadir.exists(),
                "datadir/ should be removed again on main"
            );

            Ok(())
        })
        .await
    }

    // deleting a directory and checking out the same branch
    // that has the directory should restore it (e.g., `oxen checkout .` or
    // re-checkout of current branch after unstaged deletion).
    #[tokio::test]
    async fn test_checkout_restores_deleted_directory_same_branch_content() -> Result<(), OxenError>
    {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Both branches have the same directory content
            let dir = repo.path.join("models");
            std::fs::create_dir_all(&dir)?;
            let model_file = dir.join("model.bin");
            util::fs::write_to_path(&model_file, "model data")?;
            let config_file = dir.join("config.json");
            util::fs::write_to_path(&config_file, r#"{"lr": 0.01}"#)?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add models directory")?;

            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create feature branch with same models/ content but an extra file
            let branch_name = "feature/new-stuff";
            repositories::branches::create_checkout(&repo, branch_name)?;
            let new_file = repo.path.join("new_feature.txt");
            util::fs::write_to_path(&new_file, "new feature")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add new_feature.txt")?;

            // Go back to main
            repositories::checkout(&repo, &main_branch.name).await?;

            // Delete the models directory (uncommitted)
            std::fs::remove_dir_all(&dir)?;
            assert!(!dir.exists());

            // Checkout feature branch — models/ has same content on both branches,
            // but it should still be restored because we're checking out a branch
            repositories::checkout(&repo, branch_name).await?;
            assert!(
                dir.exists(),
                "models/ directory should be restored when checking out feature branch"
            );
            assert!(model_file.exists(), "models/model.bin should be restored");
            assert!(
                config_file.exists(),
                "models/config.json should be restored"
            );
            assert!(
                new_file.exists(),
                "new_feature.txt should exist on feature branch"
            );

            // Now test re-checking out the same branch after uncommitted deletion.
            // Delete the models directory again (uncommitted) while on feature branch.
            std::fs::remove_dir_all(&dir)?;
            assert!(!dir.exists(), "models/ should be deleted");

            // Re-checkout the same branch is a no-op ("Already on branch"),
            // so the uncommitted deletion is preserved.
            repositories::checkout(&repo, branch_name).await?;
            assert!(
                !dir.exists(),
                "models/ should remain deleted — same-branch checkout is a no-op"
            );

            // But checking out main and back to feature should restore it
            repositories::checkout(&repo, &main_branch.name).await?;
            repositories::checkout(&repo, branch_name).await?;
            assert!(
                dir.exists(),
                "models/ directory should be restored after round-trip checkout"
            );
            assert!(
                model_file.exists(),
                "models/model.bin should be restored after round-trip checkout"
            );
            assert!(
                config_file.exists(),
                "models/config.json should be restored after round-trip checkout"
            );

            Ok(())
        })
        .await
    }

    // Regression test: when a file on the current branch has the same content
    // (same hash) as a different file on the target branch, checkout should
    // still remove it if it doesn't exist on the target branch.
    #[tokio::test]
    async fn test_checkout_removes_duplicate_content_file_at_different_path()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Main branch: file1.txt with some content
            let file1 = repo.path.join("file1.txt");
            util::fs::write_to_path(&file1, "shared content")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add file1.txt on main")?;

            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create feature branch and add file2.txt with identical content
            let branch_name = "feature/dup-content";
            repositories::branches::create_checkout(&repo, branch_name)?;
            let file2 = repo.path.join("file2.txt");
            util::fs::write_to_path(&file2, "shared content")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add file2.txt with same content as file1.txt")?;

            // Both files should exist on the feature branch
            assert!(file1.exists(), "file1.txt should exist on feature branch");
            assert!(file2.exists(), "file2.txt should exist on feature branch");

            // Checkout main — file2.txt should be removed even though its
            // content hash matches file1.txt on main
            repositories::checkout(&repo, &main_branch.name).await?;
            assert!(file1.exists(), "file1.txt should exist on main");
            assert!(
                !file2.exists(),
                "file2.txt should be removed on main even though it shares content hash with file1.txt"
            );

            Ok(())
        })
        .await
    }

    // Both branches share the same "data/" directory (identical content, so
    // the hash is in common_nodes). On disk, the user manually deletes the
    // directory and creates a regular file at "data" (uncommitted).
    // Checking out the other branch should detect that "data" on disk is NOT
    // a directory and restore the directory contents instead of taking the
    // fast-path exit that assumes the directory is already present.
    //
    // This exposes a bug where full_dir_path.exists() is used instead of
    // full_dir_path.is_dir(): the file satisfies .exists(), the hash is in
    // common_nodes, and the code returns Ok(()) — silently skipping the
    // entire subtree restoration.
    #[tokio::test]
    async fn test_checkout_restores_directory_when_file_exists_at_same_path()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // -- main branch: "data/" is a directory with two files --
            let dir = repo.path.join("data");
            std::fs::create_dir_all(&dir)?;
            let file1 = dir.join("file1.txt");
            let file2 = dir.join("file2.txt");
            util::fs::write_to_path(&file1, "data file 1")?;
            util::fs::write_to_path(&file2, "data file 2")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add data directory with files")?;

            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // -- feature branch: same "data/" directory (identical content) --
            // Plus an extra file so the branches diverge and checkout does work.
            repositories::branches::create_checkout(&repo, "feature/shared-data")?;
            let extra = repo.path.join("extra.txt");
            util::fs::write_to_path(&extra, "extra")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Add extra.txt, data/ is unchanged")?;

            // Sanity: data/ directory exists identically on both branches
            assert!(dir.is_dir());
            assert!(file1.exists());
            assert!(file2.exists());

            // -- Simulate user manually replacing "data/" dir with a file --
            std::fs::remove_dir_all(&dir)?;
            util::fs::write_to_path(&repo.path.join("data"), "I am a plain file, not a dir")?;

            // Sanity: "data" is now a regular file on disk
            let data_path = repo.path.join("data");
            assert!(data_path.is_file(), "data should be a regular file");
            assert!(!data_path.is_dir(), "data should NOT be a directory");

            // -- checkout main: data/ directory should be fully restored --
            repositories::checkout(&repo, &main_branch.name).await?;

            assert!(
                dir.is_dir(),
                "data/ should be restored as a directory when checking out main"
            );
            assert!(
                file1.exists(),
                "data/file1.txt should be restored when checking out main"
            );
            assert!(
                file2.exists(),
                "data/file2.txt should be restored when checking out main"
            );

            Ok(())
        })
        .await
    }
}
