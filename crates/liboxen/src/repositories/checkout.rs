//! # oxen checkout
//!
//! Checkout a branch or commit
//!

use std::path::Path;

use crate::core::df::tabular;
use crate::core::v_latest::branches::OnConflict;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};
use crate::opts::{DFOpts, RestoreOpts};
use crate::{repositories, util};

/// # Checkout a branch or commit id
/// This switches HEAD to point to the branch name or commit id,
/// it also updates all the local files to be from the commit that this branch references
pub async fn checkout(
    repo: &LocalRepository,
    value: impl AsRef<str>,
) -> Result<Option<Branch>, OxenError> {
    let value = value.as_ref();
    log::debug!("--- CHECKOUT START {value} ----");
    if repositories::branches::exists(repo, value)? {
        if repositories::branches::is_checked_out(repo, value) {
            println!("Already on branch {value}");
            return Ok(Some(repositories::branches::get_by_name(repo, value)?));
        }

        println!("Checkout branch: {value}");
        let commit = repositories::revisions::get(repo, value)?
            .ok_or_else(|| OxenError::RevisionNotFound(value.into()))?;
        let subtree_paths = match repo.subtree_paths() {
            Some(paths_vec) => paths_vec, // If Some(vec), take the inner vector
            None => vec![Path::new("").to_path_buf()],
        };
        let depth = repo.depth().unwrap_or(i32::MAX); //TODO: make repo depth not an option so that we use depth from the repo consistently.
        repositories::branches::checkout_subtrees_to_commit(repo, &commit, &subtree_paths, depth)
            .await?;
        repositories::branches::set_head(repo, value)?;
        Ok(Some(repositories::branches::get_by_name(repo, value)?))
    } else {
        // If we are already on the commit, do nothing
        if repositories::branches::is_checked_out(repo, value) {
            eprintln!("Commit already checked out {value}");
            return Ok(None);
        }

        let commit = repositories::revisions::get(repo, value)?
            .ok_or_else(|| OxenError::RevisionNotFound(value.into()))?;

        let previous_head_commit = repositories::commits::head_commit_maybe(repo)?;
        repositories::branches::checkout_commit_from_commit(
            repo,
            &commit,
            &previous_head_commit,
            OnConflict::Abort,
        )
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
pub async fn checkout_theirs(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let conflicts = repositories::merge::list_conflicts(repo)?;
    log::debug!(
        "checkout_theirs {:?} conflicts.len() {}",
        path.as_ref(),
        conflicts.len()
    );

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        // Lookup the file for the merge commit entry and copy it over
        repositories::restore::restore(
            repo,
            RestoreOpts::from_path_ref(path, conflict.merge_entry.commit_id.clone()),
        )
        .await
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Checkout a file and take our changes
/// This overwrites the current file with the changes we had in our current branch
pub async fn checkout_ours(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let conflicts = repositories::merge::list_conflicts(repo)?;
    log::debug!(
        "checkout_ours {:?} conflicts.len() {}",
        path.as_ref(),
        conflicts.len()
    );

    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        // Lookup the file for the base commit entry and copy it over
        repositories::restore(
            repo,
            RestoreOpts::from_path_ref(path, conflict.base_entry.commit_id.clone()),
        )
        .await
    } else {
        Err(OxenError::could_not_find_merge_conflict(path))
    }
}

/// # Combine Conflicting Tabular Data Files
/// This overwrites the current file with the changes in their file
pub async fn checkout_combine<P: AsRef<Path>>(
    repo: &LocalRepository,
    path: P,
) -> Result<(), OxenError> {
    let conflicts = repositories::merge::list_conflicts(repo)?;

    log::debug!(
        "checkout_combine checking path {:?} -> [{}] conflicts",
        path.as_ref(),
        conflicts.len()
    );
    // find the path that matches in the conflict, throw error if !found
    if let Some(conflict) = conflicts
        .iter()
        .find(|c| c.merge_entry.path == path.as_ref())
    {
        if util::fs::is_tabular(&conflict.base_entry.path) {
            let version_store = repo.version_store();
            let df_base = tabular::maybe_read_version_df(
                repo,
                &version_store,
                &conflict.base_entry.hash,
                &conflict.base_entry.path,
                &conflict.base_entry.commit_id,
                &DFOpts::empty(),
            )
            .await?;
            let df_merge = tabular::maybe_read_version_df(
                repo,
                &version_store,
                &conflict.merge_entry.hash,
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
    use std::path::{Path, PathBuf};

    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::core::v_latest::branches::OnConflict;
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::opts::FetchOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

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
            assert!(world_file.exists());

            // Checkout the previous commit
            repositories::checkout(&repo, first_commit.id).await?;

            // Then we do not have the branch file anymore
            assert!(!branch_file.exists());
            assert!(!world_file.exists());

            // Check status
            let status = repositories::status(&repo).await?;
            assert!(status.is_clean());

            // Checkout branch again
            repositories::checkout(&repo, branch_name).await?;

            // // Merge to main again
            // let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            // // Checkout the branch
            // repositories::checkout(&repo, second_commit.id).await?;

            // Merging main into the current branch is a no-op: main is an ancestor of
            // feature/my-branch (current HEAD), so the merge succeeds as "Already up to date" —
            // no new merge commit, HEAD unchanged.
            let head_before_merge = repositories::commits::head_commit(&repo)?;
            let merge_result = repositories::merge::merge(&repo, DEFAULT_BRANCH_NAME)
                .await?
                .expect("merge should succeed (already up to date)");
            assert_eq!(merge_result.id, head_before_merge.id);
            let head_after_merge = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_after_merge.id, head_before_merge.id);
            assert!(world_file.exists());
            assert!(hello_file.exists());

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

            assert!(
                repositories::checkout(&repo, "non-existent").await.is_err(),
                "no branch or commit is named non-existent"
            );
            assert!(
                repositories::branches::create_checkout(&repo, "test..ing").is_err(),
                "a branch name cannot contain `..`"
            );

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create and checkout branch, then check out the branch already checked out
            let branch_name = "feature/world-explorer";
            repositories::branches::create_checkout(&repo, branch_name)?;
            repositories::checkout(&repo, branch_name).await?;

            // Write a second file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;

            // Track & commit the second file in the branch
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Added world.txt")?;

            // Make sure we have both commits
            let commits = repositories::commits::list(&repo)?;
            assert_eq!(commits.len(), 2);

            let branches = repositories::branches::list(&repo).await?;
            assert_eq!(branches.len(), 2);

            // Make sure we have both files on disk in our repo dir
            assert!(hello_file.exists());
            assert!(world_file.exists());

            // Go back to the main branch
            repositories::checkout(&repo, orig_branch.name).await?;

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
            let hello_file = test::modify_txt_file(hello_file, "Hello from branch")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Changed hello.txt on branch")?;

            // Checkout the main branch
            repositories::checkout(&repo, orig_branch.name).await?;

            // Modify the hello file on the main branch
            let hello_file = test::modify_txt_file(hello_file, "Hello from main")?;

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
            let hello_file = test::modify_txt_file(hello_file, "Hello from branch")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Changed hello.txt on branch")?;

            // Checkout the main branch
            repositories::checkout(&repo, orig_branch.name).await?;

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
            let hello_file = test::modify_txt_file(hello_file, "World")?;

            // Track & commit the change in the branch
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Changed file to world")?;

            // It should say World at this point
            assert_eq!(util::fs::read_from_path(&hello_file)?, "World");

            // Go back to the main branch
            repositories::checkout(&repo, orig_branch.name).await?;

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
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create a file in a nested subdirectory, then commit
            let sub_dir = repo.path.join("annotations").join("train");
            util::fs::create_dir_all(&sub_dir)?;
            let one_shot_path = sub_dir.join("one_shot.csv");
            test::write_txt_file_to_path(&one_shot_path, "file,label\n")?;
            repositories::add(&repo, &one_shot_path).await?;
            repositories::commit(&repo, "Adding one shot")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Get OG file contents
            let og_content = util::fs::read_from_path(&one_shot_path)?;

            let branch_name = "feature/change-the-shot";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let file_contents = "file,label\ntrain/cat_1.jpg,0\n";
            let one_shot_path = test::modify_txt_file(one_shot_path, file_contents)?;
            let status = repositories::status(&repo).await?;
            assert_eq!(status.modified_files.len(), 1);
            status.print();
            repositories::add(&repo, &one_shot_path).await?;
            let status = repositories::status(&repo).await?;
            status.print();
            repositories::commit(&repo, "Changing one shot")?;

            // checkout OG and make sure it reverts
            repositories::checkout(&repo, orig_branch.name).await?;
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
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create an inline directory with a few files
            let dir_to_remove = repo.path.join("data");
            util::fs::create_dir_all(&dir_to_remove)?;
            for i in 0..3 {
                test::write_txt_file_to_path(
                    dir_to_remove.join(format!("file_{i}.txt")),
                    format!("contents {i}"),
                )?;
            }
            let og_num_files = util::fs::rcount_files_in_dir(&dir_to_remove);

            // track the dir
            repositories::add(&repo, &dir_to_remove).await?;
            repositories::commit(&repo, "Adding data dir")?;

            // Get the original branch name
            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a new branch to make the changes
            let branch_name = "feature/removing-train";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Delete the directory from disk
            util::fs::remove_dir_all(&dir_to_remove)?;

            // Track the deletion
            repositories::add(&repo, &dir_to_remove).await?;
            repositories::commit(&repo, "Removing data dir")?;

            // checkout OG and make sure it restores the data dir
            repositories::checkout(&repo, orig_branch.name).await?;
            assert!(dir_to_remove.exists());
            assert_eq!(util::fs::rcount_files_in_dir(&dir_to_remove), og_num_files);

            // checkout branch again and make sure it reverts
            repositories::checkout(&repo, branch_name).await?;
            assert!(!dir_to_remove.exists());

            Ok(())
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_checkout_in_clones_walks_history_and_keeps_untracked_files()
    -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            let extra_file = local_repo.path.join("hello.txt");
            test::write_txt_file_to_path(&extra_file, "hello")?;
            repositories::add(&local_repo, &extra_file).await?;
            repositories::commit(&local_repo, "Adding hello.txt")?;
            repositories::push(&local_repo).await?;

            test::run_empty_dir_test_async(|repo_dir| async move {
                // Checking out an older commit and then a newer one in a clone restores the files
                // the newer commit adds.
                let walk_repo =
                    repositories::clone_url(&remote_repo.remote.url, repo_dir.join("walk")).await?;
                let commits = repositories::commits::list(&walk_repo)?;
                assert_eq!(commits.len(), 2);
                let walk_hello = walk_repo.path.join("hello.txt");
                repositories::checkout(&walk_repo, &commits[1].id).await?;
                assert!(!walk_hello.exists(), "the older commit predates hello.txt");
                repositories::checkout(&walk_repo, &commits[0].id).await?;
                assert!(walk_hello.exists(), "the newer commit adds hello.txt");

                let local_repo =
                    repositories::clone_url(&remote_repo.remote.url, repo_dir.join("local"))
                        .await?;
                let branch_name = "test-branch";
                repositories::branches::create_checkout(&local_repo, branch_name)?;
                repositories::checkout(&local_repo, DEFAULT_BRANCH_NAME).await?;
                let untracked = write_untracked_files(&local_repo.path)?;
                repositories::checkout(&local_repo, branch_name).await?;
                for path in &untracked {
                    assert!(path.exists(), "checking out a local branch keeps {path:?}");
                }

                let deep_repo =
                    repositories::deep_clone_url(&remote_repo.remote.url, repo_dir.join("deep"))
                        .await?;
                let untracked = write_untracked_files(&deep_repo.path)?;
                // A branch created after cloning, so the checkout has to fetch it from the remote
                api::client::branches::create_from_branch(
                    &remote_repo,
                    branch_name,
                    DEFAULT_BRANCH_NAME,
                )
                .await?;
                repositories::fetch_all(&deep_repo, &FetchOpts::new()).await?;
                repositories::checkout(&deep_repo, branch_name).await?;
                for path in &untracked {
                    assert!(
                        path.exists(),
                        "checking out a fetched branch keeps {path:?}"
                    );
                }

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    /// Write untracked files at the top of `repo_path`, in a directory, and in a nested directory.
    fn write_untracked_files(repo_path: &Path) -> Result<Vec<PathBuf>, OxenError> {
        let files = [
            repo_path.join("file_1.txt"),
            repo_path.join("dir_1").join("file_in_dir_1.txt"),
            repo_path
                .join("dir_2")
                .join("subdir_2")
                .join("file_in_dir_2.txt"),
            repo_path
                .join("dir_2")
                .join("subdir_2")
                .join("file_in_subdir_2.txt"),
        ];
        for file in &files {
            util::fs::write_to_path(file, "untracked")?;
        }
        Ok(files.to_vec())
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
            let world_file = test::modify_txt_file(world_file, "World modified")?;
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
            test::modify_txt_file(hello_file.clone(), "Hello from feature")?;
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

    /// Asserts that checking out `branch` fails with the conflict error and leaves `path` as it was.
    async fn assert_checkout_refused(
        repo: &LocalRepository,
        branch: &str,
        path: &Path,
        case: &str,
    ) -> Result<(), OxenError> {
        let before = util::fs::read_from_path(path)?;
        let err = repositories::checkout(repo, branch).await.expect_err(case);
        assert!(
            err.to_string().contains("would be overwritten"),
            "{case}: {err}"
        );
        assert_eq!(util::fs::read_from_path(path)?, before, "{case}");
        Ok(())
    }

    // Every case of a non-directory on disk where the target tree has a directory. Checkout
    // replaces it only when it is the from tree's file at that path, unmodified, and otherwise
    // refuses and leaves the working tree as it was. `OnConflict::Overwrite` (`oxen merge
    // --abort`) replaces it regardless, and when the directory is shared between the trees (its
    // hash in common_nodes), restores the whole subtree rather than taking the fast-path exit
    // for a directory already on disk.
    #[tokio::test]
    async fn test_checkout_restores_directory_when_file_exists_at_same_path()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // -- main: "data/" is a directory with two files --
            let dir = repo.path.join("data");
            std::fs::create_dir_all(&dir)?;
            let file1 = dir.join("file1.txt");
            let file2 = dir.join("file2.txt");
            util::fs::write_to_path(&file1, "data file 1")?;
            util::fs::write_to_path(&file2, "data file 2")?;
            repositories::add(&repo, &repo.path).await?;
            let main_commit = repositories::commit(&repo, "Add data directory with files")?;
            let main_branch = repositories::branches::current_branch(&repo)?
                .expect("a new repository is on its default branch");

            // -- dir-x: adds directory "x/", and leaves "data/" unchanged --
            repositories::branches::create_checkout(&repo, "dir-x")?;
            let x = repo.path.join("x");
            util::fs::write_to_path(x.join("y.txt"), "y")?;
            repositories::add(&repo, &repo.path).await?;
            let dir_x_commit = repositories::commit(&repo, "Add x/y.txt")?;

            // -- file-x: replaces directory "x/" with a file "x" --
            repositories::branches::create_checkout(&repo, "file-x")?;
            std::fs::remove_dir_all(&x)?;
            util::fs::write_to_path(&x, "tracked")?;
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Replace x/ with file x")?;

            util::fs::write_to_path(&x, "edited")?;
            assert_checkout_refused(&repo, "dir-x", &x, "a tracked file with local edits blocks")
                .await?;

            // Rewriting the committed bytes changes the mtime, so this exercises the hash check.
            util::fs::write_to_path(&x, "tracked")?;
            repositories::checkout(&repo, "dir-x").await?;
            assert_eq!(util::fs::read_from_path(x.join("y.txt"))?, "y");

            // -- A file with no from-tree file at its path, put in place of the tracked "data/"
            // directory. Checkout handles it exactly as it handles an untracked file --
            std::fs::remove_dir_all(&dir)?;
            util::fs::write_to_path(&dir, "I am a plain file, not a dir")?;
            assert_checkout_refused(
                &repo,
                &main_branch.name,
                &dir,
                "a file in place of a tracked directory blocks it",
            )
            .await?;
            let current = repositories::branches::current_branch(&repo)?
                .expect("a refused checkout leaves HEAD on its branch");
            assert_eq!(current.name, "dir-x");

            repositories::branches::checkout_commit_from_commit(
                &repo,
                &main_commit,
                &Some(dir_x_commit),
                OnConflict::Overwrite,
            )
            .await?;
            assert!(
                dir.is_dir(),
                "data/ should be restored as a directory under Overwrite"
            );
            assert_eq!(util::fs::read_from_path(&file1)?, "data file 1");
            assert_eq!(util::fs::read_from_path(&file2)?, "data file 2");

            Ok(())
        })
        .await
    }
}
