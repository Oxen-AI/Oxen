use crate::api;
use crate::error::OxenError;
use crate::model::staged_data::StagedDataOpts;
use crate::model::LocalRepository;
use crate::model::RemoteRepository;
use crate::model::StagedData;
use crate::model::StagedEntry;
use crate::model::StagedEntryStatus;

use crate::core::v_latest::status::status_from_opts_and_staged_data;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub async fn status(
    local_repository: &LocalRepository,
    remote_repo: &RemoteRepository,
    workspace_identifier: &str,
    directory: impl AsRef<Path>,
    opts: &StagedDataOpts,
) -> Result<StagedData, OxenError> {
    let page_size = opts.limit;
    let page_num = opts.skip / page_size;

    let remote_status = api::client::workspaces::changes::list(
        remote_repo,
        workspace_identifier,
        directory,
        page_num,
        page_size,
    )
    .await?;

    let mut status = StagedData::empty();
    status.staged_dirs = remote_status.added_dirs;

    let added_files: HashMap<PathBuf, StagedEntry> =
        HashMap::from_iter(remote_status.added_files.entries.into_iter().map(|e| {
            (
                PathBuf::from(e.filename()),
                StagedEntry::empty_status(StagedEntryStatus::Added),
            )
        }));
    let added_mods: HashMap<PathBuf, StagedEntry> =
        HashMap::from_iter(remote_status.modified_files.entries.into_iter().map(|e| {
            (
                PathBuf::from(e.filename()),
                StagedEntry::empty_status(StagedEntryStatus::Modified),
            )
        }));
    let staged_removals: HashMap<PathBuf, StagedEntry> =
        HashMap::from_iter(remote_status.removed_files.entries.into_iter().map(|e| {
            (
                PathBuf::from(e.filename()),
                StagedEntry::empty_status(StagedEntryStatus::Removed),
            )
        }));
    status.staged_files = added_files
        .into_iter()
        .chain(added_mods)
        .chain(staged_removals)
        .collect();

    // Get local status
    let is_remote = false;
    let local_opts = StagedDataOpts {
        paths: opts.paths.clone(),
        skip: opts.skip,
        limit: opts.limit,
        print_all: opts.print_all,
        is_remote,
        ignore: None,
    };

    status_from_opts_and_staged_data(local_repository, &local_opts, &mut status)?;

    Ok(status)
}

#[cfg(test)]
mod tests {
    use crate::test;

    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::model::staged_data::StagedDataOpts;
    use crate::opts::clone_opts::CloneOpts;

    use crate::{api, repositories};

    // For reference, the fully synced repo structure is as follows:
    // nlp/
    //   classification/
    //     annotations/
    //       train.tsv
    //       test.tsv
    //
    // train/
    //   dog_1.jpg
    //   dog_2.jpg
    //   dog_3.jpg
    //   cat_1.jpg
    //   cat_2.jpg
    // test/
    //   1.jpg
    //   2.jpg
    // annotations/
    //   README.md
    //   train/
    //     bounding_box.csv
    //     one_shot.csv
    //     two_shot.csv
    //     annotations.txt
    //   test/
    //     annotations.csv
    // prompts.jsonl
    // labels.txt
    // LICENSE
    // README.md

    #[tokio::test]
    async fn test_repo_clean_with_all_files_unsynced_after_remote_mode_clone(
    ) -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts =
                    StagedDataOpts::from_paths_remote_mode(&[PathBuf::from(directory.clone())]);
                let status = repositories::remote_mode::status(
                    &cloned_repo,
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    &status_opts,
                )
                .await?;
                status.print();
                // Files/dirs in subdirs don't appear as separate items in unsynced_files/dirs
                assert_eq!(status.unsynced_dirs.len(), 4);
                assert_eq!(status.unsynced_files.len(), 4);

                // The repo is clean
                assert!(status.is_clean());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_mode_subdirectory_status() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let repo_path = cloned_repo.path.clone();

                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[repo_path.clone()]);
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let status = repositories::remote_mode::status(
                    &cloned_repo,
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    &status_opts,
                )
                .await?;
                status.print();

                // Files/dirs in subdirs don't appear as separate items in unsynced_files/dirs
                assert_eq!(status.unsynced_dirs.len(), 4);
                assert_eq!(status.unsynced_files.len(), 4);

                // Download specific files from the remote
                let subdir_path = PathBuf::from("annotations").join("train");
                let one_shot_path = subdir_path.join("one_shot.csv");
                let two_shot_path = subdir_path.join("two_shot.csv");
                let bounding_box_path = subdir_path.join("bounding_box.csv");

                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                repositories::remote_mode::restore(
                    &cloned_repo,
                    &[one_shot_path.clone()],
                    &head_commit.id,
                )
                .await?;
                repositories::remote_mode::restore(
                    &cloned_repo,
                    &[two_shot_path.clone()],
                    &head_commit.id,
                )
                .await?;
                repositories::remote_mode::restore(
                    &cloned_repo,
                    &[bounding_box_path.clone()],
                    &head_commit.id,
                )
                .await?;

                // Modify one_shot.csv
                let new_content = "new content coming in hot";
                test::modify_txt_file(cloned_repo.path.join(&one_shot_path), new_content)?;

                // Modify and add two_shot.csv
                let new_content = "new content coming in even hotter!";
                test::modify_txt_file(cloned_repo.path.join(&two_shot_path), new_content)?;
                api::client::workspaces::files::add(
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    vec![two_shot_path.clone()],
                    &Some(cloned_repo.clone()),
                )
                .await?;

                // Remove bounding_box.csv
                api::client::workspaces::files::rm_files(
                    &cloned_repo,
                    &remote_repo,
                    &workspace_identifier,
                    vec![bounding_box_path.clone()],
                )
                .await?;

                // Check status for corresponding changes
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[repo_path.clone()]);
                let status = repositories::remote_mode::status(
                    &cloned_repo,
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    &status_opts,
                )
                .await?;
                status.print();

                // 6 unsynced files, as creating the parent dirs for the restored files causes more subfiles to be registed as unsynced
                assert_eq!(status.unsynced_dirs.len(), 4);
                assert_eq!(status.unsynced_files.len(), 6);

                assert_eq!(status.modified_files.len(), 1);
                assert!(status.modified_files.contains(&one_shot_path));

                assert_eq!(status.staged_files.len(), 2);
                assert!(status.staged_files.contains_key(&two_shot_path));
                assert!(status.staged_files.contains_key(&bounding_box_path));

                // Stage the subdirectory itself
                api::client::workspaces::files::add(
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    vec![subdir_path.clone()],
                    &Some(cloned_repo.clone()),
                )
                .await?;

                // Re-check status
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[repo_path.clone()]);
                let status = repositories::remote_mode::status(
                    &cloned_repo,
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    &status_opts,
                )
                .await?;
                status.print();

                assert_eq!(status.unsynced_dirs.len(), 4);
                assert_eq!(status.unsynced_files.len(), 6);
                assert_eq!(status.staged_files.len(), 3);
                assert_eq!(status.modified_files.len(), 0);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // NOTE: With the current workspace::changes::status command used in remote_mode::status,
    //       We cannot detect moved files accurately, as it does not return the hashes of the staged entries
    //       TODO: Consider fixing this

    /*
    #[tokio::test]
    async fn test_remote_mode_status_move_regular_file() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                // Clone repo in remote mode
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                assert!(cloned_repo.is_remote_mode());

                let repo_path = cloned_repo.path.clone();
                log::debug!("Cloned repo path: {:?}", cloned_repo.path.canonicalize());
                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                let head_commit = repositories::commits::head_commit(&cloned_repo)?;

                let og_basename = PathBuf::from("README.md");
                repositories::remote_mode::restore(&cloned_repo, &vec![og_basename.clone()], &head_commit.id).await?;

                let og_file = cloned_repo.path.join(&og_basename);
                let new_basename = PathBuf::from("README2.md");
                let new_file = cloned_repo.path.join(&new_basename);

                util::fs::rename(&og_file, &new_file)?;

                // Status before adding should show 4 unsynced files (README.md,  LICENSE, prompts.jsonl, labels.txt) and an untracked file
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[repo_path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert_eq!(status.moved_files.len(), 0);
                assert_eq!(status.unsynced_files.len(), 4);
                assert_eq!(status.untracked_files.len(), 1);

                // Remove the previous file
                api::client::workspaces::files::rm_files(&cloned_repo, &remote_repo, &workspace_identifier, vec![og_basename.clone()]).await?;
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[repo_path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert_eq!(status.moved_files.len(), 0);
                assert_eq!(status.staged_files.len(), 1);
                assert_eq!(status.untracked_files.len(), 1);

                // Add the new file to complete the pair
                api::client::workspaces::files::add(&cloned_repo, &remote_repo, &workspace_identifier, &directory, vec![new_basename.clone()]).await?;
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[repo_path]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                assert_eq!(status.moved_files.len(), 1);
                assert_eq!(status.staged_files.len(), 2);

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }
    */
}
