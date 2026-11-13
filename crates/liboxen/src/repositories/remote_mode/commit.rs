use crate::api;
use crate::error::OxenError;
use crate::model::{LocalRepository, NewCommitBody};
use crate::opts::FetchOpts;
use crate::repositories;

use crate::repositories::Commit;

pub async fn commit(
    local_repo: &LocalRepository,
    commit_body: &NewCommitBody,
) -> Result<Commit, OxenError> {
    let workspace_identifier = if local_repo.is_remote_mode() {
        &local_repo.workspace_name.clone().unwrap()
    } else {
        return Err(OxenError::basic_str(
            "Error: Cannot run remote mode commands outside remote mode repo",
        ));
    };

    println!("Committing to remote with message: {}", commit_body.message);
    let Some(branch) = repositories::branches::current_branch(local_repo)? else {
        log::error!("Remote-mode commit No current branch found");
        return Err(OxenError::must_be_on_valid_branch());
    };

    let remote_repo = api::client::repositories::get_default_remote(local_repo).await?;

    // TODO: Do we print successful commit already?
    let commit = api::client::workspaces::commit(
        &remote_repo,
        &branch.name,
        workspace_identifier,
        commit_body,
    )
    .await?;

    // Update local tree
    let fetch_opts = FetchOpts::from_branch(&branch.name);
    repositories::fetch::fetch_branch(local_repo, &fetch_opts).await?;

    Ok(commit)
}

// Actual bugs uncovered:
// 1: The same add one with the untracked files is happenieng here
#[cfg(test)]
mod tests {

    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::repositories::remote_mode;
    use crate::repositories::remote_mode::test_helpers::{add, clone, status};
    use crate::{api, repositories, test, util};

    use crate::config::UserConfig;
    use crate::model::EntryDataType;
    use crate::model::staged_data::StagedDataOpts;
    use std::path::PathBuf;

    // One remote-mode clone of the README remote commits its own way in each case, each case
    // starting from the commits the cases before it made
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_commit() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let repo = clone(&remote_repo, &dir, "new_repo").await?;
                let cfg = UserConfig::get()?;

                // Committing a file at the root and one in a new directory leaves the workspace
                // clean and moves the clone's head to the new commit, and adding a committed file
                // again without modifying it leaves the workspace clean
                let root_file = test::add_txt_file_to_dir(&repo.path, "new file contents")?;
                let dir_path = repo.path.join("dir");
                util::fs::create_dir_all(&dir_path)?;
                let dir_file = test::add_txt_file_to_dir(&dir_path, "hello.txt")?;
                add(&repo, &remote_repo, vec![root_file, dir_file.clone()]).await?;
                let body = NewCommitBody::from_config(&cfg, "Adding new files");
                let commit = remote_mode::commit(&repo, &body).await?;
                let commit_status = status(&repo, &remote_repo).await?;
                commit_status.print();
                assert!(commit_status.is_clean());
                let head_commit = repositories::commits::head_commit(&repo)?;
                assert_eq!(head_commit.id, commit.id);
                add(&repo, &remote_repo, vec![dir_file]).await?;
                assert!(status(&repo, &remote_repo).await?.is_clean());

                // Each of several commits in a row adds one file to the tree and becomes the head,
                // and the history from the first of them to the third lists all three
                let root =
                    repositories::tree::get_root_with_children(&repo, &head_commit)?.unwrap();
                let mut files_in_tree =
                    repositories::tree::list_all_files(&root, &PathBuf::from("."))?;
                let mut previous_head_commit = head_commit;
                let mut commits = vec![];
                for i in 1..=4 {
                    let filename = format!("file_{i}.txt");
                    let file_path = PathBuf::from(&filename);
                    let full_path = repo.path.join(&file_path);
                    let file_content = format!("This is the content for file {i}");

                    test::write_txt_file_to_path(&full_path, &file_content)?;
                    add(&repo, &remote_repo, vec![file_path.clone()]).await?;
                    status(&repo, &remote_repo).await?.print();

                    let commit_message = format!("Adding {filename}");
                    let commit_body = NewCommitBody::from_config(&cfg, &commit_message);
                    let new_commit = remote_mode::commit(&repo, &commit_body).await?;

                    let new_head_commit = repositories::commits::head_commit(&repo)?;
                    assert_eq!(new_commit.id, new_head_commit.id);
                    assert_ne!(previous_head_commit.id, new_head_commit.id);

                    let commit_root =
                        repositories::tree::get_root_with_children(&repo, &new_commit)?.unwrap();
                    let new_files_in_tree =
                        repositories::tree::list_all_files(&commit_root, &PathBuf::from("."))?;
                    assert_eq!(new_files_in_tree.len(), files_in_tree.len() + 1);
                    assert!(repositories::tree::has_path(
                        &repo,
                        &new_head_commit,
                        file_path
                    )?);

                    previous_head_commit = new_head_commit;
                    files_in_tree = new_files_in_tree;
                    commits.push(new_commit);
                }
                let base_commit = &commits[0];
                let head_commit = &commits[2];
                let history = repositories::commits::list_between(&repo, base_commit, head_commit)?;
                assert_eq!(history.len(), 3);
                assert_eq!(history.first().unwrap().message, head_commit.message);
                assert_eq!(history.last().unwrap().message, base_commit.message);

                // An invalid parquet file is committed as a binary file
                let invalid_parquet_file = test::test_invalid_parquet_file();
                let file_path = PathBuf::from("invalid.parquet");
                util::fs::copy(&invalid_parquet_file, repo.path.join(&file_path))?;
                add(&repo, &remote_repo, vec![file_path.clone()]).await?;
                let commit_body = NewCommitBody::from_config(&cfg, "Adding invalid parquet file");
                remote_mode::commit(&repo, &commit_body).await?;
                let head = repositories::commits::head_commit(&repo)?;
                let tree = repositories::tree::get_root_with_children(&repo, &head)?.unwrap();
                let file_node = tree.get_by_path(&file_path)?.unwrap();
                assert_eq!(*file_node.file()?.data_type(), EntryDataType::Binary);

                // A commit with nothing staged fails and adds no commit, even with a file changed
                // in the working tree
                let commit_count = repositories::commits::list(&repo)?.len();
                let readme_path = repo.path.join("README.md");
                util::fs::write_to_path(&readme_path, "changing this guy, but not committing")?;
                let body = NewCommitBody::from_config(&cfg, "Should not work");
                assert!(remote_mode::commit(&repo, &body).await.is_err());
                assert_eq!(repositories::commits::list(&repo)?.len(), commit_count);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    // One remote-mode clone of the bounding box remote commits a file and a directory it removed,
    // then a downloaded file it changed, each case starting from the commits the cases before it
    // made
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_commit_downloaded_and_removed() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let repo = clone(&remote_repo, &dir, "new_repo").await?;
                let cfg = UserConfig::get()?;
                let workspace_id = repo.workspace_name.as_deref().unwrap();
                let directory = ".";

                // A file added and then removed from disk still commits, and committing its
                // staged removal takes it out of the head commit. These cases come before the
                // restore below, so the status the directory case prints lists the clone's
                // `annotations` directory as unsynced.
                let head = repositories::commits::head_commit(&repo)?;
                let entry_count = repositories::entries::list_for_commit(&repo, &head)?.len();
                let hello_file_path = PathBuf::from("hello.txt");
                let full_path = repo.path.join(&hello_file_path);
                test::write_txt_file_to_path(&full_path, "Hello World")?;
                add(&repo, &remote_repo, vec![hello_file_path.clone()]).await?;
                util::fs::remove_file(&full_path)?;
                let commit_body = NewCommitBody::from_config(&cfg, "My message");
                remote_mode::commit(&repo, &commit_body).await?;
                let head = repositories::commits::head_commit(&repo)?;
                let commit_list = repositories::entries::list_for_commit(&repo, &head)?;
                assert_eq!(commit_list.len(), entry_count + 1);
                api::client::workspaces::files::rm_files(
                    &repo,
                    &remote_repo,
                    workspace_id,
                    vec![hello_file_path],
                )
                .await?;
                let commit_body = NewCommitBody::from_config(&cfg, "Second Message");
                remote_mode::commit(&repo, &commit_body).await?;
                let head = repositories::commits::head_commit(&repo)?;
                let commit_list = repositories::entries::list_for_commit(&repo, &head)?;
                assert_eq!(commit_list.len(), entry_count);

                // Committing the staged removal of a committed directory, removed from disk first,
                // takes its files out of the head commit
                let dir_to_remove = PathBuf::from("train");
                let full_dir_path = repo.path.join(&dir_to_remove);
                util::fs::create_dir_all(&full_dir_path)?;
                test::add_txt_file_to_dir(&full_dir_path, "file1.txt")?;
                test::add_txt_file_to_dir(&full_dir_path, "file2.txt")?;
                let dir_file_count = util::fs::rcount_files_in_dir(&full_dir_path);
                add(&repo, &remote_repo, vec![dir_to_remove.clone()]).await?;
                let commit_body = NewCommitBody::from_config(&cfg, "Adding train directory");
                remote_mode::commit(&repo, &commit_body).await?;
                let head = repositories::commits::head_commit(&repo)?;
                let commit_list = repositories::entries::list_for_commit(&repo, &head)?;
                assert_eq!(commit_list.len(), entry_count + dir_file_count);
                util::fs::remove_dir_all(&full_dir_path)?;
                api::client::workspaces::files::rm_files(
                    &repo,
                    &remote_repo,
                    workspace_id,
                    vec![dir_to_remove],
                )
                .await?;
                let status_opts =
                    StagedDataOpts::from_paths_remote_mode(&[PathBuf::from(directory)]);
                let removal_status = repositories::remote_mode::status(
                    &repo,
                    &remote_repo,
                    workspace_id,
                    directory,
                    &status_opts,
                )
                .await?;
                removal_status.print();
                let commit_body = NewCommitBody::from_config(&cfg, "Removing train directory");
                remote_mode::commit(&repo, &commit_body).await?;
                let head = repositories::commits::head_commit(&repo)?;
                let commit_list = repositories::entries::list_for_commit(&repo, &head)?;
                println!("Commit list: {commit_list:?}");
                assert_eq!(commit_list.len(), entry_count);

                // A restored directory leaves nothing untracked or unsynced, and neither does
                // committing a change to a file in it
                let head_commit = repositories::commits::head_commit(&repo)?;
                let annotations_dir = PathBuf::from("annotations");
                remote_mode::restore(
                    &repo,
                    std::slice::from_ref(&annotations_dir),
                    &head_commit.id,
                )
                .await?;
                let restored_status = status(&repo, &remote_repo).await?;
                restored_status.print();
                assert_eq!(restored_status.untracked_files.len(), 0);
                assert_eq!(restored_status.untracked_dirs.len(), 0);
                assert_eq!(restored_status.unsynced_files.len(), 0);
                assert_eq!(restored_status.unsynced_dirs.len(), 0);
                let full_path = repo
                    .path
                    .join("annotations")
                    .join("train")
                    .join("bounding_box.csv");
                test::modify_txt_file(&full_path, "file,label\ntrain/cat_1.jpg,1000")?;
                add(&repo, &remote_repo, vec![full_path]).await?;
                let body = NewCommitBody::from_config(&cfg, "Modifying bounding_box.csv");
                remote_mode::commit(&repo, &body).await?;
                let committed_status = status(&repo, &remote_repo).await?;
                assert_eq!(committed_status.untracked_files.len(), 0);
                assert_eq!(committed_status.untracked_dirs.len(), 0);
                assert_eq!(committed_status.unsynced_files.len(), 0);
                assert_eq!(committed_status.unsynced_dirs.len(), 0);
                // TODO: Download file again to different name
                // TODO: Show that the file contents match new_contents

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
