// For unit test organization only
pub mod add;
pub mod rm;

// Commands with unique logic
pub mod checkout;
pub use checkout::checkout;
pub use checkout::create_checkout;
pub use checkout::create_checkout_branch;

pub mod commit;
pub use commit::commit;

pub mod restore;
pub use restore::restore;

pub mod status;
pub use status::status;

// pub mod sync;
// pub use sync::sync;

/// Steps the remote-mode tests share: a remote-mode clone, its workspace's status, and an add to
/// its workspace
#[cfg(test)]
mod test_helpers {
    use std::path::{Path, PathBuf};

    use crate::api;
    use crate::error::OxenError;
    use crate::model::staged_data::StagedDataOpts;
    use crate::model::{LocalRepository, RemoteRepository, StagedData};
    use crate::opts::clone_opts::CloneOpts;
    use crate::repositories;

    /// Clones `remote_repo` in remote mode into `dir/name`
    pub(super) async fn clone(
        remote_repo: &RemoteRepository,
        dir: &Path,
        name: &str,
    ) -> Result<LocalRepository, OxenError> {
        let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join(name));
        opts.is_remote = true;
        let cloned_repo = repositories::clone(&opts).await?;
        assert!(cloned_repo.is_remote_mode());
        Ok(cloned_repo)
    }

    /// The status of `repo`'s workspace on `remote_repo`, over the whole repository
    pub(super) async fn status(
        repo: &LocalRepository,
        remote_repo: &RemoteRepository,
    ) -> Result<StagedData, OxenError> {
        let workspace_identifier = repo.workspace_name.as_deref().unwrap();
        let opts = StagedDataOpts::from_paths_remote_mode(std::slice::from_ref(&repo.path));
        repositories::remote_mode::status(repo, remote_repo, workspace_identifier, ".", &opts).await
    }

    /// Adds `paths` to `repo`'s workspace on `remote_repo`, failing if any file does not upload
    pub(super) async fn add(
        repo: &LocalRepository,
        remote_repo: &RemoteRepository,
        paths: Vec<PathBuf>,
    ) -> Result<(), OxenError> {
        let workspace_identifier = repo.workspace_name.as_deref().unwrap();
        let failures = api::client::workspaces::files::add(
            remote_repo,
            workspace_identifier,
            ".",
            paths,
            &Some(repo.clone()),
        )
        .await?;
        assert!(failures.is_empty(), "files failed to upload: {failures:?}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::model::staged_data::StagedDataOpts;
    use crate::opts::clone_opts::CloneOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_list_tabular_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            // Replace the first commit's file, so its version file is only in history
            let bounding_box_path = local_repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            test::modify_txt_file(
                &bounding_box_path,
                "file,label,min_x,min_y,width,height\ntrain/dog_1.jpg,dog,101.5,32.0,385,330\n",
            )?;
            repositories::add(&local_repo, &bounding_box_path).await?;
            repositories::commit(&local_repo, "Keep one bounding box")?;
            repositories::push(&local_repo).await?;

            let local_head_commit = repositories::commits::head_commit(&local_repo)?;
            let local_root =
                repositories::tree::get_root_with_children(&local_repo, &local_head_commit)?;

            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let remote_mode_repo = repositories::clone(&opts).await?;
                assert!(remote_mode_repo.is_remote_mode());

                let cloned_head_commit = repositories::commits::head_commit(&remote_mode_repo)?;
                let cloned_root = repositories::tree::get_root_with_children(
                    &remote_mode_repo,
                    &cloned_head_commit,
                )?;
                assert_eq!(
                    local_root, cloned_root,
                    "the clone has the pushed merkle tree"
                );

                let versions_dir = util::fs::oxen_hidden_dir(&remote_mode_repo.path)
                    .join(constants::VERSIONS_DIR)
                    .join(constants::OBJECT_FILES_DIR);
                let mut versions_iter = std::fs::read_dir(versions_dir)?;
                assert!(
                    versions_iter.next().is_none(),
                    "the clone downloads no version files, from HEAD or from history"
                );

                let workspace_identifier = remote_mode_repo.workspace_name.clone().unwrap();
                let workspace =
                    api::client::workspaces::get_by_name(&remote_repo, &workspace_identifier)
                        .await?
                        .expect("a remote-mode clone creates its workspace");
                assert_eq!(
                    workspace.name,
                    Some(workspace_identifier.clone()),
                    "a remote-mode clone names its workspace"
                );

                let directory = ".".to_string();

                // Create a deeply nested directory
                let dir_path = remote_mode_repo
                    .path
                    .join("data")
                    .join("train")
                    .join("images")
                    .join("cats");
                util::fs::create_dir_all(&dir_path)?;

                // Add two tabular files
                let cats_tsv = dir_path.join("cats.tsv");
                util::fs::write(&cats_tsv, "1\t2\t3\nhello\tworld\tsup\n")?;
                let dogs_csv = dir_path.join("dogs.csv");
                util::fs::write(&dogs_csv, "1,2,3\nhello,world,sup\n")?;

                // Add a non-tabular file
                let readme_md = dir_path.join("README.md");
                util::fs::write(&readme_md, "readme....")?;

                // Add and commit all
                let files_to_add = vec![cats_tsv, dogs_csv, readme_md];
                api::client::workspaces::files::add(
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    files_to_add,
                    &Some(remote_mode_repo.clone()),
                )
                .await?;

                let commit_body =
                    NewCommitBody::from_config(&UserConfig::get()?, "Adding tabular data");
                repositories::remote_mode::commit(&remote_mode_repo, &commit_body).await?;

                // List files and verify the count
                let new_head = repositories::commits::head_commit(&remote_mode_repo)?;
                let new_files =
                    repositories::tree::list_tabular_files_in_repo(&remote_mode_repo, &new_head)?;
                assert_eq!(new_files.len(), 3);

                // Pull with the original repo and verify the count is the same
                repositories::pull(&local_repo).await?;
                let local_repo_head = repositories::commits::head_commit(&local_repo)?;
                let files =
                    repositories::tree::list_tabular_files_in_repo(&local_repo, &local_repo_head)?;

                assert_eq!(files.len(), 3);
                assert_eq!(local_repo_head.id, new_head.id);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_merkle_two_files_same_hash() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let remote_mode_repo = repositories::clone(&opts).await?;
                assert!(remote_mode_repo.is_remote_mode());

                let workspace_identifier = remote_mode_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();

                let p1 = PathBuf::from("hi.txt");
                let p2 = PathBuf::from("bye.txt");
                let full_path_1 = remote_mode_repo.path.join(&p1);
                let full_path_2 = remote_mode_repo.path.join(&p2);
                let common_contents = "the same file";

                test::write_txt_file_to_path(&full_path_1, common_contents)?;
                test::write_txt_file_to_path(&full_path_2, common_contents)?;

                // Add both files
                api::client::workspaces::files::add(
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    vec![p1.clone(), p2.clone()],
                    &Some(remote_mode_repo.clone()),
                )
                .await?;

                // Check status to verify both are staged
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[p1.clone(), p2.clone()]);
                let status = repositories::remote_mode::status(
                    &remote_mode_repo,
                    &remote_repo,
                    &workspace_identifier,
                    &directory,
                    &status_opts,
                )
                .await?;
                assert_eq!(status.staged_files.len(), 2);
                assert!(status.staged_files.contains_key(&p1));
                assert!(status.staged_files.contains_key(&p2));

                // Commit the files
                let commit_body = NewCommitBody::from_config(
                    &UserConfig::get()?,
                    "Add two files with same content",
                );

                let commit =
                    repositories::remote_mode::commit(&remote_mode_repo, &commit_body).await?;
                let head_commit = repositories::commits::head_commit(&remote_mode_repo)?;

                // Verify the new commit contains both paths
                assert!(repositories::tree::has_path(
                    &remote_mode_repo,
                    &commit,
                    p1.clone()
                )?);
                assert!(repositories::tree::has_path(
                    &remote_mode_repo,
                    &head_commit,
                    p2.clone()
                )?);

                // Pull with the original repo and verify it also contains both paths
                repositories::pull(&local_repo).await?;

                let local_repo_head = repositories::commits::head_commit(&local_repo)?;
                assert!(repositories::tree::has_path(
                    &local_repo,
                    &local_repo_head,
                    p1.clone()
                )?);
                assert!(repositories::tree::has_path(
                    &local_repo,
                    &local_repo_head,
                    p2.clone()
                )?);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
