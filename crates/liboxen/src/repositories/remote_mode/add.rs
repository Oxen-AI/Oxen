// Placeholder file, for unit test organization only
// `oxen add` currently does not have any unique logic in remote-mode

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::config::UserConfig;
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::model::NewCommitBody;
    use crate::model::StagedData;
    use crate::model::staged_data::StagedDataOpts;
    use crate::opts::clone_opts::CloneOpts;
    use crate::{api, repositories, test, util};

    // TODO: Actual bugs unconvered:
    // 1: Err_files
    // 2: Adding modified files

    // Remote-mode clones of one remote each add files their own way to their own workspace, and
    // the last one commits and adds the committed file again
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_mode_add() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|dir| async move {
                let clone = async |name: &str| -> Result<LocalRepository, OxenError> {
                    let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join(name));
                    opts.is_remote = true;
                    let cloned_repo = repositories::clone(&opts).await?;
                    assert!(cloned_repo.is_remote_mode());
                    Ok(cloned_repo)
                };
                let status = async |repo: &LocalRepository| -> Result<StagedData, OxenError> {
                    let workspace_identifier = repo.workspace_name.clone().unwrap();
                    let opts =
                        StagedDataOpts::from_paths_remote_mode(std::slice::from_ref(&repo.path));
                    repositories::remote_mode::status(
                        repo,
                        &remote_repo,
                        &workspace_identifier,
                        ".",
                        &opts,
                    )
                    .await
                };
                let add = async |repo: &LocalRepository, paths: Vec<PathBuf>| {
                    let workspace_identifier = repo.workspace_name.clone().unwrap();
                    let failures = api::client::workspaces::files::add(
                        &remote_repo,
                        &workspace_identifier,
                        ".",
                        paths,
                        &Some(repo.clone()),
                    )
                    .await?;
                    assert!(failures.is_empty(), "files failed to upload: {failures:?}");
                    Ok::<(), OxenError>(())
                };

                // Files added by their full path, their path relative to the repo, and their
                // canonical path move from untracked to staged
                let paths_repo = clone("paths_repo").await?;
                let full_path = test::add_txt_file_to_dir(&paths_repo.path, "full path")?;
                let paths_status = status(&paths_repo).await?;
                paths_status.print();
                assert_eq!(paths_status.untracked_files.len(), 1);
                let relative_path = util::fs::path_relative_to_dir(
                    test::add_txt_file_to_dir(&paths_repo.path, "relative path")?,
                    &paths_repo.path,
                )?;
                let canon_path = util::fs::canonicalize(test::add_txt_file_to_dir(
                    &paths_repo.path,
                    "canonical path",
                )?)?;
                add(&paths_repo, vec![full_path, relative_path, canon_path]).await?;
                let paths_status = status(&paths_repo).await?;
                paths_status.print();
                assert_eq!(paths_status.untracked_files.len(), 0);
                assert_eq!(paths_status.staged_files.len(), 3);

                // A file in a new subdirectory is added, modified, and added again
                let subdir_repo = clone("subdir_repo").await?;
                let file_path = PathBuf::from("new").join("dir").join("new_file.csv");
                let full_path = subdir_repo.path.join(&file_path);
                util::fs::write_to_path(
                    &full_path,
                    "cateory_1,category_2,category_3\nnew,file,contents",
                )?;
                // Printing this case's statuses runs the remote-mode printer over an untracked
                // directory and over a staged file, which no other test here reaches
                let print_opts =
                    StagedDataOpts::from_paths_remote_mode(std::slice::from_ref(&subdir_repo.path));
                let subdir_status = status(&subdir_repo).await?;
                subdir_status.print_with_params(&print_opts);
                assert_eq!(subdir_status.staged_files.len(), 0);
                assert_eq!(subdir_status.staged_dirs.len(), 0);
                assert_eq!(subdir_status.modified_files.len(), 0);
                assert_eq!(subdir_status.untracked_dirs.len(), 1);
                add(&subdir_repo, vec![full_path.clone()]).await?;
                let subdir_status = status(&subdir_repo).await?;
                subdir_status.print_with_params(&print_opts);
                assert_eq!(subdir_status.staged_files.len(), 1);
                assert_eq!(subdir_status.staged_dirs.len(), 1);
                assert_eq!(subdir_status.modified_files.len(), 0);
                assert_eq!(subdir_status.untracked_dirs.len(), 0);
                assert!(subdir_status.staged_files.contains_key(&file_path));
                test::modify_txt_file(&full_path, "file,label\ntrain/cat_1.jpg,1000")?;
                add(&subdir_repo, vec![full_path.clone()]).await?;
                let subdir_status = status(&subdir_repo).await?;
                assert_eq!(subdir_status.staged_files.len(), 1);
                assert_eq!(subdir_status.staged_dirs.len(), 1);
                // Note: While status should, in this case, display both a staged and a modified
                //       file, the current logic for regular status does not detect the modified
                //       file, so this can be skipped for now
                //
                // assert_eq!(subdir_status.modified_files.len(), 1);
                // assert!(subdir_status.modified_files.contains(&file_path));
                assert_eq!(subdir_status.untracked_dirs.len(), 0);
                add(&subdir_repo, vec![full_path]).await?;
                let subdir_status = status(&subdir_repo).await?;
                assert_eq!(subdir_status.staged_files.len(), 1);
                assert_eq!(subdir_status.staged_dirs.len(), 1);
                assert_eq!(subdir_status.modified_files.len(), 0);
                assert_eq!(subdir_status.untracked_dirs.len(), 0);

                // Three files in one subdirectory added together. Status shows an untracked
                // directory with its item count, rather than the files, until they are staged.
                let multiple_repo = clone("multiple_repo").await?;
                let sub_dir = multiple_repo.path.join("training_data");
                util::fs::create_dir_all(&sub_dir)?;
                let sub_files = vec![
                    test::add_txt_file_to_dir(&sub_dir, "Hello")?,
                    test::add_txt_file_to_dir(&sub_dir, "World")?,
                    test::add_txt_file_to_dir(&sub_dir, "!")?,
                ];
                let multiple_status = status(&multiple_repo).await?;
                assert_eq!(multiple_status.untracked_dirs.len(), 1);
                assert_eq!(multiple_status.untracked_dirs[0].1, 3);
                assert_eq!(multiple_status.staged_files.len(), 0);
                add(&multiple_repo, sub_files).await?;
                let multiple_status = status(&multiple_repo).await?;
                assert_eq!(multiple_status.untracked_dirs.len(), 0);
                assert_eq!(multiple_status.staged_files.len(), 3);

                // `*` adds every file at the root and in a subdirectory, and `n*` then adds a new
                // file and a new directory while leaving another new file out
                let glob_repo = clone("glob_repo").await?;
                test::populate_dir_with_txt_files(&glob_repo.path, "root", 3)?;
                test::populate_dir_with_txt_files(glob_repo.path.join("training_data"), "sub", 3)?;
                add(&glob_repo, vec![PathBuf::from("*")]).await?;
                let glob_status = status(&glob_repo).await?;
                assert_eq!(glob_status.untracked_files.len(), 0);
                assert_eq!(glob_status.untracked_dirs.len(), 0);
                assert_eq!(glob_status.staged_files.len(), 6);
                // This includes the root dir
                assert_eq!(glob_status.staged_dirs.len(), 2);
                test::write_txt_file_to_path(glob_repo.path.join("new_file.txt"), "new file")?;
                test::write_txt_file_to_path(glob_repo.path.join("excluded.txt"), "excluded")?;
                test::populate_dir_with_txt_files(glob_repo.path.join("new_dir"), "embedded", 1)?;
                add(&glob_repo, vec![PathBuf::from("n*")]).await?;
                let glob_status = status(&glob_repo).await?;
                assert_eq!(glob_status.untracked_files.len(), 1);
                assert_eq!(glob_status.untracked_dirs.len(), 0);
                assert_eq!(glob_status.staged_files.len(), 8);
                // This includes the root dir
                assert_eq!(glob_status.staged_dirs.len(), 3);

                // Adding a directory adds everything under it
                let recursive_repo = clone("recursive_repo").await?;
                let new_dir = recursive_repo.path.join("new");
                test::populate_dir_with_txt_files(new_dir.join("train"), "train", 2)?;
                test::populate_dir_with_txt_files(new_dir.join("test"), "test", 1)?;
                test::add_txt_file_to_dir(&new_dir, "text 1")?;
                add(&recursive_repo, vec![new_dir]).await?;
                let recursive_status = status(&recursive_repo).await?;
                assert_eq!(recursive_status.staged_dirs.len(), 3);
                assert_eq!(recursive_status.staged_files.len(), 4);

                // Adding a committed file again without modifying it leaves the repo clean
                let commit_repo = clone("commit_repo").await?;
                let dir_path = commit_repo.path.join("dir");
                util::fs::create_dir_all(&dir_path)?;
                let hello_file_path = test::add_txt_file_to_dir(&dir_path, "hello.txt")?;
                add(&commit_repo, vec![hello_file_path.clone()]).await?;
                let cfg = UserConfig::get()?;
                let body = NewCommitBody {
                    message: "Add text file".to_string(),
                    author: cfg.name,
                    email: cfg.email,
                };
                repositories::remote_mode::commit(&commit_repo, &body).await?;
                add(&commit_repo, vec![hello_file_path]).await?;
                assert!(status(&commit_repo).await?.is_clean());

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
