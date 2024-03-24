//! # oxen upload
//!
//! Upload a file from the remote repository without cloning the whole repository.
//!

use crate::api;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::opts::UploadOpts;

pub async fn upload(repo: &RemoteRepository, opts: &UploadOpts) -> Result<(), OxenError> {
    // Ping server telling it we are about to upload, todo: make configurable
    api::remote::repositories::pre_download(repo).await?;
    api::remote::entries::upload_entries(repo, opts).await?;
    // Ping server telling it we finished uploading, todo: make configurable
    api::remote::repositories::post_download(repo).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::test;
    use crate::util;

    use crate::constants;

    #[tokio::test]
    async fn test_remote_upload_file_to_root_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // write text files to dir
            let dir = repo.path.join("train");
            util::fs::create_dir_all(&dir)?;
            let num_files = 33;
            for i in 0..num_files {
                let path = dir.join(format!("file_{}.txt", i));
                util::fs::write_to_path(&path, format!("lol hi {}", i))?;
            }
            command::add(&repo, &dir)?;
            command::commit(&repo, "adding text files")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Now list the remote
            let branch = api::local::branches::current_branch(&repo)?.unwrap();

            // List the contents of the remote
            let entries =
                api::remote::dir::list(&remote_repo, &branch.name, Path::new(""), 1, 10).await?;

            // Create a new file
            let file = dir.join("new_file.txt");
            util::fs::write_to_path(&file, "new file")?;

            // Upload to root dir
            let opts = UploadOpts {
                paths: vec![file.to_path_buf()],
                dst: Path::new("").to_path_buf(),
                host: remote_repo.host(),
                remote: remote_repo.name.clone(),
                branch: None,
                message: "adding new file".to_string(),
            };
            upload(&remote_repo, &opts).await?;

            // List the contents of the remote
            let new_entries =
                api::remote::dir::list(&remote_repo, &branch.name, Path::new(""), 1, 10).await?;

            // Check that the new file is there
            assert_eq!(entries.entries.len() + 1, new_entries.entries.len());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_upload_file_to_new_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // write text files to dir
            let dir = repo.path.join("train");
            util::fs::create_dir_all(&dir)?;
            let num_files = 5;
            for i in 0..num_files {
                let path = dir.join(format!("file_{}.txt", i));
                util::fs::write_to_path(&path, format!("lol hi {}", i))?;
            }
            command::add(&repo, &dir)?;
            command::commit(&repo, "adding text files")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            command::push(&repo).await?;

            // Now list the remote
            let branch_name = "new_branch".to_string();

            // List the contents of the remote
            let entries =
                api::remote::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, Path::new(""), 1, 10)
                    .await?;

            // Create a new file
            let file = dir.join("new_file.txt");
            util::fs::write_to_path(&file, "new file")?;

            // Upload to root dir
            let opts = UploadOpts {
                paths: vec![file.to_path_buf()],
                dst: Path::new("").to_path_buf(),
                host: remote_repo.host(),
                remote: remote_repo.name.clone(),
                branch: Some(branch_name.clone()),
                message: "adding new file".to_string(),
            };
            upload(&remote_repo, &opts).await?;

            // List the contents of the remote
            let new_entries =
                api::remote::dir::list(&remote_repo, &branch_name, Path::new(""), 1, 10).await?;

            // Check that the new file is there
            assert_eq!(entries.entries.len() + 1, new_entries.entries.len());

            Ok(())
        })
        .await
    }
}
