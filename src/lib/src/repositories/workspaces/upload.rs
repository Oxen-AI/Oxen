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
    api::client::repositories::pre_upload(repo).await?;
    api::client::entries::upload_entries(repo, opts).await?;
    // Ping server telling it we finished uploading, todo: make configurable
    api::client::repositories::post_upload(repo).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::model::EntryDataType;
    use crate::repositories;
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
            repositories::add(&repo, &dir).await?;
            repositories::commit(&repo, "adding text files")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Now list the remote
            let branch = repositories::branches::current_branch(&repo)?.unwrap();

            // List the contents of the remote
            let entries =
                api::client::dir::list(&remote_repo, &branch.name, Path::new(""), 1, 10).await?;

            // Create a new file
            let file = dir.join("new_file.txt");
            util::fs::write_to_path(&file, "new file")?;

            // Upload to root dir
            let opts = UploadOpts {
                paths: vec![file.to_path_buf()],
                dst: Path::new("").to_path_buf(),
                host: remote_repo.host(),
                scheme: remote_repo.scheme(),
                remote: remote_repo.name.clone(),
                branch: None,
                message: "adding new file".to_string(),
                bearer_token: None,
            };
            upload(&remote_repo, &opts).await?;

            // List the contents of the remote
            let new_entries =
                api::client::dir::list(&remote_repo, &branch.name, Path::new(""), 1, 10).await?;

            // Check that the new file is there
            assert_eq!(entries.entries.len() + 1, new_entries.entries.len());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_upload_file_to_sub_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // write text files to dir
            let dir = repo.path.join("train");
            util::fs::create_dir_all(&dir)?;
            let num_files = 5;
            for i in 0..num_files {
                let path = dir.join(format!("file_{}.txt", i));
                util::fs::write_to_path(&path, format!("lol hi {}", i))?;
            }
            repositories::add(&repo, &dir).await?;
            repositories::commit(&repo, "adding text files")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Now list the remote
            let branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create a new file
            let file = dir.join("new_file.jsonl");
            util::fs::write_to_path(&file, "{ \"question\": \"what is the best data version control system?\", \"answer\": \"oxen\" }\n{ \"question\": \"what is the best data version control tool?\", \"answer\": \"oxen.ai\" }\n")?;

            // Upload to root dir
            let opts = UploadOpts {
                paths: vec![file.to_path_buf()],
                dst: Path::new("test").join("ing").join("data").to_path_buf(),
                host: remote_repo.host(),
                scheme: remote_repo.scheme(),
                remote: remote_repo.name.clone(),
                branch: None,
                message: "adding new file".to_string(),
                bearer_token: None,
            };
            upload(&remote_repo, &opts).await?;

            // List the contents of the remote
            let new_entries =
                api::client::dir::list(&remote_repo, &branch.name, Path::new("test/ing/data"), 1, 10).await?;

            // Check that the new file is there
            assert_eq!(1, new_entries.entries.len());

            let file_path = "test/ing/data/new_file.jsonl";
            let entry = api::client::entries::get_entry(&remote_repo, file_path, &branch.name).await?;
            assert!(entry.is_some());
            let entry = entry.unwrap();
            assert_eq!(entry.filename(), "new_file.jsonl");
            assert_eq!(entry.data_type(), EntryDataType::Tabular);

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
            repositories::add(&repo, &dir).await?;
            repositories::commit(&repo, "adding text files")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Now list the remote
            let branch_name = "new_branch".to_string();

            // List the contents of the remote
            let entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, Path::new(""), 1, 10)
                    .await?;

            // Create a new file
            let file = dir.join("new_file.txt");
            util::fs::write_to_path(&file, "new file")?;

            // Upload to root dir
            let opts = UploadOpts {
                paths: vec![file.to_path_buf()],
                dst: Path::new("").to_path_buf(),
                host: remote_repo.host(),
                scheme: remote_repo.scheme(),
                remote: remote_repo.name.clone(),
                branch: Some(branch_name.clone()),
                message: "adding new file".to_string(),
                bearer_token: None,
            };
            upload(&remote_repo, &opts).await?;

            // List the contents of the remote
            let new_entries =
                api::client::dir::list(&remote_repo, &branch_name, Path::new(""), 1, 10).await?;

            // Check that the new file is there
            assert_eq!(entries.entries.len() + 1, new_entries.entries.len());

            Ok(())
        })
        .await
    }
}
