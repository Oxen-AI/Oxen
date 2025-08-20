//! # oxen download
//!
//! Download a file from the remote repository
//!

use std::path::Path;

use crate::api;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::MetadataEntry;
use crate::model::RemoteRepository;

pub async fn download(
    repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
) -> Result<(), OxenError> {
    download_with_bearer_token(repo, remote_path, local_path, revision, None).await
}

pub async fn download_with_bearer_token(
    repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
    bearer_token: Option<&str>,
) -> Result<(), OxenError> {
    // Ping server telling it we are about to download
    api::client::repositories::pre_download(repo).await?;
    api::client::entries::download_entry_with_bearer_token(
        repo,
        remote_path.as_ref(),
        local_path.as_ref(),
        revision.as_ref(),
        bearer_token,
    )
    .await?;
    // Ping server telling it we finished downloading
    api::client::repositories::post_download(repo).await?;
    Ok(())
}

pub async fn download_dir(
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    match remote_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::download::download_dir(remote_repo, entry, remote_path, local_path)
                .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use crate::constants;

    #[tokio::test]
    async fn test_remote_download_directory() -> Result<(), OxenError> {
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
            let dir = Path::new("train");

            // Download the directory
            let output_dir = repo.path.join("output");
            download(&remote_repo, &dir, &output_dir, &branch.name).await?;

            // Check that the files are there
            for i in 0..num_files {
                let path = output_dir.join("train").join(format!("file_{}.txt", i));
                println!("checking path: {:?}", path);
                assert!(path.exists());
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_download_directory_with_large_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // write text files to dir
            let large_file_dir = PathBuf::from("train").join("large_file");
            let dir = repo.path.join(&large_file_dir);
            util::fs::create_dir_all(&dir)?;

            // Create a large file (> 4 * AVG_CHUNK_SIZE)
            let large_file_path = dir.join("large_file.bin");
            let large_file_size = 4 * constants::AVG_CHUNK_SIZE + 1024 * 1024; // 41MB
            let large_file_data = vec![0u8; large_file_size as usize];
            std::fs::write(&large_file_path, large_file_data)?;

            repositories::add(&repo, &dir).await?;
            repositories::commit(&repo, "adding text files and large file")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Now list the remote
            let branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Download the directory
            let output_dir = repo.path.join("output");
            download(&remote_repo, &large_file_dir, &output_dir, &branch.name).await?;

            // Check that the large file exists and has the correct size
            let downloaded_large_file = output_dir
                .join("train")
                .join("large_file")
                .join("large_file.bin");
            println!("downloaded_large_file: {:?}", downloaded_large_file);
            assert!(downloaded_large_file.exists());
            let metadata = util::fs::metadata(&downloaded_large_file)?;
            assert_eq!(metadata.len(), large_file_size);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_download_directory_local_path() -> Result<(), OxenError> {
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
            let dir = Path::new("train");

            // Download the directory
            let output_dir = Path::new("output");
            repositories::download(&remote_repo, &dir, &output_dir, &branch.name).await?;

            // Check that the files are there
            for i in 0..num_files {
                let path = output_dir.join("train").join(format!("file_{}.txt", i));
                assert!(path.exists());
            }

            util::fs::remove_dir_all(output_dir)?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_download_one_file() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            let file_path = "hello.txt";
            let local_path = &local_repo.path.join(file_path);
            let file_contents = "Hello World";
            util::fs::write_to_path(local_path, file_contents)?;

            repositories::add(&local_repo, local_path).await?;
            repositories::commit(&local_repo, "Added hello.txt")?;

            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, cloned_remote.url())?;
            repositories::push(&local_repo).await?;

            test::run_empty_dir_test_async(|repo_dir| async move {
                let local_path = repo_dir.join("new_name.txt");
                let revision = DEFAULT_BRANCH_NAME;

                download(&remote_repo, file_path, &local_path, revision).await?;

                assert!(local_path.exists());
                assert_eq!(util::fs::read_from_path(&local_path)?, file_contents);

                Ok(())
            })
            .await?;

            Ok(cloned_remote)
        })
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_download_dir() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            let src_path = "train";
            // Count files in local repo
            let og_count = util::fs::rcount_files_in_dir(&local_repo.path.join(src_path));

            test::run_empty_dir_test_async(|repo_dir| async move {
                let dst_path = repo_dir.join("images");
                let revision = DEFAULT_BRANCH_NAME;

                download(&remote_repo, src_path, &dst_path, revision).await?;

                assert!(dst_path.exists());
                let result_dir = &dst_path.join(src_path);
                println!("RESULT DIR: {:?}", result_dir);
                let dl_count = util::fs::rcount_files_in_dir(result_dir);
                assert_eq!(dl_count, og_count);

                Ok(())
            })
            .await?;

            Ok(cloned_remote)
        })
        .await?;

        Ok(())
    }
}
