//! # oxen workspace ls
//!
//! List files in a remote repository branch
//!

use std::path::Path;

use crate::api;
use crate::error::OxenError;
use crate::model::{Branch, RemoteRepository};
use crate::opts::PaginateOpts;
use crate::view::PaginatedDirEntries;

pub async fn ls(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    directory: &Path,
    opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    api::client::dir::list(
        remote_repo,
        &branch.name,
        directory,
        opts.page_num,
        opts.page_size,
    )
    .await
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::opts::PaginateOpts;
    use crate::test;
    use crate::util;
    use crate::view::DataTypeCount;

    use std::path::Path;

    #[tokio::test]
    async fn test_remote_ls_ten_items() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // Create 8 directories
            for n in 0..8 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(&filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(&filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            repositories::commit(&repo, "Adding all the data")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Now list the remote
            let branch = repositories::branches::current_branch(&repo)?.unwrap();
            let dir = Path::new(".");
            let opts = PaginateOpts {
                page_num: 1,
                page_size: 10,
            };
            let paginated = repositories::workspaces::ls(&remote_repo, &branch, dir, &opts).await?;
            assert_eq!(paginated.entries.len(), 10);
            assert_eq!(paginated.page_number, 1);
            assert_eq!(paginated.page_size, 10);
            assert_eq!(paginated.total_pages, 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_ls_return_data_types() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // Add one video, one markdown file, and one text file
            let video_path = test::test_video_file_with_name("basketball.mp4");
            let markdown_path = test::test_text_file_with_name("README.md");
            let text_path = test::test_text_file_with_name("hello.txt");
            util::fs::copy(video_path, repo.path.join("basketball.mp4"))?;
            util::fs::copy(markdown_path, repo.path.join("README.md"))?;
            util::fs::copy(text_path, repo.path.join("hello.txt"))?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            repositories::commit(&repo, "Adding all the data")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Now list the remote
            let branch = repositories::branches::current_branch(&repo)?.unwrap();
            let dir = Path::new("");
            let opts = PaginateOpts {
                page_num: 1,
                page_size: 10,
            };
            let paginated = repositories::workspaces::ls(&remote_repo, &branch, dir, &opts).await?;

            // serialize into an array of DataTypeCount
            let metadata = paginated.metadata.unwrap();
            let data_type_counts: Vec<DataTypeCount> = metadata.dir.data_types;

            let data_type_count_text = data_type_counts
                .iter()
                .find(|&x| x.data_type == "text")
                .unwrap();
            let data_type_count_video = data_type_counts
                .iter()
                .find(|&x| x.data_type == "video")
                .unwrap();

            assert_eq!(data_type_count_text.count, 2);
            assert_eq!(data_type_count_video.count, 1);

            // serialize into an array of MimeDataTypeCount
            /*
            let mime_type_counts: Vec<MimeTypeCount> = metadata.mime_types).unwrap();

            let count_markdown = mime_type_counts
                .iter()
                .find(|&x| x.mime_type == "text/markdown")
                .unwrap();
            let count_video = mime_type_counts
                .iter()
                .find(|&x| x.mime_type == "video/mp4")
                .unwrap();
            let count_text = mime_type_counts
                .iter()
                .find(|&x| x.mime_type == "text/plain")
                .unwrap();

            assert_eq!(count_markdown.count, 1);
            assert_eq!(count_video.count, 1);
            assert_eq!(count_text.count, 1);
            */

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_ls_return_data_types_just_top_level_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut repo| async move {
            // write text files to dir
            let dir = repo.path.join("train");
            util::fs::create_dir_all(&dir)?;
            let num_files = 33;
            for i in 0..num_files {
                let path = dir.join(format!("file_{}.txt", i));
                util::fs::write_to_path(&path, format!("lol hi {}", i))?;
            }
            repositories::add(&repo, &dir)?;
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
            let dir = Path::new("");
            let opts = PaginateOpts {
                page_num: 1,
                page_size: 10,
            };
            let paginated = repositories::workspaces::ls(&remote_repo, &branch, dir, &opts).await?;

            // serialize into an array of DataTypeCount
            let metadata = paginated.metadata.unwrap();
            let data_type_counts: Vec<DataTypeCount> = metadata.dir.data_types;

            let data_type_count_text = data_type_counts
                .iter()
                .find(|&x| x.data_type == "text")
                .unwrap();

            assert_eq!(data_type_count_text.count, num_files);

            /*
            // serialize into an array of MimeDataTypeCount
            let mime_type_counts: Vec<MimeTypeCount> =
                serde_json::from_value(metadata.mime_types.data).unwrap();

            let count_text = mime_type_counts
                .iter()
                .find(|&x| x.mime_type == "text/plain")
                .unwrap();

            assert_eq!(count_text.count, num_files);
            */

            Ok(())
        })
        .await
    }
}
