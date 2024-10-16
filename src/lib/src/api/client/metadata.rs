//! Fetch metadata about a resource from the remote.
//!

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::MetadataEntryResponse;

use std::path::Path;

/// Get the metadata about a resource from the remote.
pub async fn get_file(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<MetadataEntryResponse, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/meta/{}/{}", revision, path);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    Ok(serde_json::from_str(&body)?)
}

#[cfg(test)]
mod tests {

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::test;
    use crate::view::MetadataEntryResponse;
    use crate::{api, repositories};

    use std::path::Path;

    #[tokio::test]
    async fn test_get_file_entry() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let path = Path::new("annotations").join("README.md");
            let revision = DEFAULT_BRANCH_NAME;

            let head = repositories::commits::head_commit(&local_repo)?;

            // Try to get the entry from the local repo
            let tree = repositories::tree::get_by_commit(&local_repo, &head)?;
            let entry = tree.get_by_path(&path)?;
            assert!(entry.is_some());

            let entry = api::client::metadata::get_file(&remote_repo, revision, path)
                .await?
                .entry;

            assert_eq!(entry.filename, "README.md");
            assert!(!entry.is_dir);
            assert_eq!(entry.data_type, EntryDataType::Text);
            assert_eq!(entry.mime_type, "text/markdown");
            assert_eq!(
                Path::new(&entry.resource.clone().unwrap().path),
                Path::new("annotations").join("README.md")
            );
            assert_eq!(
                &entry.resource.clone().unwrap().version,
                Path::new(DEFAULT_BRANCH_NAME)
            );
            assert_eq!(
                entry.resource.clone().unwrap().branch.unwrap().name,
                DEFAULT_BRANCH_NAME
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_dir_entry() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let path = "train";
            let revision = DEFAULT_BRANCH_NAME;
            let entry = api::client::metadata::get_file(&remote_repo, revision, path)
                .await?
                .entry;

            assert_eq!(entry.filename, path);
            assert!(entry.is_dir);
            assert_eq!(entry.data_type, EntryDataType::Dir);
            assert!(entry.size > 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_remote_metadata() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("train");

            let meta: MetadataEntryResponse =
                api::client::metadata::get_file(&remote_repo, branch, directory).await?;
            println!("meta: {:?}", meta);

            assert_eq!(meta.entry.mime_type, "inode/directory");
            assert_eq!(meta.entry.data_type, EntryDataType::Dir);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_latest_commit_by_branch() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // Now push a new commit
            let labels_path = local_repo.path.join("labels.txt");
            let path = Path::new("labels.txt");

            test::write_txt_file_to_path(&labels_path, "I am the labels file")?;

            repositories::add(&local_repo, &labels_path)?;

            let first_commit = repositories::commit(&local_repo, "adding labels file")?;

            repositories::push(&local_repo).await?;

            let main_branch = DEFAULT_BRANCH_NAME;
            let second_branch = "second";

            repositories::branches::create_checkout(&local_repo, second_branch)?;

            test::write_txt_file_to_path(&labels_path, "I am the labels file v2")?;

            repositories::add(&local_repo, &labels_path)?;

            let second_commit = repositories::commit(&local_repo, "adding labels file v2")?;

            repositories::push(&local_repo).await?;

            let meta: MetadataEntryResponse =
                api::client::metadata::get_file(&remote_repo, main_branch, &path).await?;

            let second_meta: MetadataEntryResponse =
                api::client::metadata::get_file(&remote_repo, second_branch, &path).await?;

            assert_eq!(meta.entry.latest_commit.unwrap().id, first_commit.id);
            assert_eq!(
                second_meta.entry.latest_commit.unwrap().id,
                second_commit.id
            );

            Ok(remote_repo)
        })
        .await
    }
}
