//! Fetch metadata about a resource from the remote.
//!

use http::StatusCode;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::entry_metadata::EMetadataEntryResponseView;

use std::path::Path;

/// Get the metadata about a resource from the remote.
pub async fn get_file(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<Option<EMetadataEntryResponseView>, OxenError> {
    let path = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/meta/{}/{}", revision, path);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;
    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let body = client::parse_json_body(&url, response).await?;
    Ok(Some(serde_json::from_str(&body)?))
}

#[cfg(test)]
mod tests {

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::{EntryDataType, StagedEntryStatus};
    use crate::view::entries::EMetadataEntry;
    use crate::view::entry_metadata::EMetadataEntryResponseView;
    use crate::{api, repositories};
    use crate::{test, util};

    use std::path::Path;

    #[tokio::test]
    async fn test_get_file_entry() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            let path = Path::new("annotations").join("README.md");
            let revision = DEFAULT_BRANCH_NAME;

            let head = repositories::commits::head_commit(&local_repo)?;

            // Try to get the entry from the local repo
            let tree = repositories::tree::get_root_with_children(&local_repo, &head)?.unwrap();
            let entry = tree.get_by_path(&path)?;
            assert!(entry.is_some());

            let entry = api::client::metadata::get_file(&remote_repo, revision, path)
                .await?
                .unwrap()
                .entry;

            assert_eq!(entry.filename(), "README.md");
            assert!(!entry.is_dir());
            assert_eq!(entry.data_type(), EntryDataType::Text);
            assert_eq!(entry.mime_type(), "text/markdown");
            assert_eq!(
                Path::new(&entry.resource().unwrap().path),
                Path::new("annotations").join("README.md")
            );
            assert_eq!(
                &entry.resource().unwrap().version,
                Path::new(DEFAULT_BRANCH_NAME)
            );
            assert_eq!(
                entry.resource().unwrap().branch.unwrap().name,
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
                .unwrap()
                .entry;

            assert_eq!(entry.filename(), path);
            assert!(entry.is_dir());
            assert_eq!(entry.data_type(), EntryDataType::Dir);
            assert!(entry.size() > 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_remote_metadata() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let branch = DEFAULT_BRANCH_NAME;
            let directory = Path::new("train");

            let meta: EMetadataEntryResponseView =
                api::client::metadata::get_file(&remote_repo, branch, directory)
                    .await?
                    .unwrap();

            assert_eq!(meta.entry.mime_type(), "inode/directory");
            assert_eq!(meta.entry.data_type(), EntryDataType::Dir);

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

            repositories::add(&local_repo, &labels_path).await?;

            let first_commit = repositories::commit(&local_repo, "adding labels file")?;

            repositories::push(&local_repo).await?;

            let main_branch = DEFAULT_BRANCH_NAME;
            let second_branch = "second";

            repositories::branches::create_checkout(&local_repo, second_branch)?;

            test::write_txt_file_to_path(&labels_path, "I am the labels file v2")?;

            repositories::add(&local_repo, &labels_path).await?;

            let second_commit = repositories::commit(&local_repo, "adding labels file v2")?;

            repositories::push(&local_repo).await?;

            let meta: EMetadataEntryResponseView =
                api::client::metadata::get_file(&remote_repo, main_branch, &path)
                    .await?
                    .unwrap();

            let second_meta: EMetadataEntryResponseView =
                api::client::metadata::get_file(&remote_repo, second_branch, &path)
                    .await?
                    .unwrap();

            assert_eq!(meta.entry.latest_commit().unwrap().id, first_commit.id);
            assert_eq!(
                second_meta.entry.latest_commit().unwrap().id,
                second_commit.id
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_file_with_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let file_path = "annotations/train/file.txt";
            let workspace_id = "test_workspace_id";
            let directory_name = "annotations/train";

            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            let full_path = local_repo.path.join(file_path);
            util::fs::file_create(&full_path)?;
            util::fs::write(&full_path, b"test content")?;

            let _result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                &full_path,
            )
            .await;

            let meta: EMetadataEntryResponseView =
                api::client::metadata::get_file(&remote_repo, workspace_id, file_path)
                    .await?
                    .unwrap();

            let workspace_entry = match meta.entry {
                EMetadataEntry::WorkspaceMetadataEntry(entry) => entry,
                _ => panic!("Expected workspace metadata entry"),
            };

            assert_eq!(
                workspace_entry.changes.unwrap().status,
                StagedEntryStatus::Added
            );

            let file_path = test::test_bounding_box_csv();
            let full_path = local_repo.path.join(file_path.clone());
            util::fs::write(&full_path, "name,age\nAlice,30\nBob,25\n")?;

            let _result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                &full_path,
            )
            .await;

            let meta: EMetadataEntryResponseView =
                api::client::metadata::get_file(&remote_repo, workspace_id, file_path.clone())
                    .await?
                    .unwrap();

            let workspace_entry = match meta.entry {
                EMetadataEntry::WorkspaceMetadataEntry(entry) => entry,
                _ => panic!("Expected workspace metadata entry"),
            };

            assert_eq!(
                workspace_entry.changes.unwrap().status,
                StagedEntryStatus::Modified
            );

            Ok(remote_repo)
        })
        .await
    }
}
