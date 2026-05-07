use std::path::Path;

use polars::frame::DataFrame;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::Commit;
use crate::view::CommitResponse;
use crate::view::data_frames::{
    SelectiveRowMergeRequest, SelectiveRowMergeWithAssetsRequest, SyncFromBranchRequest,
    SyncFromBranchResponse,
};
use crate::view::json_data_frame_view::{JsonDataFrameRowResponse, VecBatchUpdateResponse};

use crate::model::RemoteRepository;

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_row {url}\n{row_id}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<JsonDataFrameRowResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => {
            let err = format!(
                "api::staging::get_row error parsing response from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn update(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
    data: String,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("update_row {url}\n{data}");

    let client = client::new_for_url(&url)?;
    let res = client
        .put(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<JsonDataFrameRowResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => {
            let err = format!(
                "api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<DataFrame, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("rm_df_mod got body: {body}");
    let response: Result<JsonDataFrameRowResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.data_frame.view.to_df().await),
        Err(err) => {
            let err = format!(
                "api::staging::rm_df_mod error parsing response from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn add(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    data: String,
) -> Result<(DataFrame, Option<String>), OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/rows/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("modify_df {url}\n{data}");

    let client = client::new_for_url(&url)?;
    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok((val.data_frame.view.to_df().await, val.row_id)),
                Err(err) => {
                    let err = format!(
                        "api::staging::modify_df error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::modify_df Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn restore_row(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    row_id: &str,
) -> Result<JsonDataFrameRowResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/restore/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .send()
        .await
    {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<JsonDataFrameRowResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!(
                        "api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::update_row Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn batch_update(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    data: String,
) -> Result<VecBatchUpdateResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {path:?}"
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/rows/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client
        .put(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await
    {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<VecBatchUpdateResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => Ok(val),
                Err(err) => {
                    let err = format!(
                        "api::staging::batch_update error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::staging::batch_update Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn selective_merge(
    remote_repo: &RemoteRepository,
    source_workspace_id: &str,
    request: &SelectiveRowMergeRequest,
) -> Result<Commit, OxenError> {
    let uri = format!("/workspaces/{source_workspace_id}/data_frames/selective_merge");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("selective_merge {url}\n{request:?}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(request).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.commit),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::workspaces::data_frames::rows::selective_merge error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn selective_merge_with_assets(
    remote_repo: &RemoteRepository,
    source_workspace_id: &str,
    request: &SelectiveRowMergeWithAssetsRequest,
) -> Result<Commit, OxenError> {
    let uri = format!("/workspaces/{source_workspace_id}/data_frames/selective_merge_with_assets");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("selective_merge_with_assets {url}\n{request:?}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(request).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.commit),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::workspaces::data_frames::rows::selective_merge_with_assets error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn sync_from_branch(
    remote_repo: &RemoteRepository,
    target_workspace_id: &str,
    request: &SyncFromBranchRequest,
) -> Result<SyncFromBranchResponse, OxenError> {
    let uri = format!("/workspaces/{target_workspace_id}/data_frames/sync_from_branch");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("sync_from_branch {url}\n{request:?}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(request).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<SyncFromBranchResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::workspaces::data_frames::rows::sync_from_branch error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {

    use serde_json::Value;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::{self, OXEN_ID_COL};
    use crate::error::OxenError;
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::test;
    use crate::view::data_frames::{
        RowMergeOp, RowSelection, SelectiveMergeTarget, SelectiveRowMergeRequest,
    };
    use crate::view::json_data_frame_view::JsonDataFrameRowResponse;
    use polars::prelude::AnyValue;

    use std::path::Path;

    #[tokio::test]
    async fn test_stage_row_on_dataframe_json() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations").join("train").join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let result =
                api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await;

            assert!(result.is_ok());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_should_not_stage_invalid_schema_for_dataframe() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let data = "{\"id\": 1, \"name\": \"greg\"}";
            let result = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string(),
            )
            .await;

            assert!(result.is_err());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_status_modified_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace = api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data: &str = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            api::client::workspaces::data_frames::index(
                &remote_repo,
                &workspace_id,
                &path,
            ).await?;
            api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string()
            ).await?;

            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let entries = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace_id,
                &directory,
                page_num,
                page_size,
            )
            .await?;
            assert_eq!(entries.modified_files.entries.len(), 1);
            assert_eq!(entries.modified_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_restore_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace = api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // Path to the CSV file
            let path = Path::new("annotations").join("train").join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            // Create a new row
            let result = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string()
            ).await;

            assert!(result.is_ok());

            let row_id: &String = result.as_ref().unwrap().1.as_ref().unwrap();

            // Get the newly created row
            let row = api::client::workspaces::data_frames::rows::get(&remote_repo, &workspace_id, &path, row_id).await?;

            // Check the "_oxen_diff_status" field
            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();
            assert_eq!(data.get("_oxen_diff_status").unwrap(), "added");

            // Restore the row
            let _restore_resp = api::client::workspaces::data_frames::rows::restore_row(&remote_repo, &workspace_id, &path, row_id).await?;

            // Get the restored row
            let restored_row: JsonDataFrameRowResponse = api::client::workspaces::data_frames::rows::get(&remote_repo, &workspace_id, &path, row_id).await?;

            // Check that the restored data is null
            let restore_data: Value = serde_json::from_value(restored_row.data_frame.view.data[0].clone()).unwrap();
            assert!(restore_data.is_null(), "Restored data is not null");

            Ok(remote_repo)
        }).await
    }

    #[tokio::test]
    async fn test_delete_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;

            // Path to the CSV file
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;

            // Extract the first _oxen_row_id from the data frame
            let binding = df.data_frame.unwrap();
            let row_id_value = binding
                .view
                .data
                .get(0)
                .and_then(|row| row.get("_oxen_id"))
                .unwrap();

            let row_id = row_id_value.as_str().unwrap();

            let row = api::client::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "unchanged");

            api::client::workspaces::data_frames::rows::delete(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let row = api::client::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "removed");
            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_update_row() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            // Path to the CSV file
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;

            // Extract the first _oxen_row_id from the data frame
            let binding = df
                .data_frame
                .unwrap();
            let row_id_value = binding
                .view
                .data
                .get(0)
                .and_then(|row| row.get("_oxen_id"))
                .unwrap();

            let row_id = row_id_value.as_str().unwrap();

            let row = api::client::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "unchanged");

            let data: &str = "{\"file\":\"lebron>jordan.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::rows::update(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
                data.to_string()
            )
            .await?;

            let row = api::client::workspaces::data_frames::rows::get(
                &remote_repo,
                &workspace_id,
                &path,
                row_id,
            )
            .await?;

            let data: Value = serde_json::from_value(row.data_frame.view.data[0].clone()).unwrap();
            assert_eq!(data.get("file").unwrap() ,"lebron>jordan.jpg");

            assert_eq!(data.get("_oxen_diff_status").unwrap(), "modified");
            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_stage_delete_row_clears_remote_status() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        };
        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let repo_dir = repo_dir.join("new_repo");

                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &repo_dir).await?;

                // Remote add row
                let path = test::test_nlp_classification_csv();

                // Index dataset
                let workspace_id = "my_workspace";
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
                api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path)
                    .await?;

                let mut opts = DFOpts::empty();
                opts.add_row =
                    Some("{\"text\": \"I am a new row\", \"label\": \"neutral\"}".to_string());
                // Grab ID from the row we just added
                let df =
                    repositories::workspaces::df(&cloned_repo, workspace_id, &path, opts).await?;
                let uuid = match df.column(OXEN_ID_COL).unwrap().get(0).unwrap() {
                    AnyValue::String(s) => s.to_string(),
                    AnyValue::StringOwned(s) => s.to_string(),
                    _ => panic!("Expected string"),
                };

                // Make sure it is listed as modified
                let directory = Path::new("");
                let status = api::client::workspaces::changes::list(
                    &remote_repo,
                    workspace_id,
                    directory,
                    constants::DEFAULT_PAGE_NUM,
                    constants::DEFAULT_PAGE_SIZE,
                )
                .await?;
                assert_eq!(status.modified_files.entries.len(), 1);

                // Delete it
                let mut delete_opts = DFOpts::empty();
                delete_opts.delete_row = Some(uuid);
                repositories::workspaces::df(&cloned_repo, workspace_id, &path, delete_opts)
                    .await?;

                // Now status should be empty
                let status = api::client::workspaces::changes::list(
                    &remote_repo,
                    workspace_id,
                    directory,
                    constants::DEFAULT_PAGE_NUM,
                    constants::DEFAULT_PAGE_SIZE,
                )
                .await?;
                assert_eq!(status.modified_files.entries.len(), 0);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_add_row_with_data() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = Path::new("annotations").join("train").join("bounding_box.csv");

            let workspace_id = "my_workspace";
            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path)
                .await?;

            // Valid data to add
            let data = r#"{"file":"image1.jpg", "label": "dog", "min_x":13, "min_y":14, "width": 100, "height": 100}"#;

            // Add the row
            let result = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                workspace_id,
                &path,
                data.to_string(),
            ).await;

            assert!(result.is_ok());

            // Retrieve the DataFrame to check if the row exists
            let df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            ).await?;

            let df_view = df.data_frame.unwrap().view;
            // Check if the new row exists in the DataFrame
            let rows = df_view.data.as_array().unwrap();

            let is_added = rows.iter().any(|row| {
                let row_value: Value = serde_json::from_value(row.clone()).unwrap();
                row_value.get("file") == Some(&Value::from("image1.jpg"))
            });


            assert!(is_added, "The added row does not exist in the DataFrame.");


            Ok(remote_repo)
        }).await
    }

    #[tokio::test]
    async fn test_add_row_with_empty_data() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = UserConfig::identifier()?;
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Create the workspace
            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                .await?;

            // Index the DataFrame to get the initial row count
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let initial_df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;
            let initial_row_count = initial_df
                .data_frame
                .unwrap()
                .view
                .data
                .as_array()
                .unwrap()
                .len();

            // Empty data to add
            let data = r#"{}"#;

            // Attempt to add the row
            let result = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string(),
            )
            .await;

            assert!(result.is_ok());

            // Index the DataFrame again to get the new row count
            let updated_df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;
            let updated_row_count = updated_df
                .data_frame
                .unwrap()
                .view
                .data
                .as_array()
                .unwrap()
                .len();

            // Assert that the row count did change
            assert_eq!(
                initial_row_count + 1,
                updated_row_count,
                "Row count should remain the same after adding empty data"
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_batch_update() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            let workspace_id = UserConfig::identifier()?;
            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            // Retrieve the DataFrame to get row IDs
            let df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;

            let df_view = df.data_frame.unwrap().view;
            let rows = df_view.data.as_array().unwrap();

            // Extract row IDs for the rows you want to update
            let oxen_id_1 = rows[0]["_oxen_id"].as_str().unwrap();
            let oxen_id_2 = rows[1]["_oxen_id"].as_str().unwrap();

            // Construct the JSON payload using the extracted row IDs
            let updates = format!(
                r#"{{
                "data": [
                    {{
                        "row_id": "{oxen_id_1}",
                        "value": {{
                            "file": "cfxsx"
                        }}
                    }},
                    {{
                        "row_id": "{oxen_id_2}",
                        "value": {{
                            "file": "yfcsx"
                        }}
                    }}
                ]
            }}"#
            );

            // Perform batch update
            let result = api::client::workspaces::data_frames::rows::batch_update(
                &remote_repo,
                &workspace_id,
                &path,
                updates.to_string(), // Convert JSON to string for the HTTP request
            )
            .await;

            assert!(result.is_ok(), "Batch update failed");

            // Retrieve the DataFrame to check if the rows have been updated
            let df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;

            let df_view = df.data_frame.unwrap().view;
            let updated_rows = df_view.data.as_array().unwrap();

            // Parse the JSON string into a Value
            let updates_value: Value = serde_json::from_str(&updates).unwrap();

            // Iterate over each update in the JSON array
            if let Some(data_array) = updates_value.get("data").and_then(|v| v.as_array()) {
                for update in data_array.iter() {
                    let row_id = update.get("row_id").and_then(|v| v.as_str()).unwrap();
                    let expected_file = update
                        .get("value")
                        .and_then(|v| v.get("file"))
                        .and_then(|v| v.as_str())
                        .unwrap();

                    let is_updated = updated_rows.iter().any(|row| {
                        let current_row: Value = serde_json::from_value(row.clone()).unwrap();
                        current_row.get("_oxen_id").and_then(|v| v.as_str()) == Some(row_id)
                            && current_row.get("file").and_then(|v| v.as_str())
                                == Some(expected_file)
                    });

                    assert!(
                        is_updated,
                        "The row with ID {row_id} was not updated to file {expected_file}"
                    );
                }
            } else {
                panic!("Expected 'data' to be an array in updates");
            }

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_batch_update_with_embeddings() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");

            let workspace_id = UserConfig::identifier()?;
            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            // Retrieve the DataFrame to get row IDs
            let df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;

            let df_view = df.data_frame.unwrap().view;
            let rows = df_view.data.as_array().unwrap();

            // Extract row IDs for the rows you want to update
            let oxen_id_1 = rows[0]["_oxen_id"].as_str().unwrap();
            let oxen_id_2 = rows[1]["_oxen_id"].as_str().unwrap();
            let oxen_id_3 = rows[2]["_oxen_id"].as_str().unwrap();

            let column_data = r#"{"name": "embedding", "data_type": "list[f64]"}"#;

            api::client::workspaces::data_frames::columns::create(
                &remote_repo,
                &workspace_id,
                &path,
                column_data.to_string(),
            )
            .await?;

            // Construct the JSON payload using the extracted row IDs
            let updates = format!(
                r#"{{
                    "data": [
                        {{
                            "row_id": "{oxen_id_1}",
                            "value": {{
                                "file": "cfxsx",
                                "embedding": [0.1, 0.2, 0.3]
                            }}
                        }},
                        {{
                            "row_id": "{oxen_id_2}",
                            "value": {{
                                "file": "yfcsx",
                                "embedding": [0.4, 0.5, 0.6]
                            }}
                        }},
                        {{
                            "row_id": "{oxen_id_3}",
                            "value": {{
                                "file": "zxcvb",
                                "embedding": [0.7, 0.8, 0.9]
                            }}
                        }}
                    ]
                }}"#
            );

            // Perform batch update
            let result = api::client::workspaces::data_frames::rows::batch_update(
                &remote_repo,
                &workspace_id,
                &path,
                updates.to_string(), // Convert JSON to string for the HTTP request
            )
            .await;

            assert!(result.is_ok(), "Batch update failed");

            // Retrieve the DataFrame to check if the rows have been updated
            let df = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &DFOpts::empty(),
            )
            .await?;

            let df_view = df.data_frame.unwrap().view;
            let updated_rows = df_view.data.as_array().unwrap();

            // Parse the JSON string into a Value
            let updates_value: Value = serde_json::from_str(&updates).unwrap();

            // Iterate over each update in the JSON array
            if let Some(data_array) = updates_value.get("data").and_then(|v| v.as_array()) {
                for update in data_array.iter() {
                    let row_id = update.get("row_id").and_then(|v| v.as_str()).unwrap();
                    let expected_file = update
                        .get("value")
                        .and_then(|v| v.get("file"))
                        .and_then(|v| v.as_str())
                        .unwrap();
                    let expected_embedding = update
                        .get("value")
                        .and_then(|v| v.get("embedding"))
                        .unwrap();

                    let is_updated = updated_rows.iter().any(|row| {
                        let current_row: Value = serde_json::from_value(row.clone()).unwrap();
                        current_row.get("_oxen_id").and_then(|v| v.as_str()) == Some(row_id)
                            && current_row.get("file").and_then(|v| v.as_str())
                                == Some(expected_file)
                            && current_row.get("embedding") == Some(expected_embedding)
                    });

                    assert!(
                        is_updated,
                        "The row with ID {row_id} was not updated to file {expected_file} with embedding {expected_embedding:?}"
                    );
                }
            } else {
                panic!("Expected 'data' to be an array in updates");
            }

            Ok(remote_repo)
        })
        .await
    }

    fn bbox_path() -> std::path::PathBuf {
        Path::new("annotations")
            .join("train")
            .join("bounding_box.csv")
    }

    async fn first_oxen_id(
        remote_repo: &crate::model::RemoteRepository,
        workspace_id: &str,
        path: &Path,
        idx: usize,
    ) -> Result<String, OxenError> {
        let df = api::client::workspaces::data_frames::get(
            remote_repo,
            workspace_id,
            path,
            &DFOpts::empty(),
        )
        .await?;
        let rows = df.data_frame.unwrap().view.data;
        let row_id = rows
            .as_array()
            .unwrap()
            .get(idx)
            .and_then(|r| r.get("_oxen_id"))
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string();
        Ok(row_id)
    }

    #[tokio::test]
    async fn test_selective_merge_add_to_branch() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = bbox_path();
            let source_workspace_id = "source-ws-add";
            let target_branch = "selective-merge-add-target";

            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &source_workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                source_workspace_id,
                &path,
            )
            .await?;

            let new_row_data = r#"{"file":"new_image.jpg", "label": "dog", "min_x":1, "min_y":2, "width": 10, "height": 20}"#;
            let (_df, new_row_id) = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                source_workspace_id,
                &path,
                new_row_data.to_string(),
            )
            .await?;
            let new_row_id = new_row_id.expect("expected a new row id from add");

            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let request = SelectiveRowMergeRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![RowSelection {
                    row_id: new_row_id.clone(),
                    op: RowMergeOp::Add,
                }],
                message: "selective add".to_string(),
                commit_author: None,
                commit_email: None,
            };

            let commit = api::client::workspaces::data_frames::rows::selective_merge(
                &remote_repo,
                source_workspace_id,
                &request,
            )
            .await?;
            assert_eq!(commit.message, "selective add");

            let after = api::client::data_frames::get(
                &remote_repo,
                target_branch,
                &path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(after.data_frame.source.size.height, 7);

            // The new file value should be present
            let p_df = after.data_frame.view.to_df().await;
            let file_col = p_df.column("file").unwrap();
            let any_match = (0..file_col.len()).any(|i| match file_col.get(i).unwrap() {
                AnyValue::String(s) => s == "new_image.jpg",
                AnyValue::StringOwned(s) => s.as_str() == "new_image.jpg",
                _ => false,
            });
            assert!(any_match, "expected new row to be present in target");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_delete_to_branch() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = bbox_path();
            let source_workspace_id = "source-ws-del";
            let target_branch = "selective-merge-del-target";

            api::client::workspaces::create(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &source_workspace_id,
            )
            .await?;
            api::client::workspaces::data_frames::index(&remote_repo, source_workspace_id, &path)
                .await?;

            let row_id = first_oxen_id(&remote_repo, source_workspace_id, &path, 0).await?;

            // Stage a delete in source
            api::client::workspaces::data_frames::rows::delete(
                &remote_repo,
                source_workspace_id,
                &path,
                &row_id,
            )
            .await?;

            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let request = SelectiveRowMergeRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![RowSelection {
                    row_id,
                    op: RowMergeOp::Delete,
                }],
                message: "selective delete".to_string(),
                commit_author: None,
                commit_email: None,
            };

            api::client::workspaces::data_frames::rows::selective_merge(
                &remote_repo,
                source_workspace_id,
                &request,
            )
            .await?;

            let after =
                api::client::data_frames::get(&remote_repo, target_branch, &path, DFOpts::empty())
                    .await?;
            assert_eq!(after.data_frame.source.size.height, 5);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_modify_to_branch() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = bbox_path();
            let source_workspace_id = "source-ws-mod";
            let target_branch = "selective-merge-mod-target";

            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &source_workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                source_workspace_id,
                &path,
            )
            .await?;

            let row_id = first_oxen_id(&remote_repo, source_workspace_id, &path, 0).await?;

            // Stage a modification in source
            let modified = r#"{"file":"renamed.jpg", "label": "cat", "min_x":1, "min_y":1, "width": 11, "height": 11}"#;
            api::client::workspaces::data_frames::rows::update(
                &remote_repo,
                source_workspace_id,
                &path,
                &row_id,
                modified.to_string(),
            )
            .await?;

            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let request = SelectiveRowMergeRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![RowSelection {
                    row_id,
                    op: RowMergeOp::Modify,
                }],
                message: "selective modify".to_string(),
                commit_author: None,
                commit_email: None,
            };

            api::client::workspaces::data_frames::rows::selective_merge(
                &remote_repo,
                source_workspace_id,
                &request,
            )
            .await?;

            let after = api::client::data_frames::get(
                &remote_repo,
                target_branch,
                &path,
                DFOpts::empty(),
            )
            .await?;
            // Same number of rows, but row 1's `file` is now "renamed.jpg"
            assert_eq!(after.data_frame.source.size.height, 6);
            let p_df = after.data_frame.view.to_df().await;
            let file_col = p_df.column("file").unwrap();
            let any_match = (0..file_col.len()).any(|i| match file_col.get(i).unwrap() {
                AnyValue::String(s) => s == "renamed.jpg",
                AnyValue::StringOwned(s) => s.as_str() == "renamed.jpg",
                _ => false,
            });
            assert!(any_match, "expected renamed row in target");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_mixed_ops_to_branch() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = bbox_path();
            let source_workspace_id = "source-ws-mix";
            let target_branch = "selective-merge-mix-target";

            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &source_workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                source_workspace_id,
                &path,
            )
            .await?;

            let modify_id = first_oxen_id(&remote_repo, source_workspace_id, &path, 1).await?;
            let delete_id = first_oxen_id(&remote_repo, source_workspace_id, &path, 2).await?;

            // Modify row 1
            let modified = r#"{"file":"mixed_renamed.jpg", "label": "dog", "min_x":1, "min_y":1, "width": 10, "height": 10}"#;
            api::client::workspaces::data_frames::rows::update(
                &remote_repo,
                source_workspace_id,
                &path,
                &modify_id,
                modified.to_string(),
            )
            .await?;

            // Delete row 2
            api::client::workspaces::data_frames::rows::delete(
                &remote_repo,
                source_workspace_id,
                &path,
                &delete_id,
            )
            .await?;

            // Add a new row
            let new_row_data = r#"{"file":"mixed_new.jpg", "label": "cat", "min_x":1, "min_y":2, "width": 30, "height": 30}"#;
            let (_df, new_row_id) = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                source_workspace_id,
                &path,
                new_row_data.to_string(),
            )
            .await?;
            let new_row_id = new_row_id.expect("expected a new row id from add");

            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let request = SelectiveRowMergeRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![
                    RowSelection {
                        row_id: modify_id,
                        op: RowMergeOp::Modify,
                    },
                    RowSelection {
                        row_id: delete_id,
                        op: RowMergeOp::Delete,
                    },
                    RowSelection {
                        row_id: new_row_id,
                        op: RowMergeOp::Add,
                    },
                ],
                message: "selective mixed".to_string(),
                commit_author: None,
                commit_email: None,
            };

            api::client::workspaces::data_frames::rows::selective_merge(
                &remote_repo,
                source_workspace_id,
                &request,
            )
            .await?;

            let after = api::client::data_frames::get(
                &remote_repo,
                target_branch,
                &path,
                DFOpts::empty(),
            )
            .await?;
            // 6 base + 1 add - 1 delete = 6
            assert_eq!(after.data_frame.source.size.height, 6);

            let p_df = after.data_frame.view.to_df().await;
            let file_col = p_df.column("file").unwrap();
            let files: Vec<String> = (0..file_col.len())
                .map(|i| match file_col.get(i).unwrap() {
                    AnyValue::String(s) => s.to_string(),
                    AnyValue::StringOwned(s) => s.to_string(),
                    _ => String::new(),
                })
                .collect();
            assert!(files.iter().any(|f| f == "mixed_renamed.jpg"));
            assert!(files.iter().any(|f| f == "mixed_new.jpg"));

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_to_workspace_target() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = bbox_path();
            let source_workspace_id = "source-ws-to-ws";
            let target_workspace_id = "target-ws";
            let target_branch = "selective-merge-ws-target";

            // Source workspace + staged add
            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &source_workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                source_workspace_id,
                &path,
            )
            .await?;

            let new_row_data = r#"{"file":"ws_target.jpg", "label": "dog", "min_x":1, "min_y":2, "width": 10, "height": 20}"#;
            let (_df, new_row_id) = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                source_workspace_id,
                &path,
                new_row_data.to_string(),
            )
            .await?;
            let new_row_id = new_row_id.expect("expected a new row id from add");

            // Target branch + target workspace
            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            api::client::workspaces::create(&remote_repo, target_branch, &target_workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                target_workspace_id,
                &path,
            )
            .await?;

            let request = SelectiveRowMergeRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Workspace {
                    workspace_id: target_workspace_id.to_string(),
                    path: path.clone(),
                    branch: target_branch.to_string(),
                },
                selections: vec![RowSelection {
                    row_id: new_row_id,
                    op: RowMergeOp::Add,
                }],
                message: "ws target add".to_string(),
                commit_author: None,
                commit_email: None,
            };

            let commit = api::client::workspaces::data_frames::rows::selective_merge(
                &remote_repo,
                source_workspace_id,
                &request,
            )
            .await?;
            assert_eq!(commit.message, "ws target add");

            // Branch HEAD should reflect the add
            let after = api::client::data_frames::get(
                &remote_repo,
                target_branch,
                &path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(after.data_frame.source.size.height, 7);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_unknown_source_row_id_errors() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = bbox_path();
            let source_workspace_id = "source-ws-unk";
            let target_branch = "selective-merge-unk-target";

            api::client::workspaces::create(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &source_workspace_id,
            )
            .await?;
            api::client::workspaces::data_frames::index(&remote_repo, source_workspace_id, &path)
                .await?;
            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let bogus_id = "00000000-0000-0000-0000-000000000000".to_string();
            let request = SelectiveRowMergeRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![RowSelection {
                    row_id: bogus_id,
                    op: RowMergeOp::Add,
                }],
                message: "should error".to_string(),
                commit_author: None,
                commit_email: None,
            };

            let result = api::client::workspaces::data_frames::rows::selective_merge(
                &remote_repo,
                source_workspace_id,
                &request,
            )
            .await;
            assert!(result.is_err(), "expected error for unknown source row_id");

            // Target branch must remain unchanged (atomicity)
            let after =
                api::client::data_frames::get(&remote_repo, target_branch, &path, DFOpts::empty())
                    .await?;
            assert_eq!(after.data_frame.source.size.height, 6);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_source_workspace_pending_changes_preserved()
    -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = bbox_path();
            let source_workspace_id = "source-ws-preserve";
            let target_branch = "selective-merge-preserve-target";

            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &source_workspace_id)
                .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                source_workspace_id,
                &path,
            )
            .await?;

            let new_row_data = r#"{"file":"preserve.jpg", "label": "dog", "min_x":1, "min_y":2, "width": 10, "height": 20}"#;
            let (_df, new_row_id) = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                source_workspace_id,
                &path,
                new_row_data.to_string(),
            )
            .await?;
            let new_row_id = new_row_id.expect("expected a new row id from add");

            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            // Confirm row currently shows up as "added" in source workspace
            let pre = api::client::workspaces::data_frames::rows::get(
                &remote_repo,
                source_workspace_id,
                &path,
                &new_row_id,
            )
            .await?;
            let pre_status: Value =
                serde_json::from_value(pre.data_frame.view.data[0].clone()).unwrap();
            assert_eq!(pre_status.get("_oxen_diff_status").unwrap(), "added");

            let request = SelectiveRowMergeRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![RowSelection {
                    row_id: new_row_id.clone(),
                    op: RowMergeOp::Add,
                }],
                message: "preserve test".to_string(),
                commit_author: None,
                commit_email: None,
            };
            api::client::workspaces::data_frames::rows::selective_merge(
                &remote_repo,
                source_workspace_id,
                &request,
            )
            .await?;

            // Source workspace's pending change should still show up as "added"
            let post = api::client::workspaces::data_frames::rows::get(
                &remote_repo,
                source_workspace_id,
                &path,
                &new_row_id,
            )
            .await?;
            let post_status: Value =
                serde_json::from_value(post.data_frame.view.data[0].clone()).unwrap();
            assert_eq!(post_status.get("_oxen_diff_status").unwrap(), "added");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_with_assets_basic_add() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            use crate::view::data_frames::SelectiveRowMergeWithAssetsRequest;

            let path = bbox_path();
            let source_workspace_id = "source-ws-assets-basic";
            let target_branch = "selective-merge-assets-basic";

            api::client::workspaces::create(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &source_workspace_id,
            )
            .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                source_workspace_id,
                &path,
            )
            .await?;

            // Stage a binary asset in the source workspace at uploads/dwight_vince.jpeg
            api::client::workspaces::files::add(
                &remote_repo,
                &source_workspace_id,
                "uploads",
                vec![test::test_img_file()],
                &None,
            )
            .await?;

            // Add a row whose `file` column references that staged asset
            let staged_asset_path = "uploads/dwight_vince.jpeg";
            let row_data = format!(
                r#"{{"file":"{staged_asset_path}", "label": "person", "min_x":1, "min_y":2, "width": 10, "height": 20}}"#
            );
            let (_df, new_row_id) = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                source_workspace_id,
                &path,
                row_data,
            )
            .await?;
            let new_row_id = new_row_id.expect("expected a new row id from add");

            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let request = SelectiveRowMergeWithAssetsRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![RowSelection {
                    row_id: new_row_id,
                    op: RowMergeOp::Add,
                }],
                asset_columns: vec!["file".to_string()],
                request_data_column: None,
                message: "publish row + asset".to_string(),
                commit_author: None,
                commit_email: None,
            };
            let commit =
                api::client::workspaces::data_frames::rows::selective_merge_with_assets(
                    &remote_repo,
                    source_workspace_id,
                    &request,
                )
                .await?;
            assert_eq!(commit.message, "publish row + asset");

            // Target branch dataframe should have the new row
            let after = api::client::data_frames::get(
                &remote_repo,
                target_branch,
                &path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(after.data_frame.source.size.height, 7);

            // Target branch should have the asset file at the same path. Use the
            // existing meta endpoint to verify it reached the branch.
            let meta_uri = format!("/meta/{target_branch}/{staged_asset_path}");
            let meta_url = api::endpoint::url_from_repo(&remote_repo, &meta_uri)?;
            let client = api::client::new_for_remote_repo(&remote_repo)?;
            let res = client.get(&meta_url).send().await?;
            assert_eq!(
                res.status().as_u16(),
                200,
                "expected asset to be present on target branch"
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_selective_merge_with_assets_missing_asset_in_source_errors()
    -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            use crate::view::data_frames::SelectiveRowMergeWithAssetsRequest;

            let path = bbox_path();
            let source_workspace_id = "source-ws-assets-missing";
            let target_branch = "selective-merge-assets-missing";

            api::client::workspaces::create(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &source_workspace_id,
            )
            .await?;
            api::client::workspaces::data_frames::index(
                &remote_repo,
                source_workspace_id,
                &path,
            )
            .await?;

            // Add a row whose `file` column points at a path that doesn't exist anywhere
            let bogus_path = "uploads/this_does_not_exist.jpeg";
            let row_data = format!(
                r#"{{"file":"{bogus_path}", "label": "person", "min_x":1, "min_y":2, "width": 10, "height": 20}}"#
            );
            let (_df, new_row_id) = api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                source_workspace_id,
                &path,
                row_data,
            )
            .await?;
            let new_row_id = new_row_id.expect("expected a new row id from add");

            api::client::branches::create_from_branch(
                &remote_repo,
                target_branch,
                DEFAULT_BRANCH_NAME,
            )
            .await?;

            let request = SelectiveRowMergeWithAssetsRequest {
                source_path: path.clone(),
                target: SelectiveMergeTarget::Branch {
                    name: target_branch.to_string(),
                    path: path.clone(),
                },
                selections: vec![RowSelection {
                    row_id: new_row_id,
                    op: RowMergeOp::Add,
                }],
                asset_columns: vec!["file".to_string()],
                request_data_column: None,
                message: "should fail".to_string(),
                commit_author: None,
                commit_email: None,
            };
            let result =
                api::client::workspaces::data_frames::rows::selective_merge_with_assets(
                    &remote_repo,
                    source_workspace_id,
                    &request,
                )
                .await;
            assert!(
                result.is_err(),
                "expected error for missing asset, got: {result:?}"
            );

            // Target branch should be unchanged (atomicity)
            let after = api::client::data_frames::get(
                &remote_repo,
                target_branch,
                &path,
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(after.data_frame.source.size.height, 6);

            Ok(remote_repo)
        })
        .await
    }

    /// Build a remote repo with a CSV that has `uuid,file` rows + a matching
    /// image at each `file` path. Used by the sync_from_branch tests to seed
    /// a custom (non-bounding-box) fixture.
    async fn setup_uuid_csv_remote(
        relative_csv_path: &str,
        rows: &[(&str, &str)], // (uuid, file relative path under repo root)
    ) -> Result<
        (
            crate::model::LocalRepository,
            crate::model::RemoteRepository,
        ),
        OxenError,
    > {
        use crate::command;
        use crate::constants::DEFAULT_REMOTE_NAME;

        let repo_dir = test::create_repo_dir(test::test_run_dir())?;
        let mut local_repo = repositories::init(&repo_dir)?;
        let version_store = local_repo.version_store()?;
        version_store.init().await?;

        // Write the CSV
        let csv_path = local_repo.path.join(relative_csv_path);
        if let Some(parent) = csv_path.parent() {
            crate::util::fs::create_dir_all(parent)?;
        }
        let mut csv = String::from("uuid,file\n");
        for (uuid, file) in rows {
            csv.push_str(&format!("{uuid},{file}\n"));
        }
        crate::util::fs::write_to_path(&csv_path, csv)?;

        // Stage the matching image at each file path
        for (_, file) in rows {
            let dst = local_repo.path.join(file);
            if let Some(parent) = dst.parent() {
                crate::util::fs::create_dir_all(parent)?;
            }
            crate::util::fs::copy(test::test_img_file(), &dst)?;
        }

        repositories::add(&local_repo, &local_repo.path).await?;
        repositories::commit(&local_repo, "seed history with uuid + assets")?;

        let remote = test::repo_remote_url_from(&local_repo.dirname());
        command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;
        let remote_repo = test::create_remote_repo(&local_repo).await?;
        repositories::push(&local_repo).await?;

        Ok((local_repo, remote_repo))
    }

    /// Append rows + images to the local repo and push. Caller is responsible
    /// for the CSV + image content already being there from `setup_uuid_csv_remote`.
    async fn append_rows_and_push(
        local_repo: &mut crate::model::LocalRepository,
        relative_csv_path: &str,
        existing_rows: &[(&str, &str)],
        new_rows: &[(&str, &str)],
        commit_message: &str,
    ) -> Result<(), OxenError> {
        let csv_path = local_repo.path.join(relative_csv_path);
        let mut csv = String::from("uuid,file\n");
        for (uuid, file) in existing_rows.iter().chain(new_rows.iter()) {
            csv.push_str(&format!("{uuid},{file}\n"));
        }
        crate::util::fs::write_to_path(&csv_path, csv)?;

        for (_, file) in new_rows {
            let dst = local_repo.path.join(file);
            if let Some(parent) = dst.parent() {
                crate::util::fs::create_dir_all(parent)?;
            }
            crate::util::fs::copy(test::test_img_file(), &dst)?;
        }
        repositories::add(local_repo, &local_repo.path).await?;
        repositories::commit(local_repo, commit_message)?;
        repositories::push(local_repo).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_sync_from_branch_basic() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        use crate::view::data_frames::{SyncFromBranchRequest, SyncSource};

        let csv = "history.csv";
        let v1 = vec![("u1", "images/a.jpg")];
        let (mut local_repo, remote_repo) = setup_uuid_csv_remote(csv, &v1).await?;

        // "old" branch pinned at v1
        let old_branch = "old";
        api::client::branches::create_from_branch(&remote_repo, old_branch, DEFAULT_BRANCH_NAME)
            .await?;

        // Advance main with two new rows + two new images
        let v2_new = vec![("u2", "images/b.jpg"), ("u3", "images/c.jpg")];
        append_rows_and_push(&mut local_repo, csv, &v1, &v2_new, "v2").await?;

        // Target workspace lives on `old`
        let target_workspace_id = "sync-target-basic";
        api::client::workspaces::create(&remote_repo, old_branch, target_workspace_id).await?;

        let request = SyncFromBranchRequest {
            source: SyncSource {
                branch: DEFAULT_BRANCH_NAME.to_string(),
                path: csv.into(),
            },
            target_path: csv.into(),
            target_branch: old_branch.to_string(),
            uuid_column: "uuid".to_string(),
            asset_columns: vec!["file".to_string()],
            request_data_column: None,
            message: "sync from main".to_string(),
            commit_author: None,
            commit_email: None,
        };
        let response = api::client::workspaces::data_frames::rows::sync_from_branch(
            &remote_repo,
            target_workspace_id,
            &request,
        )
        .await?;
        assert!(!response.already_up_to_date);
        assert_eq!(response.rows_added, 2);
        assert_eq!(response.assets_added, 2);
        assert!(response.commit.is_some());

        // `old` branch should now have all 3 rows
        let old_df =
            api::client::data_frames::get(&remote_repo, old_branch, csv, DFOpts::empty()).await?;
        assert_eq!(old_df.data_frame.source.size.height, 3);

        // Both new images should have landed on `old`
        for asset in ["images/b.jpg", "images/c.jpg"] {
            let meta_uri = format!("/meta/{old_branch}/{asset}");
            let meta_url = api::endpoint::url_from_repo(&remote_repo, &meta_uri)?;
            let client = api::client::new_for_remote_repo(&remote_repo)?;
            let res = client.get(&meta_url).send().await?;
            assert_eq!(
                res.status().as_u16(),
                200,
                "expected {asset} on `old` after sync"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_from_branch_already_up_to_date() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        use crate::view::data_frames::{SyncFromBranchRequest, SyncSource};

        let csv = "history.csv";
        let v1 = vec![("u1", "images/a.jpg"), ("u2", "images/b.jpg")];
        let (_local_repo, remote_repo) = setup_uuid_csv_remote(csv, &v1).await?;

        // Both source and target are at the same commit (main + a fresh branch)
        let target_branch = "mirror";
        api::client::branches::create_from_branch(&remote_repo, target_branch, DEFAULT_BRANCH_NAME)
            .await?;
        let target_workspace_id = "sync-target-uptodate";
        api::client::workspaces::create(&remote_repo, target_branch, target_workspace_id).await?;

        let request = SyncFromBranchRequest {
            source: SyncSource {
                branch: DEFAULT_BRANCH_NAME.to_string(),
                path: csv.into(),
            },
            target_path: csv.into(),
            target_branch: target_branch.to_string(),
            uuid_column: "uuid".to_string(),
            asset_columns: vec!["file".to_string()],
            request_data_column: None,
            message: "sync".to_string(),
            commit_author: None,
            commit_email: None,
        };
        let response = api::client::workspaces::data_frames::rows::sync_from_branch(
            &remote_repo,
            target_workspace_id,
            &request,
        )
        .await?;
        assert!(response.already_up_to_date);
        assert!(response.commit.is_none());
        assert_eq!(response.rows_added, 0);
        assert_eq!(response.assets_added, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_from_branch_source_path_not_found() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        use crate::view::data_frames::{SyncFromBranchRequest, SyncSource};

        let csv = "history.csv";
        let v1 = vec![("u1", "images/a.jpg")];
        let (_local_repo, remote_repo) = setup_uuid_csv_remote(csv, &v1).await?;
        let target_branch = "tgt-spnf";
        api::client::branches::create_from_branch(&remote_repo, target_branch, DEFAULT_BRANCH_NAME)
            .await?;
        let target_workspace_id = "sync-target-spnf";
        api::client::workspaces::create(&remote_repo, target_branch, target_workspace_id).await?;

        let request = SyncFromBranchRequest {
            source: SyncSource {
                branch: DEFAULT_BRANCH_NAME.to_string(),
                path: "does/not/exist.csv".into(),
            },
            target_path: csv.into(),
            target_branch: target_branch.to_string(),
            uuid_column: "uuid".to_string(),
            asset_columns: vec![],
            request_data_column: None,
            message: "should error".to_string(),
            commit_author: None,
            commit_email: None,
        };
        let result = api::client::workspaces::data_frames::rows::sync_from_branch(
            &remote_repo,
            target_workspace_id,
            &request,
        )
        .await;
        assert!(
            result.is_err(),
            "expected source_path_not_found error, got: {result:?}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_from_branch_target_path_not_found() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }
        use crate::view::data_frames::{SyncFromBranchRequest, SyncSource};

        let csv = "history.csv";
        let v1 = vec![("u1", "images/a.jpg")];
        let (_local_repo, remote_repo) = setup_uuid_csv_remote(csv, &v1).await?;
        let target_branch = "tgt-tpnf";
        api::client::branches::create_from_branch(&remote_repo, target_branch, DEFAULT_BRANCH_NAME)
            .await?;
        let target_workspace_id = "sync-target-tpnf";
        api::client::workspaces::create(&remote_repo, target_branch, target_workspace_id).await?;

        let request = SyncFromBranchRequest {
            source: SyncSource {
                branch: DEFAULT_BRANCH_NAME.to_string(),
                path: csv.into(),
            },
            target_path: "does/not/exist.csv".into(),
            target_branch: target_branch.to_string(),
            uuid_column: "uuid".to_string(),
            asset_columns: vec![],
            request_data_column: None,
            message: "should error".to_string(),
            commit_author: None,
            commit_email: None,
        };
        let result = api::client::workspaces::data_frames::rows::sync_from_branch(
            &remote_repo,
            target_workspace_id,
            &request,
        )
        .await;
        assert!(
            result.is_err(),
            "expected target_path_not_found error, got: {result:?}"
        );

        Ok(())
    }
}
