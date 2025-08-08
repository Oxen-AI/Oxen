use std::path::Path;

use polars::frame::DataFrame;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
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
            "Path must be a string: {:?}",
            path
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
            "Path must be a string: {:?}",
            path
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
            let err = format!("api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri =
        format!("/workspaces/{workspace_id}/data_frames/rows/{row_id}/resource/{file_path_str}");

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("rm_df_mod got body: {}", body);
    let response: Result<JsonDataFrameRowResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.data_frame.view.to_df().await),
        Err(err) => {
            let err = format!("api::staging::rm_df_mod error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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
            "Path must be a string: {:?}",
            path
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
                    let err = format!("api::staging::modify_df error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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
            "Path must be a string: {:?}",
            path
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
                    let err = format!("api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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
            "Path must be a string: {:?}",
            path
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
                    let err = format!("api::staging::batch_update error parsing response from {url}\n\nErr {err:?} \n\n{body}");
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
                        "row_id": "{}",
                        "value": {{
                            "file": "cfxsx"
                        }}
                    }},
                    {{
                        "row_id": "{}",
                        "value": {{
                            "file": "yfcsx"
                        }}
                    }}
                ]
            }}"#,
                oxen_id_1, oxen_id_2
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
                        "The row with ID {} was not updated to file {}",
                        row_id, expected_file
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
                            "row_id": "{}",
                            "value": {{
                                "file": "cfxsx",
                                "embedding": [0.1, 0.2, 0.3]
                            }}
                        }},
                        {{
                            "row_id": "{}",
                            "value": {{
                                "file": "yfcsx",
                                "embedding": [0.4, 0.5, 0.6]
                            }}
                        }},
                        {{
                            "row_id": "{}",
                            "value": {{
                                "file": "zxcvb",
                                "embedding": [0.7, 0.8, 0.9]
                            }}
                        }}
                    ]
                }}"#,
                oxen_id_1, oxen_id_2, oxen_id_3
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
                        "The row with ID {} was not updated to file {} with embedding {:?}",
                        row_id, expected_file, expected_embedding
                    );
                }
            } else {
                panic!("Expected 'data' to be an array in updates");
            }

            Ok(remote_repo)
        })
        .await
    }
}
