use serde::{Deserialize, Serialize};

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::opts::DFOpts;
use crate::view::entries::PaginatedMetadataEntriesResponse;
use crate::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use std::path::Path;

use crate::model::RemoteRepository;
use crate::view::{JsonDataFrameViewResponse, JsonDataFrameViews, StatusMessage};

pub mod columns;
pub mod rows;

#[derive(Serialize, Deserialize)]
struct PutParam {
    is_indexed: bool,
}

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<WorkspaceJsonDataFrameViewResponse, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };
    let query_str = opts.to_http_query_params();
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<WorkspaceJsonDataFrameViewResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!("workspaces::data_frames::get error parsing from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("workspaces::data_frames::get Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn is_indexed(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<bool, OxenError> {
    let res = get(remote_repo, workspace_id, path, DFOpts::empty()).await?;
    Ok(res.is_indexed)
}

pub async fn list(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    workspace_id: &str,
) -> Result<PaginatedMetadataEntriesResponse, OxenError> {
    let uri = format!("/workspaces/{workspace_id}/data_frames/branch/{branch_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<PaginatedMetadataEntriesResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!("api::workspaces::get_by_branch error parsing from {url}\n\nErr {err:?} \n\n{body}");
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err =
                format!("api::workspaces::get_by_branch Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn index(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<StatusMessage, OxenError> {
    put(
        remote_repo,
        workspace_id,
        path,
        &serde_json::json!({"is_indexed": true}),
    )
    .await
}

pub async fn unindex(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<StatusMessage, OxenError> {
    put(
        remote_repo,
        workspace_id,
        path,
        &serde_json::json!({"is_indexed": false}),
    )
    .await
}

pub async fn put(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    data: &serde_json::Value,
) -> Result<StatusMessage, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(data)?;

    let client = client::new_for_url(&url)?;
    match client.put(&url).body(params).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(response) => Ok(response),
                Err(err) => {
                    let err = format!(
                        "api::workspaces::put error parsing from {url}\n\nErr {err:?} \n\n{body}"
                    );
                    Err(OxenError::basic_str(err))
                }
            }
        }
        Err(err) => {
            let err = format!("api::workspaces::put Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn restore(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let file_name = path.as_ref().to_string_lossy();
    let uri = format!("/workspaces/{workspace_id}/data_frames/resource/{file_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("workspaces::data_frames::restore {}", url);
    let client = client::new_for_url(&url)?;
    match client.delete(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("workspaces::data_frames::restore got body: {}", body);
            Ok(())
        }
        Err(err) => {
            let err =
                format!("workspaces::data_frames::restore Request failed: {url}\n\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn diff(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    page_num: usize,
    page_size: usize,
) -> Result<JsonDataFrameViews, OxenError> {
    let file_path_str = path.to_str().unwrap();

    let uri = format!("/workspaces/{workspace_id}/data_frames/diff/{file_path_str}?page={page_num}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("diff got body: {}", body);
            let response: Result<JsonDataFrameViewResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(data) => Ok(data.data_frame),

                Err(err) => Err(OxenError::basic_str(format!(
                    "api::staging::diff error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("api::staging::diff Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::test;
    use crate::{api, repositories};

    #[tokio::test]
    async fn test_get_by_resource() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");

            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::put(
                &remote_repo,
                workspace_id,
                path,
                &serde_json::json!({"is_indexed": true}),
            )
            .await?;

            let res = api::client::workspaces::data_frames::get(
                &remote_repo,
                workspace_id,
                path,
                DFOpts::empty(),
            )
            .await?;

            assert_eq!(res.status.status_message, "resource_found");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            api::client::workspaces::data_frames::index(&remote_repo, workspace_id, path).await?;

            let res = api::client::workspaces::data_frames::list(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                workspace_id,
            )
            .await?;

            assert_eq!(res.entries.entries.len(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_index_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let path = Path::new("annotations/train/bounding_box.csv");
            let workspace_id = "some_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(workspace.is_ok());

            let res = api::client::workspaces::data_frames::index(&remote_repo, workspace_id, path)
                .await?;

            assert_eq!(res.status, "success");

            let res =
                api::client::workspaces::data_frames::unindex(&remote_repo, workspace_id, path)
                    .await?;

            assert_eq!(res.status, "success");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_data_frame_diff() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let workspace_id = "some_workspace";
            let path = Path::new("annotations/train/bounding_box.csv");

            let res =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                    .await;
            assert!(res.is_ok());

            let res = api::client::workspaces::data_frames::index(&remote_repo, workspace_id, path)
                .await?;

            assert_eq!(res.status, "success");

            let res = api::client::workspaces::data_frames::diff(
                &remote_repo,
                workspace_id,
                path,
                1,
                100,
            )
            .await;

            assert!(res.is_ok());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_restore_modified_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await;
            assert!(workspace.is_ok());

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let result_1 = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await;
            assert!(result_1.is_ok());

            let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let result_2 = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string(),
                ).await;
            assert!(result_2.is_ok());


            // Make sure both got staged
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            log::debug!("Got this diff {:?}", diff);
            assert_eq!(diff.view.size.height, 2);

            // Delete result_2
            let result_delete = api::client::workspaces::data_frames::restore(
                &remote_repo,
                &workspace_id,
                &path,
            ).await;
            assert!(result_delete.is_ok());

            // Should be cleared
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;
            assert_eq!(diff.view.size.height, 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_modified_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id)
                    .await;
            assert!(workspace.is_ok());

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::index(
                &remote_repo,
                &workspace_id,
                &path
            ).await?;

            api::client::workspaces::data_frames::rows::add(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string()
            ).await?;

            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            assert_eq!(diff.view.size.height, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_diff_delete_row_from_modified_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_bounding_box_csv_pushed(|remote_repo| async move {
            let branch_name = "add-images";
            let branch = api::client::branches::create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(branch.name, branch_name);
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id)
                    .await;
            assert!(workspace.is_ok());

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let directory = Path::new("annotations").join("train");
            let path = directory.join("bounding_box.csv");
            let data = "{\"file\":\"image1.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;

            let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string()
                ).await?;

            let data = "{\"file\":\"image2.jpg\", \"label\": \"cat\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";
            let (_df_2, row_id_2) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    data.to_string(),
                ).await?;

            // Make sure both got staged
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;

            assert_eq!(diff.view.size.height, 2);

            let uuid_2 = row_id_2.unwrap();
            // Delete result_2
            let result_delete = api::client::workspaces::data_frames::rows::delete(
                &remote_repo,
                &workspace_id,
                &path,
                &uuid_2
            ).await;
            assert!(result_delete.is_ok());

            // Make there is only one left
            let diff = api::client::workspaces::data_frames::diff(
                &remote_repo,
                &workspace_id,
                &path,
                DEFAULT_PAGE_NUM,
                DEFAULT_PAGE_SIZE
            ).await?;
            assert_eq!(diff.view.size.height, 1);

            Ok(remote_repo)
        })
        .await
    }

    // Test fast forward merge on pull
    /*
    oxen init
    oxen add .
    oxen commit -m "add data"
    oxen push
    # update data frame file on server
    oxen pull repo_a (should be fast forward)
    # update data frame file on server
    oxen pull repo_a (should be fast forward)
    */
    #[tokio::test]
    async fn test_update_df_on_server_fast_forward_pull() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|empty_dir| async move {
                let cloned_repo_dir = empty_dir.join("repo_b");
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &cloned_repo_dir).await?;

                // Read the initial data
                let bbox_filename = Path::new("annotations")
                    .join("train")
                    .join("bounding_box.csv");
                let bbox_file = cloned_repo.path.join(&bbox_filename);
                let og_df = tabular::read_df(&bbox_file, DFOpts::empty())?;

                // Update the file on the remote repo
                let user = UserConfig::get()?.to_user();
                let workspace_id = "workspace_a";
                let workspace =
                    api::client::workspaces::create(&remote_repo, &DEFAULT_BRANCH_NAME, &workspace_id)
                        .await;
                assert!(workspace.is_ok());

                // train/d-o-double-g.jpg,dog,101.5,32.0,385,330
                let directory = Path::new("annotations").join("train");
                let path = directory.join("bounding_box.csv");
                let data = "{\"file\":\"d-o-double-g.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

                api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                        &remote_repo,
                        workspace_id,
                        &path,
                        data.to_string()
                    ).await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending d-o-double-g data".to_string(),
                };
                api::client::workspaces::commit(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id, &new_commit).await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let bbox_file = cloned_repo.path.join(&bbox_filename);
                let df = tabular::read_df(&bbox_file, DFOpts::empty())?;
                assert_eq!(df.height(), og_df.height() + 1);

                // Add a more rows on this branch
                let workspace_id = "workspace_b";
                let workspace =
                    api::client::workspaces::create(&remote_repo, &DEFAULT_BRANCH_NAME, &workspace_id)
                        .await;
                assert!(workspace.is_ok());

                // train/d-o-triple-g.jpg,dog,101.5,32.0,385,330
                let directory = Path::new("annotations").join("train");
                let path = directory.join("bounding_box.csv");
                let data = "{\"file\":\"d-o-triple-g.jpg\", \"label\": \"dog\", \"min_x\":13, \"min_y\":14, \"width\": 100, \"height\": 100}";

                api::client::workspaces::data_frames::index(&remote_repo, workspace_id, &path).await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                        &remote_repo,
                        workspace_id,
                        &path,
                        data.to_string()
                    ).await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending d-o-triple-g data".to_string(),
                };
                api::client::workspaces::commit(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id, &new_commit).await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let bbox_file = cloned_repo.path.join(&bbox_filename);
                let df = tabular::read_df(&bbox_file, DFOpts::empty())?;
                assert_eq!(df.height(), og_df.height() + 2);

                Ok(empty_dir)
            })
            .await?;
            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_update_root_df_on_server_fast_forward_pull() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|empty_dir| async move {
                let cloned_repo_dir = empty_dir.join("repo_b");
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &cloned_repo_dir).await?;

                // Read the initial data
                let prompts_filename = Path::new("prompts.jsonl");
                let prompts_file = cloned_repo.path.join(prompts_filename);
                let og_df = tabular::read_df(&prompts_file, DFOpts::empty())?;

                // Update the file on the remote repo
                let user = UserConfig::get()?.to_user();
                let workspace_id = "workspace_a";
                let workspace = api::client::workspaces::create(
                    &remote_repo,
                    &DEFAULT_BRANCH_NAME,
                    &workspace_id,
                )
                .await;
                assert!(workspace.is_ok());

                // Add a row to the prompts file
                let data = "{\"prompt\": \"What is another meaning of life?\", \"label\": \"43\"}";

                api::client::workspaces::data_frames::index(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                )
                .await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                    data.to_string(),
                )
                .await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending 43 data".to_string(),
                };
                api::client::workspaces::commit(
                    &remote_repo,
                    DEFAULT_BRANCH_NAME,
                    workspace_id,
                    &new_commit,
                )
                .await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let prompts_file = cloned_repo.path.join(prompts_filename);
                let df = tabular::read_df(&prompts_file, DFOpts::empty())?;
                assert_eq!(df.height(), og_df.height() + 1);

                // Add a more rows on this branch
                let workspace_id = "workspace_b";
                let workspace = api::client::workspaces::create(
                    &remote_repo,
                    &DEFAULT_BRANCH_NAME,
                    &workspace_id,
                )
                .await;
                assert!(workspace.is_ok());

                // Add a row to the prompts file
                let data =
                    "{\"prompt\": \"What is another another meaning of life?\", \"label\": \"44\"}";

                api::client::workspaces::data_frames::index(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                )
                .await?;

                let (_df_1, _row_id_1) = api::client::workspaces::data_frames::rows::add(
                    &remote_repo,
                    workspace_id,
                    prompts_filename,
                    data.to_string(),
                )
                .await?;
                let new_commit = NewCommitBody {
                    author: user.name.to_owned(),
                    email: user.email.to_owned(),
                    message: "Appending 44 data".to_string(),
                };
                api::client::workspaces::commit(
                    &remote_repo,
                    DEFAULT_BRANCH_NAME,
                    workspace_id,
                    &new_commit,
                )
                .await?;

                // Pull in the changes
                repositories::pull(&cloned_repo).await?;

                // Check that we have the new data
                let prompts_file = cloned_repo.path.join(prompts_filename);
                let df = tabular::read_df(&prompts_file, DFOpts::empty())?;
                assert_eq!(df.height(), og_df.height() + 2);

                Ok(empty_dir)
            })
            .await?;
            Ok(remote_repo_copy)
        })
        .await
    }
}
