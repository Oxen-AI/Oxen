use serde_json::json;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::view::data_frames::embeddings::EmbeddingColumnsResponse;
use std::path::Path;

use crate::model::RemoteRepository;

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<EmbeddingColumnsResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };
    let uri = format!("/workspaces/{workspace_id}/data_frames/embeddings/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_embeddings {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<EmbeddingColumnsResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => {
            let err = format!("api::staging::get_embeddings error parsing response from {url}\n\nErr {err:?} \n\n{body}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn index(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column: &str,
    use_background_thread: bool,
) -> Result<EmbeddingColumnsResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };

    let uri = format!("/workspaces/{workspace_id}/data_frames/embeddings/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("index_embeddings {url}");

    let data = json!({
        "column": column,
        "use_background_thread": use_background_thread,
    });
    let data = data.to_string();
    let client = client::new_for_url(&url)?;
    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(data)
        .send()
        .await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<EmbeddingColumnsResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => {
            let err = format!("api::staging::update_row error parsing response from {url}\n\nErr {err:?} \n\n{body}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::config::embedding_config::EmbeddingStatus;
    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_SIZE, OXEN_ROW_ID_COL};
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::opts::DFOpts;
    use crate::test;

    use std::path::Path;

    #[tokio::test]
    async fn test_no_embeddings_in_dataframe() -> Result<(), OxenError> {
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

            // train/dog_1.jpg,dog,101.5,32.0,385,330
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let result = api::client::workspaces::data_frames::embeddings::get(
                &remote_repo,
                &workspace_id,
                &path,
            )
            .await;

            assert!(result.is_ok());
            let response = result.unwrap();
            assert_eq!(response.columns.len(), 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_index_embeddings_in_dataframe() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_embeddings_jsonl_pushed(|remote_repo| async move {
            let branch_name = DEFAULT_BRANCH_NAME;
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = Path::new("annotations")
                .join("train")
                .join("embeddings.jsonl");
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let column = "embedding";
            let use_background_thread = true;
            api::client::workspaces::data_frames::embeddings::index(
                &remote_repo,
                &workspace_id,
                &path,
                column,
                use_background_thread,
            )
            .await?;

            let mut indexing_status = EmbeddingStatus::NotIndexed;
            let mut max_retries = 100; // just so it doesn't hang forever
            while indexing_status != EmbeddingStatus::Complete && max_retries > 0 {
                let result = api::client::workspaces::data_frames::embeddings::get(
                    &remote_repo,
                    &workspace_id,
                    &path,
                )
                .await;

                assert!(result.is_ok());
                let response = result.unwrap();
                assert_eq!(response.columns.len(), 1);
                assert_eq!(response.columns[0].name, column);
                assert_eq!(response.columns[0].vector_length, 3);
                indexing_status = response.columns[0].status.clone();

                // sleep for 1 second
                std::thread::sleep(std::time::Duration::from_secs(1));

                max_retries -= 1;
            }

            assert_eq!(indexing_status, EmbeddingStatus::Complete);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_query_embeddings_by_id() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_embeddings_jsonl_pushed(|remote_repo| async move {
            let branch_name = DEFAULT_BRANCH_NAME;
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = Path::new("annotations")
                .join("train")
                .join("embeddings.jsonl");
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let column = "embedding";
            let use_background_thread = false;
            api::client::workspaces::data_frames::embeddings::index(
                &remote_repo,
                &workspace_id,
                &path,
                column,
                use_background_thread,
            )
            .await?;

            let result = api::client::workspaces::data_frames::embeddings::get(
                &remote_repo,
                &workspace_id,
                &path,
            )
            .await;

            assert!(result.is_ok());
            let response = result.unwrap();
            let indexing_status = response.columns[0].status.clone();
            assert_eq!(indexing_status, EmbeddingStatus::Complete);

            // Query the embeddings by id
            let opts = DFOpts {
                find_embedding_where: Some(format!("{} = 1", OXEN_ROW_ID_COL)),
                sort_by_similarity_to: Some(column.to_string()),
                ..DFOpts::empty()
            };
            let result = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &opts,
            )
            .await;
            assert!(result.is_ok());
            let response = result.unwrap();
            assert!(response.data_frame.is_some());
            assert_eq!(
                response.data_frame.unwrap().view.size.height,
                DEFAULT_PAGE_SIZE
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_embeddings_by_id() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_embeddings_jsonl_pushed(|remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            let branch_name = DEFAULT_BRANCH_NAME;
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = Path::new("annotations")
                .join("train")
                .join("embeddings.jsonl");
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let column = "embedding";
            let use_background_thread = true;
            api::client::workspaces::data_frames::embeddings::index(
                &remote_repo,
                &workspace_id,
                &path,
                column,
                use_background_thread,
            )
            .await?;

            let mut indexing_status = EmbeddingStatus::NotIndexed;
            let mut max_retries = 100; // just so it doesn't hang forever
            while indexing_status != EmbeddingStatus::Complete && max_retries > 0 {
                let result = api::client::workspaces::data_frames::embeddings::get(
                    &remote_repo,
                    &workspace_id,
                    &path,
                )
                .await;

                assert!(result.is_ok());
                let response = result.unwrap();
                indexing_status = response.columns[0].status.clone();

                // sleep for 1 second
                std::thread::sleep(std::time::Duration::from_secs(1));

                max_retries -= 1;
            }
            assert_eq!(indexing_status, EmbeddingStatus::Complete);

            test::run_empty_dir_test_async(|sync_dir| async move {
                let output_path = sync_dir.join("test_download.parquet");

                // Download the data frame sorted by embeddings
                let opts = DFOpts {
                    find_embedding_where: Some(format!("{} = 1", OXEN_ROW_ID_COL)),
                    sort_by_similarity_to: Some(column.to_string()),
                    output: Some(output_path.clone()),
                    ..DFOpts::empty()
                };
                api::client::workspaces::data_frames::download(
                    &remote_repo,
                    &workspace_id,
                    &path,
                    &opts,
                )
                .await?;

                assert!(output_path.exists());

                // There should be 10000 rows by 4 columns
                let df = tabular::read_df(&output_path, DFOpts::empty())?;
                println!("{df}");
                assert_eq!(df.width(), 4);
                assert_eq!(df.height(), 10000);

                Ok(sync_dir)
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_query_embeddings_invalid_query() -> Result<(), OxenError> {
        // Skip duckdb if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_remote_repo_test_embeddings_jsonl_pushed(|remote_repo| async move {
            let branch_name = DEFAULT_BRANCH_NAME;
            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            let path = Path::new("annotations")
                .join("train")
                .join("embeddings.jsonl");
            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let column = "embedding";
            let use_background_thread = true;
            api::client::workspaces::data_frames::embeddings::index(
                &remote_repo,
                &workspace_id,
                &path,
                column,
                use_background_thread,
            )
            .await?;

            let mut indexing_status = EmbeddingStatus::NotIndexed;
            let mut max_retries = 100; // just so it doesn't hang forever
            while indexing_status != EmbeddingStatus::Complete && max_retries > 0 {
                let result = api::client::workspaces::data_frames::embeddings::get(
                    &remote_repo,
                    &workspace_id,
                    &path,
                )
                .await;

                assert!(result.is_ok());
                let response = result.unwrap();
                indexing_status = response.columns[0].status.clone();

                // sleep for 1 second
                std::thread::sleep(std::time::Duration::from_secs(1));

                max_retries -= 1;
            }
            assert_eq!(indexing_status, EmbeddingStatus::Complete);

            // Query the embeddings by id
            let opts = DFOpts {
                find_embedding_where: Some("non_existent_column = test".to_string()),
                sort_by_similarity_to: Some(column.to_string()),
                ..DFOpts::empty()
            };
            let result = api::client::workspaces::data_frames::get(
                &remote_repo,
                &workspace_id,
                &path,
                &opts,
            )
            .await;
            assert!(result.is_err());

            Ok(remote_repo)
        })
        .await
    }
}
