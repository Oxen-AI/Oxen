use serde_json::json;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::opts::PaginateOpts;
use crate::view::data_frames::embeddings::EmbeddingColumnsResponse;
use crate::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
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
    let uri = format!("/workspaces/{workspace_id}/data_frames/embeddings/columns/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_embeddings {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<EmbeddingColumnsResponse, serde_json::Error> = serde_json::from_str(&body);
    Ok(response?)
}

pub async fn neighbors(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
    column: impl AsRef<str>,
    embedding: &Vec<f32>,
    paginate_opts: &PaginateOpts,
) -> Result<WorkspaceJsonDataFrameViewResponse, OxenError> {
    let Some(file_path_str) = path.to_str() else {
        return Err(OxenError::basic_str(format!(
            "Path must be a string: {:?}",
            path
        )));
    };
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/embeddings/neighbors/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("get_embeddings {url}");

    let body = json!({
        "column": column.as_ref(),
        "embedding": embedding,
        "page_size": paginate_opts.page_size,
        "page_num": paginate_opts.page_num,
    });
    let body = body.to_string();

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).body(body).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<WorkspaceJsonDataFrameViewResponse, serde_json::Error> =
        serde_json::from_str(&body);
    Ok(response?)
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

    let uri = format!("/workspaces/{workspace_id}/data_frames/embeddings/columns/{file_path_str}");
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
    Ok(response?)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::config::embedding_config::EmbeddingStatus;
    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, OXEN_ROW_ID_COL};
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::opts::{DFOpts, PaginateOpts};
    use crate::test;
    use crate::{api, repositories};

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
                page_size: Some(23),
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
                opts.page_size.unwrap()
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_query_embeddings_by_embedding() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let branch_name = DEFAULT_BRANCH_NAME;

            // Write a small embeddings.json file
            let path = Path::new("embeddings.json");
            let data = json!([
                {"id": 1, "text": "oxen is the best data platform", "embedding": [1.0, 2.0, 3.0]},
                {"id": 2, "text": "collaborate on data in oxen.ai", "embedding": [2.0, 3.0, 4.0]},
                {"id": 3, "text": "oxen is an open source data platform", "embedding": [3.0, 4.0, 5.0]},
                {"id": 4, "text": "what is a good place to collaborate on data? Oxen.ai", "embedding": [4.0, 5.0, 6.0]},
            ]);
            let full_path = local_repo.path.join(path);
            std::fs::write(&full_path, data.to_string())?;

            // Add, commit, and push the file
            repositories::add(&local_repo, &full_path).await?;
            repositories::commit(&local_repo, "Add embeddings.json")?;
            repositories::push(&local_repo).await?;

            let workspace_id = UserConfig::identifier()?;
            let workspace =
                api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            assert_eq!(workspace.id, workspace_id);

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let column = "embedding";
            let use_background_thread = false;
            api::client::workspaces::data_frames::embeddings::index(
                &remote_repo,
                &workspace_id,
                path,
                column,
                use_background_thread,
            )
            .await?;

            let embedding = vec![3.0, 4.0, 5.0];
            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 2,
            };
            let result = api::client::workspaces::data_frames::embeddings::neighbors(
                &remote_repo,
                &workspace_id,
                path,
                &column,
                &embedding,
                &paginate_opts,
            )
            .await;

            assert!(result.is_ok());
            let response = result.unwrap();
            assert!(response.data_frame.is_some());
            assert_eq!(
                response.data_frame.as_ref().unwrap().view.size.height,
                paginate_opts.page_size
            );
            let rows = response.data_frame.as_ref().unwrap().view.data.as_array().unwrap();
            assert_eq!(rows.len(), paginate_opts.page_size);
            let first_row = rows[0].as_object().unwrap();
            let first_row_id = first_row["id"].as_u64().unwrap();
            assert_eq!(first_row_id, 3);

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
                let df = tabular::read_df(&output_path, DFOpts::empty()).await?;
                println!("{df}");
                assert_eq!(df.width(), 4);
                assert_eq!(df.height(), 10000);

                Ok(())
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
