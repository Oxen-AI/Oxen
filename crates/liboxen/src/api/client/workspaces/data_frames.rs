use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::opts::DFOpts;
use crate::util;
use crate::util::fs::AtomicFile;
use crate::view::entries::PaginatedMetadataEntriesResponse;
use crate::view::json_data_frame_view::WorkspaceJsonDataFrameViewResponse;
use futures_util::StreamExt;
use std::path::Path;
use tokio_util::io::StreamReader;

use crate::model::RemoteRepository;
use crate::view::StatusMessage;

pub mod columns;
pub mod embeddings;
pub mod rows;

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<WorkspaceJsonDataFrameViewResponse, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();
    let query_str = opts.to_http_query_params();
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<WorkspaceJsonDataFrameViewResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err = format!(
                "workspaces::data_frames::get error parsing from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn download(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    opts: &DFOpts, // opts holds output path
) -> Result<(), OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();
    let query_str = opts.to_http_query_params();
    let uri =
        format!("/workspaces/{workspace_id}/data_frames/download/{file_path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    // Download the file and save it to the output path
    let Some(output_path_str) = &opts.output else {
        return Err(OxenError::basic_str("output path is required"));
    };
    let output_path = Path::new(&output_path_str);
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;

    if !res.status().is_success() {
        let status = res.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(OxenError::resource_not_found(file_path_str));
        }

        log::error!("api::client::workspaces::data_frames::download failed with status: {status}");
        let body = client::parse_json_body(&url, res).await?;
        return Err(OxenError::basic_str(format!(
            "Error: Could not download data frame {body:?}"
        )));
    }

    // Stream the response body to the output path
    log::debug!("Download writing output file {output_path:?}");
    let mut reader = StreamReader::new(
        res.bytes_stream()
            .map(|chunk| chunk.map_err(std::io::Error::other)),
    );
    AtomicFile::new(output_path).stream_async(&mut reader).await
}

pub async fn is_indexed(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: &Path,
) -> Result<bool, OxenError> {
    let res = get(remote_repo, workspace_id, path, &DFOpts::empty()).await?;
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
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<PaginatedMetadataEntriesResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err = format!(
                "api::workspaces::get_by_branch error parsing from {url}\n\nErr {err:?} \n\n{body}"
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn index(
    remote_repo: &RemoteRepository,
    workspace_id: &str,
    path: impl AsRef<Path>,
) -> Result<StatusMessage, OxenError> {
    let path = util::fs::linux_path(path.as_ref());
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
    let file_path_str = path.to_string_lossy();

    let uri = format!("/workspaces/{workspace_id}/data_frames/resource/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(data)?;

    let client = client::new_for_url(&url)?;
    let res = client.put(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err =
                format!("api::workspaces::put error parsing from {url}\n\nErr {err:?} \n\n{body}");
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
    log::debug!("workspaces::data_frames::restore {url}");
    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("workspaces::data_frames::restore got body: {body}");
    Ok(())
}

pub async fn rename_data_frame(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
) -> Result<StatusMessage, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let file_path_str = path.to_string_lossy();

    let uri = format!("/workspaces/{workspace_id}/data_frames/rename/{file_path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let params = serde_json::to_string(&serde_json::json!({
        "new_path": new_path.as_ref().to_string_lossy()
    }))?;

    let client = client::new_for_url(&url)?;
    let res = client.put(&url).body(params).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => {
            let err =
                format!("api::workspaces::put error parsing from {url}\n\nErr {err:?} \n\n{body}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::rows;
    use crate::api::client::branches::create_from_branch;
    use crate::api::client::{data_frames, workspaces};
    use crate::util::fs;

    use std::path::{Path, PathBuf};

    use http::StatusCode;

    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::model::{NewCommitBody, RemoteRepository};
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::test;

    use super::{download, get, index, list, rename_data_frame, restore, unindex};

    fn commit_body(message: &str) -> NewCommitBody {
        NewCommitBody {
            message: message.to_string(),
            author: "Test User".to_string(),
            email: "test@oxen.ai".to_string(),
        }
    }

    fn bbox_row(file: &str) -> String {
        format!(
            r#"{{"file": "{file}", "label": "dog", "min_x": 13, "min_y": 14, "width": 100, "height": 100}}"#
        )
    }

    async fn staged_height(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
        path: &Path,
    ) -> Result<usize, OxenError> {
        let res = get(remote_repo, workspace_id, path, &DFOpts::empty()).await?;
        Ok(res
            .data_frame
            .expect("data frame should be present")
            .source
            .size
            .height)
    }

    async fn committed_height(
        remote_repo: &RemoteRepository,
        branch_name: &str,
        path: &Path,
    ) -> Result<usize, OxenError> {
        let df = data_frames::get(remote_repo, branch_name, path, DFOpts::empty()).await?;
        assert_eq!(df.status.status_message, "resource_found");
        Ok(df.data_frame.view.to_df().await.height())
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_query_edit_and_commit_workspace_data_frames() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let local_path = local_repo.path.join(&path);
            let remote_repo_copy = remote_repo.clone();

            // Query and download an indexed data frame
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "query").await?;
            assert_eq!(index(&remote_repo, "query", &path).await?.status, "success");
            let res = get(&remote_repo, "query", &path, &DFOpts::empty()).await?;
            assert_eq!(res.status.status_message, "resource_found");
            assert_eq!(
                list(&remote_repo, DEFAULT_BRANCH_NAME, "query")
                    .await?
                    .entries
                    .entries
                    .len(),
                1
            );

            let dogs = "SELECT * FROM df WHERE label = 'dog'";
            let opts = DFOpts {
                sql: Some(dogs.to_string()),
                ..DFOpts::empty()
            };
            let res = get(&remote_repo, "query", &path, &opts).await?;
            assert_eq!(
                res.data_frame.expect("a page").view.to_df().await.height(),
                4
            );

            let query_repo = remote_repo.clone();
            let query_path = path.clone();
            test::run_empty_dir_test_async(|sync_dir| async move {
                let (remote_repo, path) = (query_repo, query_path);
                let output = sync_dir.join("download.csv");
                let opts = DFOpts {
                    output: Some(output.clone()),
                    ..DFOpts::empty()
                };
                download(&remote_repo, "query", &path, &opts).await?;
                assert_eq!(
                    fs::read_from_path(&output)?,
                    fs::read_from_path(&local_path)?
                );

                let output = sync_dir.join("download.jsonl");
                let opts = DFOpts {
                    output: Some(output.clone()),
                    ..DFOpts::empty()
                };
                download(&remote_repo, "query", &path, &opts).await?;
                let original = tabular::read_df(&local_path, DFOpts::empty()).await?;
                let downloaded = tabular::read_df(&output, DFOpts::empty()).await?;
                assert_eq!(
                    (downloaded.height(), downloaded.width()),
                    (original.height(), original.width())
                );

                for (sql, shape) in [
                    (dogs, (4, 6)),
                    ("SELECT label, COUNT(*) FROM df GROUP BY label", (2, 2)),
                ] {
                    let output = sync_dir.join("query.csv");
                    let opts = DFOpts {
                        sql: Some(sql.to_string()),
                        output: Some(output.clone()),
                        ..DFOpts::empty()
                    };
                    download(&remote_repo, "query", &path, &opts).await?;
                    let df = tabular::read_df(&output, DFOpts::empty()).await?;
                    assert_eq!((df.height(), df.width()), shape, "{sql}");
                }

                let output = sync_dir.join("should_not_exist.csv");
                let opts = DFOpts {
                    output: Some(output.clone()),
                    ..DFOpts::empty()
                };
                let result = download(
                    &remote_repo,
                    "query",
                    Path::new("this/path/does_not_exist.csv"),
                    &opts,
                )
                .await;
                assert!(
                    matches!(result, Err(OxenError::ResourceNotFound(_))),
                    "{result:?}"
                );
                assert!(!output.exists());
                Ok(())
            })
            .await?;
            assert_eq!(
                unindex(&remote_repo, "query", &path).await?.status,
                "success"
            );

            // Stage, delete, and discard row edits: the csv holds 6 committed rows
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "edit").await?;
            index(&remote_repo, "edit", &path).await?;
            rows::add(&remote_repo, "edit", &path, bbox_row("image1.jpg")).await?;
            let (_df, added_id) =
                rows::add(&remote_repo, "edit", &path, bbox_row("image2.jpg")).await?;
            assert_eq!(staged_height(&remote_repo, "edit", &path).await?, 8);
            let added_id = added_id.expect("the added row's id");
            rows::delete(&remote_repo, "edit", &path, &added_id).await?;
            assert_eq!(staged_height(&remote_repo, "edit", &path).await?, 7);
            restore(&remote_repo, "edit", &path).await?;
            assert_eq!(
                staged_height(&remote_repo, "edit", &path).await?,
                6,
                "restore discards the staged edits"
            );

            // Rename a data frame, with and without a staged row, each on its own branch
            let og_height = committed_height(&remote_repo, DEFAULT_BRANCH_NAME, &path).await?;
            for (branch_name, new_path, add_row) in [
                (
                    "rename",
                    PathBuf::from("new/dir/bounding_box_renamed.csv"),
                    false,
                ),
                (
                    "edit-rename",
                    Path::new("annotations")
                        .join("train")
                        .join("bounding_box_edited.csv"),
                    true,
                ),
                (
                    "edit-rename-new-dir",
                    Path::new("annotations")
                        .join("train")
                        .join("nonexistent_folder")
                        .join("bounding_box_edited.csv"),
                    true,
                ),
            ] {
                create_from_branch(&remote_repo, branch_name, DEFAULT_BRANCH_NAME).await?;
                workspaces::create(&remote_repo, branch_name, branch_name).await?;
                index(&remote_repo, branch_name, &path).await?;
                if add_row {
                    rows::add(
                        &remote_repo,
                        branch_name,
                        &path,
                        bbox_row("train/dog_4.jpg"),
                    )
                    .await?;
                }
                let res = rename_data_frame(&remote_repo, branch_name, &path, &new_path).await?;
                assert_eq!(res.status, "success");
                workspaces::commit(
                    &remote_repo,
                    branch_name,
                    branch_name,
                    &commit_body(branch_name),
                )
                .await?;

                let expected = og_height + usize::from(add_row);
                assert_eq!(
                    committed_height(&remote_repo, branch_name, &new_path).await?,
                    expected,
                    "{branch_name}"
                );
                assert_eq!(
                    committed_height(&remote_repo, branch_name, &path).await?,
                    og_height,
                    "{branch_name}"
                );
            }

            // Index a parquet file with a binary column
            let binary_name = "binary_col.parquet";
            fs::copy(
                test::test_binary_column_parquet_file(),
                local_repo.path.join(binary_name),
            )?;
            // A data frame at the repo root, for the pull below
            let prompts_path = Path::new("prompts.jsonl");
            fs::write_to_path(
                local_repo.path.join(prompts_path),
                "{\"prompt\": \"What is the meaning of life?\", \"label\": \"42\"}\n",
            )?;
            // A data frame stored as a JSON array rather than JSON lines
            fs::write_to_path(
                local_repo.path.join("labels.json"),
                r#"[{"id": 1, "label": "cat"}, {"id": 2, "label": "dog"}]"#,
            )?;
            repositories::add(&local_repo, &local_repo.path).await?;
            repositories::commit(&local_repo, "Add a binary column parquet file and prompts")?;
            repositories::push(&local_repo).await?;
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "binary").await?;
            assert_eq!(
                index(&remote_repo, "binary", binary_name).await?.status,
                "success"
            );
            let result = rename_data_frame(&remote_repo, "binary", binary_name, prompts_path).await;
            assert!(
                matches!(
                    result,
                    Err(OxenError::HttpStatusError {
                        status: StatusCode::CONFLICT,
                        ..
                    })
                ),
                "renaming onto the committed {prompts_path:?} is a conflict, got {result:?}"
            );

            // Unstaging an edited data frame discards its edits, one path at a time or several,
            // whether its extension or only its content makes it tabular
            workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "unstage").await?;
            for (df_path, row, one_path) in [
                (
                    Path::new("labels.json"),
                    r#"{"id": 3, "label": "bird"}"#.to_string(),
                    true,
                ),
                (path.as_path(), bbox_row("unstaged.jpg"), false),
            ] {
                index(&remote_repo, "unstage", df_path).await?;
                let og_height = staged_height(&remote_repo, "unstage", df_path).await?;
                rows::add(&remote_repo, "unstage", df_path, row).await?;
                if one_path {
                    workspaces::changes::rm(&remote_repo, "unstage", df_path).await?;
                } else {
                    workspaces::files::rm_files_from_staged(
                        &local_repo,
                        &remote_repo,
                        "unstage",
                        vec![local_repo.path.join(df_path)],
                    )
                    .await?;
                }
                let status = workspaces::changes::list(
                    &remote_repo,
                    "unstage",
                    Path::new(""),
                    DEFAULT_PAGE_NUM,
                    DEFAULT_PAGE_SIZE,
                )
                .await?;
                assert!(
                    status.modified_files.entries.is_empty(),
                    "{df_path:?} is no longer staged, got {:?}",
                    status.modified_files.entries
                );
                index(&remote_repo, "unstage", df_path).await?;
                assert_eq!(
                    staged_height(&remote_repo, "unstage", df_path).await?,
                    og_height,
                    "{df_path:?} was unstaged without its added row"
                );
            }

            // A clone fast-forwards on each pull of a data frame committed from a workspace, nested
            // or at the root
            test::run_empty_dir_test_async(|empty_dir| async move {
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &empty_dir.join("repo_b"))
                        .await?;
                for (workspace_id, df_path, row) in [
                    ("workspace_a", path.as_path(), bbox_row("d-o-double-g.jpg")),
                    (
                        "workspace_b",
                        prompts_path,
                        r#"{"prompt": "What is another meaning of life?", "label": "43"}"#
                            .to_string(),
                    ),
                ] {
                    let og =
                        tabular::read_df(cloned_repo.path.join(df_path), DFOpts::empty()).await?;
                    workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id).await?;
                    index(&remote_repo, workspace_id, df_path).await?;
                    rows::add(&remote_repo, workspace_id, df_path, row).await?;
                    workspaces::commit(
                        &remote_repo,
                        DEFAULT_BRANCH_NAME,
                        workspace_id,
                        &commit_body(workspace_id),
                    )
                    .await?;

                    repositories::pull(&cloned_repo).await?;
                    let df =
                        tabular::read_df(cloned_repo.path.join(df_path), DFOpts::empty()).await?;
                    assert_eq!(df.height(), og.height() + 1, "{df_path:?}");
                }
                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }
}
