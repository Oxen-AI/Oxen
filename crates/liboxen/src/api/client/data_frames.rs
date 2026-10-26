use std::path::Path;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::opts::DFOpts;
use crate::util;
use crate::view::commit::CommitResponse;
use crate::view::data_frames::FromDirectoryRequest;
use crate::view::{JsonDataFrameViewResponse, StatusMessage};

pub async fn get(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
    opts: DFOpts,
) -> Result<JsonDataFrameViewResponse, OxenError> {
    let path_str = util::fs::to_unix_str(path);
    let query_str = opts.to_http_query_params();
    let uri = format!("/data_frames/{commit_or_branch}/{path_str}?{query_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("got body: {body}");
    let response: Result<JsonDataFrameViewResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(val) => {
            log::debug!("got JsonDataFrameViewResponse: {val:?}");
            Ok(val)
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn index(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
) -> Result<StatusMessage, OxenError> {
    let path_str = path.as_ref().to_str().unwrap();
    let uri = format!("/data_frames/index/{commit_or_branch}/{path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    let res = client.post(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);

    match response {
        Ok(val) => {
            log::debug!("got StatusMessage: {val:?}");
            Ok(val)
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn from_directory(
    remote_repo: &RemoteRepository,
    commit_or_branch: &str,
    path: impl AsRef<Path>,
    request: FromDirectoryRequest,
) -> Result<CommitResponse, OxenError> {
    let path_str = util::fs::to_unix_str(path);
    let uri = format!("/data_frames/from_directory/{commit_or_branch}/{path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let json_body = serde_json::to_string(&request)?;

    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(json_body)
        .send()
        .await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("got body: {body}");

    let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => {
            log::debug!("got CommitResponse: {val:?}");
            Ok(val)
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::repositories::branches::create_checkout;
    use crate::repositories::data_frames::schemas;
    use crate::util::fs;

    use std::path::Path;

    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_PAGE_SIZE};
    use crate::error::OxenError;
    use crate::opts::{DFOpts, SliceRange};
    use crate::repositories;
    use crate::test;
    use crate::view::data_frames::FromDirectoryRequest;
    use crate::view::data_frames::columns::NewColumn;

    use serde_json::json;

    use super::{from_directory, get, index};

    fn listing_request(output_path: &str, recursive: bool) -> FromDirectoryRequest {
        FromDirectoryRequest {
            output_path: Some(output_path.to_string()),
            extra_columns: None,
            commit_message: Some(format!("Generated {output_path}")),
            user_name: Some("test_user".to_string()),
            user_email: Some("test@example.com".to_string()),
            recursive: Some(recursive),
        }
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_page_slice_query_and_list_remote_data_frames() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = local_repo.path.clone();
            // 1,000 rows by 11 columns, whose lefteye_x cycles through 60 to 79
            let large_csv = Path::new("large_files").join("test.csv");
            let rows = 1_000;
            let lefteye_x = |row: usize| 60 + row % 20;
            let mut csv = String::from(
                "image_id,lefteye_x,lefteye_y,righteye_x,righteye_y,nose_x,nose_y,\
                 leftmouth_x,leftmouth_y,rightmouth_x,rightmouth_y\n",
            );
            for row in 1..=rows {
                let x = lefteye_x(row);
                csv.push_str(&format!(
                    "{row:06}.jpg,{x},109,106,113,77,142,73,152,108,154\n"
                ));
            }
            fs::create_dir_all(repo_dir.join("large_files"))?;
            test::write_txt_file_to_path(repo_dir.join(&large_csv), csv)?;
            // prompt,response,is_correct,response_time,difficulty
            let mixed_csv = Path::new("csvs").join("test.csv");
            let parquet = Path::new("data").join("test.parquet");
            for (path, from) in [
                (
                    &mixed_csv,
                    test::test_csv_file_with_name("mixed_data_types.csv"),
                ),
                (&parquet, test::test_1k_parquet()),
                (
                    &Path::new("test_images").join("image1.jpg"),
                    test::test_img_file_with_name("cat_1.jpg"),
                ),
                (
                    &Path::new("test_images").join("image2.png"),
                    test::test_img_file_with_name("cat_rgba.png"),
                ),
                (
                    &Path::new("test_images").join("image3.jpeg"),
                    test::test_img_file_with_name("dog_1.jpg"),
                ),
                (
                    &Path::new("test_mixed").join("image1.jpg"),
                    test::test_img_file_with_name("cat_1.jpg"),
                ),
            ] {
                fs::create_dir_all(repo_dir.join(path).parent().expect("a parent"))?;
                fs::copy(from, repo_dir.join(path))?;
            }
            for (path, contents) in [
                (Path::new("test_files").join("file1.txt"), "content1"),
                (Path::new("test_files").join("file2.txt"), "content2"),
                (
                    Path::new("test_files").join("subdir").join("file3.txt"),
                    "content3",
                ),
                (Path::new("test_mixed").join("file1.txt"), "content1"),
            ] {
                fs::create_dir_all(repo_dir.join(&path).parent().expect("a parent"))?;
                test::write_txt_file_to_path(repo_dir.join(path), contents)?;
            }
            repositories::add(&local_repo, &repo_dir).await?;
            repositories::commit(&local_repo, "add data frames, images, and text files")?;

            let large_ref = large_csv.to_string_lossy().to_string();
            let schema_metadata = json!({"description": "A dataset of faces", "task": "gen_faces"});
            let column_metadata = json!({"root": "images"});
            schemas::add_column_metadata(&local_repo, &large_ref, "image_id", &column_metadata)?;
            schemas::add_schema_metadata(&local_repo, &large_ref, &schema_metadata)?;
            repositories::commit(&local_repo, "add test.csv schema metadata")?;

            let remote_repo = test::connect_remote_repo(&mut local_repo).await?;
            let result = get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &mixed_csv,
                DFOpts::empty(),
            )
            .await;
            assert!(result.is_err(), "nothing is pushed yet");
            repositories::push(&local_repo).await?;

            // Pages of a csv, with the schema metadata committed on it
            for (page, page_size) in [(None, 10), (Some(1), 20)] {
                let opts = DFOpts {
                    page,
                    page_size: Some(page_size),
                    ..DFOpts::empty()
                };
                let df = get(&remote_repo, DEFAULT_BRANCH_NAME, &large_csv, opts)
                    .await?
                    .data_frame;
                assert_eq!((df.source.size.height, df.source.size.width), (rows, 11));
                assert_eq!(df.view.pagination.page_number, 1);
                assert_eq!(df.view.pagination.page_size, page_size);
                assert_eq!(df.view.pagination.total_entries, rows);
                assert_eq!(df.view.pagination.total_pages, rows.div_ceil(page_size));
                assert_eq!(df.view.data.as_array().expect("rows").len(), page_size);
                for schema in [&df.source.schema, &df.view.schema] {
                    assert_eq!(schema.metadata, Some(schema_metadata.clone()));
                    assert_eq!(schema.fields[0].metadata, Some(column_metadata.clone()));
                }
            }

            // Pages of a sql query over an indexed csv
            index(&remote_repo, DEFAULT_BRANCH_NAME, &large_ref).await?;
            let matching = (1..=rows).filter(|&row| lefteye_x(row) > 70).count();
            for page in [1, 2] {
                let opts = DFOpts {
                    page: Some(page),
                    page_size: Some(100),
                    sql: Some(
                        "SELECT image_id,lefteye_x,lefteye_y FROM df WHERE lefteye_x > 70"
                            .to_string(),
                    ),
                    ..DFOpts::empty()
                };
                let df = get(&remote_repo, DEFAULT_BRANCH_NAME, &large_csv, opts)
                    .await?
                    .data_frame;
                assert_eq!((df.source.size.height, df.source.size.width), (rows, 11));
                assert_eq!((df.view.size.height, df.view.size.width), (100, 4));
                assert_eq!(df.view.data.as_array().expect("rows").len(), 100);
                assert_eq!(df.view.pagination.page_number, page);
                assert_eq!(df.view.pagination.page_size, 100);
                assert_eq!(df.view.pagination.total_entries, matching);
                assert_eq!(df.view.pagination.total_pages, matching.div_ceil(100));
            }

            // Columns, pages, and a slice of a parquet file
            for (page, page_size, slice, height, first_title) in [
                (None, None, None, DEFAULT_PAGE_SIZE, "Anarchism"),
                (Some(4), Some(5), None, 5, "Ayn Rand"),
                (None, None, Some(SliceRange::new(330, 333)?), 3, "April 26"),
            ] {
                let opts = DFOpts {
                    page,
                    page_size,
                    slice,
                    columns: Some("id,title".to_string()),
                    ..DFOpts::empty()
                };
                let df = get(&remote_repo, DEFAULT_BRANCH_NAME, &parquet, opts)
                    .await?
                    .data_frame;
                assert_eq!((df.source.size.height, df.source.size.width), (1024, 3));
                assert_eq!(
                    (df.view.size.height, df.view.size.width),
                    (height, 2),
                    "{first_title}"
                );
                assert_eq!(df.view.data.as_array().expect("rows").len(), height);
                assert_eq!(df.view.data[0]["title"], first_title);
            }
            let opts = DFOpts {
                page: Some(4),
                page_size: Some(5),
                ..DFOpts::empty()
            };
            let pagination = get(&remote_repo, DEFAULT_BRANCH_NAME, &parquet, opts)
                .await?
                .data_frame
                .view
                .pagination;
            assert_eq!(
                (
                    pagination.page_number,
                    pagination.total_entries,
                    pagination.total_pages
                ),
                (4, 1024, 205)
            );
            let pagination = get(&remote_repo, DEFAULT_BRANCH_NAME, &parquet, DFOpts::empty())
                .await?
                .data_frame
                .view
                .pagination;
            assert_eq!(
                (
                    pagination.page_number,
                    pagination.page_size,
                    pagination.total_pages
                ),
                (1, DEFAULT_PAGE_SIZE, 11)
            );

            // Schema metadata committed on a branch is served once the branch is pushed
            let branch_name = "new_branch";
            create_checkout(&local_repo, branch_name)?;
            let mixed_ref = mixed_csv.to_string_lossy().to_string();
            let schema_metadata =
                json!({"task": "chat_bot", "description": "some generic description"});
            let column_metadata = json!({"values": [0, 1, 2]});
            schemas::add_schema_metadata(&local_repo, &mixed_ref, &schema_metadata)?;
            schemas::add_column_metadata(&local_repo, &mixed_ref, "difficulty", &column_metadata)?;
            repositories::commit(&local_repo, "add test.csv schema metadata")?;
            let result = get(&remote_repo, branch_name, &mixed_ref, DFOpts::empty()).await;
            assert!(result.is_err(), "the branch is not pushed yet");
            repositories::push(&local_repo).await?;
            let schema = get(&remote_repo, branch_name, &mixed_ref, DFOpts::empty())
                .await?
                .data_frame
                .source
                .schema;
            let fields: Vec<(&str, &str)> = schema
                .fields
                .iter()
                .map(|f| (f.name.as_str(), f.dtype.as_str()))
                .collect();
            assert_eq!(
                fields,
                [
                    ("prompt", "str"),
                    ("response", "str"),
                    ("is_correct", "bool"),
                    ("response_time", "f64"),
                    ("difficulty", "i64")
                ]
            );
            assert_eq!(schema.metadata, Some(schema_metadata));
            assert_eq!(schema.fields[4].metadata, Some(column_metadata));

            // List a directory as a data frame, recursively and with extra columns
            let mut request = listing_request("file_listing.parquet", true);
            request.extra_columns = Some(vec![
                NewColumn {
                    name: "file_size".to_string(),
                    data_type: "float".to_string(),
                },
                NewColumn {
                    name: "file_type".to_string(),
                    data_type: "float".to_string(),
                },
            ]);
            let response =
                from_directory(&remote_repo, DEFAULT_BRANCH_NAME, "test_files", request).await?;
            assert_eq!(response.status.status_message, "resource_created");
            assert!(!response.commit.id.is_empty());
            assert_eq!(response.commit.message, "Generated file_listing.parquet");
            let files = get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "file_listing.parquet",
                DFOpts::empty(),
            )
            .await?;
            let p_df = files.data_frame.view.to_df().await;
            assert_eq!((p_df.height(), p_df.width()), (3, 3));
            let file_paths: Vec<&str> = p_df.column("file_path")?.str()?.no_null_iter().collect();
            for path in [
                Path::new("test_files").join("file1.txt"),
                Path::new("test_files").join("file2.txt"),
                Path::new("test_files").join("subdir").join("file3.txt"),
            ] {
                assert!(
                    file_paths.contains(&path.to_string_lossy().as_ref()),
                    "{path:?} in {file_paths:?}"
                );
            }

            let outside = FromDirectoryRequest {
                commit_message: None,
                ..listing_request("../file_listing.parquet", true)
            };
            let err = from_directory(&remote_repo, DEFAULT_BRANCH_NAME, "test_files", outside)
                .await
                .expect_err("an output path above the workspace is refused");
            assert!(
                err.to_string().contains("outside the working tree"),
                "{err}"
            );

            // A listing of only images renders its paths as images, and a mixed one does not
            let response = from_directory(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "test_images",
                listing_request("image_listing.parquet", false),
            )
            .await?;
            assert_eq!(response.status.status_message, "resource_created");
            assert_eq!(response.commit.message, "Generated image_listing.parquet");
            let files = get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "image_listing.parquet",
                DFOpts::empty(),
            )
            .await?;
            assert_eq!(files.data_frame.view.to_df().await.height(), 3);
            let field = files
                .data_frame
                .view
                .schema
                .fields
                .iter()
                .find(|f| f.name == "file_path");
            let metadata = field
                .expect("a file_path column")
                .metadata
                .as_ref()
                .expect("render metadata");
            assert_eq!(metadata["_oxen"]["render"]["func"], "image");

            from_directory(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "test_mixed",
                listing_request("mixed_listing.parquet", false),
            )
            .await?;
            let files = get(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "mixed_listing.parquet",
                DFOpts::empty(),
            )
            .await?;
            let field = files
                .data_frame
                .view
                .schema
                .fields
                .iter()
                .find(|f| f.name == "file_path");
            let render = field
                .expect("a file_path column")
                .metadata
                .as_ref()
                .and_then(|m| m["_oxen"].get("render"));
            assert!(
                render.is_none(),
                "a mixed listing has no image render metadata"
            );

            Ok(())
        })
        .await
    }
}
