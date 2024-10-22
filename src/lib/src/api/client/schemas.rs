//! # Remote Schemas
//!
//! Interact with remote schemas.
//!

use std::path::Path;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::schema::SchemaWithPath;
use crate::view::ListSchemaResponse;

pub async fn list(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<SchemaWithPath>, OxenError> {
    let revision = revision.as_ref();

    let uri = format!("/schemas/{revision}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<ListSchemaResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got ListSchemaResponse: {:?}", val);
                    Ok(val.schemas)
                }
                Err(err) => Err(OxenError::basic_str(format!(
                    "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn get(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<Option<SchemaWithPath>, OxenError> {
    let revision = revision.as_ref();
    let path = path.as_ref();

    let uri = format!("/schemas/{revision}/{}", path.to_string_lossy());
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<ListSchemaResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got SchemaResponse: {:?}", val);
                    if !val.schemas.is_empty() {
                        Ok(val.schemas.into_iter().next())
                    } else {
                        Ok(None)
                    }
                }
                Err(err) => Err(OxenError::basic_str(format!(
                    "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use std::path::PathBuf;

    use serde_json::json;

    #[tokio::test]
    async fn test_remote_list_schemas() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|mut local_repo| async move {
            // Create the remote repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Push the repo
            repositories::push(&local_repo).await?;

            let large_dir = local_repo.path.join("csvs");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_csv_file_with_name("mixed_data_types.csv");
            util::fs::copy(from_file, &csv_file)?;

            repositories::add(&local_repo, &csv_file)?;
            repositories::commit(&local_repo, "add test.csv")?;

            // List no schemas
            let schemas = api::client::schemas::list(&remote_repo, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(schemas.len(), 0);

            // Push the repo
            repositories::push(&local_repo).await?;

            // List the one schema
            let schemas = api::client::schemas::list(&remote_repo, DEFAULT_BRANCH_NAME).await?;
            assert_eq!(schemas.len(), 1);

            // prompt,response,is_correct,response_time,difficulty
            let schema = &schemas[0].schema;
            assert_eq!(schema.fields.len(), 5);
            assert_eq!(schema.fields[0].name, "prompt");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "response");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "is_correct");
            assert_eq!(schema.fields[2].dtype, "bool");
            assert_eq!(schema.fields[3].name, "response_time");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "difficulty");
            assert_eq!(schema.fields[4].dtype, "i64");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_get_schema2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("csvs");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_csv_file_with_name("mixed_data_types.csv");
            util::fs::copy(from_file, &csv_file)?;

            // Add the file
            repositories::add(&local_repo, &csv_file)?;
            repositories::commit(&local_repo, "add test.csv")?;

            // Add some metadata to the schema
            /*
            prompt,response,is_correct,response_time,difficulty
            who is it?,issa me,true,0.5,1
            */
            let schema_ref = &PathBuf::from("csvs")
                .join("test.csv")
                .to_string_lossy()
                .to_string();

            let schema_metadata = json!({
                "task": "chat_bot",
                "description": "some generic description",
            });

            let column_name = "difficulty".to_string();
            let column_metadata = json!(
                {
                    "values": [0, 1, 2]
                }
            );
            repositories::data_frames::schemas::add_schema_metadata(
                &local_repo,
                schema_ref,
                &schema_metadata,
            )?;
            repositories::data_frames::schemas::add_column_metadata(
                &local_repo,
                schema_ref,
                &column_name,
                &column_metadata,
            )?;

            repositories::commit(&local_repo, "add test.csv schema metadata")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Cannot get schema that does not exist
            let result =
                api::client::schemas::get(&remote_repo, DEFAULT_BRANCH_NAME, schema_ref).await;
            assert!(result.is_err());

            // Push the repo
            repositories::push(&local_repo).await?;

            // List the one schema
            let schema =
                api::client::schemas::get(&remote_repo, DEFAULT_BRANCH_NAME, schema_ref).await?;

            assert!(schema.is_some());
            let schema = schema.unwrap().schema;

            // prompt,response,is_correct,response_time,difficulty
            assert_eq!(schema.fields.len(), 5);
            assert_eq!(schema.fields[0].name, "prompt");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "response");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "is_correct");
            assert_eq!(schema.fields[2].dtype, "bool");
            assert_eq!(schema.fields[3].name, "response_time");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "difficulty");
            assert_eq!(schema.fields[4].dtype, "i64");

            // Check the metadata
            assert_eq!(schema.metadata, Some(schema_metadata));
            assert_eq!(schema.fields[4].metadata, Some(column_metadata));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_get_schema_on_branch() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("csvs");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_csv_file_with_name("mixed_data_types.csv");
            util::fs::copy(from_file, &csv_file)?;

            // Add the file
            repositories::add(&local_repo, &csv_file)?;
            repositories::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            let schema_ref = &PathBuf::from("csvs")
                .join("test.csv")
                .to_string_lossy()
                .to_string();

            // Cannot get schema that does not exist
            let result =
                api::client::schemas::get(&remote_repo, DEFAULT_BRANCH_NAME, schema_ref).await;
            assert!(result.is_err());

            // Push the repo
            repositories::push(&local_repo).await?;

            // Create a new branch
            let branch_name = "new_branch";
            repositories::branches::create_checkout(&local_repo, branch_name)?;
            // Add some metadata to the schema
            /*
            prompt,response,is_correct,response_time,difficulty
            who is it?,issa me,true,0.5,1
            */
            let schema_metadata = json!({
                "task": "chat_bot",
                "description": "some generic description",
            });

            let column_name = "difficulty".to_string();
            let column_metadata = json!(
                {
                    "values": [0, 1, 2]
                }
            );
            repositories::data_frames::schemas::add_schema_metadata(
                &local_repo,
                schema_ref,
                &schema_metadata,
            )?;
            repositories::data_frames::schemas::add_column_metadata(
                &local_repo,
                schema_ref,
                &column_name,
                &column_metadata,
            )?;

            repositories::commit(&local_repo, "add test.csv schema metadata")?;

            // Push the repo
            repositories::push(&local_repo).await?;

            // List the one schema
            let schema = api::client::schemas::get(&remote_repo, branch_name, schema_ref).await?;

            assert!(schema.is_some());
            let schema = schema.unwrap().schema;

            // prompt,response,is_correct,response_time,difficulty
            assert_eq!(schema.fields.len(), 5);
            assert_eq!(schema.fields[0].name, "prompt");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "response");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "is_correct");
            assert_eq!(schema.fields[2].dtype, "bool");
            assert_eq!(schema.fields[3].name, "response_time");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "difficulty");
            assert_eq!(schema.fields[4].dtype, "i64");

            // Check the metadata
            assert_eq!(schema.metadata, Some(schema_metadata));
            assert_eq!(schema.fields[4].metadata, Some(column_metadata));

            Ok(())
        })
        .await
    }
}
