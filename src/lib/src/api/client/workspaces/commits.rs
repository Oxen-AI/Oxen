use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{Branch, Commit, NewCommitBody, RemoteRepository};
use crate::view::CommitResponse;

pub async fn commit(
    remote_repo: &RemoteRepository,
    branch_name: &str,
    identifier: &str,
    commit: &NewCommitBody,
) -> Result<Commit, OxenError> {
    let uri = format!("/workspaces/{identifier}/commit/{branch_name}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("commit_staged {}\n{:?}", url, commit);

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(&commit).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("commit_staged got body: {}", body);
    let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => {
            let commit = val.commit;
            // make sure to call our /complete call to kick off the post-push hooks
            let branch = Branch {
                name: branch_name.to_string(),
                commit_id: commit.id.clone(),
            };
            api::client::commits::post_push_complete(remote_repo, &branch, &commit.id).await?;
            api::client::repositories::post_push(remote_repo, &branch, &commit.id).await?;
            Ok(commit)
        },
        Err(err) => Err(OxenError::basic_str(format!(
            "api::staging::commit_staged error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::api;
    use crate::config::UserConfig;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::test;

    #[tokio::test]
    async fn test_commit_staged_multiple_files() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "add-data";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let directory_name = "data";
            let paths = vec![
                test::test_img_file(),
                test::test_img_file_with_name("cole_anthony.jpeg"),
            ];
            api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            let result = api::client::workspaces::files::add_many(
                &remote_repo,
                &workspace_id,
                directory_name,
                paths,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add staged data".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::client::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await?;

            let remote_commit = api::client::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit.is_some());
            assert_eq!(commit.id, remote_commit.unwrap().id);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_added_column_in_dataframe() -> Result<(), OxenError> {
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
            let column_name = "my_new_column";
            let data = format!(r#"{{"name":"{}", "data_type": "str"}}"#, column_name);

            api::client::workspaces::data_frames::index(&remote_repo, &workspace_id, &path).await?;
            let result = api::client::workspaces::data_frames::columns::create(
                &remote_repo,
                &workspace_id,
                &path,
                data.to_string(),
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Update row".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::client::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await?;

            let remote_commit = api::client::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit.is_some());
            assert_eq!(commit.id, remote_commit.unwrap().id);

            let df =
                api::client::data_frames::get(&remote_repo, branch_name, &path, DFOpts::empty())
                    .await?;

            assert_eq!(
                df.data_frame.source.schema.fields.len(),
                df.data_frame.view.schema.fields.len()
            );

            if !df
                .data_frame
                .view
                .schema
                .fields
                .iter()
                .any(|field| field.name == column_name)
            {
                panic!("Column `{}` does not exist in the data frame", column_name);
            }

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_commit_same_data_frame_file_twice() -> Result<(), OxenError> {
        test::run_remote_created_and_readme_remote_repo_test(|remote_repo| async move {
            let branch_name = "main";
            let branch = api::client::branches::create_from_branch(
                &remote_repo,
                branch_name,
                DEFAULT_BRANCH_NAME,
            )
            .await?;
            assert_eq!(branch.name, branch_name);

            let workspace_id = UserConfig::identifier()?;
            let directory_name = "";
            let paths = vec![test::test_100_parquet()];
            api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            let result = api::client::workspaces::files::add_many(
                &remote_repo,
                &workspace_id,
                directory_name,
                paths,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Adding 100 row parquet".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let commit =
                api::client::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await?;

            let remote_commit = api::client::commits::get_by_id(&remote_repo, &commit.id).await?;
            assert!(remote_commit.is_some());
            assert_eq!(commit.id, remote_commit.unwrap().id);

            // List the files on main
            let revision = "main";
            let path = "";
            let page = 1;
            let page_size = 100;
            let entries =
                api::client::dir::list(&remote_repo, revision, path, page, page_size).await?;

            // There should be the README and the parquet file
            assert_eq!(entries.total_entries, 2);
            assert_eq!(entries.entries.len(), 2);

            // Add the same file again
            let workspace_id = UserConfig::identifier()? + "2";
            api::client::workspaces::create(&remote_repo, &branch_name, &workspace_id).await?;
            let paths = vec![test::test_100_parquet()];
            let result = api::client::workspaces::files::add_many(
                &remote_repo,
                &workspace_id,
                directory_name,
                paths,
            )
            .await;
            assert!(result.is_ok());

            // Commit the changes
            let body = NewCommitBody {
                message: "Adding 100 row parquet AGAIN".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            let result =
                api::client::workspaces::commit(&remote_repo, branch_name, &workspace_id, &body)
                    .await;
            assert!(result.is_err());

            // List the files on main
            let entries =
                api::client::dir::list(&remote_repo, revision, path, page, page_size).await?;
            assert_eq!(entries.total_entries, 2);
            assert_eq!(entries.entries.len(), 2);

            Ok(remote_repo)
        })
        .await
    }
}
