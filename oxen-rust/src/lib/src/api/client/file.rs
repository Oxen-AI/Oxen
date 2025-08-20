use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::commit::NewCommitBody;
use crate::model::RemoteRepository;
use crate::view::CommitResponse;

use reqwest::multipart::{Form, Part};
use std::path::Path;

use bytes::Bytes;

/// Helper function that contains the common logic for putting a file
async fn put_file_impl(
    client: reqwest::Client,
    url: String,
    file_path: impl AsRef<Path>,
    file_name: Option<impl AsRef<str>>,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let file_path = file_path.as_ref();
    let file_part = Part::file(file_path).await?;
    let file_part = if let Some(file_name) = file_name {
        file_part.file_name(file_name.as_ref().to_string())
    } else {
        file_part
    };
    let mut form = Form::new().part("file", file_part);

    if let Some(body) = commit_body {
        form = form.text("name", body.author);
        form = form.text("email", body.email);
        form = form.text("message", body.message);
    }

    let req = client.put(&url).multipart(form);

    let res = req.send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response)
}

pub async fn put_file(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    directory: impl AsRef<str>,
    file_path: impl AsRef<Path>,
    file_name: Option<impl AsRef<str>>,
    commit_body: Option<NewCommitBody>,
) -> Result<CommitResponse, OxenError> {
    let branch = branch.as_ref();
    let directory = directory.as_ref();
    let file_path = file_path.as_ref();
    let uri = format!("/file/{branch}/{directory}");
    log::debug!("put_file {uri:?}, file_path {file_path:?}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    put_file_impl(client, url, file_path, file_name, commit_body).await
}

pub async fn put_file_with_bearer_token(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    directory: impl AsRef<str>,
    file_path: impl AsRef<Path>,
    file_name: Option<impl AsRef<str>>,
    commit_body: Option<NewCommitBody>,
    bearer_token: &str,
) -> Result<CommitResponse, OxenError> {
    let branch = branch.as_ref();
    let directory = directory.as_ref();
    let file_path = file_path.as_ref();
    let uri = format!("/file/{branch}/{directory}");
    log::debug!("put_file_with_bearer_token {uri:?}, file_path {file_path:?}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url_with_bearer_token(&url, bearer_token)?;
    put_file_impl(client, url, file_path, file_name, commit_body).await
}

pub async fn get_file(
    remote_repo: &RemoteRepository,
    branch: impl AsRef<str>,
    file_path: impl AsRef<Path>,
) -> Result<Bytes, OxenError> {
    let branch = branch.as_ref();
    let file_path = file_path.as_ref().to_str().unwrap();
    let uri = format!("/file/{branch}/{file_path}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = res.bytes().await?;
    Ok(body)
}

#[cfg(test)]
mod tests {

    use actix_web::web::Bytes;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::{api, test, util};


    #[tokio::test]
    async fn test_get_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let file_path = test::test_bounding_box_csv();
            let bytes = api::client::file::get_file(&remote_repo, branch_name, file_path).await;

            assert!(bytes.is_ok());
            assert!(!bytes.unwrap().is_empty());

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

            let bytes = api::client::file::get_file(&remote_repo, workspace_id, file_path).await;

            assert!(bytes.is_ok());
            assert!(!bytes.as_ref().unwrap().is_empty());
            assert_eq!(bytes.unwrap(), Bytes::from_static(b"test content"));

            Ok(remote_repo)
        })
        .await
    }
}
