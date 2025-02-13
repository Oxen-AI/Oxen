use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::commit::NewCommitBody;
use crate::model::RemoteRepository;
use crate::view::CommitResponse;

use reqwest::multipart::{Form, Part};
use std::path::Path;

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
    let file_part = Part::file(file_path).await?;
    let file_part = if let Some(file_name) = file_name {
        file_part.file_name(file_name.as_ref().to_string())
    } else {
        file_part
    };
    let form = Form::new().part("file", file_part);

    let mut req = client.put(&url).multipart(form);

    if let Some(body) = commit_body {
        req = req
            .header("oxen-commit-author", body.author)
            .header("oxen-commit-email", body.email)
            .header("oxen-commit-message", body.message);
    }

    let res = req.send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: CommitResponse = serde_json::from_str(&body)?;
    Ok(response)
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::{api, repositories, test};

    #[tokio::test]
    async fn test_update_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let branch_name = "main";
            let directory_name = "test_data";
            let file_path = test::test_img_file();
            let commit_body = NewCommitBody {
                author: "Test Author".to_string(),
                email: "test@example.com".to_string(),
                message: "Update file test".to_string(),
            };

            let response = api::client::file::put_file(
                &remote_repo,
                branch_name,
                directory_name,
                &file_path,
                Some("test.jpeg"),
                Some(commit_body),
            )
            .await?;

            assert_eq!(response.status.status_message, "resource_created");

            // Pull changes from remote to local repo
            repositories::pull(&local_repo).await?;

            // Check that the file exists in the local repo after pulling
            let file_path_in_repo = local_repo.path.join(directory_name).join("test.jpeg");
            assert!(file_path_in_repo.exists());

            Ok(remote_repo)
        })
        .await
    }
}
