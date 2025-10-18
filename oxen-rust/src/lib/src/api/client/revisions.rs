use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{ParsedResource, RemoteRepository};
use crate::view::ParseResourceResponse;

pub async fn get(
    repository: &RemoteRepository,
    revision: impl AsRef<str>,
) -> Result<Option<ParsedResource>, OxenError> {
    let revision = revision.as_ref();
    let uri = format!("/revisions/{revision}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("api::client::revisions::get {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    if res.status() == 404 {
        return Ok(None);
    }

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("api::client::revisions::get Got response {body}");
    let response: Result<ParseResourceResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(j_res) => Ok(Some(j_res.resource)),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::revisions::get() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::error::OxenError;

    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_get_revision_from_commit() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
            let commit = repositories::commits::head_commit(&local_repo)?;

            let revision = api::client::revisions::get(&remote_repo, &commit.id).await?;

            assert!(revision.is_some());
            assert!(revision.unwrap().commit.unwrap().id == commit.id);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_revision_from_branch() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
            let branch = repositories::branches::current_branch(&local_repo)?.unwrap();

            let revision = api::client::revisions::get(&remote_repo, &branch.name).await?;

            assert!(revision.is_some());
            assert!(revision.unwrap().commit.unwrap().id == branch.commit_id);

            Ok(remote_repo)
        })
        .await
    }
}
