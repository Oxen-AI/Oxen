use crate::api::client;
use crate::api::requests::{RepoNew, TransferNamespaceRequest};
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, Remote, RemoteRepository};
use crate::view::repository::RepositoryCreationResponse;
use crate::view::{RepositoryResponse, StatusMessage};
use crate::{api, repositories};
use serde_json::json;
use serde_json::value;
use std::fmt;

const CLONE: &str = "clone";
const PUSH: &str = "push";
const PUSH_WORKSPACE: &str = "push_workspace";
const UPLOAD: &str = "upload";
const DOWNLOAD: &str = "download";
const PULL: &str = "pull";
const FETCH: &str = "fetch";

enum ActionEventState {
    Started,
    Completed,
}

impl fmt::Display for ActionEventState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ActionEventState::Started => write!(f, "started"),
            ActionEventState::Completed => write!(f, "completed"),
        }
    }
}

/// Gets remote "origin" that is set on the local repo
pub async fn get_default_remote(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let remote = repo
        .get_remote(DEFAULT_REMOTE_NAME)
        .ok_or_else(|| OxenError::RemoteNotSet(DEFAULT_REMOTE_NAME.to_string()))?;
    api::client::repositories::get_by_remote(&remote).await
}

pub async fn get_by_remote_repo(repo: &RemoteRepository) -> Result<RemoteRepository, OxenError> {
    get_by_remote(&repo.remote).await
}

pub async fn get_by_name_host_and_scheme(
    name: impl AsRef<str>,
    host: impl AsRef<str>,
    scheme: impl AsRef<str>,
) -> Result<RemoteRepository, OxenError> {
    let name = name.as_ref();
    let url = api::endpoint::remote_url_from_name_and_scheme(host.as_ref(), name, scheme.as_ref());
    log::debug!("get_by_name_host_and_scheme({name}) remote url: {url}");
    get_by_url(&url).await
}

pub async fn get_by_name_host_and_remote(
    name: impl AsRef<str>,
    host: impl AsRef<str>,
    scheme: impl AsRef<str>,
    remote: impl AsRef<str>,
) -> Result<RemoteRepository, OxenError> {
    let name = name.as_ref();
    let scheme = scheme.as_ref();
    let url = api::endpoint::remote_url_from_name_and_scheme(host.as_ref(), name, scheme);
    log::debug!("get_by_name_host_and_remote({name}) remote url: {url}");
    let remote = Remote::new(remote.as_ref(), &url);
    get_by_remote(&remote).await
}

pub async fn exists(repo: &RemoteRepository) -> Result<bool, OxenError> {
    match get_by_remote_repo(repo).await {
        Ok(_) => Ok(true),
        Err(OxenError::RemoteRepoNotFound(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

pub async fn get_by_url(url: &str) -> Result<RemoteRepository, OxenError> {
    let remote = Remote::new(DEFAULT_REMOTE_NAME, url);
    get_by_remote(&remote).await
}

/// Resolve `remote` against its server, taking the UUID the repository reports where the remote has
/// none recorded. Errors when the recorded and reported UUIDs disagree.
#[tracing::instrument(skip_all)]
pub async fn get_by_remote(remote: &Remote) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_remote(remote, "")?;
    log::debug!("get_by_remote url: {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    log::debug!("get_by_remote status: {}", res.status());
    if 404 == res.status() {
        return Err(OxenError::RemoteRepoNotFound(remote.url.as_str().into()));
    }

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("repositories::get_by_remote {url}\n {body}");

    let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(j_res) => {
            remote.ensure_same_repo_uuid(j_res.repository.repo_uuid)?;
            Ok(RemoteRepository::from_view(&j_res.repository, remote))
        }
        Err(err) => {
            log::debug!("Err: {err}");
            Err(OxenError::basic_str(format!(
                "get_by_remote Could not deserialize repository [{url}]"
            )))
        }
    }
}

pub async fn create_empty(repo: RepoNew) -> Result<RemoteRepository, OxenError> {
    let namespace = repo.namespace.as_ref();
    let repo_name = repo.name.as_ref();
    let host = repo.host();
    let scheme = repo.scheme();

    let url = api::endpoint::url_from_host_and_scheme(&host, "", &scheme);
    let body = serde_json::to_string(&repo)?;
    log::debug!("Create remote: {} {}\n{}", url, repo.repo_id(), body);
    // no user agent, otherwise the create will fail when going through the hub
    let client = client::new_for_url_no_user_agent(&url)?;
    log::debug!("client: {client:?}");

    match client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("repositories::create response {body}");
            let response: RepositoryCreationResponse = serde_json::from_str(&body)?;
            Ok(RemoteRepository::from_creation_view(
                &response.repository,
                &Remote::new(
                    DEFAULT_REMOTE_NAME,
                    &api::endpoint::remote_url_from_namespace_name_scheme(
                        &host, namespace, repo_name, &scheme,
                    ),
                ),
            ))
        }
        Err(err) => {
            log::error!("Failed to create remote url {url}\n{err:?}");
            let err = format!(
                "Create repository could not connect to {url}. Make sure you have the correct server and that it is running."
            );
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn create(repo_new: RepoNew) -> Result<RemoteRepository, OxenError> {
    let host = repo_new.host();
    let scheme = repo_new.scheme();
    let url = api::endpoint::url_from_host_and_scheme(&host, "", scheme);

    // convert repo_new to json with serde
    log::debug!("Create remote: {url}\n{repo_new:?}");

    // no user agent, otherwise the create will fail when going through the hub
    let client = client::new_for_url_no_user_agent(&url)?;
    if let Ok(res) = client.post(&url).json(&repo_new).send().await {
        let body = client::parse_json_body(&url, res).await?;

        log::debug!("repositories::create response {body}");
        let response: Result<RepositoryCreationResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(RemoteRepository::from_creation_view(
                &response.repository,
                &Remote::new(
                    DEFAULT_REMOTE_NAME,
                    &api::endpoint::remote_url_from_namespace_name_scheme(
                        &host,
                        &repo_new.namespace,
                        &repo_new.name,
                        &repo_new.scheme(),
                    ),
                ),
            )),
            Err(err) => {
                let err = format!(
                    "Could not create or find repository [{}]: {err}\n{body}",
                    repo_new.repo_id()
                );
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        let err = format!(
            "Create repository could not connect to {url}. Make sure you have the correct server and that it is running."
        );
        Err(OxenError::basic_str(err))
    }
}

/// Send an HTTP call to re-create a local repository on oxen-server.
///
/// NOTE: Ignores the Merkle store kind in the local repository. The server controls which kind
///       it uses when creating new repositories. A repository can be migrated manually by using
///       the server's migration endpoint and the file <> lmdb bi-directional optional migration.
pub async fn create_from_local(
    repository: &LocalRepository,
    mut repo_new: RepoNew,
) -> Result<RemoteRepository, OxenError> {
    let host = repo_new.host();
    let url = api::endpoint::url_from_host(&host, "");
    repo_new.root_commit = repositories::commits::root_commit_maybe(repository).await?;

    // convert repo_new to json with serde
    // let params = serde_json::to_string(&repo_new)?;
    log::debug!("repositories::create_from_local: {url}\n{repo_new:?}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).json(&repo_new).send().await?;
    let body = client::parse_json_body(&url, res).await?;

    log::debug!("repositories::create_from_local response {body}");
    let response = serde_json::from_str::<RepositoryCreationResponse>(&body);
    match response {
        Ok(response) => Ok(RemoteRepository::from_creation_view(
            &response.repository,
            &Remote::new(
                DEFAULT_REMOTE_NAME,
                &api::endpoint::remote_url_from_namespace_name(
                    &host,
                    &repo_new.namespace,
                    &repo_new.name,
                ),
            ),
        )),
        Err(err) => Err(OxenError::FailCreateOrFindRemoteRepo {
            repo_id: repo_new.repo_id(),
            err,
            body,
        }),
    }
}

pub async fn delete(repository: &RemoteRepository) -> Result<StatusMessage, OxenError> {
    let url = repository.api_url()?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.delete(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response = serde_json::from_str::<StatusMessage>(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(format!(
                "Could not delete repository \n\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::delete() Request failed",
        ))
    }
}

pub async fn delete_from_url(url: String) -> Result<StatusMessage, OxenError> {
    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.delete(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response = serde_json::from_str::<StatusMessage>(&body);
        match response {
            Ok(val) => Ok(val),
            Err(_) => Err(OxenError::basic_str(format!(
                "Could not delete repository \n\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::delete() Request failed",
        ))
    }
}

/// Move `repository` into `to_namespace`, recording `to_namespace_name` as what that namespace is
/// called where `to_namespace` addresses it by UUID.
pub async fn transfer_namespace(
    repository: &RemoteRepository,
    to_namespace: &str,
    to_namespace_name: Option<&str>,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_repo(repository, "/transfer")?;
    let params = serde_json::to_string(&TransferNamespaceRequest {
        namespace: to_namespace.to_string(),
        namespace_name: to_namespace_name.map(str::to_string),
    })?;

    let client = client::new_for_url(&url)?;

    if let Ok(res) = client.patch(&url).body(params).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response = serde_json::from_str::<RepositoryResponse>(&body);
        match response {
            Ok(response) => {
                // Update remote to reflect new namespace
                let (scheme, host) = api::client::get_scheme_and_host_from_url(&url)?;

                let new_remote_url = api::endpoint::remote_url_from_namespace_name_scheme(
                    &host,
                    &response.repository.namespace,
                    &repository.name,
                    &scheme,
                );
                // A move changes where the remote points, not the repository's identity.
                let new_remote = Remote {
                    url: new_remote_url,
                    ..repository.remote.clone()
                };

                Ok(RemoteRepository::from_view(
                    &response.repository,
                    &new_remote,
                ))
            }
            Err(err) => {
                let err = format!("Could not transfer repository: {err}\n{body}");
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        Err(OxenError::basic_str(
            "api::repositories::transfer_namespace() Request failed",
        ))
    }
}

pub async fn pre_clone(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = CLONE;
    action_hook(repository, action_name, ActionEventState::Started, None).await
}

pub async fn post_clone(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = CLONE;
    action_hook(repository, action_name, ActionEventState::Completed, None).await
}

pub async fn pre_upload(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = UPLOAD;
    action_hook(repository, action_name, ActionEventState::Started, None).await
}

pub async fn post_upload(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = UPLOAD;
    action_hook(repository, action_name, ActionEventState::Completed, None).await
}

pub async fn pre_download(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = DOWNLOAD;
    action_hook(repository, action_name, ActionEventState::Started, None).await
}

pub async fn post_download(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = DOWNLOAD;
    action_hook(repository, action_name, ActionEventState::Completed, None).await
}

#[tracing::instrument(skip_all)]
pub async fn pre_pull(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = PULL;
    action_hook(repository, action_name, ActionEventState::Started, None).await
}

#[tracing::instrument(skip_all)]
pub async fn post_pull(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = PULL;
    action_hook(repository, action_name, ActionEventState::Completed, None).await
}

#[tracing::instrument(skip_all)]
pub async fn pre_fetch(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = FETCH;
    action_hook(repository, action_name, ActionEventState::Started, None).await
}

#[tracing::instrument(skip_all)]
pub async fn post_fetch(repository: &RemoteRepository) -> Result<(), OxenError> {
    let action_name = FETCH;
    action_hook(repository, action_name, ActionEventState::Completed, None).await
}

pub async fn pre_push(
    repository: &RemoteRepository,
    branch: &Branch,
    commit_id: &str,
) -> Result<(), OxenError> {
    let action_name = PUSH;
    let body = json!({
        "branch": {
            "name": branch.name,
            "commit_id": commit_id
        }
    });
    action_hook(
        repository,
        action_name,
        ActionEventState::Started,
        Some(body),
    )
    .await
}

pub async fn pre_push_workspace(repository: &RemoteRepository) -> Result<(), OxenError> {
    action_hook(repository, PUSH_WORKSPACE, ActionEventState::Started, None).await
}

pub async fn post_push(
    repository: &RemoteRepository,
    branch: &Branch,
    commit_id: &str,
) -> Result<(), OxenError> {
    let action_name = PUSH;
    let body = json!({
        "branch": {
            "name": branch.name,
            "commit_id": commit_id
        }
    });
    action_hook(
        repository,
        action_name,
        ActionEventState::Completed,
        Some(body),
    )
    .await
}

pub async fn post_push_workspace(repository: &RemoteRepository) -> Result<(), OxenError> {
    action_hook(
        repository,
        PUSH_WORKSPACE,
        ActionEventState::Completed,
        None,
    )
    .await
}

async fn action_hook(
    repository: &RemoteRepository,
    action_name: &str,
    state: ActionEventState,
    body: Option<value::Value>,
) -> Result<(), OxenError> {
    let uri = format!("/action/{state}/{action_name}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    let client = client::new_for_url(&url)?;

    let mut request = client.post(&url);

    // Add body if the action type requires it
    if let Some(body_data) = body {
        request = request.json(&body_data);
    }

    match request.send().await {
        Ok(_) => Ok(()),
        _ => {
            let err = "api::repositories::action_hook() Request failed";
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        create, create_from_local, delete, get_by_remote_repo, get_default_remote, post_clone,
        post_push, pre_clone, pre_push, transfer_namespace,
    };
    use crate::api::client::dir;
    use crate::api::requests::RepoNew;
    use crate::config::UserConfig;
    use crate::constants::{DEFAULT_BRANCH_NAME, DEFAULT_NAMESPACE};
    use crate::error::OxenError;
    use crate::model::Branch;
    use crate::model::file::{FileContents, FileNew};
    use crate::test;
    use crate::view::entries::EMetadataEntry;
    use uuid::Uuid;

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_remote_repository_lifecycle() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let namespace = DEFAULT_NAMESPACE;
            let name = local_repo.dirname();

            let repo_new =
                RepoNew::from_namespace_name_host(namespace, &name, test::test_host(), None);
            let remote_repo = create_from_local(&local_repo, repo_new).await?;
            assert_eq!(
                remote_repo.name, name,
                "the remote takes the local repo's name"
            );

            let by_name = get_by_remote_repo(&remote_repo).await?;
            assert_eq!(by_name.namespace, remote_repo.namespace);
            assert_eq!(by_name.name, remote_repo.name);

            // The clone and push hooks each post to their action endpoint.
            let mut server = mockito::Server::new_async().await;
            let mut mocked = remote_repo.clone();
            mocked.remote.url = format!("{}/{namespace}/{name}", server.url());
            let actions = format!("/api/repos/{namespace}/{name}/action");
            let branch = Branch {
                name: DEFAULT_BRANCH_NAME.to_string(),
                commit_id: "1".to_string(),
            };

            let hook = server
                .mock("POST", format!("{actions}/started/clone").as_str())
                .create_async()
                .await;
            pre_clone(&mocked).await?;
            hook.assert();

            let hook = server
                .mock("POST", format!("{actions}/completed/clone").as_str())
                .create_async()
                .await;
            post_clone(&mocked).await?;
            hook.assert();

            let hook = server
                .mock("POST", format!("{actions}/started/push").as_str())
                .create_async()
                .await;
            pre_push(&mocked, &branch, &branch.commit_id).await?;
            hook.assert();

            let hook = server
                .mock("POST", format!("{actions}/completed/push").as_str())
                .create_async()
                .await;
            post_push(&mocked, &branch, &branch.commit_id).await?;
            hook.assert();

            // Every operation on an existing repository resolves through `get_by_remote`, so a URL
            // that has come to point at a different repository is refused there rather than
            // addressing the other repository's storage.
            let mut attached = remote_repo.clone();
            attached.remote.repo_uuid = Some(Uuid::new_v4());
            test::attach_remote_repo(&mut local_repo, &attached)?;
            let result = get_default_remote(&local_repo).await;
            assert!(
                matches!(result, Err(OxenError::RemotePointsAtDifferentRepo { .. })),
                "expected a refusal, got: {result:?}"
            );

            let transferred = transfer_namespace(&remote_repo, "new-namespace", None).await?;
            assert_eq!(transferred.namespace, "new-namespace");
            assert_eq!(transferred.name, remote_repo.name);

            delete(&transferred).await?;
            test::wait_until_remote_repo_deleted(&transferred.remote.url).await?;

            // The transfer freed the name, so a repository created from files can take it.
            let files = vec![FileNew {
                path: PathBuf::from("README"),
                contents: FileContents::Text(String::from("Hello world!")),
                user: UserConfig::get()?.to_user(),
            }];
            let mut repo_new =
                RepoNew::from_files(namespace, &name, &test::test_host(), files, None);
            repo_new.scheme = Some("http".to_string());
            let with_readme = create(repo_new).await?;
            assert_eq!(with_readme.name, name);

            let entries = dir::list_root(&with_readme).await?;
            assert_eq!(
                entries.entries.len(),
                1,
                "the repository holds only the file it was given"
            );
            match &entries.entries[0] {
                EMetadataEntry::MetadataEntry(meta) => assert_eq!(meta.filename, "README"),
                _ => panic!("Expected a MetadataEntry"),
            }

            delete(&with_readme).await?;
            Ok(())
        })
        .await
    }
}
