use crate::api;
use crate::api::remote::client;
use crate::constants::{DEFAULT_HOST, DEFAULT_REMOTE_NAME};
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository, Remote, RemoteRepository, RepositoryNew};
use crate::view::repository::{RepositoryDataTypesResponse, RepositoryDataTypesView};
use crate::view::{NamespaceView, RepositoryResponse, StatusMessage};
use serde_json::json;
use serde_json::value;
use std::fmt;

const CLONE: &str = "clone";
const PUSH: &str = "push";

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
        .ok_or(OxenError::remote_not_set(DEFAULT_REMOTE_NAME))?;
    let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };
    Ok(remote_repo)
}

pub async fn get_by_remote_repo(
    repo: &RemoteRepository,
) -> Result<Option<RemoteRepository>, OxenError> {
    get_by_remote(&repo.remote).await
}

/// Attempts to find a repo by name on the remote "origin". For example ox/CatDog
pub async fn get_by_name_default(
    name: impl AsRef<str>,
) -> Result<Option<RemoteRepository>, OxenError> {
    get_by_host_remote_name(DEFAULT_HOST, DEFAULT_REMOTE_NAME, name).await
}

pub async fn get_by_host_remote_name(
    host: impl AsRef<str>,
    remote: impl AsRef<str>,
    name: impl AsRef<str>,
) -> Result<Option<RemoteRepository>, OxenError> {
    let name = name.as_ref();
    let url = api::endpoint::remote_url_from_name(host.as_ref(), name);
    log::debug!(
        "api::remote::repositories::get_by_host_remote_name({}) remote url: {}",
        name,
        url
    );
    let remote = Remote {
        name: String::from(remote.as_ref()),
        url,
    };
    get_by_remote(&remote).await
}

pub async fn exists(repo: &RemoteRepository) -> Result<bool, OxenError> {
    let repo = get_by_remote_repo(repo).await?;
    Ok(repo.is_some())
}

pub async fn get_by_remote(remote: &Remote) -> Result<Option<RemoteRepository>, OxenError> {
    log::debug!("api::remote::repositories::get_by_remote({:?})", remote);
    let url = api::endpoint::url_from_remote(remote, "")?;
    log::debug!("api::remote::repositories::get_by_remote url: {}", url);

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            if 404 == res.status() {
                return Ok(None);
            }

            let body = client::parse_json_body(&url, res).await?;
            log::debug!("repositories::get_by_remote {}\n {}", url, body);

            let response: Result<RepositoryResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(Some(RemoteRepository::from_view(&j_res.repository, remote))),
                Err(err) => {
                    log::debug!("Err: {}", err);
                    Err(OxenError::basic_str(format!(
                        "api::repositories::get_by_remote() Could not deserialize repository [{url}]"
                    )))
                }
            }
        }
        Err(err) => {
            log::error!("Failed to get remote url {url}\n{err:?}");
            Err(OxenError::basic_str(format!(
                "api::repositories::get_by_remote() Request failed at url {url}"
            )))
        }
    }
}

pub async fn get_repo_data_by_remote(
    remote: &Remote,
) -> Result<Option<RepositoryDataTypesView>, OxenError> {
    log::debug!(
        "api::remote::repositories::get_repo_data_by_remote({:?})",
        remote
    );
    let url = api::endpoint::url_from_remote(remote, "")?;
    log::debug!(
        "api::remote::repositories::get_repo_data_by_remote url: {}",
        url
    );

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            if 404 == res.status() {
                return Ok(None);
            }

            let body = client::parse_json_body(&url, res).await?;
            log::debug!("repositories::get_repo_data_by_remote {}\n {}", url, body);

            let response: Result<RepositoryDataTypesResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(Some(j_res.repository)),
                Err(err) => {
                    log::debug!("Err: {}", err);
                    Err(OxenError::basic_str(format!(
                        "api::repositories::get_repo_data_by_remote() Could not deserialize repository [{url}]"
                    )))
                }
            }
        }
        Err(err) => {
            log::error!("Failed to get remote url {url}\n{err:?}");
            Err(OxenError::basic_str(format!(
                "api::repositories::get_repo_data_by_remote() Request failed at url {url}"
            )))
        }
    }
}

pub async fn create_empty<S: AsRef<str>>(
    namespace: &str,
    name: &str,
    host: S,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_host(host.as_ref(), "");
    let params = json!({ "name": name, "namespace": namespace });
    log::debug!("Create remote: {} {} {}\n{}", url, namespace, name, params);

    let client = client::new_for_url_no_user_agent(&url)?;
    log::debug!("client: {:?}", client);
    match client.post(&url).json(&params).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;

            log::debug!("repositories::create response {}", body);
            let response: RepositoryResponse = serde_json::from_str(&body)?;
            Ok(RemoteRepository::from_view(
                &response.repository,
                &Remote {
                    url: api::endpoint::remote_url_from_namespace_name(
                        host.as_ref(),
                        namespace,
                        name,
                    ),
                    name: String::from("origin"),
                },
            ))
        }
        Err(err) => {
            log::error!("Failed to create remote url {url}\n{err:?}");
            let err = format!("Create repository could not connect to {url}. Make sure you have the correct server and that it is running.");
            Err(OxenError::basic_str(err))
        }
    }
}

pub async fn create<S: AsRef<str>>(
    repo_new: RepositoryNew,
    host: S,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_host(host.as_ref(), "");

    // convert repo_new to json with serde
    let params = serde_json::to_string(&repo_new)?;
    log::debug!("Create remote: {}\n{}", url, params);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).json(&params).send().await {
        let body = client::parse_json_body(&url, res).await?;

        log::debug!("repositories::create response {}", body);
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(RemoteRepository::from_view(
                &response.repository,
                &Remote {
                    url: api::endpoint::remote_url_from_namespace_name(
                        host.as_ref(),
                        &repo_new.namespace,
                        &repo_new.name,
                    ),
                    name: String::from(DEFAULT_REMOTE_NAME),
                },
            )),
            Err(err) => {
                let err = format!(
                    "Could not create or find repository [{}]: {err}\n{body}",
                    repo_new.name
                );
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        let err = format!("Create repository could not connect to {url}. Make sure you have the correct server and that it is running.");
        Err(OxenError::basic_str(err))
    }
}

pub async fn create_from_local<S: AsRef<str>>(
    repository: &LocalRepository,
    mut repo_new: RepositoryNew,
    host: S,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_host(host.as_ref(), "");
    let root_commit = api::local::commits::root_commit(repository)?;
    repo_new.root_commit = Some(root_commit);

    // convert repo_new to json with serde
    // let params = serde_json::to_string(&repo_new)?;
    log::debug!("repositories::create_from_local: {}\n{:?}", url, repo_new);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).json(&repo_new).send().await {
        let body = client::parse_json_body(&url, res).await?;

        log::debug!("repositories::create_from_local response {}", body);
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(RemoteRepository::from_view(
                &response.repository,
                &Remote {
                    url: api::endpoint::remote_url_from_namespace_name(
                        host.as_ref(),
                        &repo_new.namespace,
                        &repo_new.name,
                    ),
                    name: String::from(DEFAULT_REMOTE_NAME),
                },
            )),
            Err(err) => {
                let err = format!(
                    "Could not create or find repository [{}]: {err}\n{body}",
                    repo_new.name
                );
                Err(OxenError::basic_str(err))
            }
        }
    } else {
        let err = format!("Create repository could not connect to {url}. Make sure you have the correct server and that it is running.");
        Err(OxenError::basic_str(err))
    }
}

pub async fn delete(repository: &RemoteRepository) -> Result<StatusMessage, OxenError> {
    let url = repository.api_url()?;
    log::debug!("Deleting repository: {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.delete(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
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

pub async fn transfer_namespace(
    repository: &RemoteRepository,
    to_namespace: &str,
) -> Result<RemoteRepository, OxenError> {
    let url = api::endpoint::url_from_repo(repository, "/transfer")?;
    let params = serde_json::to_string(&NamespaceView {
        namespace: to_namespace.to_string(),
    })?;

    let client = client::new_for_url(&url)?;

    if let Ok(res) = client.patch(&url).body(params).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<RepositoryResponse, serde_json::Error> = serde_json::from_str(&body);

        match response {
            Ok(response) => {
                // Update remote to reflect new namespace
                let host = api::remote::client::get_host_from_url(&repository.remote.url)?;
                let new_remote_url = api::endpoint::remote_url_from_namespace_name(
                    &host,
                    &response.repository.namespace,
                    &repository.name,
                );
                let new_remote = Remote {
                    url: new_remote_url,
                    name: repository.remote.name.clone(),
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

async fn action_hook(
    repository: &RemoteRepository,
    action_name: &str,
    state: ActionEventState,
    body: Option<value::Value>,
) -> Result<(), OxenError> {
    let uri = format!("/action/{}/{}", state, action_name);
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
    use crate::api;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::RepositoryNew;
    use crate::test;
    use mockito;

    #[tokio::test]
    async fn test_repo_pre_and_post_clone() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let mut server = mockito::Server::new_async().await;
            let server_url = server.url();

            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();

            let mut remote_repo = test::create_remote_repo(&local_repo).await?;
            let original_remote_url = remote_repo.remote.url;
            remote_repo.remote.url = format!("{server_url}/{namespace}/{name}");

            let path = format!("/api/repos/{namespace}/{name}/action/started/clone");
            let mock_pre_clone = server.mock("POST", &path[..]).create_async().await;

            api::remote::repositories::pre_clone(&remote_repo).await?;
            mock_pre_clone.assert();

            let path = format!("/api/repos/{namespace}/{name}/action/completed/clone");
            let mock_post_clone = server.mock("POST", &path[..]).create_async().await;

            api::remote::repositories::post_clone(&remote_repo).await?;
            mock_post_clone.assert();

            // cleanup
            remote_repo.remote.url = original_remote_url;

            api::remote::repositories::delete(&remote_repo).await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_repo_pre_and_post_push() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let mut server = mockito::Server::new_async().await;
            let server_url = server.url();

            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();

            let mut remote_repo = test::create_remote_repo(&local_repo).await?;
            let original_remote_url = remote_repo.remote.url;
            remote_repo.remote.url = format!("{server_url}/{namespace}/{name}");

            let path = format!("/api/repos/{namespace}/{name}/action/started/push");
            let mock_pre_push = server.mock("POST", &path[..]).create_async().await;

            // Branch and commit id we're pushing
            let branch =
                api::local::branches::get_by_name(&local_repo, DEFAULT_BRANCH_NAME)?.unwrap();
            let commit_id = branch.commit_id.clone();

            api::remote::repositories::pre_push(&remote_repo, &branch, &commit_id).await?;
            mock_pre_push.assert();

            let path = format!("/api/repos/{namespace}/{name}/action/completed/push");
            let mock_post_push = server.mock("POST", &path[..]).create_async().await;

            api::remote::repositories::post_push(&remote_repo, &branch, &commit_id).await?;
            mock_post_push.assert();

            // cleanup
            remote_repo.remote.url = original_remote_url;

            api::remote::repositories::delete(&remote_repo).await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_create_remote_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repo_new = RepositoryNew::new(namespace, &name);
            let repository = api::remote::repositories::create_from_local(
                &local_repo,
                repo_new,
                test::test_host(),
            )
            .await?;
            println!("got repository: {repository:?}");
            assert_eq!(repository.name, name);

            // cleanup
            api::remote::repositories::delete(&repository).await?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_by_name() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repo_new = RepositoryNew::new(namespace, name);
            let repository = api::remote::repositories::create_from_local(
                &local_repo,
                repo_new,
                test::test_host(),
            )
            .await?;
            let url_repo = api::remote::repositories::get_by_remote_repo(&repository)
                .await?
                .unwrap();

            assert_eq!(repository.namespace, url_repo.namespace);
            assert_eq!(repository.name, url_repo.name);

            // cleanup
            api::remote::repositories::delete(&repository).await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repo_new = RepositoryNew::new(namespace, name);
            let repository = api::remote::repositories::create_from_local(
                &local_repo,
                repo_new,
                test::test_host(),
            )
            .await?;

            // delete
            api::remote::repositories::delete(&repository).await?;

            // We delete in a background thread, so give it a second
            std::thread::sleep(std::time::Duration::from_secs(1));

            let result = api::remote::repositories::get_by_remote_repo(&repository).await;
            assert!(result.is_ok());
            assert!(result.unwrap().is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_transfer_remote_repository() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let namespace = constants::DEFAULT_NAMESPACE;
            let name = local_repo.dirname();
            let repo_new = RepositoryNew::new(namespace, &name);
            let repository = api::remote::repositories::create_from_local(
                &local_repo,
                repo_new,
                test::test_host(),
            )
            .await?;

            let new_namespace = "new-namespace";
            let new_repository =
                api::remote::repositories::transfer_namespace(&repository, new_namespace).await?;

            assert_eq!(new_repository.namespace, new_namespace);
            assert_eq!(new_repository.name, name);

            // Delete repo - cleanup + check for correct remote namespace transfer
            api::remote::repositories::delete(&new_repository).await?;

            Ok(())
        })
        .await
    }
}
