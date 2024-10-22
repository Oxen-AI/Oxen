use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_FOUND, MSG_RESOURCE_UPDATED, STATUS_SUCCESS};
use liboxen::view::repository::{
    DataTypeView, RepositoryCreationResponse, RepositoryCreationView, RepositoryDataTypesResponse,
    RepositoryDataTypesView, RepositoryListView, RepositoryStatsResponse, RepositoryStatsView,
};
use liboxen::view::{
    DataTypeCount, ListRepositoryResponse, NamespaceView, RepositoryResponse, RepositoryView,
    StatusMessage,
};

use liboxen::model::RepoNew;

use actix_files::NamedFile;
use actix_web::{HttpRequest, HttpResponse};
use std::path::PathBuf;

pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;

    let namespace_path = &app_data.path.join(&namespace);

    let repos: Vec<RepositoryListView> = repositories::list_repos_in_namespace(namespace_path)
        .iter()
        .map(|repo| RepositoryListView {
            name: repo.dirname(),
            namespace: namespace.to_string(),
            min_version: Some(repo.min_version().to_string()),
        })
        .collect();
    let view = ListRepositoryResponse {
        status: StatusMessage::resource_found(),
        repositories: repos,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, &namespace, &name)?;
    let mut size: u64 = 0;
    let mut data_types: Vec<DataTypeCount> = vec![];

    // If we have a commit on the main branch, we can get the size and data types from the commit
    if let Ok(Some(commit)) = repositories::revisions::get(&repository, DEFAULT_BRANCH_NAME) {
        if let Some(dir_node) =
            repositories::entries::get_directory(&repository, &commit, PathBuf::from(""))?
        {
            size = dir_node.num_bytes;
            data_types = dir_node
                .data_type_counts
                .into_iter()
                .map(|(data_type, count)| DataTypeCount {
                    data_type,
                    count: count as usize,
                })
                .collect();
        }
    }

    // Return the repository view
    Ok(HttpResponse::Ok().json(RepositoryDataTypesResponse {
        status: STATUS_SUCCESS.to_string(),
        status_message: MSG_RESOURCE_FOUND.to_string(),
        repository: RepositoryDataTypesView {
            namespace,
            name,
            size,
            data_types,
            min_version: Some(repository.min_version().to_string()),
            is_empty: repositories::is_empty(&repository)?,
        },
    }))
}

// Need this endpoint to get the size and data types for a repo from the UI
pub async fn stats(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    if let (Some(name), Some(namespace)) = (name, namespace) {
        match repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repo)) => {
                let stats = repositories::get_repo_stats(&repo);
                let data_types: Vec<DataTypeView> = stats
                    .data_types
                    .values()
                    .map(|s| DataTypeView {
                        data_type: s.data_type.to_owned(),
                        file_count: s.file_count,
                        data_size: s.data_size,
                    })
                    .collect();
                Ok(HttpResponse::Ok().json(RepositoryStatsResponse {
                    status: StatusMessage::resource_found(),
                    repository: RepositoryStatsView {
                        data_size: stats.data_size,
                        data_types,
                    },
                }))
            }
            Ok(None) => {
                log::debug!("404 Could not find repo: {}", name);
                Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
            }
            Err(err) => {
                log::debug!("Err finding repo: {} => {:?}", name, err);
                Ok(
                    HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()),
                )
            }
        }
    } else {
        let msg = "Could not find `name` or `namespace` param...";
        Ok(HttpResponse::BadRequest().json(StatusMessage::error(msg)))
    }
}

pub async fn create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    println!("controllers::repositories::create body:\n{}", body);
    let data: Result<RepoNew, serde_json::Error> = serde_json::from_str(&body);
    match data {
        Ok(data) => match repositories::create(&app_data.path, data.to_owned()) {
            Ok(repo) => match repositories::commits::latest_commit(&repo) {
                Ok(latest_commit) => Ok(HttpResponse::Ok().json(RepositoryCreationResponse {
                    status: STATUS_SUCCESS.to_string(),
                    status_message: MSG_RESOURCE_FOUND.to_string(),
                    repository: RepositoryCreationView {
                        namespace: data.namespace.clone(),
                        latest_commit: Some(latest_commit.clone()),
                        name: data.name.clone(),
                        min_version: Some(repo.min_version().to_string()),
                    },
                })),
                Err(OxenError::NoCommitsFound(_)) => {
                    Ok(HttpResponse::Ok().json(RepositoryCreationResponse {
                        status: STATUS_SUCCESS.to_string(),
                        status_message: MSG_RESOURCE_FOUND.to_string(),
                        repository: RepositoryCreationView {
                            namespace: data.namespace.clone(),
                            latest_commit: None,
                            name: data.name.clone(),
                            min_version: Some(repo.min_version().to_string()),
                        },
                    }))
                }
                Err(err) => {
                    log::error!("Err repositories::commits::latest_commit: {:?}", err);
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::error("Failed to get latest commit.")))
                }
            },
            Err(OxenError::RepoAlreadyExists(path)) => {
                log::debug!("Repo already exists: {:?}", path);
                Ok(HttpResponse::Conflict().json(StatusMessage::error("Repo already exists.")))
            }
            Err(err) => {
                println!("Err repositories::create: {err:?}");
                log::error!("Err repositories::create: {:?}", err);
                Ok(HttpResponse::InternalServerError().json(StatusMessage::error("Invalid body.")))
            }
        },
        Err(err) => {
            log::error!("Err repositories::create parse error: {:?}", err);
            Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid body.")))
        }
    }
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;

    let Ok(repository) = get_repo(&app_data.path, &namespace, &name) else {
        return Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()));
    };

    // Delete in a background thread because it could take awhile
    std::thread::spawn(move || match repositories::delete(repository) {
        Ok(_) => log::info!("Deleted repo: {}/{}", namespace, name),
        Err(err) => log::error!("Err deleting repo: {}", err),
    });

    Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
}

pub async fn transfer_namespace(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    // Parse body
    let from_namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let data: NamespaceView = serde_json::from_str(&body)?;
    let to_namespace = data.namespace;
    repositories::transfer_namespace(&app_data.path, &name, &from_namespace, &to_namespace)?;
    let repo =
        repositories::get_by_namespace_and_name(&app_data.path, &to_namespace, &name)?.unwrap();

    // Return repository view under new namespace
    Ok(HttpResponse::Ok().json(RepositoryResponse {
        status: STATUS_SUCCESS.to_string(),
        status_message: MSG_RESOURCE_UPDATED.to_string(),
        repository: RepositoryView {
            namespace: to_namespace,
            name,
            min_version: Some(repo.min_version().to_string()),
            is_empty: repositories::is_empty(&repo)?,
        },
    }))
}

pub async fn get_file_for_branch(req: HttpRequest) -> Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let filepath: PathBuf = req.match_info().query("filename").parse().unwrap();
    let branch_name: &str = req.match_info().get("branch_name").unwrap();

    let branch = repositories::branches::get_by_name(&repo, branch_name)?
        .ok_or(OxenError::remote_branch_not_found(branch_name))?;
    let version_path = util::fs::version_path_for_commit_id(&repo, &branch.commit_id, &filepath)?;
    log::debug!(
        "get_file_for_branch looking for {:?} -> {:?}",
        filepath,
        version_path
    );
    Ok(NamedFile::open(version_path)?)
}

pub async fn get_file_for_commit_id(req: HttpRequest) -> Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource
        .clone()
        .commit
        .ok_or(OxenError::resource_not_found(
            resource.version.to_string_lossy(),
        ))?;

    let version_path = util::fs::version_path_for_commit_id(&repo, &commit.id, &resource.path)?;
    log::debug!(
        "get_file_for_commit_id looking for {:?} -> {:?}",
        resource.path,
        version_path
    );
    Ok(NamedFile::open(version_path)?)
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::constants;
    use liboxen::error::OxenError;
    use liboxen::model::{Commit, RepoNew};
    use liboxen::util;

    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::repository::RepositoryCreationResponse;
    use liboxen::view::{ListRepositoryResponse, NamespaceView, RepositoryResponse};
    use time::OffsetDateTime;

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_repositories_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();

        let namespace = "repositories";
        let uri = format!("/api/repos/{namespace}");
        let req = test::namespace_request(&sync_dir, queue, &uri, namespace);

        let resp = controllers::repositories::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 0);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_index_multiple_repos() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();

        let namespace = "Test-Namespace";
        test::create_local_repo(&sync_dir, namespace, "Testing-1")?;
        test::create_local_repo(&sync_dir, namespace, "Testing-2")?;

        let uri = format!("/api/repos/{namespace}");
        let req = test::namespace_request(&sync_dir, queue, &uri, namespace);
        let resp = controllers::repositories::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 2);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_show() -> Result<(), OxenError> {
        log::info!("starting test");
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Test-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;
        log::info!("test created local repo: {}", name);

        let uri = format!("/api/repos/{namespace}/{name}");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

        let resp = controllers::repositories::show(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let timestamp = OffsetDateTime::now_utc();
        let root_commit = Commit {
            id: String::from("1234"),
            parent_ids: vec![],
            message: String::from(constants::INITIAL_COMMIT_MSG),
            author: String::from("Ox"),
            email: String::from("ox@oxen.ai"),
            timestamp,
            root_hash: None,
        };
        let repo_new = RepoNew::from_root_commit("Testing-Name", "Testing-Namespace", root_commit);
        let data = serde_json::to_string(&repo_new)?;
        let req = test::request(&sync_dir, queue, "/api/repos");

        let resp = controllers::repositories::create(req, data).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        let repo_response: RepositoryCreationResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, repo_new.name);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_repositories_transfer_namespace() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Test-Namespace";
        let name = "Testing-Name";
        let queue = test::init_queue();
        test::create_local_repo(&sync_dir, namespace, name)?;

        // Create new repo in a namespace so it exists
        let new_namespace = "New-Namespace";
        let new_name = "Newbie";
        test::create_local_repo(&sync_dir, new_namespace, new_name)?;

        let uri = format!("/api/repos/{namespace}/{name}/transfer");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

        let params = NamespaceView {
            namespace: new_namespace.to_string(),
        };
        let resp =
            controllers::repositories::transfer_namespace(req, serde_json::to_string(&params)?)
                .await
                .unwrap();

        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;

        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);
        assert_eq!(repo_response.repository.namespace, new_namespace);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
