use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use futures_util::stream::StreamExt; // Import StreamExt for the next() method
use futures_util::TryStreamExt;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::model::file::{FileContents, FileNew};
use liboxen::repositories;
use liboxen::view::http::{MSG_RESOURCE_FOUND, MSG_RESOURCE_UPDATED, STATUS_SUCCESS};
use liboxen::view::repository::{
    DataTypeView, RepositoryCreationResponse, RepositoryCreationView, RepositoryDataTypesResponse,
    RepositoryDataTypesView, RepositoryListView, RepositoryStatsResponse, RepositoryStatsView,
};
use liboxen::view::{
    DataTypeCount, ListRepositoryResponse, NamespaceView, RepositoryResponse, RepositoryView,
    StatusMessage,
};

use actix_multipart::Multipart; // Gives us Multipart
use liboxen::model::{RepoNew, User};

use actix_web::{web, HttpRequest, HttpResponse, Result};
use serde_json::from_slice;
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
            size = dir_node.num_bytes();
            data_types = dir_node
                .data_type_counts()
                .iter()
                .map(|(data_type, count)| DataTypeCount {
                    data_type: data_type.to_string(),
                    count: *count as usize,
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
                let stats = repositories::stats::get_stats(&repo)?;
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
                log::debug!("404 Could not find repo: {name}");
                Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
            }
            Err(err) => {
                log::debug!("Err finding repo: {name} => {err:?}");
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

pub async fn update_size(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;

    let repository = get_repo(&app_data.path, &namespace, &name)?;
    repositories::size::update_size(&repository)?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

pub async fn get_size(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;

    let repository = get_repo(&app_data.path, &namespace, &name)?;
    let size = repositories::size::get_size(&repository)?;
    Ok(HttpResponse::Ok().json(size))
}

pub async fn create(
    req: HttpRequest,
    mut payload: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    if let Some(content_type) = req.headers().get("Content-Type") {
        if content_type == "application/json" {
            let mut body_bytes = Vec::new();
            while let Some(chunk) = payload.next().await {
                let chunk = chunk.map_err(|e| {
                    println!("Failed to read payload: {e:?}");
                    OxenHttpError::BadRequest("Failed to read payload".into())
                })?;
                body_bytes.extend_from_slice(&chunk);
            }
            let json_data: RepoNew = from_slice(&body_bytes).map_err(|e| {
                println!("Failed to parse JSON: {e:?}");
                OxenHttpError::BadRequest("Invalid JSON".into())
            })?;
            return handle_json_creation(app_data, json_data).await;
        } else {
            content_type
                .to_str()
                .unwrap_or("")
                .starts_with("multipart/form-data");
            {
                let multipart = Multipart::new(req.headers(), payload);
                return handle_multipart_creation(app_data, multipart).await;
            }
        }
    }
    Err(OxenHttpError::BadRequest("Unsupported Content-Type".into()))
}

async fn handle_json_creation(
    app_data: &OxenAppData,
    data: RepoNew,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let repo_new_clone = data.clone();
    match repositories::create(&app_data.path, data).await {
        Ok(repo) => match repositories::commits::latest_commit(&repo.local_repo) {
            Ok(latest_commit) => Ok(HttpResponse::Ok().json(RepositoryCreationResponse {
                status: STATUS_SUCCESS.to_string(),
                status_message: MSG_RESOURCE_FOUND.to_string(),
                repository: RepositoryCreationView {
                    namespace: repo_new_clone.namespace.clone(),
                    latest_commit: Some(latest_commit.clone()),
                    name: repo_new_clone.name.clone(),
                    min_version: Some(repo.local_repo.min_version().to_string()),
                },
                metadata_entries: None,
            })),
            Err(OxenError::NoCommitsFound(_)) => {
                Ok(HttpResponse::Ok().json(RepositoryCreationResponse {
                    status: STATUS_SUCCESS.to_string(),
                    status_message: MSG_RESOURCE_FOUND.to_string(),
                    repository: RepositoryCreationView {
                        namespace: repo_new_clone.namespace.clone(),
                        latest_commit: None,
                        name: repo_new_clone.name.clone(),
                        min_version: Some(repo.local_repo.min_version().to_string()),
                    },
                    metadata_entries: None,
                }))
            }
            Err(err) => {
                println!("Err repositories::create: {err:?}");
                log::error!("Err repositories::commits::latest_commit: {err:?}");
                Ok(HttpResponse::InternalServerError()
                    .json(StatusMessage::error("Failed to get latest commit.")))
            }
        },
        Err(OxenError::RepoAlreadyExists(path)) => {
            log::debug!("Repo already exists: {path:?}");
            Ok(HttpResponse::Conflict().json(StatusMessage::error("Repo already exists.")))
        }
        Err(err) => {
            println!("Err repositories::create: {err:?}");
            log::error!("Err repositories::create: {err:?}");
            Ok(HttpResponse::InternalServerError().json(StatusMessage::error("Invalid body.")))
        }
    }
}

async fn handle_multipart_creation(
    app_data: &OxenAppData,
    mut multipart: Multipart,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let mut repo_new: Option<RepoNew> = None;
    let mut files: Vec<FileNew> = vec![];
    let mut name: Option<String> = None;
    let mut email: Option<String> = None;

    // Parse multipart form fields
    while let Some(mut field) = multipart
        .try_next()
        .await
        .map_err(OxenHttpError::MultipartError)?
    {
        let disposition = field.content_disposition().ok_or(OxenHttpError::NotFound)?;
        let field_name = disposition
            .get_name()
            .ok_or(OxenHttpError::NotFound)?
            .to_string(); // Convert to owned String

        match field_name.as_str() {
            "new_repo" => {
                let mut body = String::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    body.push_str(
                        std::str::from_utf8(&chunk)
                            .map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?,
                    );
                }
                repo_new = Some(serde_json::from_str(&body)?);
            }
            "name" | "email" => {
                let mut bytes = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    bytes.extend_from_slice(&chunk);
                }
                let value = String::from_utf8(bytes)
                    .map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;

                if field_name == "name" {
                    name = Some(value);
                } else {
                    email = Some(value);
                }
            }
            "file[]" | "file" => {
                let filename = disposition.get_filename().map_or_else(
                    || uuid::Uuid::new_v4().to_string(),
                    sanitize_filename::sanitize,
                );

                let mut contents = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    contents.extend_from_slice(&chunk);
                }

                files.push(FileNew {
                    path: PathBuf::from(&filename),
                    contents: FileContents::Binary(contents),
                    user: User {
                        name: name
                            .clone()
                            .ok_or(OxenHttpError::BadRequest("Name is required".into()))?,
                        email: email
                            .clone()
                            .ok_or(OxenHttpError::BadRequest("Email is required".into()))?,
                    },
                });
            }
            _ => continue,
        }
    }

    // Handle repository creation
    let Some(mut repo_data) = repo_new else {
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Missing new_repo field")));
    };

    repo_data.files = if !files.is_empty() { Some(files) } else { None };
    let repo_data_clone = repo_data.clone();

    // Create repository
    match repositories::create(&app_data.path, repo_data).await {
        Ok(repo) => match repositories::commits::latest_commit(&repo.local_repo) {
            Ok(latest_commit) => Ok(HttpResponse::Ok().json(RepositoryCreationResponse {
                status: STATUS_SUCCESS.to_string(),
                status_message: MSG_RESOURCE_FOUND.to_string(),
                repository: RepositoryCreationView {
                    namespace: repo_data_clone.namespace,
                    latest_commit: Some(latest_commit),
                    name: repo_data_clone.name,
                    min_version: Some(repo.local_repo.min_version().to_string()),
                },
                metadata_entries: repo.entries,
            })),
            Err(OxenError::NoCommitsFound(_)) => {
                Ok(HttpResponse::Ok().json(RepositoryCreationResponse {
                    status: STATUS_SUCCESS.to_string(),
                    status_message: MSG_RESOURCE_FOUND.to_string(),
                    repository: RepositoryCreationView {
                        namespace: repo_data_clone.namespace,
                        latest_commit: None,
                        name: repo_data_clone.name,
                        min_version: Some(repo.local_repo.min_version().to_string()),
                    },
                    metadata_entries: repo.entries,
                }))
            }
            Err(err) => {
                log::error!("Err repositories::commits::latest_commit: {err:?}");
                Ok(HttpResponse::InternalServerError()
                    .json(StatusMessage::error("Failed to get latest commit.")))
            }
        },
        Err(OxenError::RepoAlreadyExists(path)) => {
            log::debug!("Repo already exists: {path:?}");
            Ok(HttpResponse::Conflict().json(StatusMessage::error("Repo already exists.")))
        }
        Err(err) => {
            log::error!("Err repositories::create: {err:?}");
            Ok(HttpResponse::InternalServerError().json(StatusMessage::error("Invalid body.")))
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
    std::thread::spawn(move || match repositories::delete(&repository) {
        Ok(_) => log::info!("Deleted repo: {namespace}/{name}"),
        Err(err) => log::error!("Err deleting repo: {err}"),
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

    log::debug!("transfer_namespace from: {from_namespace} to: {to_namespace}");

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

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::error::OxenError;

    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{ListRepositoryResponse, NamespaceView, RepositoryResponse};

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_repositories_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "repositories";
        let uri = format!("/api/repos/{namespace}");
        let req = test::namespace_request(&sync_dir, &uri, namespace);

        let resp = controllers::repositories::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 0);

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_index_multiple_repos() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Test-Namespace";
        test::create_local_repo(&sync_dir, namespace, "Testing-1")?;
        test::create_local_repo(&sync_dir, namespace, "Testing-2")?;

        let uri = format!("/api/repos/{namespace}");
        let req = test::namespace_request(&sync_dir, &uri, namespace);
        let resp = controllers::repositories::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 2);

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_show() -> Result<(), OxenError> {
        log::info!("starting test");
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Test-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;
        log::info!("test created local repo: {name}");

        let uri = format!("/api/repos/{namespace}/{name}");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::repositories::show(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_repositories_transfer_namespace() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Test-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;

        // Create new repo in a namespace so it exists
        let new_namespace = "New-Namespace";
        let new_name = "Newbie";
        test::create_local_repo(&sync_dir, new_namespace, new_name)?;

        let uri = format!("/api/repos/{namespace}/{name}/transfer");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

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
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }
}
