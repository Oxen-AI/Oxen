use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::{get_repo, get_repo_async};
use crate::params::{app_data, path_param};

use futures_util::TryStreamExt;
use futures_util::stream::StreamExt;
use liboxen::api::requests::RepoNew;
// Import StreamExt for the next() method
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::core::repo_locks;
use liboxen::error::OxenError;
use liboxen::model::file::{FileContents, FileNew};
use liboxen::model::parsed_resource::ParsedResourceView;
use liboxen::model::{Branch, ParsedResource};
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
use liboxen::model::User;

use actix_web::{HttpRequest, HttpResponse, Result, web};
use serde_json::from_slice;
use std::path::PathBuf;
use utoipa;

/// List repositories
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}",
    tag = "Repositories",
    description = "List all repositories in a namespace.",
    params(
        ("namespace" = String, Path, description = "Namespace to list repositories from", example = "ox"),
    ),
    responses(
        (status = 200, description = "List of repositories", body = ListRepositoryResponse),
        (status = 404, description = "Namespace not found")
    )
)]
pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();

    let namespace_path = &app_data.path.join(&namespace);

    let repos: Vec<RepositoryListView> = repositories::list_repos_in_namespace(namespace_path)
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

/// Get repository details
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}",
    tag = "Repositories",
    description = "Get repository details including size and data types from the main branch.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "Repository details", body = RepositoryDataTypesResponse),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();

    // Get the repository or return error
    let repository = get_repo_async(app_data, &namespace, &name).await?;
    let mut size: u64 = 0;
    let mut data_types: Vec<DataTypeCount> = vec![];
    let mut default_resource: Option<ParsedResourceView> = None;

    // If we have a commit on the main branch, we can get the size and data types from the commit
    if let Ok(Some(commit)) =
        repositories::revisions::get_async(&repository, DEFAULT_BRANCH_NAME).await
    {
        if let Some(dir_node) =
            repositories::entries::get_directory_async(&repository, &commit, PathBuf::from(""))
                .await?
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

        // The resolved commit is the head of the default branch, so its id is that branch's
        // commit id; build the branch from it rather than re-reading the refs DB.
        let branch = Branch {
            name: DEFAULT_BRANCH_NAME.to_string(),
            commit_id: commit.id.clone(),
        };
        default_resource = Some(ParsedResourceView::from(ParsedResource {
            commit: Some(commit),
            branch: Some(branch),
            workspace: None,
            path: PathBuf::from(""),
            version: PathBuf::from(DEFAULT_BRANCH_NAME),
            resource: PathBuf::from(DEFAULT_BRANCH_NAME),
        }));
    }

    // A repo with no branches is empty; derive it from the same scan rather than a second read.
    let branch_count = repositories::branches::list(&repository).await?.len();

    // Return the repository view
    Ok(HttpResponse::Ok().json(RepositoryDataTypesResponse {
        status: STATUS_SUCCESS.to_string(),
        status_message: MSG_RESOURCE_FOUND.to_string(),
        repository: RepositoryDataTypesView {
            repository: RepositoryView {
                namespace,
                name,
                min_version: Some(repository.min_version().to_string()),
                is_empty: branch_count == 0,
                storage_kind: repository.storage_config().kind,
            },
            size,
            data_types,
            branch_count,
            default_resource,
        },
    }))
}

/// Get repository stats
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/stats",
    description = "Get the total number of files, the total size of the files, and the number of different file types.",
    tag = "Repositories",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "Repository statistics", body = RepositoryStatsResponse),
        (status = 404, description = "Repository not found"),
    )
)]
pub async fn stats(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace: Option<&str> = path_param(&req, "namespace").ok();
    let name: Option<&str> = path_param(&req, "repo_name").ok();
    if let (Some(name), Some(namespace)) = (name, namespace) {
        match repositories::get_by_namespace_and_name(
            &app_data.path,
            namespace,
            name,
            app_data.config.storage.s3(),
        ) {
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

/// Update repository size
#[utoipa::path(
    put,
    path = "/api/repos/{namespace}/{repo_name}/size",
    tag = "Repositories",
    description = "Recalculate and update the cached repository size.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "Repository size updated", body = StatusMessage),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn update_size(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();

    let repository = get_repo(app_data, &namespace, &name)?;
    let _write = repo_locks::acquire_write(&repository)?;
    repositories::size::update_size(&repository)?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

/// Get repository size
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/size",
    tag = "Repositories",
    description = "Get the cached size of the repository in bytes.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "Repository size in bytes", body = u64),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn get_size(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();

    let repository = get_repo(app_data, &namespace, &name)?;
    // `size::get_size` writes the size cache on a miss (a GET that mutates — tech-debt ENG-1374);
    // guard the whole handler so a stop-the-world op (migration/prune/fsck) blocks it.
    let _write = repo_locks::acquire_write(&repository)?;
    let size = repositories::size::get_size(&repository)?;
    Ok(HttpResponse::Ok().json(size))
}

/// Create repository
#[utoipa::path(
    post,
    path = "/api/repos",
    tag = "Repositories",
    description = "Create a new repository, optionally with initial files via JSON or multipart form.",
    request_body(
        content = RepoNew,
        description = "Repository creation payload (JSON or Multipart)",
        content_type = "application/json",
        example = json!({
            "namespace": "ox",
            "name": "Cat-Dog-Classifier",
            "user": {
                "name": "bessie",
                "email": "bessie@oxen.ai"
            },
            "description": "A repository for image classification"
        })
    ),
    responses(
        (status = 200, description = "Repository created", body = RepositoryCreationResponse),
        (status = 400, description = "Invalid payload"),
        (status = 409, description = "Repository already exists"),
    )
)]
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
            return create_repo_response(app_data, json_data).await;
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
                            .ok_or_else(|| OxenHttpError::BadRequest("Name is required".into()))?,
                        email: email
                            .clone()
                            .ok_or_else(|| OxenHttpError::BadRequest("Email is required".into()))?,
                    },
                });
            }
            _ => continue,
        }
    }

    // Handle repository creation
    let repo_data = {
        let Some(mut repo_data) = repo_new else {
            return Ok(
                HttpResponse::BadRequest().json(StatusMessage::error("Missing new_repo field"))
            );
        };

        repo_data.files = if !files.is_empty() { Some(files) } else { None };
        repo_data
    };

    // Create repository
    create_repo_response(app_data, repo_data).await
}

/// Create the repository from a [`RepoNew`] and build the response that both creation routes
/// (JSON and multipart) send back. `data.storage_kind` is resolved against the server's storage
/// policy (`None` selects the server default).
async fn create_repo_response(
    app_data: &OxenAppData,
    mut data: RepoNew,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    data.storage_kind = Some(app_data.config.storage.resolve(data.storage_kind)?);
    let namespace = data.namespace.clone();
    let name = data.name.clone();
    match repositories::create(&app_data.path, data, app_data.config.storage.s3()).await {
        Ok(repo) => {
            // The repository exists by this point, so a failed lookup only degrades the
            // response's latest_commit to None rather than failing the creation.
            let latest_commit = match repositories::commits::latest_commit(&repo) {
                Ok(commit) => Some(commit),
                Err(OxenError::NoCommitsFound) => None,
                Err(err) => {
                    log::error!("Err repositories::commits::latest_commit: {err:?}");
                    None
                }
            };
            Ok(HttpResponse::Ok().json(RepositoryCreationResponse {
                status: STATUS_SUCCESS.to_string(),
                status_message: MSG_RESOURCE_FOUND.to_string(),
                repository: RepositoryCreationView {
                    namespace,
                    name,
                    latest_commit,
                    min_version: Some(repo.min_version().to_string()),
                    storage_kind: repo.storage_config().kind,
                },
            }))
        }
        Err(err) => Ok(map_create_error_to_response(err)),
    }
}

/// Map an [`OxenError`] returned by [`repositories::create`] to the HTTP
/// response that both creation routes (JSON and multipart) send back.
///
/// Kept as a free function so both handlers stay byte-identical on the error
/// path; any new variant only needs to be added here.
fn map_create_error_to_response(err: OxenError) -> HttpResponse {
    match err {
        OxenError::RepoAlreadyExists(path) => {
            log::debug!("Repo already exists: {path:?}");
            HttpResponse::Conflict().json(StatusMessage::error("Repo already exists."))
        }
        OxenError::InvalidRepoName(name) => {
            log::debug!("Invalid repo name: {name}");
            HttpResponse::BadRequest().json(StatusMessage::error(format!(
                "Invalid repository or namespace name '{name}'. Must match [a-zA-Z0-9][a-zA-Z0-9_.-]+"
            )))
        }
        err => {
            log::error!("Err repositories::create: {err:?}");
            HttpResponse::InternalServerError().json(StatusMessage::error("Invalid body."))
        }
    }
}

/// Delete repository
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}",
    tag = "Repositories",
    description = "Delete a repository. Deletion runs in the background.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Cat-Dog-Classifier"),
    ),
    responses(
        (status = 200, description = "Repository deletion started", body = StatusMessage),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();

    let Ok(repository) = get_repo(app_data, &namespace, &name) else {
        return Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()));
    };

    // Delete in a background task because it could take awhile; the blocking directory
    // removal runs inside delete's own spawn_blocking.
    tokio::spawn(async move {
        match repositories::delete(&repository).await {
            Ok(_) => log::info!("Deleted repo: {namespace}/{name}"),
            Err(err) => log::error!("Err deleting repo: {err}"),
        }
    });

    Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
}

/// Transfer repository namespace
#[utoipa::path(
    patch,
    path = "/api/repos/{namespace}/{repo_name}/transfer",
    tag = "Repositories",
    description = "Transfer a repository to a different namespace.",
    params(
        ("namespace" = String, Path, description = "Current namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Cat-Dog-Classifier"),
    ),
    request_body(
        content = NamespaceView,
        description = "Target namespace to transfer the repository to.",
        example = json!({
            "namespace": "new_org"
        })
    ),
    responses(
        (status = 200, description = "Repository transferred successfully", body = RepositoryResponse),
        (status = 400, description = "Invalid body or target namespace"),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn transfer_namespace(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    // Parse body
    let from_namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let data: NamespaceView = serde_json::from_str(&body)?;
    let to_namespace = data.namespace;

    log::debug!("transfer_namespace from: {from_namespace} to: {to_namespace}");

    let repo = repositories::transfer_namespace(
        &app_data.path,
        &name,
        &from_namespace,
        &to_namespace,
        app_data.config.storage.s3(),
    )?;

    // Return repository view under new namespace
    Ok(HttpResponse::Ok().json(RepositoryResponse {
        status: STATUS_SUCCESS.to_string(),
        status_message: MSG_RESOURCE_UPDATED.to_string(),
        repository: RepositoryView {
            namespace: to_namespace,
            name,
            min_version: Some(repo.min_version().to_string()),
            is_empty: repositories::is_empty(&repo).await?,
            storage_kind: repo.storage_config().kind,
        },
    }))
}
