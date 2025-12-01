use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{web, HttpRequest, HttpResponse, Result};
use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::view::fork::ForkRequest;
use liboxen::view::StatusMessage;

/// Fork a repository
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/fork",
    tag = "Fork",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository to fork", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository to fork", example = "yolov7-repo"),
    ),
    request_body(
        content = ForkRequest,
        description = "Fork target details, including the new namespace and optional new repository name.",
    ),
    responses(
        (status = 202, description = "Fork initiated successfully", body = StatusMessage),
        (status = 409, description = "Repository already exists at destination namespace/name", body = StatusMessage),
        (status = 404, description = "Original repository not found")
    )
)]
pub async fn fork(
    req: HttpRequest,
    body: web::Json<ForkRequest>,
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("Forking repository with request: {req:?}");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let original_repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let new_repo_namespace = body.namespace.clone();

    let new_repo_name = body.new_repo_name.clone().unwrap_or(repo_name.clone());

    let new_repo_path = app_data.path.join(&new_repo_namespace).join(&new_repo_name);

    match repositories::fork::start_fork(original_repo.path, new_repo_path.clone()) {
        Ok(fork_start_response) => {
            log::info!("Successfully forked repository to {:?}", &new_repo_path);
            Ok(HttpResponse::Accepted().json(fork_start_response))
        }
        Err(OxenError::RepoAlreadyExistsAtDestination(path)) => {
            log::debug!("Repo already exists: {path:?}");
            Ok(HttpResponse::Conflict()
                .json(StatusMessage::error("Repo already exists at destination.")))
        }
        Err(err) => {
            log::error!("Failed to fork repository: {err:?}");
            Err(OxenHttpError::from(err))
        }
    }
}

/// Fork Status
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/fork/status",
    tag = "Fork",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the forked repository", example = "new-user"),
        ("repo_name" = String, Path, description = "Name of the forked repository", example = "yolov7-repo"),
    ),
    responses(
        (status = 200, description = "Fork status returned successfully", body = StatusMessage),
        (status = 404, description = "Fork status not found or repository does not exist")
    )
)]
pub async fn get_status(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    log::debug!("Getting fork status for repo: {namespace}/{repo_name}");

    let repo_path = app_data.path.join(&namespace).join(&repo_name);

    match repositories::fork::get_fork_status(&repo_path) {
        Ok(status) => Ok(HttpResponse::Ok().json(status)),
        Err(OxenError::ForkStatusNotFound(_)) => {
            Ok(HttpResponse::NotFound().json(StatusMessage::error("Fork status not found")))
        }
        Err(e) => {
            log::error!("Failed to get fork status: {e}");
            Err(OxenHttpError::from(e))
        }
    }
}
