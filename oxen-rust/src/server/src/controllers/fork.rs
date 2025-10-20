use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{web, HttpRequest, HttpResponse, Result};
use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::view::fork::ForkRequest;
use liboxen::view::StatusMessage;

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
