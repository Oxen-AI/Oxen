pub mod chunks;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{HttpRequest, HttpResponse};
use liboxen::view::versions::{VersionFile, VersionFileResponse};
use liboxen::view::StatusMessage;

pub async fn metadata(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let exists = repo.version_store()?.version_exists(&version_id)?;
    if !exists {
        return Err(OxenHttpError::NotFound);
    }

    let data = repo.version_store()?.get_version(&version_id)?;
    Ok(HttpResponse::Ok().json(VersionFileResponse {
        status: StatusMessage::resource_found(),
        version: VersionFile {
            hash: version_id,
            size: data.len() as u64,
        },
    }))
}

pub async fn download(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!(
        "download file for repo: {:?}, file_hash: {}",
        repo.path,
        version_id
    );

    let version_store = repo.version_store()?;

    let file_data = version_store.get_version(&version_id)?;
    Ok(HttpResponse::Ok().body(file_data))
}
