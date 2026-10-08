use actix_web::{HttpRequest, HttpResponse, web};
use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::storage::StorageKind;
use liboxen::view::StatusMessage;
use serde::Deserialize;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{app_data, path_param};
use crate::tasks;

#[derive(Deserialize, Debug)]
pub struct UpdateStorageRequest {
    /// The backend to move the repository's version files to.
    pub kind: StorageKind,
}

/// PUT /storage
/// Move the repository's version files to the requested storage backend and switch it there.
pub async fn update(
    req: HttpRequest,
    body: web::Json<UpdateStorageRequest>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo_async(app_data, namespace, repo_name).await?;
    let kind = app_data.config.storage.resolve(Some(body.kind))?;

    // Runs to the end even if the client disconnects, so a move never stops between switching the
    // repository and deleting the copies it left.
    tokio::spawn(tasks::inherit_hub(async move {
        repositories::storage::move_to(&repo, kind).await
    }))
    .await
    .map_err(OxenError::from)??;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}
