use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::web::BytesMut;
use actix_web::{web, HttpRequest, HttpResponse};
use futures_util::stream::StreamExt as _;
use liboxen::constants::AVG_CHUNK_SIZE;
use liboxen::core;
use liboxen::repositories;
use liboxen::view::versions::CompleteVersionUploadRequest;
use liboxen::view::StatusMessage;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ChunkQuery {
    pub offset: Option<u64>,
    pub size: Option<u64>,
}

pub async fn upload(
    req: HttpRequest,
    mut body: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let chunk_number = path_param(&req, "chunk_number")?;
    let chunk_number = chunk_number.parse::<u32>().map_err(|_| {
        OxenHttpError::BadRequest(
            format!("Invalid chunk number, must be a number: {}", chunk_number).into(),
        )
    })?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!(
        "/upload version {} chunk {} to repo: {:?}",
        version_id,
        chunk_number,
        repo.path
    );

    let version_store = repo.version_store()?;
    // Stream payload in smaller chunks
    let mut buffered = BytesMut::new();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
        buffered.extend_from_slice(&chunk);
    }
    version_store.store_version_chunk(&version_id, chunk_number, &buffered)?;

    Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
}

pub async fn complete(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!("/complete version chunk upload to repo: {:?}", repo.path);

    // Try to deserialize the body
    let request: Result<CompleteVersionUploadRequest, serde_json::Error> =
        serde_json::from_str(&body);
    if let Ok(request) = request {
        // There should only be a single file in the request
        if request.files.len() != 1 {
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(
                "Expected a single file in the request",
            )));
        }

        let file = &request.files[0];
        log::debug!("Received {} chunks", file.upload_results.len());
        let version_store = repo.version_store()?;

        let chunks = version_store.list_version_chunks(&version_id)?;
        log::debug!("Found {} chunks", chunks.len());

        if chunks.len() != file.upload_results.len() {
            return Ok(
                HttpResponse::BadRequest().json(StatusMessage::error(format!(
                    "Number of chunks does not match expected number of chunks: {} != {}",
                    chunks.len(),
                    file.upload_results.len()
                ))),
            );
        }

        // Combine all the chunks for a version file into a single file
        let cleanup = true;
        let version_path = version_store.combine_version_chunks(&version_id, cleanup)?;

        // If the workspace id is provided, stage the file
        if let Some(workspace_id) = request.workspace_id {
            let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
                return Ok(HttpResponse::NotFound().json(StatusMessage::error(format!(
                    "Workspace not found: {}",
                    workspace_id
                ))));
            };
            // TODO: Can we just replace workspaces::files::add with this?
            // repositories::workspaces::files::add(&workspace, &version_path)?;
            let dst_path = if let Some(dst_dir) = &file.dst_dir {
                dst_dir.join(file.file_name.clone())
            } else {
                PathBuf::from(file.file_name.clone())
            };
            core::v_latest::workspaces::files::add_version_file(
                &workspace,
                &version_path,
                &dst_path,
            )?;
        }

        return Ok(HttpResponse::Ok().json(StatusMessage::resource_found()));
    }

    Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid request body")))
}

pub async fn download(
    req: HttpRequest,
    query: web::Query<ChunkQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let offset = query.offset.unwrap_or(0);
    let size = query.size.unwrap_or(AVG_CHUNK_SIZE);

    log::debug!(
        "download_chunk for repo: {:?}, file_hash: {}, offset: {}, size: {}",
        repo.path,
        version_id,
        offset,
        size
    );

    let version_store = repo.version_store()?;

    let chunk_data = version_store.get_version_chunk(&version_id, offset, size)?;
    Ok(HttpResponse::Ok().body(chunk_data))
}
