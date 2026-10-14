use actix_web::{HttpRequest, HttpResponse, web};
use futures_util::stream::StreamExt as _;
use liboxen::error::OxenError;
use liboxen::repositories::versions::uploads::{PartStored, Upload};
use liboxen::view::StatusMessage;
use liboxen::view::versions::{
    BeginVersionUploadRequest, VersionUploadPartResponse, VersionUploadResponse,
};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo_async;
use crate::params::{app_data, path_param, request_relative_path};
use crate::tasks;

/// POST /versions/uploads
/// Begin uploading a version file in parts. When the request gives a destination, the file is
/// staged there once the upload completes.
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/versions/uploads",
    tag = "Version Files",
    description = "Begin uploading a large file in parts. A file may be at most 1 TiB, a provisional limit. The response gives the upload id, the size of every part but the last, the number of parts, and how many parts the upload accepts at once, starting at the first part not yet hashed. When the request gives a destination, a workspace and a path, the file is staged there once its last part arrives.",
    summary = "Begin a version file upload",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
    ),
    request_body(content = BeginVersionUploadRequest, content_type = "application/json"),
    responses(
        (status = 200, description = "Upload begun", body = VersionUploadResponse),
        (status = 400, description = "Invalid hash, size, or destination"),
        (status = 404, description = "Repository or workspace not found"),
        (status = 429, description = "Repository busy with maintenance"),
    )
)]
pub async fn begin(
    req: HttpRequest,
    body: web::Json<BeginVersionUploadRequest>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo_async(app_data, namespace, repo_name).await?;

    let mut request = body.into_inner();
    if let Some(destination) = &mut request.destination {
        destination.path = request_relative_path(&destination.path, false, "destination path")?;
    }

    let upload = Upload::begin(&repo, &request.hash, request.size, request.destination).await?;
    Ok(HttpResponse::Ok().json(VersionUploadResponse {
        status: StatusMessage::resource_created(),
        upload_id: upload.id().to_string(),
        part_size: upload.part_size(),
        num_parts: upload.num_parts(),
        part_window: upload.part_window(),
    }))
}

/// PUT /versions/uploads/{upload_id}/parts/{part_number}
/// Store one part of an upload. The part that completes the upload publishes the file and stages it
/// in the upload's workspace.
#[utoipa::path(
    put,
    path = "/api/repos/{namespace}/{repo_name}/versions/uploads/{upload_id}/parts/{part_number}",
    tag = "Version Files",
    description = "Upload one part of a file. A part may arrive up to `part_window` parts from the earliest part not yet hashed, counting that part, and one further ahead is refused with 429 and Retry-After, as is a part sent again while its first copy is still being stored. Until the upload completes, a part sent again after it was stored is accepted and ignored. Once it completes, any part gets 404, as for an upload that was aborted, so a client that lost the response for the last part checks whether the version file exists (`GET /versions/{hash}/metadata`) and, for an upload with a destination, whether the workspace staged it. Each part but the last is exactly the upload's part size. The response for the part that completes the file reports `completed: true`, once the file is verified against its hash and staged in the upload's workspace, if it has one.",
    summary = "Upload one part of a version file",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("upload_id" = String, Path, description = "The id the upload began with"),
        ("part_number" = u64, Path, description = "The part's number, counting from 1", example = 1),
    ),
    request_body(content = Vec<u8>, content_type = "application/octet-stream"),
    responses(
        (status = 200, description = "Part stored", body = VersionUploadPartResponse),
        (status = 400, description = "Part out of range, of the wrong length, or the file's bytes do not match its hash"),
        (status = 404, description = "Upload not found: it never began, was aborted, or already completed"),
        (status = 429, description = "Part arrived too far ahead of the parts before it, or an earlier copy of it is still being stored. Send it again after Retry-After"),
    )
)]
pub async fn upload_part(
    req: HttpRequest,
    mut body: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let upload_id = path_param(&req, "upload_id")?;
    let part_number: u64 = path_param(&req, "part_number")?
        .parse()
        .map_err(|_| OxenHttpError::BadRequest("Invalid part number".into()))?;
    let repo = get_repo_async(app_data, namespace, repo_name).await?;

    let Some(receiving) = Upload::find(&repo, upload_id)?.receive_part(part_number)? else {
        return Ok(HttpResponse::Ok().json(VersionUploadPartResponse {
            status: StatusMessage::resource_created(),
            completed: false,
        }));
    };
    let size = receiving.size();
    let mut part = web::BytesMut::with_capacity(size as usize);
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
        if (part.len() + chunk.len()) as u64 > size {
            return Err(OxenError::VersionUploadRefused(
                format!("part {part_number} is longer than {size} bytes (upload {upload_id})")
                    .into(),
            )
            .into());
        }
        part.extend_from_slice(&chunk);
    }

    // Runs to the end even if the client disconnects, so a part is never left half recorded.
    let stored = tokio::spawn(tasks::inherit_hub(receiving.store(part.freeze())))
        .await
        .map_err(OxenError::from)??;

    Ok(HttpResponse::Ok().json(VersionUploadPartResponse {
        status: StatusMessage::resource_created(),
        completed: stored == PartStored::Completed,
    }))
}
