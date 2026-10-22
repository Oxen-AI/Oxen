use crate::errors::OxenHttpError;
use crate::helpers::{
    BodyEncoding, ReceivedBody, file_stream_response, get_repo, get_repo_async, receive_body,
};
use crate::params::{app_data, path_param};
use crate::tasks;

use liboxen::constants::{MAX_FILE_SIZE, stream_segment_size};
use liboxen::core::repo_locks;
use liboxen::core::staged::get_staged_db_manager;
use liboxen::core::v_latest::workspaces::files::{InspectedVersionFile, add_version_files};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::metadata::metadata_video::VideoThumbnail;
use liboxen::repositories;
use liboxen::storage::VersionStore;
use liboxen::util;
use liboxen::view::workspaces::RenameRequest;
use liboxen::view::{FilePathsResponse, StatusMessage, StatusMessageDescription};

use actix_multipart::Multipart;
use actix_web::{HttpRequest, HttpResponse, web};
use futures_util::TryStreamExt as _;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::task::{JoinError, JoinSet};
use utoipa;

#[derive(utoipa::ToSchema)]
pub struct FileUpload {
    #[schema(value_type = String, format = Binary)]
    pub file: Vec<u8>,
}

/// Combined query parameters for workspace file operations (image resize and video thumbnail)
#[derive(Deserialize, Debug)]
pub struct WorkspaceFileQueryParams {
    // Shared parameters (can be used for both image resize and video thumbnail)
    pub width: Option<u32>,
    pub height: Option<u32>,
    // Video thumbnail specific parameters
    pub timestamp: Option<f64>,
    pub thumbnail: Option<bool>,
}

/// Get file from workspace
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path}",
    description = "Get a file from a workspace.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("path" = String, Path, description = "The path to the file in the workspace", example = "images/train/dog_1.jpg"),
        ("width" = Option<u32>, Query, description = "Width for image resize or video thumbnail", example = 320),
        ("height" = Option<u32>, Query, description = "Height for image resize or video thumbnail", example = 240),
        ("timestamp" = Option<f64>, Query, description = "Timestamp in seconds to extract video thumbnail from", example = 1.0),
        ("thumbnail" = Option<bool>, Query, description = "Set to true to generate a video thumbnail instead of returning the full video", example = true)
    ),
    responses(
        (status = 200, description = "File content returned as a stream. Content-Type varies: matches the file's MIME type for regular files and image resizes, or 'image/jpeg' for video thumbnails",
            body = Vec<u8>,
            headers(
                ("oxen-revision-id" = String, description = "The commit ID of the file version")
            )
        ),
        (status = 404, description = "Workspace or File not found"),
        (status = 400, description = "Invalid parameters")
    )
)]
pub async fn get(
    req: HttpRequest,
    query: web::Query<WorkspaceFileQueryParams>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, namespace, repo_name)?;
    // This GET writes a resized/thumbnail derived blob on a cache miss (tech-debt ENG-1373); guard
    // the whole handler so a stop-the-world op (migration/prune/fsck) blocks it.
    let _write = repo_locks::begin_write(&repo)?;
    let version_store = repo.version_store();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Err(OxenHttpError::NotFound);
    };

    let path = path_param(&req, "path")?.to_string();
    log::debug!("got workspace file path {:?}", path);

    // First, look for the file in the workspace staged_db. Scoped so the staged db handle is
    // released before the awaits below, rather than held for the length of the response.
    let staged_node = {
        let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
        staged_db_manager.read_from_staged_db(&path)?
    };
    let file_node = match staged_node {
        Some(staged_node) => match staged_node.node.node {
            EMerkleTreeNode::File(f) => Ok(f),
            _ => Err(OxenError::NotAFile(PathBuf::from(&path).into())),
        }?,
        None => {
            // If the file isn't in the workspace staged_db, look for it in the base repo
            if let Some(file_node) = repositories::tree::get_file_by_path(
                &workspace.base_repo,
                &workspace.commit,
                &path,
            )? {
                file_node
            } else {
                return Err(OxenHttpError::InternalOxenError(
                    OxenError::resource_not_found(&path),
                ));
            }
        }
    };

    let file_hash = file_node.hash();
    let hash_str = file_hash.to_string();
    let mime_type = file_node.mime_type();
    let num_bytes = file_node.num_bytes();
    let last_commit_id = file_node.last_commit_id().to_string();
    let query_params = query.into_inner();

    // Handle image resize
    if (query_params.width.is_some() || query_params.height.is_some())
        && mime_type.starts_with("image/")
    {
        let img_resize = ImgResize {
            width: query_params.width,
            height: query_params.height,
        };
        log::debug!("img_resize {img_resize:?}");

        let (file_stream, content_length) = util::fs::handle_image_resize(
            Arc::clone(&version_store),
            hash_str.clone(),
            &PathBuf::from(&path),
            img_resize,
        )
        .await?;

        return Ok(
            file_stream_response(mime_type, &last_commit_id, Some(content_length))
                .streaming(file_stream),
        );
    }

    // Handle video thumbnail - requires thumbnail=true parameter
    if query_params.thumbnail == Some(true) && mime_type.starts_with("video/") {
        let video_thumbnail = VideoThumbnail {
            width: query_params.width,
            height: query_params.height,
            timestamp: query_params.timestamp.or(Some(1.0)),
            thumbnail: query_params.thumbnail,
        };
        log::debug!("video_thumbnail {video_thumbnail:?}");

        let stream = util::fs::handle_video_thumbnail(
            Arc::clone(&version_store),
            hash_str,
            video_thumbnail,
            &workspace.dir(),
        )
        .await?;

        return Ok(file_stream_response("image/jpeg", &last_commit_id, None).streaming(stream));
    }

    log::debug!("did not hit the resize or thumbnail cache");

    // Stream the file
    let stream = version_store.get_version_stream(&hash_str).await?;

    Ok(file_stream_response(mime_type, &last_commit_id, Some(num_bytes)).streaming(stream))
}

/// Add files to workspace
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path}",
    description = "Upload and stage files to a workspace. Accept a multipart with either gzipped or uncompressed file parts. Use the filename from the file part and compute the file hash from the content.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("path" = String, Path, description = "The target path to upload the file to", example = "data/train")
    ),
    request_body(
        content_type = "multipart/form-data",
        description = "Multipart upload of file. Each file should be sent as a separate file part",
        content = FileUpload,
    ),
    responses(
        (status = 200, description = "File successfully uploaded to workspace", body = FilePathsResponse),
        (status = 404, description = "Workspace not found"),
        (status = 400, description = "Invalid upload request")
    )
)]
pub async fn add(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let _write = repo_locks::begin_write(&repo)?;
    let directory = path_param(&req, "path")?.to_string();

    let Some(workspace) = repositories::workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let received = save_parts(payload, &repo, &directory, &workspace.dir()).await?;
    log::debug!("Stored {} files", received.len());

    let (ret_files, stage_err_files) = add_version_files(&workspace, received).await?;
    for err in &stage_err_files {
        log::error!("Error staging file {:?}: {}", err.path, err.error);
    }

    Ok(HttpResponse::Ok().json(FilePathsResponse {
        status: StatusMessage::resource_created(),
        paths: ret_files,
    }))
}

/// Stage files for removal
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/files",
    description = "Stage files for removal from the repository. Accepts both files and directories.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745")
    ),
    request_body(
        content = Vec<String>,
        description = "List of paths to stage for removal",
        example = json!(["images/train/dog_1.jpg", "annotations/incorrect.xml"])
    ),
    responses(
        (status = 200, description = "Files successfully removed", body = FilePathsResponse),
        (status = 206, description = "Some files could not be found/removed (returns paths of files not found)", body = FilePathsResponse),
        (status = 404, description = "Workspace not found")
    )
)]
pub async fn rm_files(
    req: HttpRequest,
    payload: web::Json<Vec<PathBuf>>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let _write = repo_locks::begin_write(&repo)?;

    let Some(workspace) = repositories::workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let paths_to_remove: Vec<PathBuf> = payload.into_inner();

    let err_files = repositories::workspaces::files::rm(&workspace, &paths_to_remove).await?;
    log::debug!("rm ✅ success! staged files {paths_to_remove:?} as removed");
    log::debug!("err_files: {err_files:?}");

    if err_files.is_empty() {
        Ok(HttpResponse::Ok().json(FilePathsResponse {
            status: StatusMessage::resource_deleted(),
            paths: paths_to_remove,
        }))
    } else {
        let error_paths: Vec<PathBuf> = err_files
            .into_iter()
            .filter_map(|err_info| err_info.path)
            .collect();

        // Return a partial content response with all the paths
        Ok(HttpResponse::PartialContent().json(FilePathsResponse {
            status: StatusMessage::resource_not_found(),
            paths: error_paths,
        }))
    }
}

pub async fn validate(req: HttpRequest, _body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let repo = get_repo(app_data, namespace, repo_name)?;

    let Some(_workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
}

/// Move or rename a file or directory within the workspace
#[utoipa::path(
    patch,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path}",
    description = "Move or rename a file or directory within the workspace. A directory moves with everything under it.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("path" = String, Path, description = "The current path to the file or directory to move/rename", example = "images/train/dog_1.jpg")
    ),
    request_body(
        content = RenameRequest,
        description = "The new path for the file",
        example = json!({"new_path": "images/train/renamed_dog_1.jpg"})
    ),
    responses(
        (status = 200, description = "File successfully moved/renamed", body = StatusMessage),
        (status = 400, description = "Invalid request (empty new_path, a file's new_path already exists, or new_path is the source or inside it)"),
        (status = 404, description = "Workspace or file not found"),
        (status = 409, description = "A directory's new_path already exists, something is staged at new_path, a file is at a parent of new_path, or the path is staged for removal (including a directory already moved away)")
    )
)]
pub async fn mv(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let _write = repo_locks::begin_write(&repo)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    // Parse request body
    let body: RenameRequest = serde_json::from_str(&body)?;

    // Validate new_path is not empty
    if body.new_path.is_empty() {
        return Err(OxenHttpError::BadRequest("new_path cannot be empty".into()));
    }

    // Validate and normalize new_path
    let new_path = util::fs::validate_and_normalize_path(&body.new_path)?;

    let Some(workspace) = repositories::workspaces::get_async(&repo, &workspace_id).await? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let is_dir = repositories::workspaces::files::is_dir(&workspace, &path).await?;

    // A file move onto an existing path is a bad request. A directory move checks its
    // destination itself and refuses an existing one with a 409.
    if !is_dir
        && repositories::tree::get_node_by_path_async(&repo, &workspace.commit, &new_path)
            .await?
            .is_some()
    {
        return Err(OxenHttpError::BadRequest(
            "new_path already exists in the repository".into(),
        ));
    }

    // For tabular files, use the data_frames rename instead
    if util::fs::is_tabular(&path) && !is_dir {
        repositories::workspaces::data_frames::rename(&workspace, &path, &new_path).await?;
    } else {
        repositories::workspaces::files::mv(&workspace, &path, &new_path).await?;
    }

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

/// A file received in a multipart upload, stored, and inspected for staging.
struct ReceivedFile {
    file: InspectedVersionFile,
    /// The part's position among the upload's file parts.
    part_index: usize,
}

/// Receive each file part of `payload`, gunzipping a gzipped part, and store the files in the
/// repository's version store, several at once. A part that arrives in at most
/// `stream_segment_size()` bytes stays in memory until it is stored and inspected, and a longer one
/// is written to a temporary file in `dir`. A part's filename is its path relative to `directory`.
///
/// Returns the files stored, in the order their parts arrived. A part that fails to arrive, to
/// store, or to be inspected is logged and left out.
async fn save_parts(
    mut payload: Multipart,
    repo: &LocalRepository,
    directory: &str,
    dir: &Path,
) -> Result<Vec<InspectedVersionFile>, OxenHttpError> {
    let version_store = repo.version_store();
    let max_in_flight = util::concurrency::default_num_threads() * 4;
    let mut in_flight: JoinSet<Option<ReceivedFile>> = JoinSet::new();
    let mut received = vec![];
    let mut part_count = 0;

    while let Some(field) = payload.try_next().await.map_err(actix_web::Error::from)? {
        let Some(content_disposition) = field.content_disposition().cloned() else {
            continue;
        };
        if !matches!(content_disposition.get_name(), Some("file[]" | "file")) {
            continue;
        }
        let Some(filename) = content_disposition.get_filename() else {
            return Err(OxenHttpError::BadRequest(
                "Missing filename in multipart request".into(),
            ));
        };
        // `workspace_staged_path` joins and normalizes both, preserving subdirectories, rejecting
        // absolute paths and `..` traversal from either the untrusted `directory` or the filename,
        // and collapsing an empty or `.` directory to the workspace root. The client reconciles
        // against the same helper.
        let dst_path =
            util::fs::workspace_staged_path(directory, Path::new(filename)).map_err(|e| {
                OxenHttpError::BadRequest(
                    format!(
                        "Invalid staging path (directory {directory:?}, file {filename:?}): {e}"
                    )
                    .into(),
                )
            })?;
        let part_index = part_count;
        part_count += 1;
        let is_gzipped = field
            .content_type()
            .map(|mime| mime.type_() == mime::APPLICATION && mime.subtype() == "gzip")
            .unwrap_or(false);
        let encoding = if is_gzipped {
            BodyEncoding::Gzip {
                max_decompressed: stream_segment_size(),
            }
        } else {
            BodyEncoding::Plain
        };

        let body = match receive_body(field, dir.to_path_buf(), MAX_FILE_SIZE, encoding).await? {
            Ok(body) => body,
            Err(e) => {
                tracing::error!(
                    oxen.file_path = %dst_path.display(),
                    exception.message = %e,
                    "Failed to receive an uploaded file"
                );
                continue;
            }
        };

        // Bound the stores in flight before dispatching the next one.
        while in_flight.len() >= max_in_flight {
            if let Some(result) = in_flight.join_next().await {
                received.extend(store_task_result(result)?);
            }
        }

        let version_store = Arc::clone(&version_store);
        in_flight.spawn(tasks::inherit_hub(async move {
            let hash = body.hash();
            match store_and_inspect(version_store, body, dst_path.clone()).await {
                Ok(file) => Some(ReceivedFile { file, part_index }),
                Err(e) => {
                    tracing::error!(
                        oxen.file_path = %dst_path.display(),
                        oxen.file_hash = %hash,
                        exception.message = %e,
                        "Failed to store an uploaded file"
                    );
                    None
                }
            }
        }));
    }

    while let Some(result) = in_flight.join_next().await {
        received.extend(store_task_result(result)?);
    }
    // Stores finish in any order, and staging in arrival order keeps the last of two parts with
    // one path.
    received.sort_by_key(|received| received.part_index);

    Ok(received.into_iter().map(|received| received.file).collect())
}

/// Store `body` in `version_store`, and inspect it to stage at `dst_path`.
async fn store_and_inspect(
    version_store: Arc<dyn VersionStore>,
    body: ReceivedBody,
    dst_path: PathBuf,
) -> Result<InspectedVersionFile, OxenError> {
    match body {
        ReceivedBody::Memory { data, hash } => {
            let inspected = tasks::spawn_blocking_per_item({
                let data = data.clone();
                move || InspectedVersionFile::from_bytes(dst_path, hash, data)
            });
            let hash_str = hash.to_string();
            let stored = version_store.store_version(&hash_str, data);
            let (_, file) = tokio::try_join!(stored, async { inspected.await? })?;
            Ok(file)
        }
        ReceivedBody::Spooled(spooled) => {
            let hash = spooled.hash;
            let copy = version_store
                .store_version_from_temp_file(hash, spooled.file, spooled.len)
                .await?;
            tasks::spawn_blocking_per_item(move || {
                InspectedVersionFile::from_file(dst_path, hash, &copy)
            })
            .await?
        }
    }
}

/// A finished store task's file, or `None` for a file it failed to store. A task that panicked
/// fails the whole upload with a 500.
fn store_task_result(
    result: Result<Option<ReceivedFile>, JoinError>,
) -> Result<Option<ReceivedFile>, OxenHttpError> {
    result
        .map_err(|e| OxenError::internal_error(format!("version store task panicked: {e}")).into())
}

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    use actix_web::http::{StatusCode, header};
    use actix_web::test::{TestRequest, call_service, read_body};
    use actix_web::{App, web};
    use liboxen::constants::stream_segment_size;
    use liboxen::core::staged::get_staged_db_manager;
    use liboxen::error::OxenError;
    use liboxen::model::merkle_tree::node::EMerkleTreeNode;
    use liboxen::model::metadata::generic_metadata::GenericMetadata;
    use liboxen::repositories;
    use liboxen::repositories::workspaces;
    use liboxen::util;
    use liboxen::util::fs::write_to_path;
    use liboxen::view::FilePathsResponse;
    use liboxen::view::versions::{
        BeginVersionUploadRequest, CompleteVersionUploadRequest, CompletedFileUpload,
        VersionUploadPartResponse, VersionUploadResponse, VersionUploadStart, WorkspaceDestination,
    };
    use liboxen::view::workspaces::RenameRequest;
    use std::path::{Path, PathBuf};

    use actix_multipart::test::create_form_data_payload_and_headers;
    use actix_web::web::Bytes;
    use mime;

    #[actix_web::test]
    async fn test_controllers_workspace_files() -> Result<(), OxenError> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        util::fs::write_to_path(repo.path.join("exports.csv/a.txt"), "A")?;
        util::fs::write_to_path(repo.path.join("exports.csv/b.csv"), "x,y\n1,2\n")?;
        let rows = "id,label\n1,a\n2,b\n";
        write_to_path(repo.path.join("data").join("rows.csv"), rows)?;
        repositories::add(&repo, &repo.path).await?;
        let commit = repositories::commit(&repo, "First commit")?;

        let workspace_id = uuid::Uuid::new_v4().to_string();
        repositories::workspaces::create(&repo, &commit, &workspace_id, false)?;

        let files_route = "/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path:.*}";
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(files_route, web::get().to(controllers::workspaces::files::get))
                .route(files_route, web::post().to(controllers::workspaces::files::add))
                .route(files_route, web::patch().to(controllers::workspaces::files::mv))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/uploads",
                    web::post().to(controllers::versions::uploads::begin),
                )
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/uploads/{upload_id}/parts/{part_number}",
                    web::put().to(controllers::versions::uploads::upload_part),
                )
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/{version_id}/create",
                    web::post().to(controllers::versions::chunks::create),
                )
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/{version_id}/chunks",
                    web::put().to(controllers::versions::chunks::upload),
                )
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/{version_id}/complete",
                    web::post().to(controllers::versions::chunks::complete),
                ),
        )
        .await;

        // Request a file that does not exist in the workspace or the base repo
        let file_path = "this_file_does_not_exist.txt";
        let uri =
            format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{file_path}");

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);

        let uri =
            format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/hello.txt");

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_LENGTH).unwrap(),
            "Hello".len().to_string().as_str()
        );
        assert_eq!(
            resp.headers()
                .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
                .unwrap(),
            header::CONTENT_LENGTH.as_str()
        );

        // Drain the body to close the streamed file handle before cleanup; on NFS an
        // unlinked-but-still-open file lingers as .nfsXXXX and the rmdir fails with ENOTEMPTY.
        actix_http::body::to_bytes(resp.into_body()).await.unwrap();

        // Upload a raw (uncompressed) file via multipart; the server hashes the content, stores it
        // in the version store, and stages it. The form-data filename carries the destination path
        // within the target directory ("data").
        let file_content = "uploaded contents";
        let (body, headers) = create_form_data_payload_and_headers(
            "file[]",
            Some("uploaded.txt".to_string()),
            Some(mime::TEXT_PLAIN),
            Bytes::from(file_content),
        );

        let uri = format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/data");

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()));
        let req = headers
            .into_iter()
            .fold(req, |req, hdr| req.insert_header(hdr))
            .set_payload(body)
            .to_request();

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let response: FilePathsResponse = serde_json::from_slice(&bytes)?;
        assert_eq!(response.status.status, "success");
        assert!(
            response
                .paths
                .iter()
                .any(|p| p.file_name().and_then(|f| f.to_str()) == Some("uploaded.txt")),
            "expected a staged path ending in uploaded.txt, got {:?}",
            response.paths
        );

        // The server computed the content hash and stored the blob in the version store.
        let file_hash = util::hasher::hash_buffer(file_content.as_bytes());
        let stored = repo.version_store().get_version(&file_hash).await?;
        assert_eq!(stored, file_content.as_bytes());

        // A filename's subdirectory stays under the target directory, an empty target directory
        // (the URI the client builds for the root) stages at the workspace root, and a filename
        // that leaves the target directory is refused.
        let root_uri = format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/");
        for (target, filename, status, staged) in [
            (
                &uri,
                "nested/dog.txt",
                StatusCode::OK,
                Some("data/nested/dog.txt"),
            ),
            (&root_uri, "root.txt", StatusCode::OK, Some("root.txt")),
            (&uri, "../escape.txt", StatusCode::BAD_REQUEST, None),
        ] {
            let (body, headers) = create_form_data_payload_and_headers(
                "file[]",
                Some(filename.to_string()),
                Some(mime::TEXT_PLAIN),
                Bytes::from(filename),
            );
            let req = headers
                .into_iter()
                .fold(TestRequest::post().uri(target), |req, hdr| {
                    req.insert_header(hdr)
                })
                .set_payload(body)
                .to_request();
            let resp = call_service(&app, req).await;
            assert_eq!(resp.status(), status, "{filename}");
            if let Some(staged) = staged {
                let response: FilePathsResponse = serde_json::from_slice(&read_body(resp).await)?;
                assert_eq!(
                    response.paths,
                    vec![PathBuf::from(staged)],
                    "{filename} stages at {staged}"
                );
            }
        }

        // A CSV over `stream_segment_size()` is spooled to disk, a smaller one stays in memory, and
        // both are staged with the rows they hold. A file uploaded again with the content its
        // commit has is not staged, even after a change to it was.
        let mut big = String::from("id,label\n");
        let mut big_rows = 0;
        while big.len() as u64 <= stream_segment_size() {
            big.push_str(&format!("{big_rows},label {big_rows}\n"));
            big_rows += 1;
        }
        let small = "id,label\n3,c\n4,d\n".to_string();
        for (name, content) in [
            ("big.csv", big),
            ("small.csv", small),
            ("rows.csv", "id,label\n1,a\n".into()),
            ("rows.csv", rows.into()),
        ] {
            let (body, headers) = create_form_data_payload_and_headers(
                "file[]",
                Some(name.to_string()),
                Some(mime::TEXT_CSV),
                Bytes::from(content),
            );
            let req = headers
                .into_iter()
                .fold(TestRequest::post().uri(&uri), |req, hdr| {
                    req.insert_header(hdr)
                })
                .set_payload(body)
                .to_request();
            assert_eq!(
                call_service(&app, req).await.status(),
                StatusCode::OK,
                "{name}"
            );
        }
        let workspace = workspaces::get(&repo, &workspace_id)?
            .ok_or_else(|| OxenError::WorkspaceNotFound(workspace_id.clone().into()))?;
        let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
        for (path, height) in [("data/big.csv", big_rows), ("data/small.csv", 2)] {
            let staged = staged_db_manager
                .read_from_staged_db(path)?
                .unwrap_or_else(|| panic!("{path} is staged"));
            let EMerkleTreeNode::File(file_node) = staged.node.node else {
                panic!("{path} is staged as a file");
            };
            let Some(GenericMetadata::MetadataTabular(metadata)) = file_node.metadata() else {
                panic!(
                    "{path} is staged with tabular metadata: {:?}",
                    file_node.metadata()
                );
            };
            assert_eq!(
                (metadata.tabular.width, metadata.tabular.height),
                (2, height),
                "{path} is staged with its columns and rows"
            );
        }
        assert!(
            staged_db_manager
                .read_from_staged_db("data/rows.csv")?
                .is_none(),
            "a file uploaded again with its committed content is not staged"
        );
        drop(staged_db_manager);
        drop(workspace);

        // A version upload's parts may arrive out of order, and the part that completes the file
        // stages it in the workspace.
        let data: Vec<u8> = (0..stream_segment_size() + 10)
            .map(|i| (i % 251) as u8)
            .collect();
        let hash = util::hasher::hash_buffer(&data);
        let uploads = format!("/oxen/{namespace}/{repo_name}/versions/uploads");
        let req = TestRequest::post()
            .uri(&uploads)
            .set_json(BeginVersionUploadRequest {
                hash: hash.clone(),
                size: data.len() as u64,
                destination: Some(WorkspaceDestination {
                    workspace_id: workspace_id.clone(),
                    path: "data/big.bin".into(),
                }),
            })
            .to_request();
        let resp = call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let begun: VersionUploadResponse = serde_json::from_slice(&read_body(resp).await)?;
        let VersionUploadStart::AwaitingParts {
            upload_id,
            part_size,
            num_parts,
            part_window,
        } = begun.file
        else {
            panic!("the store does not have the file yet: {:?}", begun.file);
        };
        assert_eq!(num_parts, 2);
        assert!(
            part_window >= num_parts,
            "the window covers every part of a two-part file: {part_window}"
        );

        let part_size = part_size as usize;
        let parts_uri = |n: u64| format!("{uploads}/{upload_id}/parts/{n}");
        let mut completed = Vec::new();
        for (part_number, part) in [(2, &data[part_size..]), (1, &data[..part_size])] {
            let req = TestRequest::put()
                .uri(&parts_uri(part_number))
                .set_payload(part.to_vec())
                .to_request();
            let resp = call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK, "part {part_number}");
            let stored: VersionUploadPartResponse = serde_json::from_slice(&read_body(resp).await)?;
            completed.push(stored.completed);
        }
        assert_eq!(
            completed,
            [false, true],
            "only the part that fills the file completes it"
        );
        assert_eq!(repo.version_store().get_version(&hash).await?, data);
        let workspace = repositories::workspaces::get(&repo, &workspace_id)?
            .ok_or_else(|| OxenError::WorkspaceNotFound(workspace_id.clone().into()))?;
        let status = repositories::workspaces::status::status(&workspace)?;
        drop(workspace);
        assert!(
            status.staged_files.contains_key(Path::new("data/big.bin")),
            "the completed upload is staged at the requested path: {:?}",
            status.staged_files.keys()
        );

        let req = TestRequest::put()
            .uri(&parts_uri(1))
            .set_payload(data[..part_size].to_vec())
            .to_request();
        let resp = call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "a completed upload takes no more parts"
        );

        // Beginning the upload again, as a client that lost the last part's response does, stages
        // the stored file with no parts to send.
        let req = TestRequest::post()
            .uri(&uploads)
            .set_json(BeginVersionUploadRequest {
                hash: hash.clone(),
                size: data.len() as u64,
                destination: Some(WorkspaceDestination {
                    workspace_id: workspace_id.clone(),
                    path: "data/again.bin".into(),
                }),
            })
            .to_request();
        let resp = call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let again: VersionUploadResponse = serde_json::from_slice(&read_body(resp).await)?;
        assert!(
            matches!(again.file, VersionUploadStart::AlreadyStored),
            "a stored file needs no parts: {:?}",
            again.file
        );
        let workspace = repositories::workspaces::get(&repo, &workspace_id)?
            .ok_or_else(|| OxenError::WorkspaceNotFound(workspace_id.clone().into()))?;
        let status = repositories::workspaces::status::status(&workspace)?;
        drop(workspace);
        assert!(
            status
                .staged_files
                .contains_key(Path::new("data/again.bin")),
            "a stored file is staged at the destination it is begun with: {:?}",
            status.staged_files.keys()
        );

        // A released client uploads in chunks, which `complete` assembles and stages.
        let chunked: Vec<u8> = (0..stream_segment_size() + 10)
            .map(|i| (i % 241) as u8)
            .collect();
        let chunked_hash = util::hasher::hash_buffer(&chunked);
        let version = format!("/oxen/{namespace}/{repo_name}/versions/{chunked_hash}");
        let req = TestRequest::post()
            .uri(&format!("{version}/create"))
            .set_payload("{}")
            .to_request();
        assert_eq!(call_service(&app, req).await.status(), StatusCode::OK);
        let half = chunked.len() / 2;
        for (offset, chunk) in [(half, &chunked[half..]), (0, &chunked[..half])] {
            let req = TestRequest::put()
                .uri(&format!("{version}/chunks?offset={offset}"))
                .set_payload(chunk.to_vec())
                .to_request();
            assert_eq!(call_service(&app, req).await.status(), StatusCode::OK);
        }
        let req = TestRequest::post()
            .uri(&format!("{version}/complete"))
            .set_json(CompleteVersionUploadRequest {
                files: vec![CompletedFileUpload {
                    hash: chunked_hash.clone(),
                    file_name: "chunked.bin".to_string(),
                    dst_dir: Some("data".into()),
                    num_chunks: Some(2),
                    upload_results: None,
                }],
                workspace_id: Some(workspace_id.clone()),
            })
            .to_request();
        assert_eq!(call_service(&app, req).await.status(), StatusCode::OK);
        assert_eq!(
            repo.version_store().get_version(&chunked_hash).await?,
            chunked,
            "`complete` stores the chunks as one file"
        );
        let workspace = repositories::workspaces::get(&repo, &workspace_id)?
            .ok_or_else(|| OxenError::WorkspaceNotFound(workspace_id.clone().into()))?;
        let status = repositories::workspaces::status::status(&workspace)?;
        drop(workspace);
        assert!(
            status
                .staged_files
                .contains_key(Path::new("data/chunked.bin")),
            "`complete` stages the file at its destination: {:?}",
            status.staged_files.keys()
        );

        // Build a gzipped part that inflates to one byte past the decompression cap. Highly
        // compressible zero bytes keep the compressed body tiny while the decompressed size trips
        // the limit — the decompression-bomb shape the endpoint guards against. Size via
        // stream_segment_size() so the test tracks the active cap (128 KiB under bin/test).
        let decompressed = vec![0u8; stream_segment_size() as usize + 1];
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&decompressed)?;
        let gzipped = encoder.finish()?;

        // A part labeled gzip that is not gzip data, then two valid gzipped parts for one path,
        // follow the bomb, to check refused parts leave the rest, and that the later of two parts
        // for one path is the one staged.
        let gzip = |content: &[u8]| -> std::io::Result<Vec<u8>> {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(content)?;
            encoder.finish()
        };
        let after = gzip(b"after the bomb")?;
        let later = gzip(b"later for the same path")?;
        // An uncompressed gzip of `stream_segment_size()` bytes arrives longer than that, so it is
        // spooled to disk and gunzipped there.
        let large: Vec<u8> = (0..stream_segment_size())
            .map(|i| (i % 251) as u8)
            .collect();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::none());
        encoder.write_all(&large)?;
        let large_gzipped = encoder.finish()?;
        let boundary = "test-bomb-boundary";
        let mut body: Vec<u8> = Vec::new();
        for (filename, part) in [
            ("bomb.bin", &gzipped),
            ("corrupt.bin", &b"not gzip data".to_vec()),
            ("after.txt", &after),
            ("after.txt", &later),
            ("large.bin", &large_gzipped),
        ] {
            body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
            body.extend_from_slice(
                format!(
                    "Content-Disposition: form-data; name=\"file[]\"; filename=\"{filename}\"\r\n"
                )
                .as_bytes(),
            );
            body.extend_from_slice(b"Content-Type: application/gzip\r\n\r\n");
            body.extend_from_slice(part);
            body.extend_from_slice(b"\r\n");
        }
        body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

        let uri = format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/data");

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .insert_header((
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            ))
            .set_payload(body.clone())
            .to_request();

        let resp = actix_web::test::call_service(&app, req).await;
        // The endpoint drops the offending part rather than failing the whole request, so the
        // response is still 200 and only the valid parts are staged.
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);

        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let response: FilePathsResponse = serde_json::from_slice(&bytes)?;
        assert_eq!(
            response.paths,
            ["data/after.txt", "data/after.txt", "data/large.bin"].map(PathBuf::from),
            "the gzip bomb and the part that is not gzip data are refused"
        );
        assert!(
            repo.version_store()
                .version_exists(&util::hasher::hash_buffer(&large))
                .await?,
            "a spooled gzipped part is stored gunzipped"
        );
        // Scoped so the workspace's handles on the repository close before the cleanup below.
        {
            let workspace =
                repositories::workspaces::get(&repo, &workspace_id)?.expect("the workspace exists");
            let staged = get_staged_db_manager(&workspace.workspace_repo)?
                .read_from_staged_db("data/after.txt")?
                .expect("after.txt is staged");
            let EMerkleTreeNode::File(file_node) = staged.node.node else {
                panic!("after.txt is staged as a file");
            };
            assert_eq!(
                file_node.hash().to_string(),
                util::hasher::hash_buffer(b"later for the same path"),
                "the later of two parts for one path is the one staged"
            );
        }

        // The decompressed content was never hashed or stored, so its blob must be absent.
        let bomb_hash = util::hasher::hash_buffer(&decompressed);
        assert!(
            !repo.version_store().version_exists(&bomb_hash).await?,
            "gzip bomb contents should not have been stored"
        );

        // Cut the body one byte into the first part's data, so no part is stored.
        let first_data = body
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("the body has a part header")
            + 4;
        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .insert_header((
                header::CONTENT_TYPE,
                format!("multipart/form-data; boundary={boundary}"),
            ))
            .set_payload(body[..first_data + 1].to_vec())
            .to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let response: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(
            response["error"]["detail"], "Multipart stream is incomplete",
            "a truncated body reports the multipart error's own reason"
        );

        // A directory named like a tabular file moves as a directory, not through the data frame
        // rename, whether it is committed or exists only as staged files. These moves run in a
        // workspace of their own.
        let workspace_id = uuid::Uuid::new_v4().to_string();
        let workspace = repositories::workspaces::create(&repo, &commit, &workspace_id, true)?;
        let fresh = workspace.workspace_repo.path.join("fresh.csv/x.txt");
        util::fs::write_to_path(&fresh, "X")?;
        repositories::workspaces::files::add(&workspace, &fresh).await?;

        for (path, new_path) in [("exports.csv", "renamed"), ("fresh.csv", "fresh_renamed")] {
            let uri =
                format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path}");
            let req = TestRequest::patch()
                .uri(&uri)
                .set_json(RenameRequest {
                    new_path: new_path.to_string(),
                })
                .to_request();
            let resp = call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK, "moving {path} to {new_path}");
        }

        let uri = format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/renamed");
        for (new_path, status, error_type) in [
            ("renamed/inner", StatusCode::BAD_REQUEST, "move_into_itself"),
            (
                "fresh_renamed/x.txt/inner",
                StatusCode::CONFLICT,
                "not_a_directory",
            ),
        ] {
            let req = TestRequest::patch()
                .uri(&uri)
                .set_json(RenameRequest {
                    new_path: new_path.to_string(),
                })
                .to_request();
            let resp = call_service(&app, req).await;
            assert_eq!(resp.status(), status, "moving renamed to {new_path}");
            let body: serde_json::Value = actix_web::test::read_body_json(resp).await;
            assert_eq!(
                body["error"]["type"], error_type,
                "moving renamed to {new_path} is refused as such: {body}"
            );
        }

        let status = repositories::workspaces::status::status(&workspace)?;
        for path in ["renamed/a.txt", "renamed/b.csv", "fresh_renamed/x.txt"] {
            assert_eq!(
                status
                    .staged_files
                    .get(Path::new(path))
                    .map(|entry| entry.status.clone()),
                Some(liboxen::model::StagedEntryStatus::Added),
                "{path} should be staged as added"
            );
        }

        drop((app, workspace));
        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }
}
