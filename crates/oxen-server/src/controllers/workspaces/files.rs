use crate::errors::OxenHttpError;
use crate::helpers::{file_stream_response, get_repo};
use crate::params::{app_data, path_param};

use liboxen::core;
use liboxen::core::staged::get_staged_db_manager;
use liboxen::error::OxenError;
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::metadata::metadata_video::VideoThumbnail;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::workspaces::RenameRequest;
use liboxen::view::{
    ErrorFilesResponse, FilePathsResponse, FileWithHash, StatusMessage, StatusMessageDescription,
};

use actix_web::{HttpRequest, HttpResponse, web};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use utoipa;

#[derive(utoipa::ToSchema)]
pub struct FileUpload {
    #[schema(value_type = String, format = Binary)]
    pub file: Vec<u8>,
}

/// Query parameters for staging operations
#[derive(Deserialize, Debug, Default)]
pub struct StagingQueryParams {
    pub update_timestamp: Option<bool>,
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
    let version_store = repo.version_store();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Err(OxenHttpError::NotFound);
    };

    let path = path_param(&req, "path")?.to_string();
    log::debug!("got workspace file path {:?}", &path);

    // First, look for the file in the workspace staged_db
    let staged_db_manager = get_staged_db_manager(&workspace.workspace_repo)?;
    let file_node = match staged_db_manager.read_from_staged_db(&path)? {
        Some(staged_node) => match staged_node.node.node {
            EMerkleTreeNode::File(f) => Ok(f),
            _ => Err(OxenError::basic_str(
                "Only single file download is supported",
            )),
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

        let stream =
            util::fs::handle_video_thumbnail(Arc::clone(&version_store), hash_str, video_thumbnail)
                .await?;

        return Ok(file_stream_response("image/jpeg", &last_commit_id, None).streaming(stream));
    }

    log::debug!("did not hit the resize or thumbnail cache");

    // Stream the file
    let stream = version_store.get_version_stream(&hash_str).await?;

    Ok(file_stream_response(mime_type, &last_commit_id, Some(num_bytes)).streaming(stream))
}

/// Stage files to workspace
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/files/batch/{directory}",
    description = "Stage file nodes to a workspace. Do not upload file contents to the repository.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("directory" = String, Path, description = "The directory to stage the files into", example = "data/train"),
        ("update_timestamp" = Option<bool>, Query, description = "Force staging even if file content has not changed, updating the file timestamp", example = false)
    ),
    request_body(
        content = Vec<FileWithHash>,
        description = "List of files and their pre-calculated hashes (must exist in version store).",
        example = json!([
            {
                "path": "images/train/dog.jpg",
                "hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            }
        ])
    ),
    responses(
        (status = 200, description = "Files staged successfully", body = ErrorFilesResponse),
        (status = 404, description = "Workspace not found")
    )
)]
pub async fn add_version_files(
    req: HttpRequest,
    payload: web::Json<Vec<FileWithHash>>,
    query: web::Query<StagingQueryParams>,
) -> Result<HttpResponse, OxenHttpError> {
    // Add file to staging
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let directory = path_param(&req, "directory")?.to_string();
    let update_timestamp = query.update_timestamp.unwrap_or(false);

    let repo = get_repo(app_data, namespace, repo_name)?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let files_with_hash: Vec<FileWithHash> = payload.into_inner();
    log::debug!(
        "Calling add version files from the core workspace logic with {} files (update_timestamp: {})",
        files_with_hash.len(),
        update_timestamp,
    );
    let err_files = core::v_latest::workspaces::files::add_version_files(
        &repo,
        &workspace,
        &files_with_hash,
        &directory,
        update_timestamp,
    )
    .await?;

    log::debug!("Staging complete with {:?} err files", err_files.len());

    // Return the error files for retry
    Ok(HttpResponse::Ok().json(ErrorFilesResponse {
        status: StatusMessage::resource_created(),
        err_files,
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
    let repo = get_repo(app_data, namespace, repo_name)?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let paths_to_remove: Vec<PathBuf> = payload.into_inner();

    let mut ret_files = vec![];
    let mut err_files = vec![];

    for path in &paths_to_remove {
        err_files.extend(repositories::workspaces::files::rm(&workspace, &path).await?);
        log::debug!("rm ✅ success! staged file {path:?} as removed");
        ret_files.push(path);
    }

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

/// Move or rename a file within the workspace
#[utoipa::path(
    patch,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path}",
    description = "Move or rename a file within the workspace.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("path" = String, Path, description = "The current path to the file to move/rename", example = "images/train/dog_1.jpg")
    ),
    request_body(
        content = RenameRequest,
        description = "The new path for the file",
        example = json!({"new_path": "images/train/renamed_dog_1.jpg"})
    ),
    responses(
        (status = 200, description = "File successfully moved/renamed", body = StatusMessage),
        (status = 400, description = "Invalid request (empty new_path or new_path already exists)"),
        (status = 404, description = "Workspace or file not found")
    )
)]
pub async fn mv(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let workspace_id = path_param(&req, "workspace_id")?.to_string();
    let repo = get_repo(app_data, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    // Parse request body
    let body: RenameRequest = serde_json::from_str(&body)?;

    // Validate new_path is not empty
    if body.new_path.is_empty() {
        return Err(OxenHttpError::BadRequest("new_path cannot be empty".into()));
    }

    // Validate and normalize new_path
    let new_path = util::fs::validate_and_normalize_path(&body.new_path)?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    // Check if new_path already exists in the workspace or base repo
    if repositories::tree::get_node_by_path(&repo, &workspace.commit, &new_path)?.is_some() {
        return Err(OxenHttpError::BadRequest(
            "new_path already exists in the repository".into(),
        ));
    }

    // For tabular files, use the data_frames rename instead
    if util::fs::is_tabular(&path) {
        repositories::workspaces::data_frames::rename(&workspace, &path, &new_path).await?;
    } else {
        repositories::workspaces::files::mv(&workspace, &path, &new_path)?;
    }

    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    use actix_web::http::header;
    use actix_web::{App, web};
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    #[actix_web::test]
    async fn test_get_nonexistent_file_returns_404() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        // Create a file and commit so we have a valid commit for the workspace
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let commit = repositories::commit(&repo, "First commit")?;

        // Create a workspace
        let workspace_id = uuid::Uuid::new_v4().to_string();
        repositories::workspaces::create(&repo, &commit, &workspace_id, false)?;

        // Request a file that does not exist in the workspace or the base repo
        let file_path = "this_file_does_not_exist.txt";
        let uri =
            format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{file_path}");

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path:.*}",
                    web::get().to(controllers::workspaces::files::get),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_workspace_file_get_exposes_content_length() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Workspace-Get-Headers";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let hello_file = repo.path.join("hello.txt");
        let file_content = "Hello";
        util::fs::write_to_path(&hello_file, file_content)?;
        repositories::add(&repo, &hello_file).await?;
        let commit = repositories::commit(&repo, "First commit")?;

        let workspace_id = uuid::Uuid::new_v4().to_string();
        repositories::workspaces::create(&repo, &commit, &workspace_id, false)?;

        let uri =
            format!("/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/hello.txt");

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/workspaces/{workspace_id}/files/{path:.*}",
                    web::get().to(controllers::workspaces::files::get),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;

        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::CONTENT_LENGTH).unwrap(),
            file_content.len().to_string().as_str()
        );
        assert_eq!(
            resp.headers()
                .get(header::ACCESS_CONTROL_EXPOSE_HEADERS)
                .unwrap(),
            header::CONTENT_LENGTH.as_str()
        );

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
