use crate::errors::OxenHttpError;
use crate::helpers::{
    ByteRange, create_user_from_options, file_range_response, file_stream_response, get_repo,
    get_repo_async, parse_byte_range, range_not_satisfiable_response,
};
use crate::params::{
    app_data, parse_resource, parse_resource_async, path_param, query_param, request_relative_path,
};

use actix_multipart::form::text::Text;
use actix_multipart::form::{FieldReader, Limits, MultipartForm};
use actix_multipart::{Field, MultipartError};
use actix_web::http::Method;
use actix_web::{HttpRequest, HttpResponse, web};
use bytes::Bytes;
use futures_util::TryStreamExt as _;
use futures_util::future::LocalBoxFuture;
use futures_util::stream;
use liboxen::core::repo_locks;
use liboxen::core::staged::get_staged_db_manager;
use liboxen::error::OxenError;
use liboxen::model::Commit;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::file::{FileContents, FileNew, TempFileNew};
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::metadata::metadata_video::VideoThumbnail;
use liboxen::repositories::commits;
use liboxen::repositories::{self, branches};
use liboxen::util;
use liboxen::util::fs::AtomicFile;
use liboxen::view::{CommitResponse, StatusMessage};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::slice;
use std::sync::Arc;
use tokio::task::spawn_blocking;
use utoipa::ToSchema;

#[derive(MultipartForm, ToSchema)]
#[schema(
    title = "FileUploadBody",
    description = "Multipart form for uploading files. Use `file` for a single full-path upload, or `files[]` for uploading one or more files into a directory.",
    example = json!({
        "files[]": ["<binary data>"],
        "message": "Adding a picture of a cow",
        "name": "bessie",
        "email": "bessie@oxen.ai"
    })
)]
pub struct FileUploadBody {
    #[schema(value_type = Option<String>, example = "bessie")]
    name: Option<Text<String>>,
    #[schema(value_type = Option<String>, example = "bessie@oxen.ai")]
    email: Option<Text<String>>,
    #[schema(value_type = Option<String>, example = "Adding a new image to the training set")]
    message: Option<Text<String>>,
    /// Deprecated: use `files[]` instead.
    #[schema(value_type = Option<String>, format = Binary, deprecated)]
    file: Option<MultipartTempFileNew>,
    #[multipart(rename = "files[]")]
    #[schema(value_type = Vec<String>, format = Binary)]
    files: Vec<MultipartTempFileNew>,
}

impl FileUploadBody {
    pub fn name(&self) -> Option<String> {
        self.name.as_ref().map(|s| s.to_string())
    }
    pub fn email(&self) -> Option<String> {
        self.email.as_ref().map(|s| s.to_string())
    }
    pub fn message(&self) -> Option<String> {
        self.message.as_ref().map(|s| s.to_string())
    }
    pub fn file(&self) -> Option<&TempFileNew> {
        self.file.as_ref().map(|f| &f.0)
    }
    pub fn files(&self) -> Vec<&TempFileNew> {
        self.files.iter().map(|f| &f.0).collect()
    }
}

/// Newtype wrapper around `TempFileNew` that implements actix-multipart's `FieldReader` trait,
/// so it can be used directly in a `#[derive(MultipartForm)]` struct.
#[derive(Debug)]
pub struct MultipartTempFileNew(TempFileNew);

impl<'t> FieldReader<'t> for MultipartTempFileNew {
    type Future = LocalBoxFuture<'t, Result<Self, MultipartError>>;

    fn read_field(_req: &'t HttpRequest, mut field: Field, limits: &'t mut Limits) -> Self::Future {
        Box::pin(async move {
            let filename = field
                .content_disposition()
                .and_then(|cd| cd.get_filename().map(sanitize_filename::sanitize))
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

            let mut contents = Vec::new();
            while let Some(chunk) = field.try_next().await? {
                limits.try_consume_limits(chunk.len(), true)?;
                contents.extend_from_slice(&chunk);
            }

            Ok(MultipartTempFileNew(TempFileNew {
                path: PathBuf::from(filename),
                contents: FileContents::Binary(contents),
            }))
        })
    }
}

/// Combined query parameters for file operations (image resize and video thumbnail)
/// Since both ImgResize and VideoThumbnail share width/height fields, we combine them here
#[derive(Deserialize, Debug)]
pub struct FileQueryParams {
    // Shared parameters (can be used for both image resize and video thumbnail)
    pub width: Option<u32>,
    pub height: Option<u32>,
    // Video thumbnail specific parameters
    pub timestamp: Option<f64>,
    pub thumbnail: Option<bool>,
}

/// Download File
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/file/{resource}",
    tag = "Files",
    description = "Download a file from the repository. Supports image resizing and video thumbnail generation via query parameters.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Voice-Data"),
        ("resource" = String, Path, description = "Path to the file (including branch/commit info)", example = "main/audio/moo.wav"),
        ("width" = Option<u32>, Query, description = "Width for image resize or video thumbnail", example = 320),
        ("height" = Option<u32>, Query, description = "Height for image resize or video thumbnail", example = 240),
        ("timestamp" = Option<f64>, Query, description = "Timestamp in seconds to extract video thumbnail from (default: 1.0)", example = 1.0),
        ("thumbnail" = Option<bool>, Query, description = "Set to true to generate a video thumbnail instead of returning the full video", example = true)
    ),
    responses(
        (status = 200, description = "File content stream", content_type = "application/octet-stream", body = Vec<u8>),
        (status = 404, description = "File not found")
    )
)]
pub async fn get(
    req: HttpRequest,
    query: web::Query<FileQueryParams>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, &namespace, &repo_name)?;
    // This GET writes a resized/thumbnail derived blob on a cache miss (tech-debt ENG-1373); guard
    // the whole handler so a stop-the-world op (migration/prune/fsck) blocks it.
    let _write = repo_locks::begin_write(&repo)?;
    let version_store = repo.version_store();
    let resource = parse_resource(&req, &repo)?;
    let workspace = resource.workspace.as_ref();
    let path = resource.path.clone();

    // Use workspace_repo for staged DB operations, base_repo for commit tree lookups
    let (staged_repo, base_repo) = match workspace {
        Some(ws) => (&ws.workspace_repo, &repo),
        None => (&repo, &repo),
    };

    let entry = match workspace {
        Some(ws) => {
            // Scoped so the staged db handle is released before the awaits below, rather than
            // held for the length of the response.
            let staged_node = {
                let staged_db_manager = get_staged_db_manager(staged_repo)?;
                staged_db_manager.read_from_staged_db(&path)?
            };
            // Try staged DB first
            if let Some(staged_node) = staged_node {
                match staged_node.node.node {
                    EMerkleTreeNode::File(f) => Ok(f),
                    _ => Err(OxenError::NotAFile(path.clone().into())),
                }?
            } else {
                // Fall back to commit tree using workspace's commit
                let commit = &ws.commit;
                repositories::tree::get_file_by_path(base_repo, commit, &path)?
                    .ok_or_else(|| OxenError::path_does_not_exist(path.clone()))?
            }
        }
        None => {
            let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
            repositories::tree::get_file_by_path(base_repo, &commit, &path)?
                .ok_or_else(|| OxenError::path_does_not_exist(path.clone()))?
        }
    };

    let file_hash = entry.hash();
    let hash_str = file_hash.to_string();
    let mime_type = entry.mime_type();
    let num_bytes = entry.num_bytes();
    let last_commit_id = entry.last_commit_id().to_string();
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
            version_store.clone(),
            hash_str.clone(),
            &path,
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
            timestamp: query_params.timestamp,
            thumbnail: query_params.thumbnail,
        };
        log::debug!("video_thumbnail {video_thumbnail:?}");

        let tmp_dir = util::fs::oxen_hidden_dir(&repo.path).join("tmp");
        let stream = util::fs::handle_video_thumbnail(
            Arc::clone(&version_store),
            hash_str,
            video_thumbnail,
            &tmp_dir,
        )
        .await?;

        return Ok(file_stream_response("image/jpeg", &last_commit_id, None).streaming(stream));
    }

    log::debug!("did not hit the resize or thumbnail cache");

    // HTTP/1 actix still polls a HEAD response's body to the end and discards it, so streaming
    // the blob here would read the whole file (a full object download on S3) for nothing. Check
    // the blob exists instead, so a missing one is the same error GET returns, and answer with
    // GET's headers over an empty body.
    if req.method() == Method::HEAD {
        if !version_store.version_exists(&hash_str).await? {
            return Err(OxenError::VersionStoreBlobMissing { hash: hash_str }.into());
        }
        return Ok(
            file_stream_response(mime_type, &last_commit_id, Some(num_bytes))
                .streaming(stream::empty::<Result<Bytes, std::io::Error>>()),
        );
    }

    // A viewer scrubbing a video asks for the part it needs, so read that rather than the file.
    match parse_byte_range(&req, num_bytes) {
        ByteRange::Unsatisfiable => Ok(range_not_satisfiable_response(num_bytes)),
        ByteRange::Partial { start, end } => {
            let stream = version_store
                .get_version_range(&hash_str, start, end - start + 1)
                .await?;
            Ok(
                file_range_response(mime_type, &last_commit_id, start, end, num_bytes)
                    .streaming(stream),
            )
        }
        ByteRange::Whole => {
            let stream = version_store.get_version_stream(&hash_str).await?;
            Ok(file_stream_response(mime_type, &last_commit_id, Some(num_bytes)).streaming(stream))
        }
    }
}

/// Upload files
#[utoipa::path(
    put,
    path = "/api/repos/{namespace}/{repo_name}/file/{resource}",
    tag = "Files",
    description = "Upload files via multipart form and commit them. Use `files[]` for directory uploads, or `file` for a single full-path upload. For backward compatibility, `file` also uploads into the target directory when `{resource}` already resolves to a directory.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path of the directory to add files in (including branch)", example = "main/train/images"),
    ),
    request_body(
        content_type = "multipart/form-data",
        content = FileUploadBody
    ),
    responses(
        (status = 200, description = "Files committed successfully", body = CommitResponse),
        (status = 400, description = "Bad Request"),
        (status = 404, description = "Branch or path not found")
    )
)]
pub async fn put(
    req: HttpRequest,
    MultipartForm(form): MultipartForm<FileUploadBody>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::put path {:?}", req.path());
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, &namespace, &repo_name)?;
    let _write = repo_locks::begin_write(&repo)?;

    // If there's no head commit, handle initial upload
    if repositories::commits::head_commit_maybe(&repo)?.is_none() {
        return handle_initial_put_empty_repo(&req, form, &repo).await;
    }

    let name = form.name();
    let email = form.email();
    let message = form.message();
    let file_parts = form.file();
    let files_array_parts = form.files();

    let resource = parse_resource(&req, &repo)?;

    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or_else(|| OxenError::local_branch_not_found(resource.version.to_string_lossy()))?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    let upload_mode = resolve_upload_mode(
        file_parts,
        &files_array_parts,
        node.as_ref().is_some_and(|n| n.is_dir()),
    )?;
    ensure_no_file_ancestors_in_tree(&repo, &commit, &resource.path, &resource.path)?;
    match upload_mode {
        MultipartUploadMode::SingleFile => {
            if resource.path.as_os_str().is_empty() {
                return Err(OxenHttpError::BadRequest(
                    "Invalid target path: expected a full file path for `file` uploads".into(),
                ));
            }
        }
        MultipartUploadMode::DirectoryFromFile | MultipartUploadMode::DirectoryFromFilesArray => {
            if node.as_ref().is_some_and(|n| n.is_file()) {
                return Err(OxenHttpError::BadRequest(
                    format!(
                        "Target path must be a directory: {}",
                        resource.path.display()
                    )
                    .into(),
                ));
            }
        }
    }

    let user = create_user_from_options(name.clone(), email.clone())?;
    let files = build_files_from_upload_parts(
        &resource.path,
        upload_mode,
        file_parts,
        &files_array_parts,
        &user,
    )?;
    let workspace = repositories::workspaces::create_temporary(&repo, &commit).await?;

    for file in &files {
        ensure_no_file_ancestors_in_tree(&repo, &commit, &file.path, &resource.path)?;
    }

    process_and_add_files(&repo, Some(&workspace), files).await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: name.unwrap_or_default(),
        email: email.unwrap_or_default(),
        message: message
            .unwrap_or_else(|| format!("Auto-commit files to {}", resource.path.to_string_lossy())),
    };

    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name).await?;

    log::debug!("file::put workspace commit ✅ success! commit {commit:?}");

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

/// Delete file
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/file/{resource}",
    description = "Remove a file from the repository. Stage the file as removed to a workspace and commit the removal. Can remove files or directories.",
    tag = "Files",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the file to be deleted (including branch)", example = "main/train/images/n01440764_10026.JPEG"),
    ),
    request_body(
        content_type = "multipart/form-data",
        content = FileUploadBody,
    ),
    responses(
        (status = 200, description = "File removed successfully", body = CommitResponse),
        (status = 404, description = "Branch or path not found")
    )
)]
// TODO: `content` above should be a different type that doesn't include `files` and `file` fields
pub async fn delete(
    req: HttpRequest,
    MultipartForm(form): MultipartForm<FileUploadBody>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::delete path {:?}", req.path());
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, &namespace, &repo_name)?;
    let _write = repo_locks::begin_write(&repo)?;

    // Parse the resource (branch/commit/path)
    let resource = parse_resource(&req, &repo)?;

    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or_else(|| OxenError::local_branch_not_found(resource.version.to_string_lossy()))?;
    let commit = resource.commit.clone().ok_or(OxenHttpError::NotFound)?;
    let path = resource.path;

    let name = form.name();
    let email = form.email();
    let message = form.message();

    log::debug!("file::delete creating workspace for commit: {commit}");
    let workspace = repositories::workspaces::create_temporary(&repo, &commit).await?;

    // Stage the path as removed
    log::debug!("file::delete staging path {path:?}");
    repositories::workspaces::files::rm(&workspace, slice::from_ref(&path)).await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: name.clone().unwrap_or("".to_string()),
        email: email.clone().unwrap_or("".to_string()),
        message: message
            .clone()
            .unwrap_or(format!("Remove {}", path.to_string_lossy())),
    };

    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name).await?;

    log::debug!("file::delete workspace commit ✅ success! commit {commit:?}");

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_deleted(),
        commit,
    }))
}

#[derive(ToSchema, Deserialize)]
#[schema(
    title = "FileMoveBody",
    description = "Body for moving/renaming a file",
    example = json!({
        "new_path": "new/path/to/file.txt",
        "message": "Renamed file to new location",
        "name": "bessie",
        "email": "bessie@oxen.ai"
    })
)]
pub struct FileMoveBody {
    #[schema(example = "new/path/to/file.txt")]
    pub new_path: String,
    #[schema(example = "Moved file to new location")]
    pub message: Option<String>,
    #[schema(example = "bessie")]
    pub name: Option<String>,
    #[schema(example = "bessie@oxen.ai")]
    pub email: Option<String>,
}

/// Move/Rename file
#[utoipa::path(
    patch,
    path = "/api/repos/{namespace}/{repo_name}/file/{resource}",
    tag = "Files",
    description = "Move or rename a file within the repository and commit the change.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the source file (including branch)", example = "main/train/images/old_name.jpg"),
    ),
    request_body(
        content_type = "application/json",
        content = FileMoveBody
    ),
    responses(
        (status = 200, description = "File moved/renamed successfully", body = CommitResponse),
        (status = 400, description = "Bad Request"),
        (status = 404, description = "Branch or file not found")
    )
)]
pub async fn mv(req: HttpRequest, body: String) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::mv path {:?}", req.path());
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;
    let _write = repo_locks::begin_write(&repo)?;

    // Parse the resource (branch/commit/path)
    let resource = parse_resource_async(&req, &repo).await?;

    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or_else(|| OxenError::local_branch_not_found(resource.version.to_string_lossy()))?;
    let commit = resource.commit.clone().ok_or(OxenHttpError::NotFound)?;
    let source_path = resource.path;

    // Parse the request body
    let body: FileMoveBody = serde_json::from_str(&body)?;

    // Validate new_path is not empty
    if body.new_path.is_empty() {
        return Err(OxenHttpError::BadRequest("new_path cannot be empty".into()));
    }

    // Validate and normalize new_path
    let new_path = util::fs::validate_and_normalize_path(&body.new_path)?;

    // Verify source file exists. Both this and the collision check below walk the Merkle tree
    // synchronously, so they go off the worker the same way the move itself does.
    if repositories::tree::get_file_by_path_async(&repo, &commit, &source_path)
        .await?
        .is_none()
    {
        return Err(OxenHttpError::NotFound);
    }

    // Check if new_path already exists (file OR directory)
    if repositories::tree::get_node_by_path_async(&repo, &commit, &new_path)
        .await?
        .is_some()
    {
        return Err(OxenHttpError::BadRequest(
            "new_path already exists in the repository".into(),
        ));
    }

    log::debug!("file::mv creating workspace for commit: {commit}");
    let workspace = repositories::workspaces::create_temporary(&repo, &commit).await?;

    // Stage the move
    log::debug!("file::mv moving {source_path:?} to {new_path:?}");
    repositories::workspaces::files::mv(&workspace, &source_path, &new_path).await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: body.name.clone().unwrap_or_default(),
        email: body.email.clone().unwrap_or_default(),
        message: body.message.clone().unwrap_or_else(|| {
            format!(
                "Move {} to {}",
                source_path.to_string_lossy(),
                new_path.to_string_lossy()
            )
        }),
    };

    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name).await?;

    log::debug!("file::mv workspace commit ✅ success! commit {commit:?}");

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_updated(),
        commit,
    }))
}

// Helper: when the repository has no commits yet, accept the upload as the first commit
async fn handle_initial_put_empty_repo(
    req: &HttpRequest,
    form: FileUploadBody,
    repo: &liboxen::model::LocalRepository,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(query_param(req, "resource"));

    let mut resource = resource.components();
    let branch_name = resource
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .unwrap_or("main".to_string());
    let path_string = resource.collect::<PathBuf>().to_string_lossy().to_string();
    let path = PathBuf::from(path_string);

    let name = form.name();
    let email = form.email();
    let message = form.message();
    let file_parts = form.file();
    let files_array_parts = form.files();

    let upload_mode = resolve_upload_mode(file_parts, &files_array_parts, false)?;

    let user = create_user_from_options(name, email)?;
    let files =
        build_files_from_upload_parts(&path, upload_mode, file_parts, &files_array_parts, &user)?;

    // If the user supplied files, add and commit them
    let mut commit: Option<Commit> = None;

    let has_files = !files.is_empty();
    process_and_add_files(repo, None, files).await?;

    if has_files {
        let commit_message = message.unwrap_or_else(|| "Initial commit".to_string());
        commit = Some(commits::commit_with_user(repo, &commit_message, &user)?);
        branches::create(repo, &branch_name, &commit.as_ref().unwrap().id)?;
    }

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit: commit.unwrap(),
    }))
}

// Helper function for processing files and adding to repo/workspace
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum MultipartUploadMode {
    SingleFile,
    DirectoryFromFile,
    DirectoryFromFilesArray,
}

fn resolve_upload_mode(
    file_parts: Option<&TempFileNew>,
    files_array_parts: &[&TempFileNew],
    target_is_existing_directory: bool,
) -> Result<MultipartUploadMode, OxenHttpError> {
    if file_parts.is_some() && !files_array_parts.is_empty() {
        return Err(OxenHttpError::BadRequest(
            "Ambiguous multipart payload: use either `file` or `files[]`, not both".into(),
        ));
    }
    if file_parts.is_some() {
        if target_is_existing_directory {
            return Ok(MultipartUploadMode::DirectoryFromFile);
        }
        return Ok(MultipartUploadMode::SingleFile);
    }
    if !files_array_parts.is_empty() {
        return Ok(MultipartUploadMode::DirectoryFromFilesArray);
    }

    Err(OxenHttpError::BadRequest(
        "Missing file data: expected `file` or `files[]` multipart parts".into(),
    ))
}

fn build_files_from_upload_parts(
    target_path: &Path,
    upload_mode: MultipartUploadMode,
    file_parts: Option<&TempFileNew>,
    files_array_parts: &[&TempFileNew],
    user: &liboxen::model::User,
) -> Result<Vec<FileNew>, OxenHttpError> {
    match upload_mode {
        MultipartUploadMode::SingleFile => {
            if target_path.as_os_str().is_empty() {
                return Err(OxenHttpError::BadRequest(
                    "Invalid target path: expected a full file path for `file` uploads".into(),
                ));
            }

            let temp_file = take_single_file_part(file_parts)?;
            Ok(vec![FileNew {
                path: request_relative_path(target_path, false, "target path")?,
                contents: temp_file.contents.clone(),
                user: user.clone(),
            }])
        }
        MultipartUploadMode::DirectoryFromFile => {
            let temp_file = take_single_file_part(file_parts)?;
            let normalized_target_dir =
                request_relative_path(target_path, true, "target directory")?;
            let normalized_file_path =
                request_relative_path(&temp_file.path, false, "uploaded file")?;
            Ok(vec![FileNew {
                path: normalized_target_dir.join(normalized_file_path),
                contents: temp_file.contents.clone(),
                user: user.clone(),
            }])
        }
        MultipartUploadMode::DirectoryFromFilesArray => {
            let normalized_target_dir =
                request_relative_path(target_path, true, "target directory")?;
            files_array_parts
                .iter()
                .map(|temp_file| {
                    let normalized_file_path =
                        request_relative_path(&temp_file.path, false, "uploaded file")?;
                    Ok(FileNew {
                        path: normalized_target_dir.join(normalized_file_path),
                        contents: temp_file.contents.clone(),
                        user: user.clone(),
                    })
                })
                .collect()
        }
    }
}

fn take_single_file_part(file_part: Option<&TempFileNew>) -> Result<&TempFileNew, OxenHttpError> {
    file_part.ok_or_else(|| {
        OxenHttpError::BadRequest("Missing file data: expected one `file` part".into())
    })
}

fn ensure_no_file_ancestors_in_tree(
    repo: &liboxen::model::LocalRepository,
    commit: &Commit,
    path_to_check: &Path,
    display_path: &Path,
) -> Result<(), OxenHttpError> {
    let mut ancestor = PathBuf::new();
    let components: Vec<_> = path_to_check.components().collect();

    for component in components.iter().take(components.len().saturating_sub(1)) {
        ancestor.push(component.as_os_str());
        if repositories::tree::get_node_by_path(repo, commit, &ancestor)?
            .as_ref()
            .is_some_and(|node| node.is_file())
        {
            return Err(OxenHttpError::BadRequest(
                format!(
                    "Target path must be a directory: {}",
                    display_path.display()
                )
                .into(),
            ));
        }
    }

    Ok(())
}

async fn process_and_add_files(
    repo: &liboxen::model::LocalRepository,
    workspace: Option<&liboxen::repositories::workspaces::TemporaryWorkspace>,
    files: Vec<FileNew>,
) -> Result<(), OxenError> {
    if files.is_empty() {
        return Ok(());
    }
    log::debug!("repositories::create files: {:?}", files.len());
    let root = match workspace {
        Some(ws) => ws.dir(),
        None => repo.path.clone(),
    };
    let payloads: Vec<(PathBuf, Bytes)> = files
        .into_iter()
        .map(|file| (root.join(file.path), file.contents.into_bytes()))
        .collect();
    let paths: Vec<PathBuf> = payloads.iter().map(|(path, _)| path.clone()).collect();

    // Every publish fsyncs, so the whole materialization runs in one offload.
    spawn_blocking(move || {
        for (path, bytes) in &payloads {
            AtomicFile::new(path).write(bytes)?;
        }
        Ok::<(), OxenError>(())
    })
    .await??;

    for filepath in &paths {
        if let Some(ws) = workspace {
            repositories::workspaces::files::add(ws, filepath).await?;
        } else {
            repositories::add(repo, filepath).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MultipartUploadMode, build_files_from_upload_parts, ensure_no_file_ancestors_in_tree,
    };
    use crate::errors::OxenHttpError;
    use crate::test;
    use liboxen::model::User;
    use liboxen::model::file::{FileContents, TempFileNew};
    use std::path::{Path, PathBuf};

    use actix_multipart_test::MultiPartFormDataBuilder;
    use actix_web::http::header::{self, HeaderMap, HeaderName};
    use actix_web::http::{Method, StatusCode};
    use actix_web::{App, body, web};
    use liboxen::view::CommitResponse;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    use crate::app_data::OxenAppData;
    use crate::controllers;

    /// Send `method` to `uri` through the GET and HEAD routes `services::file` registers, and
    /// return the status, headers and the whole body the handler produced.
    async fn call_file_route(
        sync_dir: &Path,
        method: Method,
        uri: &str,
    ) -> (StatusCode, HeaderMap, web::Bytes) {
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.to_path_buf()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::get().to(controllers::file::get),
                )
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::head().to(controllers::file::get),
                ),
        )
        .await;
        let req = actix_web::test::TestRequest::default()
            .method(method)
            .uri(uri)
            .to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = actix_http::body::to_bytes(resp.into_body())
            .await
            .expect("the handler's response body should collect");
        (status, headers, body)
    }

    /// Build a multipart PUT of `upload` to `uri`, sending it under each of `fields`.
    fn put_request(
        sync_dir: &Path,
        uri: &str,
        upload: &Path,
        fields: &[&str],
        content_type: &str,
        filename: &str,
        message: Option<&str>,
    ) -> actix_http::Request {
        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        for &field in fields {
            multipart_form_data_builder.with_file(
                PathBuf::from(upload),
                field,
                content_type,
                filename,
            );
        }
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        if let Some(message) = message {
            multipart_form_data_builder.with_text("message", message);
        }
        let (header, body) = multipart_form_data_builder.build();
        actix_web::test::TestRequest::put()
            .uri(uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .insert_header(header)
            .set_payload(body)
            .to_request()
    }

    #[actix_web::test]
    async fn test_controllers_file_get_and_put() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        let existing_file = repo.path.join("data/existing.txt");
        util::fs::write_to_path(&existing_file, "existing")?;
        let readme_file = repo.path.join("README.md");
        util::fs::write_to_path(&readme_file, "Initial commit")?;
        let notes_file = repo.path.join("notes/hero.md");
        util::fs::create_dir_all(notes_file.parent().unwrap())?;
        util::fs::write_to_path(&notes_file, "# Existing Hero")?;
        for path in [&hello_file, &existing_file, &readme_file, &notes_file] {
            repositories::add(&repo, path).await?;
        }
        let commit = repositories::commit(&repo, "First commit")?;

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                )
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::get().to(controllers::file::get),
                ),
        )
        .await;
        let file_uri = |path: &str| format!("/oxen/{namespace}/{repo_name}/file/main/{path}");
        let get = |path: &str| {
            actix_web::test::TestRequest::get()
                .uri(&file_uri(path))
                .app_data(OxenAppData::new(sync_dir.to_path_buf()))
                .to_request()
        };

        let resp = actix_web::test::call_service(&app, get("data/hello.txt")).await;
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
        // A file served whole still says a seek would have worked, which is what makes a player
        // ask for one at all.
        assert_eq!(resp.headers().get(header::ACCEPT_RANGES).unwrap(), "bytes");
        // Drain the body to close the streamed file handle before cleanup; on NFS an
        // unlinked-but-still-open file lingers as .nfsXXXX and the rmdir fails with ENOTEMPTY.
        actix_http::body::to_bytes(resp.into_body()).await.unwrap();

        let ranged = |path: &str, range: &str| {
            actix_web::test::TestRequest::get()
                .uri(&file_uri(path))
                .app_data(OxenAppData::new(sync_dir.to_path_buf()))
                .insert_header((header::RANGE, range.to_string()))
                .to_request()
        };

        let resp = actix_web::test::call_service(&app, ranged("data/hello.txt", "bytes=1-3")).await;
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            resp.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes 1-3/5"
        );
        assert_eq!(resp.headers().get(header::ACCEPT_RANGES).unwrap(), "bytes");
        assert_eq!(body::to_bytes(resp.into_body()).await.unwrap(), "ell");

        let resp = actix_web::test::call_service(&app, ranged("data/hello.txt", "bytes=99-")).await;
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            resp.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes */5"
        );
        body::to_bytes(resp.into_body()).await.unwrap();

        // A path no commit contains is an ordinary 404 and must not be confused with the server
        // having lost data.
        let resp = actix_web::test::call_service(&app, get("never/committed.txt")).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::NOT_FOUND);

        // HEAD answers with GET's status and headers for a file, a missing path and a directory,
        // and never streams the blob: its body is empty where GET's carries the file.
        let revision_header = HeaderName::from_static("oxen-revision-id");
        for (path, expected) in [
            ("data/hello.txt", StatusCode::OK),
            ("data/missing.txt", StatusCode::NOT_FOUND),
            ("data", StatusCode::NOT_FOUND),
        ] {
            let uri = file_uri(path);
            let (get_status, get_headers, get_body) =
                call_file_route(&sync_dir, Method::GET, &uri).await;
            let (head_status, head_headers, head_body) =
                call_file_route(&sync_dir, Method::HEAD, &uri).await;

            assert_eq!(get_status, expected, "GET {path}");
            assert_eq!(head_status, get_status, "HEAD {path}");
            for name in [
                header::CONTENT_TYPE,
                header::CONTENT_LENGTH,
                header::ACCESS_CONTROL_EXPOSE_HEADERS,
                revision_header.clone(),
            ] {
                assert_eq!(
                    head_headers.get(&name),
                    get_headers.get(&name),
                    "{name} {path}"
                );
            }
            if expected == StatusCode::OK {
                assert_eq!(
                    get_headers
                        .get(header::CONTENT_LENGTH)
                        .expect("GET of a file should send its content length"),
                    "5"
                );
                assert!(get_headers.contains_key(&revision_header));
                assert_eq!(get_body, "Hello");
                assert!(head_body.is_empty(), "HEAD streamed {head_body:?}");
            }
        }

        let err = ensure_no_file_ancestors_in_tree(
            &repo,
            &commit,
            Path::new("notes/hero.md/image.png"),
            Path::new("notes/hero.md/image.png"),
        )
        .unwrap_err();
        assert!(matches!(err, OxenHttpError::BadRequest(_)));

        let image_file = repo.path.join("image.png");
        util::fs::write_to_path(&image_file, "image-bytes")?;
        let req = put_request(
            &sync_dir,
            &file_uri("notes/hero.md/image.png"),
            &image_file,
            &["file"],
            "image/png",
            "image.png",
            Some("add image"),
        );
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::BAD_REQUEST,
            "an upload beneath an existing file is refused"
        );

        let hero_file = repo.path.join("hero.md");
        util::fs::write_to_path(&hero_file, "# Hero Content")?;
        let req = put_request(
            &sync_dir,
            &file_uri("pages/home/hero.md"),
            &hero_file,
            &["file", "files[]"],
            "text/markdown",
            "hero.md",
            Some("add hero"),
        );
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::BAD_REQUEST,
            "a payload with both `file` and `files[]` is ambiguous"
        );

        let req = put_request(
            &sync_dir,
            &file_uri("pages/home/hero.md"),
            &hero_file,
            &["file"],
            "text/markdown",
            "hero.md",
            Some("add hero"),
        );
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let put_body = body::to_bytes(resp.into_body()).await.unwrap();
        let put_resp: CommitResponse = serde_json::from_slice(&put_body)?;
        assert!(!put_resp.commit.id.is_empty());
        let resp = actix_web::test::call_service(&app, get("pages/home/hero.md")).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let body = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "# Hero Content");

        let req = put_request(
            &sync_dir,
            &file_uri("pages/about"),
            &hero_file,
            &["files[]"],
            "text/markdown",
            "hero.md",
            Some("add hero"),
        );
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let resp = actix_web::test::call_service(&app, get("pages/about/hero.md")).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let body = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "# Hero Content");

        let hi_file = repo.path.join("hi.txt");
        util::fs::write_to_path(&hi_file, "Hello from file field")?;
        let req = put_request(
            &sync_dir,
            &file_uri("data"),
            &hi_file,
            &["file"],
            "text/plain",
            "hi.txt",
            Some("add hello"),
        );
        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::OK,
            "a `file` field sent to an existing directory lands inside it"
        );
        let resp = actix_web::test::call_service(&app, get("data/hi.txt")).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let body = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "Hello from file field");

        util::fs::write_to_path(&hello_file, "Updated Content!")?;
        let req = put_request(
            &sync_dir,
            &file_uri("data"),
            &hello_file,
            &["files[]"],
            "text/plain",
            "hello.txt",
            Some("some_message"),
        );
        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let resp: CommitResponse = serde_json::from_slice(&bytes)?;
        assert_eq!(resp.status.status, "success");
        let entry =
            repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/hello.txt"))?
                .unwrap();
        let hash = entry.hash().to_string();
        let uploaded_content = repo.version_store().get_version(&hash).await?;
        assert_eq!(
            String::from_utf8(uploaded_content).unwrap(),
            "Updated Content!"
        );

        // A blob the tree still references is server-side data loss, not an absent resource, so
        // GET and HEAD both answer 5xx and report the hash instead of a bare NotFound. This drops
        // the blob, so it runs last.
        repo.version_store().delete_version(&hash).await?;
        let uri = file_uri("data/hello.txt");
        for method in [Method::GET, Method::HEAD] {
            let (status, _, body) = call_file_route(&sync_dir, method.clone(), &uri).await;
            assert_eq!(
                status,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{method} after blob loss"
            );
            let body: serde_json::Value =
                serde_json::from_slice(&body).expect("an error response should carry a JSON body");
            assert_eq!(body["error"]["type"], "version_blob_missing");
            assert_eq!(body["error"]["hash"], hash);
        }

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_put_to_an_empty_repo() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Empty-Repo-Put";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let upload_file = repo.path.join("payload.txt");
        util::fs::write_to_path(&upload_file, "payload")?;

        // repo.path is <sync_dir>/<namespace>/<repo_name>, so this resolves into sync_dir.
        let uri = format!("/oxen/{namespace}/{repo_name}/file/branch/../../escaped.txt");
        let req = put_request(
            &sync_dir,
            &uri,
            &upload_file,
            &["file"],
            "text/plain",
            "payload.txt",
            None,
        );
        let put_resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert!(!sync_dir.join("escaped.txt").exists());

        let config_path = repo.path.join(".oxen").join("config.toml");
        let config_before = std::fs::read(&config_path)?;
        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(
            repo.path.join("payload.txt"),
            "file",
            "text/plain",
            "payload.txt",
        );
        let (header, body) = multipart_form_data_builder.build();
        let put_req = actix_web::test::TestRequest::put()
            .uri(&format!(
                "/oxen/{namespace}/{repo_name}/file/main/.OXEN/config.toml"
            ))
            .insert_header(header)
            .set_payload(body)
            .to_request();
        let put_resp = actix_web::test::call_service(&app, put_req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            std::fs::read(&config_path)?,
            config_before,
            "a target inside .oxen leaves the repository's metadata alone"
        );

        // The first upload to an empty repo records the commit message it was sent with.
        let hero_file = repo.path.join("hero.md");
        util::fs::write_to_path(&hero_file, "# Hero Content")?;
        let uri = format!("/oxen/{namespace}/{repo_name}/file/first-upload-branch/hero.md");
        let req = put_request(
            &sync_dir,
            &uri,
            &hero_file,
            &["file"],
            "text/markdown",
            "hero.md",
            Some("first upload message"),
        );
        let put_resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::OK);
        let put_body = body::to_bytes(put_resp.into_body()).await.unwrap();
        let put_resp: CommitResponse = serde_json::from_slice(&put_body)?;
        assert_eq!(put_resp.commit.message, "first upload message");

        test::cleanup_repo_and_sync_dir(repo, &sync_dir)?;
        Ok(())
    }

    fn single_file_upload_parts() -> (TempFileNew, User) {
        (
            TempFileNew {
                path: PathBuf::from("hello.txt"),
                contents: FileContents::Text("Hello".to_string()),
            },
            User {
                name: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            },
        )
    }

    #[test]
    fn test_build_files_single_file_rejects_parent_dir_target() {
        let (temp_file, user) = single_file_upload_parts();
        let err = build_files_from_upload_parts(
            Path::new("../../outside.txt"),
            MultipartUploadMode::SingleFile,
            Some(&temp_file),
            &[],
            &user,
        )
        .unwrap_err();

        assert!(matches!(err, OxenHttpError::BadRequest(_)));
    }

    #[test]
    fn test_build_files_single_file_rejects_absolute_target() {
        let (temp_file, user) = single_file_upload_parts();
        let err = build_files_from_upload_parts(
            Path::new("/tmp/outside.txt"),
            MultipartUploadMode::SingleFile,
            Some(&temp_file),
            &[],
            &user,
        )
        .unwrap_err();

        assert!(matches!(err, OxenHttpError::BadRequest(_)));
    }

    #[test]
    fn test_build_files_single_file_collapses_current_dir_target() {
        let (temp_file, user) = single_file_upload_parts();
        let files = build_files_from_upload_parts(
            Path::new("./pages/./home.txt"),
            MultipartUploadMode::SingleFile,
            Some(&temp_file),
            &[],
            &user,
        )
        .unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, PathBuf::from("pages/home.txt"));
    }
}
