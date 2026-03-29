use crate::auth::access_keys::AccessKeyManager;
use crate::errors::OxenHttpError;
use crate::helpers::{create_user_from_options, file_stream_response, get_repo};
use crate::params::{app_data, parse_resource, path_param};

use actix_multipart::form::text::Text;
use actix_multipart::form::{FieldReader, Limits, MultipartForm};
use actix_multipart::{Field, MultipartError};
use actix_web::{HttpRequest, HttpResponse, web};
use futures_util::TryStreamExt as _;
use futures_util::future::LocalBoxFuture;
use liboxen::core::staged::get_staged_db_manager;
use liboxen::error::OxenError;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::file::{FileContents, FileNew, TempFileNew};
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::metadata::metadata_video::VideoThumbnail;
use liboxen::model::{Commit, User};
use liboxen::repositories::commits;
use liboxen::repositories::{self, branches};
use liboxen::util;
use liboxen::view::{CommitResponse, StatusMessage};

use serde::Deserialize;
use serde_json::Value;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use utoipa::ToSchema;

const ALLOWED_IMPORT_DOMAINS: [&str; 3] = ["huggingface.co", "kaggle.com", "oxen.ai"];

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
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let version_store = repo.version_store()?;
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
            let staged_db_manager = get_staged_db_manager(staged_repo)?;
            // Try staged DB first
            if let Some(staged_node) = staged_db_manager.read_from_staged_db(&path)? {
                match staged_node.node.node {
                    EMerkleTreeNode::File(f) => Ok(f),
                    _ => Err(OxenError::basic_str(
                        "Only single file download is supported",
                    )),
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
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

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

    // Extract claimed commit hash from HTTP header
    let claimed_commit_hash = req
        .headers()
        .get("oxen-based-on")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());

    // Check if the resource path is a file and handle conflicts
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    if let Some(ref n) = node
        && n.is_file()
    {
        // Get current commit hash for the file
        let current_commit_hash = n.latest_commit_id()?.to_string();

        // Only fail if claimed hash is provided but doesn't match current hash
        if let Some(claimed_hash) = claimed_commit_hash
            && current_commit_hash != claimed_hash
        {
            return Err(OxenHttpError::BasicError(
                format!(
                    "File has been modified since claimed revision. Current: {}, Claimed: {}. Your changes would overwrite another change without that being from a merge",
                    current_commit_hash, claimed_hash
                )
                .into(),
            ));
        }
    }

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
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    for file in &files {
        ensure_no_file_ancestors_in_tree(&repo, &commit, &file.path, &resource.path)?;
    }

    process_and_add_files(&repo, Some(&workspace), &files).await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: name.unwrap_or_default(),
        email: email.unwrap_or_default(),
        message: message.unwrap_or_else(|| {
            format!("Auto-commit files to {}", &resource.path.to_string_lossy())
        }),
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
    description = "Remove a file from the repository. Stage the file as removed to a workspace and commit the removal.",
    tag = "Files",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the file to be deleted (including branch)", example = "main/train/images/n01440764_10026.JPEG"),
    ),
    responses(
        (status = 200, description = "File removed successfully", body = CommitResponse),
        (status = 404, description = "Branch or path not found")
    )
)]
pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::delete path {:?}", req.path());
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    // Parse the resource (branch/commit/path) - DELETE operations require existing commits
    let resource = parse_resource(&req, &repo)?;

    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or_else(|| OxenError::local_branch_not_found(resource.version.to_string_lossy()))?;
    let commit = resource.commit.clone().ok_or(OxenHttpError::NotFound)?;

    // Extract claimed commit hash from HTTP header
    let claimed_commit_hash = req
        .headers()
        .get("oxen-based-on")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.to_string());

    // Check if the resource path exists and is a file
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    let node = node.ok_or_else(|| OxenHttpError::NotFound)?;

    if !node.is_file() {
        return Err(OxenHttpError::BadRequest(
            format!("Cannot delete directory: {}", resource.path.display()).into(),
        ));
    }

    // Get current commit hash for the file and validate oxen-based-on header if provided
    let current_commit_hash = node.latest_commit_id()?.to_string();
    if let Some(claimed_hash) = claimed_commit_hash
        && current_commit_hash != claimed_hash
    {
        return Err(OxenHttpError::BasicError(
            format!(
                "File has been modified since claimed revision. Current: {}, Claimed: {}. Your changes would overwrite another change without that being from a merge",
                current_commit_hash, claimed_hash
            )
            .into(),
        ));
    }

    // Get authenticated user from bearer token
    let authenticated_user = get_authenticated_user(&req)?;
    let user = match authenticated_user {
        Some(user) => user,
        None => {
            return Err(OxenHttpError::BadRequest(
                "Bearer token required for DELETE operations".into(),
            ));
        }
    };

    // Create temporary workspace
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    // Stage the deletion using the relative path (not absolute workspace path)
    repositories::workspaces::files::rm(&workspace, &resource.path).await?;

    // Commit workspace with deletion
    let commit_body = NewCommitBody {
        author: user.name.clone(),
        email: user.email.clone(),
        message: format!("Delete file {}", resource.path.display()),
    };

    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name).await?;

    log::debug!(
        "file::delete workspace commit ✅ success! commit {:?}",
        commit
    );

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
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    // Parse the resource (branch/commit/path)
    let resource = parse_resource(&req, &repo)?;

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

    // Verify source file exists
    if repositories::entries::get_file(&repo, &commit, &source_path)?.is_none() {
        return Err(OxenHttpError::NotFound);
    }

    // Check if new_path already exists (file OR directory)
    if repositories::tree::get_node_by_path(&repo, &commit, &new_path)?.is_some() {
        return Err(OxenHttpError::BadRequest(
            "new_path already exists in the repository".into(),
        ));
    }

    log::debug!("file::mv creating workspace for commit: {commit}");
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    // Stage the move
    log::debug!("file::mv moving {source_path:?} to {new_path:?}");
    repositories::workspaces::files::mv(&workspace, &source_path, &new_path)?;

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
    let resource: PathBuf = PathBuf::from(req.match_info().query("resource"));

    let mut resource_components = resource.components();
    let branch_name = resource_components
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .unwrap_or("main".to_string());
    let path_string = resource_components
        .collect::<PathBuf>()
        .to_string_lossy()
        .to_string();
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

    process_and_add_files(repo, None, &files).await?;

    if !files.is_empty() {
        let user_ref = &files[0].user; // Use the user from the first file, since it's the same for all
        let commit_message = message.unwrap_or_else(|| "Initial commit".to_string());
        commit = Some(commits::commit_with_user(repo, &commit_message, user_ref)?);
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
                path: target_path.to_path_buf(),
                contents: temp_file.contents.clone(),
                user: user.clone(),
            }])
        }
        MultipartUploadMode::DirectoryFromFile => {
            let temp_file = take_single_file_part(file_parts)?;
            let normalized_target_dir =
                normalize_relative_upload_path(target_path, true, "target directory")?;
            let normalized_file_path =
                normalize_relative_upload_path(&temp_file.path, false, "uploaded file")?;
            Ok(vec![FileNew {
                path: normalized_target_dir.join(normalized_file_path),
                contents: temp_file.contents.clone(),
                user: user.clone(),
            }])
        }
        MultipartUploadMode::DirectoryFromFilesArray => {
            let normalized_target_dir =
                normalize_relative_upload_path(target_path, true, "target directory")?;
            files_array_parts
                .iter()
                .map(|temp_file| {
                    let normalized_file_path =
                        normalize_relative_upload_path(&temp_file.path, false, "uploaded file")?;
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

/// import files from hf/kaggle (create a workspace and commit)
pub async fn import(
    req: HttpRequest,
    body: web::Json<Value>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    // Resource must specify branch for committing the workspace
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;
    let directory = resource.path.clone();
    log::debug!("workspace::files::import_file Got directory: {directory:?}");

    // commit info
    let author = req.headers().get("oxen-commit-author");
    let email = req.headers().get("oxen-commit-email");
    let message = req.headers().get("oxen-commit-message");

    log::debug!(
        "file::import commit info author:{:?}, email:{:?}, message:{:?}",
        author,
        email,
        message
    );

    // Make sure the resource path is not already a file
    let node = repositories::tree::get_node_by_path(&repo, &commit, &resource.path)?;
    if node.is_some() && node.unwrap().is_file() {
        return Err(OxenHttpError::BasicError(
            format!(
                "Target path must be a directory: {}",
                resource.path.display()
            )
            .into(),
        ));
    }

    // Create temporary workspace
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    log::debug!("workspace::files::import_file workspace created!");

    // extract auth key from req body
    let auth = body
        .get("headers")
        .and_then(|headers| headers.as_object())
        .and_then(|map| map.get("Authorization"))
        .and_then(|auth| auth.as_str())
        .unwrap_or_default();

    let download_url = body
        .get("download_url")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    // Validate URL domain
    let url_parsed = url::Url::parse(download_url)
        .map_err(|_| OxenHttpError::BadRequest("Invalid URL".into()))?;
    let domain = url_parsed
        .domain()
        .ok_or_else(|| OxenHttpError::BadRequest("Invalid URL domain".into()))?;
    if !ALLOWED_IMPORT_DOMAINS.iter().any(|&d| domain.ends_with(d)) {
        return Err(OxenHttpError::BadRequest("URL domain not allowed".into()));
    }

    // parse filename from the given url
    let filename = if url_parsed.domain() == Some("huggingface.co") {
        url_parsed.path_segments().and_then(|segments| {
            let segments: Vec<_> = segments.collect();
            if segments.len() >= 2 {
                let last_two = &segments[segments.len() - 2..];
                Some(format!("{}_{}", last_two[0], last_two[1]))
            } else {
                None
            }
        })
    } else {
        url_parsed
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .map(|s| s.to_string())
    }
    .ok_or_else(|| OxenHttpError::BadRequest("Invalid filename in URL".into()))?;

    // download and save the file into the workspace
    repositories::workspaces::files::import(download_url, auth, directory, filename, &workspace)
        .await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: author.map_or("".to_string(), |a| a.to_str().unwrap().to_string()),
        email: email.map_or("".to_string(), |e| e.to_str().unwrap().to_string()),
        message: message.map_or(
            format!("Import files to {}", &resource.path.to_string_lossy()),
            |m| m.to_str().unwrap().to_string(),
        ),
    };

    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name).await?;
    log::debug!("workspace::commit ✅ success! commit {:?}", commit);

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

// Helper function to extract authenticated user from bearer token
fn get_authenticated_user(req: &HttpRequest) -> Result<Option<User>, OxenHttpError> {
    // Extract bearer token from Authorization header
    let auth_header = req.headers().get("authorization");

    if let Some(auth_value) = auth_header {
        if let Ok(auth_str) = auth_value.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                let app_data = app_data(req)?;

                log::debug!(
                    "Attempting to validate bearer token: {}...",
                    &token[..std::cmp::min(20, token.len())]
                );
                log::debug!("AccessKeyManager path: {:?}", &app_data.path);

                match AccessKeyManager::new_read_only(&app_data.path) {
                    Ok(keygen) => {
                        log::debug!("AccessKeyManager created successfully");
                        match keygen.get_claim(token) {
                            Ok(Some(claim)) => {
                                log::debug!(
                                    "Token validated successfully for user: {}",
                                    claim.name()
                                );
                                return Ok(Some(User {
                                    name: claim.name().to_string(),
                                    email: claim.email().to_string(),
                                }));
                            }
                            Ok(None) => {
                                log::debug!("Token validation returned None");
                            }
                            Err(e) => {
                                log::debug!("Token validation error: {:?}", e);
                            }
                        }
                    }
                    Err(err) => {
                        log::debug!("AccessKeyManager creation failed: {:?}", err);
                        // Treat missing keys DB as "no authentication configured" instead of crashing
                    }
                }
            } else {
                log::debug!("Authorization header does not start with 'Bearer '");
            }
        } else {
            log::debug!("Could not parse authorization header as string");
        }
    } else {
        log::debug!("No authorization header found");
    }

    Ok(None)
}

fn normalize_relative_upload_path(
    path: &Path,
    allow_empty: bool,
    path_label: &str,
) -> Result<PathBuf, OxenHttpError> {
    if path.is_absolute() {
        return Err(OxenHttpError::BadRequest(
            format!("Invalid {path_label}: absolute paths are not allowed").into(),
        ));
    }

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(OxenHttpError::BadRequest(
                    format!(
                        "Invalid {path_label}: path traversal is not allowed: {}",
                        path.display()
                    )
                    .into(),
                ));
            }
        }
    }

    if !allow_empty && normalized.as_os_str().is_empty() {
        return Err(OxenHttpError::BadRequest(
            format!("Invalid {path_label}: path cannot be empty").into(),
        ));
    }

    Ok(normalized)
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
    files: &[FileNew],
) -> Result<(), OxenError> {
    if !files.is_empty() {
        log::debug!("repositories::create files: {:?}", files.len());
        for file in files {
            let path = &file.path;
            let contents = &file.contents;

            let filepath = if let Some(ws) = workspace {
                ws.dir().join(path)
            } else {
                repo.path.join(path)
            };

            if let Some(parent) = filepath.parent()
                && !parent.exists()
            {
                util::fs::create_dir_all(parent)?;
            }

            match contents {
                FileContents::Text(text) => {
                    util::fs::write(&filepath, text.as_bytes())?;
                }
                FileContents::Binary(bytes) => {
                    util::fs::write(&filepath, bytes)?;
                }
            }

            // Add the file to staging
            if let Some(ws) = workspace {
                repositories::workspaces::files::add(ws, &filepath).await?;
            } else {
                repositories::add(repo, &filepath).await?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ensure_no_file_ancestors_in_tree, normalize_relative_upload_path};
    use crate::errors::OxenHttpError;
    use crate::test;
    use std::path::{Path, PathBuf};

    use actix_multipart_test::MultiPartFormDataBuilder;
    use actix_web::http::header;
    use actix_web::{App, body, web};
    use liboxen::view::CommitResponse;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    use crate::app_data::OxenAppData;
    use crate::controllers;

    #[actix_web::test]
    async fn test_controllers_file_put() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        util::fs::write_to_path(&hello_file, "Updated Content!")?;
        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(
            hello_file,   // First argument: Path to the actual file on disk
            "files[]",    // Second argument: Field name (as expected by your server)
            "text/plain", // Content type
            "hello.txt",  // Filename for the multipart form
        );
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        multipart_form_data_builder.with_text("message", "some_message");
        let (header, body) = multipart_form_data_builder.build();
        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data");
        let req = actix_web::test::TestRequest::put()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "data")
            .param("repo_name", repo_name);

        let req = req.insert_header(header).set_payload(body).to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        // Check that the file was updated
        let entry =
            repositories::entries::get_file(&repo, &resp.commit, PathBuf::from("data/hello.txt"))?
                .unwrap();
        let version_store = repo.version_store()?;
        let uploaded_content = version_store.get_version(&entry.hash().to_string()).await?;
        assert_eq!(
            String::from_utf8(uploaded_content).unwrap(),
            "Updated Content!"
        );

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_get_exposes_content_length() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Get-Headers";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        let file_content = "Hello";
        util::fs::write_to_path(&hello_file, file_content)?;
        repositories::add(&repo, &hello_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let uri = format!("/oxen/{namespace}/{repo_name}/file/main/data/hello.txt");
        let req = actix_web::test::TestRequest::get()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::get().to(controllers::file::get),
                ),
        )
        .await;

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

    #[actix_web::test]
    async fn test_controllers_file_put_single_file_to_full_resource_path() -> Result<(), OxenError>
    {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Full-Path-Put";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let readme_file = repo.path.join("README.md");
        util::fs::write_to_path(&readme_file, "Initial commit")?;
        repositories::add(&repo, &readme_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let upload_file = repo.path.join("hero.md");
        util::fs::write_to_path(&upload_file, "# Hero Content")?;

        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(upload_file, "file", "text/markdown", "hero.md");
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        multipart_form_data_builder.with_text("message", "add hero");
        let (header, body) = multipart_form_data_builder.build();

        let put_uri = format!("/oxen/{namespace}/{repo_name}/file/main/pages/home/hero.md");
        let put_req = actix_web::test::TestRequest::put()
            .uri(&put_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/pages/home/hero.md")
            .param("repo_name", repo_name)
            .insert_header(header)
            .set_payload(body)
            .to_request();

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

        let put_resp = actix_web::test::call_service(&app, put_req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::OK);
        let put_body = body::to_bytes(put_resp.into_body()).await.unwrap();
        let put_body = std::str::from_utf8(&put_body).unwrap();
        let put_resp: CommitResponse = serde_json::from_str(put_body)?;
        assert!(!put_resp.commit.id.is_empty());

        let get_uri = format!("/oxen/{namespace}/{repo_name}/file/main/pages/home/hero.md");
        let get_req = actix_web::test::TestRequest::get()
            .uri(&get_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/pages/home/hero.md")
            .param("repo_name", repo_name)
            .to_request();

        let get_resp = actix_web::test::call_service(&app, get_req).await;
        assert_eq!(get_resp.status(), actix_web::http::StatusCode::OK);
        let body = actix_http::body::to_bytes(get_resp.into_body())
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "# Hero Content");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_import() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let author = "test_user";
        let email = "ox@oxen.ai";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        util::fs::create_dir_all(repo.path.join("data"))?;
        let hello_file = repo.path.join("data/hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let uri = format!("/oxen/{namespace}/{repo_name}/file/import/main/data");

        // import a file from oxen for testing
        let body = serde_json::json!({"download_url": "https://hub.oxen.ai/api/repos/datasets/GettingStarted/file/main/tables/cats_vs_dogs.tsv"});

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("repo_name", repo_name)
            .insert_header(("oxen-commit-author", author))
            .insert_header(("oxen-commit-email", email))
            .set_json(&body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/import/{resource:.*}",
                    web::post().to(controllers::file::import),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        let entry = repositories::entries::get_file(
            &repo,
            &resp.commit,
            PathBuf::from("data/cats_vs_dogs.tsv"),
        )?
        .unwrap();
        let version_store = repo.version_store()?;
        let version_path = version_store
            .get_version_path(&entry.hash().to_string())
            .await?;
        assert!(version_path.exists());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_put_rejects_upload_beneath_existing_file()
    -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-File-Ancestor-Put";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let existing_file = repo.path.join("pages/home/hero.md");
        util::fs::create_dir_all(existing_file.parent().unwrap())?;
        util::fs::write_to_path(&existing_file, "# Existing Hero")?;
        repositories::add(&repo, &existing_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let upload_file = repo.path.join("image.png");
        util::fs::write_to_path(&upload_file, "image-bytes")?;

        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(upload_file, "file", "image/png", "image.png");
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        multipart_form_data_builder.with_text("message", "add image");
        let (header, body) = multipart_form_data_builder.build();

        let put_uri =
            format!("/oxen/{namespace}/{repo_name}/file/main/pages/home/hero.md/image.png");
        let put_req = actix_web::test::TestRequest::put()
            .uri(&put_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/pages/home/hero.md/image.png")
            .param("repo_name", repo_name)
            .insert_header(header)
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let put_resp = actix_web::test::call_service(&app, put_req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_put_empty_repo_preserves_commit_message() -> Result<(), OxenError>
    {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Empty-Repo-Put";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let upload_file = repo.path.join("hero.md");
        util::fs::write_to_path(&upload_file, "# Hero Content")?;

        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(upload_file, "file", "text/markdown", "hero.md");
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        multipart_form_data_builder.with_text("message", "first upload message");
        let (header, body) = multipart_form_data_builder.build();

        let put_uri = format!("/oxen/{namespace}/{repo_name}/file/first-upload-branch/hero.md");
        let put_req = actix_web::test::TestRequest::put()
            .uri(&put_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "first-upload-branch/hero.md")
            .param("repo_name", repo_name)
            .insert_header(header)
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let put_resp = actix_web::test::call_service(&app, put_req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::OK);
        let put_body = body::to_bytes(put_resp.into_body()).await.unwrap();
        let put_body = std::str::from_utf8(&put_body).unwrap();
        let put_resp: CommitResponse = serde_json::from_str(put_body)?;

        assert_eq!(put_resp.commit.message, "first upload message");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_put_files_array_to_directory() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Dir-Put";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let readme_file = repo.path.join("README.md");
        util::fs::write_to_path(&readme_file, "Initial commit")?;
        repositories::add(&repo, &readme_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let upload_file = repo.path.join("hero.md");
        util::fs::write_to_path(&upload_file, "# Hero Content")?;

        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(upload_file, "files[]", "text/markdown", "hero.md");
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        multipart_form_data_builder.with_text("message", "add hero");
        let (header, body) = multipart_form_data_builder.build();

        let put_uri = format!("/oxen/{namespace}/{repo_name}/file/main/pages/home");
        let put_req = actix_web::test::TestRequest::put()
            .uri(&put_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/pages/home")
            .param("repo_name", repo_name)
            .insert_header(header)
            .set_payload(body)
            .to_request();

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

        let put_resp = actix_web::test::call_service(&app, put_req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::OK);

        let get_uri = format!("/oxen/{namespace}/{repo_name}/file/main/pages/home/hero.md");
        let get_req = actix_web::test::TestRequest::get()
            .uri(&get_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/pages/home/hero.md")
            .param("repo_name", repo_name)
            .to_request();

        let get_resp = actix_web::test::call_service(&app, get_req).await;
        assert_eq!(get_resp.status(), actix_web::http::StatusCode::OK);
        let body = actix_http::body::to_bytes(get_resp.into_body())
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "# Hero Content");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_put_file_field_to_existing_directory() -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Compat-Dir-Put";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let existing_dir = repo.path.join("data");
        util::fs::create_dir_all(&existing_dir)?;
        let existing_file = existing_dir.join("existing.txt");
        util::fs::write_to_path(&existing_file, "existing")?;
        let readme_file = repo.path.join("README.md");
        util::fs::write_to_path(&readme_file, "Initial commit")?;
        repositories::add(&repo, &readme_file).await?;
        repositories::add(&repo, &existing_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let upload_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&upload_file, "Hello from file field")?;

        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(upload_file, "file", "text/plain", "hello.txt");
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        multipart_form_data_builder.with_text("message", "add hello");
        let (header, body) = multipart_form_data_builder.build();

        let put_uri = format!("/oxen/{namespace}/{repo_name}/file/main/data");
        let put_req = actix_web::test::TestRequest::put()
            .uri(&put_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/data")
            .param("repo_name", repo_name)
            .insert_header(header)
            .set_payload(body)
            .to_request();

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

        let put_resp = actix_web::test::call_service(&app, put_req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::OK);

        let get_uri = format!("/oxen/{namespace}/{repo_name}/file/main/data/hello.txt");
        let get_req = actix_web::test::TestRequest::get()
            .uri(&get_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/data/hello.txt")
            .param("repo_name", repo_name)
            .to_request();

        let get_resp = actix_web::test::call_service(&app, get_req).await;
        assert_eq!(get_resp.status(), actix_web::http::StatusCode::OK);
        let body = actix_http::body::to_bytes(get_resp.into_body())
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "Hello from file field");

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_put_ambiguous_payload_returns_bad_request()
    -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Ambiguous-Put";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let readme_file = repo.path.join("README.md");
        util::fs::write_to_path(&readme_file, "Initial commit")?;
        repositories::add(&repo, &readme_file).await?;
        let _commit = repositories::commit(&repo, "First commit")?;

        let upload_file = repo.path.join("hero.md");
        util::fs::write_to_path(&upload_file, "# Hero Content")?;

        let mut multipart_form_data_builder = MultiPartFormDataBuilder::new();
        multipart_form_data_builder.with_file(
            upload_file.clone(),
            "file",
            "text/markdown",
            "hero.md",
        );
        multipart_form_data_builder.with_file(upload_file, "files[]", "text/markdown", "hero.md");
        multipart_form_data_builder.with_text("name", "some_name");
        multipart_form_data_builder.with_text("email", "some_email");
        multipart_form_data_builder.with_text("message", "add hero");
        let (header, body) = multipart_form_data_builder.build();

        let put_uri = format!("/oxen/{namespace}/{repo_name}/file/main/pages/home/hero.md");
        let put_req = actix_web::test::TestRequest::put()
            .uri(&put_uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .param("namespace", namespace)
            .param("resource", "main/pages/home/hero.md")
            .param("repo_name", repo_name)
            .insert_header(header)
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/file/{resource:.*}",
                    web::put().to(controllers::file::put),
                ),
        )
        .await;

        let put_resp = actix_web::test::call_service(&app, put_req).await;
        assert_eq!(put_resp.status(), actix_web::http::StatusCode::BAD_REQUEST);

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[test]
    fn test_normalize_relative_upload_path_collapses_current_dir_components() {
        let normalized =
            normalize_relative_upload_path(Path::new("./pages/./home"), true, "target directory")
                .unwrap();

        assert_eq!(normalized, PathBuf::from("pages/home"));
    }

    #[test]
    fn test_normalize_relative_upload_path_rejects_parent_dir_components() {
        let err =
            normalize_relative_upload_path(Path::new("../../outside.txt"), false, "uploaded file")
                .unwrap_err();

        assert!(matches!(err, OxenHttpError::BadRequest(_)));
    }

    #[test]
    fn test_normalize_relative_upload_path_rejects_absolute_paths() {
        let err =
            normalize_relative_upload_path(Path::new("/tmp/outside.txt"), false, "uploaded file")
                .unwrap_err();

        assert!(matches!(err, OxenHttpError::BadRequest(_)));
    }

    #[actix_web::test]
    async fn test_ensure_no_file_ancestors_in_tree_rejects_existing_file_ancestor()
    -> Result<(), OxenError> {
        liboxen::test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-File-Ancestor-Helper";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let existing_file = repo.path.join("pages/home/hero.md");
        util::fs::create_dir_all(existing_file.parent().unwrap())?;
        util::fs::write_to_path(&existing_file, "# Existing Hero")?;
        repositories::add(&repo, &existing_file).await?;
        let commit = repositories::commit(&repo, "First commit")?;

        let err = ensure_no_file_ancestors_in_tree(
            &repo,
            &commit,
            Path::new("pages/home/hero.md/image.png"),
            Path::new("pages/home/hero.md/image.png"),
        )
        .unwrap_err();

        assert!(matches!(err, OxenHttpError::BadRequest(_)));

        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
