use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::core::staged::staged_db_manager::with_staged_db_manager;
use liboxen::error::OxenError;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::file::{FileContents, FileNew, TempFileNew, TempFilePathNew};
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::{Commit, User};
use liboxen::repositories::{self, branches};
use liboxen::util;
use liboxen::view::{CommitResponse, StatusMessage};

use actix_multipart::Multipart;
use actix_web::{web, HttpRequest, HttpResponse};
use futures_util::TryStreamExt as _;
use liboxen::repositories::commits;
use serde::Deserialize;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;
use utoipa::ToSchema;

const ALLOWED_IMPORT_DOMAINS: [&str; 3] = ["huggingface.co", "kaggle.com", "oxen.ai"];

#[derive(ToSchema, Deserialize)]
#[schema(
    title = "FileUploadBody",
    description = "Body for uploading a file via multipart/form-data",
    example = json!({
        "file": "<binary data>", 
        "message": "Adding a picture of a cow",
        "name": "bessie",
        "email": "bessie@oxen.ai"
    })
)]
pub struct FileUploadBody {
    #[schema(value_type = String, format = Binary)]
    pub file: Vec<u8>,
    #[schema(example = "Adding a new image to the training set")]
    pub message: Option<String>,
    #[schema(example = "bessie")]
    pub name: Option<String>,
    #[schema(example = "bessie@oxen.ai")]
    pub email: Option<String>,
}

#[derive(ToSchema, Deserialize)]
#[schema(
    title = "ZipUploadBody",
    description = "Body for uploading a zip archive via multipart/form-data",
    example = json!({
        "file": "<binary zip data>", 
        "commit_message": "Importing full archive of grazing data",
        "name": "ox",
        "email": "ox@oxen.ai"
    })
)]
pub struct ZipUploadBody {
    #[schema(value_type = String, format = Binary)]
    pub file: Vec<u8>,
    #[schema(example = "Importing dataset archive")]
    pub commit_message: Option<String>,
    #[schema(example = "ox")]
    pub name: Option<String>,
    #[schema(example = "ox@oxen.ai")]
    pub email: Option<String>,
}

#[derive(ToSchema, Deserialize)]
#[schema(
    title = "ImportFileBody",
    description = "Body for importing a file from a URL",
    example = json!({
        "download_url": "https://huggingface.co/datasets/user/dataset/resolve/main/data.csv",
        "headers": {
            "Authorization": "Bearer <token>"
        }
    })
)]
pub struct ImportFileBody {
    #[schema(example = "https://huggingface.co/datasets/user/dataset/resolve/main/data.csv")]
    pub download_url: String,
    #[schema(value_type = Object, example = json!({"Authorization": "Bearer token"}))]
    pub headers: Option<Value>,
}

/// Download File
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/file/{resource}",
    tag = "Files",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Voice-Data"),
        ("resource" = String, Path, description = "Path to the file (including branch/commit info)", example = "main/audio/moo.wav"),
        ImgResize
    ),
    responses(
        (status = 200, description = "File content stream", content_type = "application/octet-stream", body = Vec<u8>),
        (status = 404, description = "File not found")
    )
)]
pub async fn get(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let version_store = repo.version_store()?;
    let resource = parse_resource(&req, &repo)?;
    let workspace_ref = resource.workspace.as_ref();

    let repo = if let Some(workspace) = workspace_ref {
        &workspace.workspace_repo
    } else {
        &repo
    };

    let path = resource.path.clone();

    // if resource is workspace, get file node from the staged db
    let entry = match workspace_ref {
        Some(_workspace_ref) => with_staged_db_manager(repo, |staged_db_manager| {
            let staged_node = staged_db_manager
                .read_from_staged_db(&path)?
                .ok_or_else(|| OxenError::basic_str("File not found in staged DB"))?;

            let file_node = match staged_node.node.node {
                EMerkleTreeNode::File(f) => Ok(f),
                _ => Err(OxenError::basic_str(
                    "Only single file download is supported",
                )),
            }?;
            Ok(file_node)
        }),
        None => {
            // Otherwise get file node from commit tree
            let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
            let file_node = repositories::tree::get_file_by_path(repo, &commit, &path)?
                .ok_or(OxenError::path_does_not_exist(path.clone()))?;
            Ok(file_node)
        }
    }?;

    let file_hash = entry.hash();
    let hash_str = file_hash.to_string();
    let mime_type = entry.mime_type();
    let last_commit_id = entry.last_commit_id().to_string();
    let version_path = version_store.get_version_path(&hash_str)?;

    // TODO: refactor out of here and check for type,
    // but seeing if it works to resize the image and cache it to disk if we have a resize query
    let img_resize = query.into_inner();
    if (img_resize.width.is_some() || img_resize.height.is_some())
        && mime_type.starts_with("image/")
    {
        log::debug!("img_resize {img_resize:?}");

        let resized_path = util::fs::handle_image_resize(
            Arc::clone(&version_store),
            hash_str,
            &path,
            &version_path,
            img_resize,
        )?;
        log::debug!("In the resize cache! {resized_path:?}");

        // Generate stream for the resized image
        let file = File::open(&resized_path).await?;
        let reader = BufReader::new(file);
        let stream = ReaderStream::new(reader);

        return Ok(HttpResponse::Ok()
            .content_type(mime_type)
            .insert_header(("oxen-revision-id", last_commit_id.as_str()))
            .streaming(stream));
    } else {
        log::debug!("did not hit the resize cache");
    }

    // Stream the file
    let stream = version_store.get_version_stream(&hash_str).await?;

    Ok(HttpResponse::Ok()
        .content_type(mime_type)
        .insert_header(("oxen-revision-id", last_commit_id.as_str()))
        .streaming(stream))
}

/// Put file
#[utoipa::path(
    put,
    path = "/api/repos/{namespace}/{repo_name}/file/{resource}",
    tag = "Files",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the file (including branch)", example = "main/train/n01440764/images/n01440764_10026.JPEG"),
    ),
    request_body(
        content_type = "multipart/form-data",
        content = FileUploadBody
    ),
    responses(
        (status = 200, description = "File committed successfully", body = CommitResponse),
        (status = 400, description = "Bad Request"),
        (status = 404, description = "Branch or path not found")
    )
)]
pub async fn put(
    req: HttpRequest,
    payload: Multipart,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::put path {:?}", req.path());
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    // Try to parse the resource (branch/commit/path). If the repo has no commits yet this will
    // fail, so fall back to an initial-upload helper.
    let resource = match parse_resource(&req, &repo) {
        Ok(res) => res,
        Err(parse_err) => {
            if repositories::commits::head_commit_maybe(&repo)?.is_none() {
                return handle_initial_put_empty_repo(req, payload, &repo).await;
            } else {
                return Err(parse_err);
            }
        }
    };

    // Resource must specify branch because we need to commit the workspace back to a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;
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

    let (name, email, message, temp_files) = parse_multipart_fields_for_repo(payload).await?;

    let user = create_user_from_options(name.clone(), email.clone())?;

    let mut files: Vec<FileNew> = vec![];
    for temp_file in temp_files {
        files.push(FileNew {
            path: temp_file.path,
            contents: temp_file.contents,
            user: user.clone(), // Clone the user for each file
        });
    }
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    process_and_add_files(
        &repo,
        Some(&workspace),
        resource.path.clone(),
        files.clone(),
    )
    .await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: name.clone().unwrap_or("".to_string()),
        email: email.clone().unwrap_or("".to_string()),
        message: message.clone().unwrap_or(format!(
            "Auto-commit files to {}",
            &resource.path.to_string_lossy()
        )),
    };

    let commit = repositories::workspaces::commit(&workspace, &commit_body, branch.name).await?;

    log::debug!("file::put workspace commit ✅ success! commit {commit:?}");

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

/// Upload zip archive
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/archive/{resource}",
    tag = "Files",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Wiki-Text"),
        ("resource" = String, Path, description = "Destination path (including branch)", example = "main/archive"),
    ),
    request_body(
        content_type = "multipart/form-data",
        content = ZipUploadBody
    ),
    responses(
        (status = 200, description = "Zip archive decompressed and committed", body = CommitResponse),
        (status = 400, description = "Bad Request")
    )
)]
pub async fn upload_zip(
    req: HttpRequest,
    payload: Multipart,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::upload_zip path {:?}", req.path());

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let resource = parse_resource(&req, &repo)?;
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let directory = resource.path.clone();
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;

    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;
    let (commit_message, name, email, temp_files) =
        parse_multipart_fields_for_upload_zip(payload, &workspace, directory).await?;

    let user = create_user_from_options(name.clone(), email.clone())?;

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

    let commit_message = commit_message.unwrap_or("Upload zip file".to_string());

    let commit = repositories::workspaces::files::upload_zip(
        &commit_message,
        &user,
        temp_files,
        &workspace,
        &branch,
    )
    .await?;
    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

// Helper: when the repository has no commits yet, accept the upload as the first commit on the
// default branch ("main").
async fn handle_initial_put_empty_repo(
    req: HttpRequest,
    payload: Multipart,
    repo: &liboxen::model::LocalRepository,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(req.match_info().query("resource"));
    let path_string = resource
        .components()
        .skip(1)
        .collect::<PathBuf>()
        .to_string_lossy()
        .to_string();

    let (name, email, _message, temp_files) = parse_multipart_fields_for_repo(payload).await?;

    let user = create_user_from_options(name.clone(), email.clone())?;

    // Convert temporary files to FileNew with the complete user information
    let mut files: Vec<FileNew> = vec![];
    for temp_file in temp_files {
        files.push(FileNew {
            path: temp_file.path,
            contents: temp_file.contents,
            user: user.clone(), // Clone the user for each file
        });
    }

    // If the user supplied files, add and commit them
    let mut commit: Option<Commit> = None;

    process_and_add_files(repo, None, PathBuf::from(&path_string), files.clone()).await?;

    if !files.is_empty() {
        let user_ref = &files[0].user; // Use the user from the first file, since it's the same for all
        commit = Some(commits::commit_with_user(repo, "Initial commit", user_ref)?);
        branches::create(repo, "main", &commit.as_ref().unwrap().id)?;
    }

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit: commit.unwrap(),
    }))
}

/// Import file from URL
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/import/{resource}",
    tag = "Files",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Common-Crawl"),
        ("resource" = String, Path, description = "Destination path (including branch)", example = "main/data"),
    ),
    request_body(
        content = ImportFileBody,
        description = "Import configuration",
        example = json!({
            "download_url": "https://huggingface.co/datasets/user/dataset/resolve/main/data.csv",
            "headers": {
                "Authorization": "Bearer <token>"
            }
        })
    ),
    responses(
        (status = 200, description = "File imported and committed", body = CommitResponse),
        (status = 400, description = "Bad Request / Invalid URL")
    )
)]
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

    log::debug!("file::import commit info author:{author:?}, email:{email:?}, message:{message:?}");

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
    log::debug!("workspace::commit ✅ success! commit {commit:?}");

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

async fn parse_multipart_fields_for_repo(
    mut payload: Multipart,
) -> actix_web::Result<
    (
        Option<String>,
        Option<String>,
        Option<String>,
        Vec<TempFileNew>,
    ),
    OxenHttpError,
> {
    let mut name: Option<String> = None;
    let mut email: Option<String> = None;
    let mut message: Option<String> = None;
    let mut temp_files: Vec<TempFileNew> = vec![];

    while let Some(mut field) = payload
        .try_next()
        .await
        .map_err(OxenHttpError::MultipartError)?
    {
        let disposition = field.content_disposition().ok_or(OxenHttpError::NotFound)?;
        let field_name = disposition
            .get_name()
            .ok_or(OxenHttpError::NotFound)?
            .to_string();

        match field_name.as_str() {
            "name" | "email" => {
                let mut bytes = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    bytes.extend_from_slice(&chunk);
                }
                let value = String::from_utf8(bytes)
                    .map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;

                if field_name == "name" {
                    name = Some(value);
                } else {
                    email = Some(value);
                }
            }
            "message" => {
                let mut bytes = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    bytes.extend_from_slice(&chunk);
                }
                let value = String::from_utf8(bytes)
                    .map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
                message = Some(value);
            }
            "files[]" | "file" => {
                let filename = disposition.get_filename().map_or_else(
                    || uuid::Uuid::new_v4().to_string(),
                    sanitize_filename::sanitize,
                );

                let mut contents = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    contents.extend_from_slice(&chunk);
                }

                temp_files.push(TempFileNew {
                    path: PathBuf::from(&filename),
                    contents: FileContents::Binary(contents),
                });
            }
            _ => {}
        }
    }

    Ok((name, email, message, temp_files))
}

async fn parse_multipart_fields_for_upload_zip(
    mut payload: Multipart,
    workspace: &liboxen::repositories::workspaces::TemporaryWorkspace,
    directory: PathBuf,
) -> actix_web::Result<
    (
        Option<String>,
        Option<String>,
        Option<String>,
        Vec<TempFilePathNew>,
    ),
    OxenHttpError,
> {
    let mut commit_message: Option<String> = None;
    let mut temp_files: Vec<TempFilePathNew> = vec![];
    let mut fields_data: Vec<(String, PathBuf)> = Vec::new();
    let mut name: Option<String> = None;
    let mut email: Option<String> = None;

    while let Some(mut field) = payload
        .try_next()
        .await
        .map_err(OxenHttpError::MultipartError)?
    {
        let disposition = field.content_disposition().ok_or(OxenHttpError::NotFound)?;
        let field_name = disposition
            .get_name()
            .ok_or(OxenHttpError::NotFound)?
            .to_string();

        match field_name.as_str() {
            "name" | "email" => {
                let mut bytes = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    bytes.extend_from_slice(&chunk);
                }
                let value = String::from_utf8(bytes)
                    .map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;

                if field_name == "name" {
                    name = Some(value);
                } else {
                    email = Some(value);
                }
            }
            "commit_message" => {
                let mut bytes = Vec::new();
                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    bytes.extend_from_slice(&chunk);
                }
                let value = String::from_utf8(bytes)
                    .map_err(|e| OxenHttpError::BadRequest(e.to_string().into()))?;
                commit_message = Some(value);
            }
            "files[]" | "file" => {
                let filename = disposition.get_filename().map_or_else(
                    || uuid::Uuid::new_v4().to_string(),
                    sanitize_filename::sanitize,
                );

                let workspace_path = workspace.dir().join(directory.clone()).join(&filename);

                // Create parent directories if they don't exist
                if let Some(parent) = workspace_path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|e| {
                        OxenHttpError::BadRequest(
                            format!("Failed to create directories: {e}").into(),
                        )
                    })?;
                }

                // Create the file in the workspace directory
                let mut file = tokio::fs::File::create(&workspace_path)
                    .await
                    .map_err(|e| {
                        OxenHttpError::BadRequest(format!("Failed to create file: {e}").into())
                    })?;

                while let Some(chunk) = field
                    .try_next()
                    .await
                    .map_err(OxenHttpError::MultipartError)?
                {
                    tokio::io::AsyncWriteExt::write_all(&mut file, &chunk)
                        .await
                        .map_err(|e| {
                            OxenHttpError::BadRequest(
                                format!("Failed to write to file: {e}").into(),
                            )
                        })?;
                }

                // Flush to ensure data is written
                tokio::io::AsyncWriteExt::flush(&mut file)
                    .await
                    .map_err(|e| {
                        OxenHttpError::BadRequest(format!("Failed to flush file: {e}").into())
                    })?;

                fields_data.push((filename, workspace_path));
            }
            _ => {}
        }
    }

    for (_filename, temp_path) in fields_data {
        temp_files.push(TempFilePathNew {
            path: directory.clone(),
            temp_file_path: temp_path,
        });
    }

    Ok((commit_message, name, email, temp_files))
}

// Helper function for user creation
fn create_user_from_options(
    name: Option<String>,
    email: Option<String>,
) -> actix_web::Result<User, OxenHttpError> {
    Ok(User {
        name: name.ok_or(OxenHttpError::BadRequest("Name is required".into()))?,
        email: email.ok_or(OxenHttpError::BadRequest("Email is required".into()))?,
    })
}

// Helper function for processing files and adding to repo/workspace
async fn process_and_add_files(
    repo: &liboxen::model::LocalRepository,
    workspace: Option<&liboxen::repositories::workspaces::TemporaryWorkspace>,
    base_path: PathBuf,
    files: Vec<FileNew>,
) -> Result<(), OxenError> {
    if !files.is_empty() {
        log::debug!("repositories::create files: {:?}", files.len());
        for file in files.clone() {
            let path = &file.path;
            let contents = &file.contents;

            let full_dir = if let Some(ws) = workspace {
                ws.dir().join(base_path.clone()) // Use workspace dir if provided
            } else {
                repo.path.join(base_path.clone()) // Use repo path if no workspace
            };

            if !full_dir.exists() {
                util::fs::create_dir_all(&full_dir)?;
            }

            let filepath = full_dir.join(path);

            match contents {
                FileContents::Text(text) => {
                    util::fs::write(&filepath, text.as_bytes())?;
                }
                FileContents::Binary(bytes) => {
                    util::fs::write(&filepath, bytes)?;
                }
            }

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
    use std::path::PathBuf;

    use actix_multipart_test::MultiPartFormDataBuilder;
    use actix_web::{web, App};
    use liboxen::view::CommitResponse;

    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_file_put() -> Result<(), OxenError> {
        test::init_test_env();
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
            "file",       // Second argument: Field name (as expected by your server)
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
    async fn test_controllers_file_import_tabular_file() -> Result<(), OxenError> {
        // We get duckdb errors on windows, so skip this test because it has a tabular file
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::init_test_env();
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
        let version_path = version_store.get_version_path(&entry.hash().to_string())?;
        assert!(version_path.exists());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_import_text_file() -> Result<(), OxenError> {
        test::init_test_env();
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

        let uri = format!("/oxen/{namespace}/{repo_name}/file/import/main/notebooks");

        // import a file from oxen for testing
        let body = serde_json::json!({"download_url": "https://hub.oxen.ai/api/repos/datasets/GettingStarted/file/main/notebooks/chat.py"});

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
            PathBuf::from("notebooks/chat.py"),
        )?
        .unwrap();
        let version_store = repo.version_store()?;
        let version_path = version_store.get_version_path(&entry.hash().to_string())?;
        assert!(version_path.exists());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }
}
