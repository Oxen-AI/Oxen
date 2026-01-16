use crate::errors::OxenHttpError;
use crate::helpers::{create_user_from_options, get_repo};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::core::staged::with_staged_db_manager;
use liboxen::error::OxenError;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::file::{FileContents, FileNew, TempFileNew};
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::metadata::metadata_video::VideoThumbnail;
use liboxen::model::Commit;
use liboxen::repositories::{self, branches};
use liboxen::util;
use liboxen::view::{CommitResponse, StatusMessage};

use actix_multipart::Multipart;
use actix_web::{web, HttpRequest, HttpResponse};
use futures_util::TryStreamExt as _;
use liboxen::repositories::commits;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;
use utoipa::ToSchema;

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
    security( ("api_key" = []) ),
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
        Some(ws) => with_staged_db_manager(staged_repo, |staged_db_manager| {
            // Try staged DB first
            if let Some(staged_node) = staged_db_manager.read_from_staged_db(&path)? {
                let file_node = match staged_node.node.node {
                    EMerkleTreeNode::File(f) => Ok(f),
                    _ => Err(OxenError::basic_str(
                        "Only single file download is supported",
                    )),
                }?;
                return Ok(file_node);
            }

            // Fall back to commit tree using workspace's commit
            let commit = &ws.commit;
            let file_node = repositories::tree::get_file_by_path(base_repo, commit, &path)?
                .ok_or(OxenError::path_does_not_exist(path.clone()))?;
            Ok(file_node)
        }),
        None => {
            let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
            let file_node = repositories::tree::get_file_by_path(base_repo, &commit, &path)?
                .ok_or(OxenError::path_does_not_exist(path.clone()))?;
            Ok(file_node)
        }
    }?;

    let file_hash = entry.hash();
    let hash_str = file_hash.to_string();
    let mime_type = entry.mime_type();
    let last_commit_id = entry.last_commit_id().to_string();
    let version_path = version_store.get_version_path(&hash_str)?;

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

        let file_stream = util::fs::handle_image_resize(
            Arc::clone(&version_store),
            hash_str.clone(),
            &path,
            &version_path,
            img_resize,
        )
        .await?;

        return Ok(HttpResponse::Ok()
            .content_type(mime_type)
            .insert_header(("oxen-revision-id", last_commit_id.as_str()))
            .streaming(file_stream));
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

        let thumbnail_path = util::fs::handle_video_thumbnail(
            Arc::clone(&version_store),
            hash_str,
            &path,
            &version_path,
            video_thumbnail,
        )?;
        log::debug!("In the thumbnail cache! {thumbnail_path:?}");

        // Generate stream for the thumbnail (always JPEG)
        let file = File::open(&thumbnail_path).await?;
        let reader = BufReader::new(file);
        let stream = ReaderStream::new(reader);

        return Ok(HttpResponse::Ok()
            .content_type("image/jpeg")
            .insert_header(("oxen-revision-id", last_commit_id.as_str()))
            .streaming(stream));
    }

    log::debug!("did not hit the resize or thumbnail cache");

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

    // If there's no head commit, handle initial upload
    if repositories::commits::head_commit_maybe(&repo)?.is_none() {
        return handle_initial_put_empty_repo(req, payload, &repo).await;
    }

    let resource = match parse_resource(&req, &repo) {
        Ok(res) => res,
        Err(parse_err) => {
            return Err(parse_err);
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

/// Delete file
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/file/{resource}",
    tag = "Files",
    security( ("api_key" = []) ),
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
pub async fn delete(
    req: HttpRequest,
    payload: Multipart,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("file::delete path {:?}", req.path());
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
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.clone().ok_or(OxenHttpError::NotFound)?;
    let path = resource.path;

    // Get the commit info from the payload
    let (name, email, message, _temp_files) = parse_multipart_fields_for_repo(payload).await?;

    log::debug!("file::delete creating workspace for commit: {commit}");
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

    // Stage the path as removed
    log::debug!("file::delete staging path {path:?}");
    repositories::workspaces::files::rm(&workspace, &path).await?;

    // Commit workspace
    let commit_body = NewCommitBody {
        author: name.clone().unwrap_or("".to_string()),
        email: email.clone().unwrap_or("".to_string()),
        message: message
            .clone()
            .unwrap_or(format!("Remove {}", &path.to_string_lossy())),
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
    security( ("api_key" = []) ),
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
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let commit = resource.commit.clone().ok_or(OxenHttpError::NotFound)?;
    let source_path = resource.path;

    // Parse the request body
    let body: FileMoveBody = serde_json::from_str(&body)?;

    // Validate new_path
    if body.new_path.is_empty() {
        return Err(OxenHttpError::BadRequest("new_path cannot be empty".into()));
    }

    let new_path = PathBuf::from(&body.new_path);

    // Verify source file exists
    if repositories::entries::get_file(&repo, &commit, &source_path)?.is_none() {
        return Err(OxenHttpError::NotFound);
    }

    // Check if new_path already exists
    if repositories::entries::get_file(&repo, &commit, &new_path)?.is_some() {
        return Err(OxenHttpError::BadRequest(
            "new_path already exists in the repository".into(),
        ));
    }

    log::debug!("file::mv creating workspace for commit: {commit}");
    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;

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
    req: HttpRequest,
    payload: Multipart,
    repo: &liboxen::model::LocalRepository,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(req.match_info().query("resource"));

    let mut resource = resource.components();
    let branch_name = resource
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .unwrap_or("main".to_string());
    let path_string = resource.collect::<PathBuf>().to_string_lossy().to_string();
    let path = PathBuf::from(path_string);

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

    process_and_add_files(repo, None, path, files.clone()).await?;

    if !files.is_empty() {
        let user_ref = &files[0].user; // Use the user from the first file, since it's the same for all
        commit = Some(commits::commit_with_user(repo, "Initial commit", user_ref)?);
        branches::create(repo, &branch_name, &commit.as_ref().unwrap().id)?;
    }

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit: commit.unwrap(),
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
}
