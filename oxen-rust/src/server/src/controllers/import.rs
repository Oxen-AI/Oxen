use actix_multipart::Multipart;
use actix_web::{web, HttpRequest, HttpResponse};
use futures_util::TryStreamExt as _;
use serde::Deserialize;
use serde_json::Value;
use std::path::{Path, PathBuf};
use utoipa::ToSchema;

use crate::errors::OxenHttpError;
use crate::helpers::{create_user_from_options, get_repo};
use crate::params::{app_data, parse_resource, path_param};

use liboxen::core::v_latest::workspaces::files::decompress_zip;
use liboxen::error::OxenError;
use liboxen::model::file::TempFilePathNew;
use liboxen::model::NewCommitBody;
use liboxen::repositories;
use liboxen::view::{CommitResponse, StatusMessage};

const ALLOWED_IMPORT_DOMAINS: [&str; 3] = ["huggingface.co", "kaggle.com", "oxen.ai"];

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

/// Import file from URL
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/import/{resource}",
    tag = "Import",
    description = "Import a file from a remote URL (huggingface.co, kaggle.com, oxen.ai) and commit it to the repository.",
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

/// Upload zip archive
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/import/upload/{resource}",
    tag = "Import",
    description = "Upload and decompress a zip archive into the repository and commit the contents.",
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

    // If there's no head commit, handle initial upload
    if repositories::commits::head_commit_maybe(&repo)?.is_none() {
        return handle_initial_upload_zip_empty_repo(req, payload, &repo).await;
    }

    let resource = match parse_resource(&req, &repo) {
        Ok(res) => res,
        Err(parse_err) => {
            return Err(parse_err);
        }
    };

    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;
    let directory = resource.path.clone();
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;

    let workspace = repositories::workspaces::create_temporary(&repo, &commit)?;
    let workspace_path = workspace.dir();
    let (commit_message, name, email, temp_files) =
        parse_multipart_fields_for_upload_zip(payload, &workspace_path, &directory).await?;

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

async fn handle_initial_upload_zip_empty_repo(
    req: HttpRequest,
    payload: Multipart,
    repo: &liboxen::model::LocalRepository,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let resource: PathBuf = PathBuf::from(req.match_info().query("resource"));

    // Parse the resource for the path and branch name
    let mut resource = resource.components();
    let branch_name = resource
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .unwrap_or("main".to_string());
    let path_string = resource.collect::<PathBuf>().to_string_lossy().to_string();
    let path = PathBuf::from(path_string);

    // Unzip the files into the root dir
    let root_dir = repo.path.clone();

    // Parse the payload for the files and commit info
    let (message, name, email, temp_files) =
        parse_multipart_fields_for_upload_zip(payload, &root_dir, &path).await?;

    let user = create_user_from_options(name.clone(), email.clone())?;
    let commit_message = message.unwrap_or("Upload zip file".to_string());

    // Unzip the files and add

    for temp_file in temp_files {
        let files = decompress_zip(&temp_file.temp_file_path)?;
        repositories::add::add_all(repo, &files).await?;
    }

    // Commit the files
    let commit = repositories::commits::commit_with_user(repo, &commit_message, &user)?;

    // Create the branch
    repositories::branches::create(repo, &branch_name, &commit.id)?;

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}

async fn parse_multipart_fields_for_upload_zip(
    mut payload: Multipart,
    data_path: &Path,
    directory: &Path,
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

                let full_path = data_path.join(directory).join(&filename);

                // Create parent directories if they don't exist
                if let Some(parent) = full_path.parent() {
                    tokio::fs::create_dir_all(parent).await.map_err(|e| {
                        OxenHttpError::BadRequest(
                            format!("Failed to create directories: {e}").into(),
                        )
                    })?;
                }

                // Create the file in the workspace directory
                let mut file = tokio::fs::File::create(&full_path).await.map_err(|e| {
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

                fields_data.push((filename, full_path));
            }
            _ => {}
        }
    }

    for (_filename, temp_path) in fields_data {
        temp_files.push(TempFilePathNew {
            path: directory.to_path_buf(),
            temp_file_path: temp_path,
        });
    }

    Ok((commit_message, name, email, temp_files))
}

#[cfg(test)]
mod tests {

    use crate::app_data::OxenAppData;
    use crate::controllers;

    use liboxen::view::CommitResponse;
    use liboxen::{repositories, util};

    use liboxen::error::OxenError;

    use actix_web::{web, App};
    use std::path::PathBuf;

    #[actix_web::test]
    async fn test_controllers_file_import_tabular_file() -> Result<(), OxenError> {
        // We get duckdb errors on windows, so skip this test because it has a tabular file
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

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
                    web::post().to(controllers::import::import),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        let resp: CommitResponse = serde_json::from_str(body)?;
        assert_eq!(resp.status.status, "success");

        let entry =
            repositories::entries::get_file(&repo, &resp.commit, "data/cats_vs_dogs.tsv")?.unwrap();
        let version_store = repo.version_store()?;
        let version_path = version_store.get_version_path(&entry.hash().to_string())?;
        assert!(version_path.exists());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_file_import_text_file() -> Result<(), OxenError> {
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
                    web::post().to(controllers::import::import),
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
