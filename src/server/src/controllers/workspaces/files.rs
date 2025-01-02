use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_files::NamedFile;

use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::Workspace;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::{FilePathsResponse, StatusMessage};

use actix_web::{web, HttpRequest, HttpResponse};

use actix_multipart::Multipart;
use actix_web::Error;
use flate2::read::GzDecoder;
use futures::StreamExt;
use futures_util::TryStreamExt as _;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};

const BUFFER_SIZE_THRESHOLD: usize = 262144; // 256kb

pub async fn get(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let workspace = repositories::workspaces::get(&repo, workspace_id)?;
    let path = path_param(&req, "path")?;

    // The path in a workspace context is just the working path of the workspace repo
    let path = workspace.workspace_repo.path.join(path);

    log::debug!("got workspace file path {:?}", path);

    // TODO: This probably isn't the best place for the resize logic
    let img_resize = query.into_inner();
    if img_resize.width.is_some() || img_resize.height.is_some() {
        let resized_path = util::fs::resized_path_for_staged_entry(
            repo,
            &path,
            img_resize.width,
            img_resize.height,
        )?;

        util::fs::resize_cache_image(&path, &resized_path, img_resize)?;
        return Ok(NamedFile::open(resized_path)?);
    }
    Ok(NamedFile::open(path)?)
}

pub async fn add(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let directory = PathBuf::from(path_param(&req, "path")?);

    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    log::debug!("add_file directory {:?}", directory);

    let files = save_parts(&workspace, &directory, payload).await?;
    let mut ret_files = vec![];

    for file in files.iter() {
        log::debug!("add_file file {:?}", file);
        let path = repositories::workspaces::files::add(&workspace, file)?;
        log::debug!("add_file ✅ success! staged file {:?}", path);
        ret_files.push(path);
    }
    Ok(HttpResponse::Ok().json(FilePathsResponse {
        status: StatusMessage::resource_created(),
        paths: ret_files,
    }))
}

pub async fn add_stream(
    req: HttpRequest,
    mut payload: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    // whether if file chunk is compressed
    let is_gzip_encoded = req
        .headers()
        .get("Content-Encoding")
        .map_or(false, |v| v == "gzip");

    let filename = req
        .headers()
        .get("X-Filename")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| OxenHttpError::BadRequest("Missing X-Filename header".into()))?;

    let total_size = req
        .headers()
        .get("X-Total-Size")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| {
            OxenHttpError::BadRequest("Missing or invalid X-Total-Size header".into())
        })?;

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let directory = PathBuf::from(path_param(&req, "path")?);

    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    log::debug!("workspace::files::add_stream Got uploaded file name: {filename:?}");

    let workspace_dir = workspace.dir();

    log::debug!("workspace::files::add_stream Got workspace dir: {workspace_dir:?}");

    let full_dir = workspace_dir.join(directory);

    log::debug!("workspace::files::add_stream Got full dir: {full_dir:?}");

    if !full_dir.exists() {
        std::fs::create_dir_all(&full_dir)?;
    }

    let filepath = full_dir.join(filename);

    log::debug!("workspace::files::add_stream Got filepath: {:?}", filepath);

    let mut files = vec![];

    let bytes_written = if filepath.exists() {
        std::fs::metadata(&filepath)?.len()
    } else {
        0
    };

    log::debug!(
        "workspace::files::add_stream file has written bytes: {:?}",
        bytes_written
    );
    let mut buffer = web::BytesMut::new();

    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|_| OxenHttpError::BadRequest("Error reading payload".into()))?;

        // check if received eof signal
        if chunk.len() == 1 && chunk[0] == 0 {
            // validate file size match
            if bytes_written == total_size {
                files.push(filepath.clone());

                let path = repositories::workspaces::files::add(&workspace, filepath)?;
                log::debug!("add_stream ✅ success! staged file {:?}", path);

                return Ok(HttpResponse::Ok().json(FilePathsResponse {
                    status: StatusMessage::resource_created(),
                    paths: files,
                }));
            } else {
                log::error!(
                    "Upload stream incomplete. Expected {} bytes but received {} bytes",
                    total_size,
                    bytes_written
                );
                return Ok(HttpResponse::InternalServerError()
                    .json(StatusMessage::internal_server_error()));
            }
        } else {
            // not eof, save stream to file
            let processed_chunk = if is_gzip_encoded {
                let mut decoder = GzDecoder::new(&chunk[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed).map_err(|e| {
                    OxenHttpError::BadRequest(
                        format!("Failed to decompress gzip data: {}", e).into(),
                    )
                })?;
                decompressed
            } else {
                chunk.to_vec()
            };
            buffer.extend_from_slice(&processed_chunk);

            if buffer.len() > BUFFER_SIZE_THRESHOLD {
                save_stream(&filepath, buffer.split().freeze().to_vec()).await?;
            }
        }
    }

    if !buffer.is_empty() {
        save_stream(&filepath, buffer.freeze().to_vec()).await?;
    }

    files.push(filepath.clone());

    Ok(HttpResponse::Ok().json(FilePathsResponse {
        status: StatusMessage::resource_created(),
        paths: files,
    }))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let user_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    let workspace = repositories::workspaces::get(&repo, user_id)?;

    // This may not be in the commit if it's added, so have to parse tabular-ness from the path.
    if util::fs::is_tabular(&path) {
        repositories::workspaces::data_frames::restore(&repo, &workspace, &path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else if repositories::workspaces::files::exists(&workspace, &path)? {
        repositories::workspaces::files::delete(&workspace, &path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
    }
}

async fn save_parts(
    workspace: &Workspace,
    directory: &Path,
    mut payload: Multipart,
) -> Result<Vec<PathBuf>, Error> {
    let mut files: Vec<PathBuf> = vec![];

    // iterate over multipart stream
    while let Some(mut field) = payload.try_next().await? {
        // A multipart/form-data stream has to contain `content_disposition`
        let Some(content_disposition) = field.content_disposition() else {
            continue;
        };

        log::debug!(
            "workspace::files::save_parts content_disposition.get_name() {:?}",
            content_disposition.get_name()
        );

        // Filter to process only fields with the name "file[]" or "file"
        // (the old client is sending "file" instead of "file[]", but "file[]" makes sense for more than 1 file)
        if let Some(name) = content_disposition.get_name() {
            if "file[]" == name || "file" == name {
                let upload_filename = content_disposition.get_filename().map_or_else(
                    || uuid::Uuid::new_v4().to_string(),
                    sanitize_filename::sanitize,
                );

                log::debug!(
                    "workspace::files::save_parts Got uploaded file name: {upload_filename:?}"
                );

                let workspace_dir = workspace.dir();

                log::debug!("workspace::files::save_parts Got workspace dir: {workspace_dir:?}");

                let full_dir = workspace_dir.join(directory);

                log::debug!("workspace::files::save_parts Got full dir: {full_dir:?}");

                if !full_dir.exists() {
                    std::fs::create_dir_all(&full_dir)?;
                }

                // Need copy to pass to thread and return the name
                let filepath = full_dir.join(&upload_filename);
                let filepath_cpy = filepath.clone();
                log::debug!(
                    "workspace::files::save_parts writing file to {:?}",
                    filepath
                );

                // File::create is blocking operation, use threadpool
                let mut f = web::block(|| std::fs::File::create(filepath)).await??;

                // Field in turn is stream of *Bytes* object
                while let Some(chunk) = field.try_next().await? {
                    // filesystem operations are blocking, we have to use threadpool
                    f = web::block(move || f.write_all(&chunk).map(|_| f)).await??;
                }

                files.push(filepath_cpy);
            }
        }
    }

    Ok(files)
}

async fn save_stream(filepath: &PathBuf, chunk: Vec<u8>) -> Result<&PathBuf, Error> {
    log::debug!(
        "workspace::files::save_stream writing {} bytes to file",
        chunk.len()
    );

    let filepath_cpy = filepath.clone();

    let mut file = web::block(move || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(filepath_cpy)
    })
    .await??;

    log::debug!("workspace::files::save_stream is writing file");

    web::block(move || file.write_all(&chunk).map(|_| file)).await??;

    Ok(filepath)
}
