use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use liboxen::core;
use liboxen::core::staged::with_staged_db_manager;
use liboxen::error::OxenError;
use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::LocalRepository;
use liboxen::model::Workspace;
use liboxen::repositories;
use liboxen::util;
use liboxen::util::hasher;
use liboxen::view::{
    ErrorFileInfo, ErrorFilesResponse, FilePathsResponse, FileWithHash, StatusMessage,
    StatusMessageDescription,
};

use actix_web::{web, HttpRequest, HttpResponse};

use actix_multipart::Multipart;
use actix_web::Error;
use flate2::read::GzDecoder;
use futures_util::TryStreamExt as _;
use std::io::Read as StdRead;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;

pub async fn get(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let version_store = repo.version_store()?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Err(OxenHttpError::NotFound);
    };

    let path = path_param(&req, "path")?;
    log::debug!("got workspace file path {:?}", &path);

    // Get the file from the version store
    let file_node = with_staged_db_manager(&workspace.workspace_repo, |staged_db_manager| {
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
    })?;

    let file_hash = file_node.hash();
    let hash_str = file_hash.to_string();
    let mime_type = file_node.mime_type();
    let last_commit_id = file_node.last_commit_id().to_string();
    let version_path = version_store.get_version_path(&hash_str)?;
    log::debug!("got workspace file version path {:?}", &version_path);

    // TODO: This probably isn't the best place for the resize logic
    let img_resize = query.into_inner();
    if (img_resize.width.is_some() || img_resize.height.is_some())
        && mime_type.starts_with("image/")
    {
        log::debug!("img_resize {:?}", img_resize);

        let resized_path = util::fs::handle_image_resize(
            Arc::clone(&version_store),
            hash_str,
            &PathBuf::from(path),
            &version_path,
            img_resize,
        )?;
        log::debug!("In the resize cache! {:?}", resized_path);

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

pub async fn add(req: HttpRequest, payload: Multipart) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let directory = path_param(&req, "path")?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let version_store = repo.version_store()?;

    let (upload_files, err_files) = save_parts(payload, &repo).await?;
    log::debug!("Save multiparts found {} err_files", err_files.len());
    log::debug!(
        "Calling add version files from the core workspace logic with {} files",
        upload_files.len(),
    );

    let mut ret_files = vec![];
    for upload_file in upload_files {
        let file_name = upload_file.path.file_name().unwrap();
        let dst_path = PathBuf::from(&directory).join(file_name);
        let version_path = version_store.get_version_path(&upload_file.hash)?;

        let ret_file = match core::v_latest::workspaces::files::add_version_file_with_hash(
            &workspace,
            &version_path,
            &dst_path,
            &upload_file.hash,
        ) {
            Ok(ret_file) => ret_file,
            Err(e) => {
                log::error!("Error adding file {version_path:?}: {e:?}");
                continue;
            }
        };

        ret_files.push(ret_file);
        println!("add_file ✅ success! staged file {:?}", upload_file);
    }

    Ok(HttpResponse::Ok().json(FilePathsResponse {
        status: StatusMessage::resource_created(),
        paths: ret_files,
    }))
}

pub async fn add_version_files(
    req: HttpRequest,
    payload: web::Json<Vec<FileWithHash>>,
) -> Result<HttpResponse, OxenHttpError> {
    // Add version file to staging
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let directory = path_param(&req, "directory")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let files_with_hash: Vec<FileWithHash> = payload.into_inner();
    log::debug!(
        "Calling add version files from the core workspace logic with {} files",
        files_with_hash.len(),
    );
    let err_files = core::v_latest::workspaces::files::add_version_files(
        &repo,
        &workspace,
        &files_with_hash,
        &directory,
    )?;

    // Return the error files for retry
    Ok(HttpResponse::Ok().json(ErrorFilesResponse {
        status: StatusMessage::resource_created(),
        err_files,
    }))
}

pub async fn delete(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    remove_file_from_workspace(&repo, &workspace, &path)
}

// Stage files as removed
pub async fn rm_files(
    req: HttpRequest,
    payload: web::Json<Vec<PathBuf>>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let paths_to_remove: Vec<PathBuf> = payload.into_inner();

    let mut ret_files = vec![];
    let mut err_files = vec![];

    for path in &paths_to_remove {
        err_files.extend(repositories::workspaces::files::rm(&workspace, &path).await?);
        println!("rm ✅ success! staged file {:?} as removed", path);
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

// Remove files from staging
pub async fn rm_files_from_staged(
    req: HttpRequest,
    payload: web::Json<Vec<PathBuf>>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let version_store = repo.version_store()?;
    log::debug!("rm_files_from_staged found repo {repo_name}, workspace_id {workspace_id}");

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let paths_to_remove: Vec<PathBuf> = payload.into_inner();

    let mut err_paths = vec![];

    for path in paths_to_remove {
        let Some(staged_entry) =
            with_staged_db_manager(&workspace.workspace_repo, |staged_db_manager| {
                // Try to read existing staged entry
                staged_db_manager.read_from_staged_db(&path)
            })?
        else {
            continue;
        };

        match remove_file_from_workspace(&repo, &workspace, &path) {
            Ok(_) => {
                // Also remove file contents from version store
                version_store
                    .delete_version(&staged_entry.node.hash.to_string())
                    .await?;
            }
            Err(e) => {
                log::debug!("Failed to stage file {path:?} for removal: {:?}", e);
                err_paths.push(path);
            }
        }
    }

    if err_paths.is_empty() {
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::PartialContent().json(FilePathsResponse {
            paths: err_paths,
            status: StatusMessage::resource_not_found(),
        }))
    }
}

pub async fn validate(_req: HttpRequest, _body: String) -> Result<HttpResponse, OxenHttpError> {
    Ok(HttpResponse::Ok().json(StatusMessage::resource_found()))
}

// Read the payload files into memory, compute the hash, and save to version store
// Unlike controllers::versions::save_multiparts, the hash must be computed here,
// As this function expects the filename to be the file path, not the hash
pub async fn save_parts(
    mut payload: Multipart,
    repo: &LocalRepository,
) -> Result<(Vec<FileWithHash>, Vec<ErrorFileInfo>), Error> {
    // Receive a multipart request and save the files to the version store
    let version_store = repo.version_store().map_err(|oxen_err: OxenError| {
        log::error!("Failed to get version store: {:?}", oxen_err);
        actix_web::error::ErrorInternalServerError(oxen_err.to_string())
    })?;
    let gzip_mime: mime::Mime = "application/gzip".parse().unwrap();

    let mut upload_files: Vec<FileWithHash> = vec![];
    let mut err_files: Vec<ErrorFileInfo> = vec![];

    while let Some(mut field) = payload.try_next().await? {
        let Some(content_disposition) = field.content_disposition().cloned() else {
            continue;
        };

        if let Some(name) = content_disposition.get_name() {
            if name == "file[]" || name == "file" {
                // The file path is passed in as the filename
                let upload_filename = content_disposition.get_filename().map_or_else(
                    || {
                        Err(actix_web::error::ErrorBadRequest(
                            "Missing hash in multipart request",
                        ))
                    },
                    |fhash_os_str| Ok(fhash_os_str.to_string()),
                )?;

                let mut field_bytes = Vec::new();
                while let Some(chunk) = field.try_next().await? {
                    field_bytes.extend_from_slice(&chunk);
                }

                let is_gzipped = field
                    .content_type()
                    .map(|mime| {
                        mime.type_() == gzip_mime.type_() && mime.subtype() == gzip_mime.subtype()
                    })
                    .unwrap_or(false);

                let upload_filename_copy = upload_filename.clone();

                let (upload_filehash, data_to_store) =
                    match actix_web::web::block(move || -> Result<(String, Vec<u8>), OxenError> {
                        if is_gzipped {
                            log::debug!(
                                "Decompressing gzipped data for file: {upload_filename_copy:?}"
                            );

                            // Decompress the data if it is gzipped
                            let mut decoder = GzDecoder::new(&field_bytes[..]);
                            let mut decompressed_bytes: Vec<u8> = Vec::new();
                            decoder.read_to_end(&mut decompressed_bytes).map_err(|e| {
                                OxenError::basic_str(format!(
                                    "Failed to decompress gzipped data: {}",
                                    e
                                ))
                            })?;

                            // Hash file contents
                            let hash = hasher::hash_buffer(&decompressed_bytes);

                            Ok((hash, decompressed_bytes))
                        } else {
                            log::debug!("Data for file {upload_filename_copy:?} is not gzipped.");

                            // Only hash file contents
                            let hash = hasher::hash_buffer(&field_bytes);
                            Ok((hash, field_bytes))
                        }
                    })
                    .await
                    {
                        Ok(Ok((hash, data))) => (hash, data),
                        Ok(Err(e)) => {
                            log::error!(
                                "Failed to decompress data for file {}: {:?}",
                                &upload_filename,
                                e
                            );
                            record_error_file(
                                &mut err_files,
                                upload_filename.clone(),
                                None,
                                format!("Failed to decompress data: {:?}", e),
                            );
                            continue;
                        }
                        Err(e) => {
                            log::error!(
                                "Failed to execute blocking decompression task for file {}: {}",
                                &upload_filename,
                                e
                            );
                            record_error_file(
                                &mut err_files,
                                upload_filename.clone(),
                                None,
                                format!("Failed to execute blocking decompression: {}", e),
                            );
                            continue;
                        }
                    };

                match version_store
                    .store_version(&upload_filehash, &data_to_store)
                    .await
                {
                    Ok(_) => {
                        upload_files.push(FileWithHash {
                            hash: upload_filehash.to_string(),
                            path: upload_filename.into(),
                        });
                        log::info!("Successfully stored version for hash: {}", &upload_filehash);
                    }
                    Err(e) => {
                        log::error!(
                            "Failed to store version for hash {}: {}",
                            &upload_filehash,
                            e
                        );
                        record_error_file(
                            &mut err_files,
                            upload_filehash.clone(),
                            None,
                            format!("Failed to store version: {}", e),
                        );
                        continue;
                    }
                }
            }
        }
    }

    Ok((upload_files, err_files))
}

fn remove_file_from_workspace(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: &PathBuf,
) -> Result<HttpResponse, OxenHttpError> {
    // This may not be in the commit if it's added, so have to parse tabular-ness from the path.
    if util::fs::is_tabular(path) {
        repositories::workspaces::data_frames::restore(repo, workspace, path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else if repositories::workspaces::files::exists(workspace, path)? {
        repositories::workspaces::files::delete(workspace, path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
    }
}

// Record the error file info for retry
fn record_error_file(
    err_files: &mut Vec<ErrorFileInfo>,
    filehash: String,
    filepath: Option<PathBuf>,
    error: String,
) {
    let info = ErrorFileInfo {
        hash: filehash,
        path: filepath,
        error,
    };
    err_files.push(info);
}
