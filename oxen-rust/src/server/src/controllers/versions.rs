pub mod chunks;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use actix_multipart::Multipart;
use actix_web::{web, Error, HttpRequest, HttpResponse};
use async_compression::tokio::bufread::GzipDecoder;
use async_compression::tokio::write::GzipEncoder;
use async_zip::base::write::ZipFileWriter;
use async_zip::Compression;
use bytes::Bytes;
use flate2::read::GzDecoder;
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt as _};
use liboxen::error::OxenError;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::storage::VersionStore;
use liboxen::util;
use liboxen::view::versions::{CleanCorruptedVersionsResponse, VersionFile, VersionFileResponse};
use liboxen::view::{ErrorFileInfo, ErrorFilesResponse, FileWithHash, StatusMessage};
use mime;
use std::io::Read as StdRead;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::BufReader;
use tokio::io::{AsyncRead, AsyncWriteExt};
use tokio::sync::Semaphore;
use tokio_stream::Stream;
use tokio_tar::Builder;
use tokio_util::compat::FuturesAsyncWriteCompatExt;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tokio_util::io::{ReaderStream, StreamReader};
use utoipa::ToSchema;

// Duplex buffer capacity:
// Acts as an in-memory pipe between the tar/gzip writer task and the HTTP response reader.
// Must be large enough to absorb bursty writes from gzip + tar and avoid backpressure issue
const DOWNLOAD_BUFFER_SIZE: usize = 2 * 1024 * 1024;
// Stream buffer size for BufReader / BufWriter:
// Used to smooth out inconsistent chunk sizes from the S3 async stream and
// provide a stable read/write pattern for tar and gzip across platforms.
const STREAM_BUFFER_SIZE: usize = 64 * 1024;

/// Get version file metadata
#[utoipa::path(
    get,
    path = "/{namespace}/{repo_name}/versions/{version_id}/metadata",
    tag = "Version Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("version_id" = String, Path, description = "The hash ID of the file version", example = "1eb45ac94f3eab120f3a"),
    ),
    responses(
        (status = 200, description = "File metadata found", body = VersionFileResponse),
        (status = 404, description = "Version file not found"),
    )
)]
pub async fn metadata(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let version_id = path_param(&req, "version_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let exists = repo.version_store()?.version_exists(&version_id).await?;
    if !exists {
        return Err(OxenHttpError::NotFound);
    }

    let file_size = repo.version_store()?.get_version_size(&version_id).await?;
    Ok(HttpResponse::Ok().json(VersionFileResponse {
        status: StatusMessage::resource_found(),
        version: VersionFile {
            hash: version_id,
            size: file_size,
        },
    }))
}

// Clean corrupted version files for the remote repo
pub async fn clean(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let version_store = repo.version_store()?;
    let result = version_store.clean_corrupted_versions().await?;

    Ok(HttpResponse::Ok().json(CleanCorruptedVersionsResponse {
        status: StatusMessage::resource_found(),
        result,
    }))
}

// TODO: Refactor places that call /file/{resource:*} to use this new version store download endpoint
/// Download version file
#[utoipa::path(
    get,
    path = "/{namespace}/{repo_name}/versions/{resource}",
    tag = "Version Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "The resource identifier (e.g., 'main/path/to/file.ext' or a commit ID/branch name followed by the file path)", example = "main/images/dog_1.jpg"),
        ImgResize
    ),
    responses(
        (status = 200, description = "File content returned as a stream. Content-Type matches the file's MIME type (e.g., image/jpeg), or the resized image's MIME type.",
            body = Vec<u8>,
            headers(
                ("oxen-revision-id" = String, description = "The commit ID of the file version")
            )
        ),
        (status = 404, description = "Repository, commit, or file not found"),
    )
)]
pub async fn download(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let version_store = repo.version_store()?;
    let resource = parse_resource(&req, &repo)?;
    let commit = resource
        .clone()
        .commit
        .ok_or_else(|| OxenError::basic_str("Resource commit not found"))?;
    let path = resource.path.clone();

    log::debug!("Download resource {namespace}/{repo_name}/{resource} version file");

    let entry = repositories::entries::get_file(&repo, &commit, &path)?
        .ok_or(OxenError::path_does_not_exist(path.clone()))?;
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

        let file_stream = util::fs::handle_image_resize(
            Arc::clone(&version_store),
            hash_str,
            &path,
            &version_path,
            img_resize,
        )
        .await?;

        return Ok(HttpResponse::Ok()
            .content_type(mime_type)
            .insert_header(("oxen-revision-id", last_commit_id.as_str()))
            .streaming(file_stream));
    } else {
        log::debug!("did not hit the resize cache");
    }

    let stream = version_store.get_version_stream(&hash_str).await?;
    Ok(HttpResponse::Ok()
        .content_type(mime_type)
        .insert_header(("oxen-revision-id", last_commit_id.as_str()))
        .streaming(stream))
}

/// Batch download version files
#[utoipa::path(
    post,
    path = "/{namespace}/{repo_name}/versions/batch-download",
    tag = "Version Files",
    summary = "Batch download files (Tarball)",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = Vec<u8>,
        content_type = "application/gzip",
        description = "Gzip compressed binary payload containing a line-delimited list of merkle hashes to download",
    ),
    responses(
        (status = 200, description = "Tarball of all requested files, gzipped.", content_type = "application/gzip"),
    )
)]
pub async fn batch_download(
    req: HttpRequest,
    mut body: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    log::debug!(
        "batch_download got repo [{}] and content_ids size {}",
        repo_name,
        bytes.len()
    );

    let mut gz = GzDecoder::new(&bytes[..]);
    let mut line_delimited_files = String::new();
    if let Err(e) = gz.read_to_string(&mut line_delimited_files) {
        log::error!("Failed to decompress gzip payload: {e}");
        return Err(OxenHttpError::from(e));
    }

    let file_hashes: Vec<String> = line_delimited_files
        .lines()
        .map(str::trim)
        .filter(|hash| !hash.is_empty())
        .filter(|hash| hash.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|hash| hash.to_string())
        .collect();

    log::debug!("Got {} file hashes", file_hashes.len());

    stream_versions_tar_gz(&repo, file_hashes).await
}

pub async fn stream_versions_tar_gz(
    repo: &LocalRepository,
    file_hashes: Vec<String>,
) -> Result<HttpResponse, OxenHttpError> {
    let version_store = repo.version_store()?;
    // In-memory pipe between tar-gzip writer task -> reader
    let (writer, reader) = tokio::io::duplex(DOWNLOAD_BUFFER_SIZE);

    let version_store_clone = version_store.clone();
    let file_hashes_clone = file_hashes.clone();

    let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel();

    let writer_task = async move {
        // Use a buffer to provide a stable stream, avoid inconsistent chunk size returned in S3 stream causing Windows panic
        let buffered_writer = tokio::io::BufWriter::with_capacity(STREAM_BUFFER_SIZE, writer);
        let enc = GzipEncoder::new(buffered_writer);
        let mut tar = Builder::new(enc);

        let result = download_files_parallel(
            &version_store_clone,
            &file_hashes_clone,
            &mut tar,
            &error_tx,
        )
        .await;

        let had_error = result.is_err();
        if let Err(e) = result {
            log::error!("Failed to download files: {e}");
            error_tx.send(e).ok();
        }

        if let Err(e) = finish_tar(tar, &error_tx).await {
            log::error!("Failed to finish tar: {e}");
        }

        if had_error {
            log::warn!("Stream closed due to earlier error");
        } else {
            log::info!("Streaming completed successfully");
        }
    };

    tokio::spawn(writer_task);

    let stream = ReaderStream::new(reader).map(move |chunk| {
        if let Ok(err) = error_rx.try_recv() {
            log::error!("Stream error: {err}");
            return Err(OxenHttpError::from(err));
        }
        chunk.map_err(OxenHttpError::from)
    });

    Ok(HttpResponse::Ok()
        .content_type("application/gzip")
        .streaming(stream))
}

async fn download_files_parallel(
    version_store: &Arc<dyn VersionStore>,
    file_hashes: &[String],
    tar: &mut Builder<GzipEncoder<tokio::io::BufWriter<tokio::io::DuplexStream>>>,
    error_tx: &tokio::sync::mpsc::UnboundedSender<OxenError>,
) -> Result<(), OxenError> {
    // Higher concurrency for S3 as it has higher latency when getting version file stream
    let concurrency = version_store.concurrency() as usize;
    let semaphore = Arc::new(Semaphore::new(concurrency));

    for chunk in file_hashes.chunks(concurrency * 2) {
        let mut download_tasks = FuturesUnordered::new();

        // Get version stream in parallel to reduce network latency
        for hash in chunk {
            let hash = hash.to_string();
            let version_store = version_store.clone();
            let semaphore = semaphore.clone();

            let task = tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();

                let (size_result, stream_result) = tokio::join!(
                    version_store.get_version_size(&hash),
                    version_store.get_version_stream(&hash)
                );

                let size = size_result?;
                let stream = stream_result?;

                Ok::<_, OxenError>((hash, size, stream))
            });

            download_tasks.push(task);
        }

        // Append to tar in sequence
        while let Some(result) = download_tasks.next().await {
            match result {
                Ok(Ok((hash, size, stream))) => {
                    if let Err(e) = append_to_tar_with_hash(tar, hash, size, stream).await {
                        log::error!("Failed to append to tar: {e}");
                        error_tx.send(e).ok();
                        return Err(OxenError::basic_str("Failed to append to tar"));
                    }
                }
                Ok(Err(e)) => {
                    log::error!("Failed to download file: {e}");
                    error_tx.send(e).ok();
                    return Err(OxenError::basic_str("Failed to download file"));
                }
                Err(e) => {
                    let err = OxenError::basic_str(format!("Download task panicked: {e}"));
                    log::error!("{err}");
                    error_tx.send(err).ok();
                    return Err(OxenError::basic_str("Download task panicked"));
                }
            }
        }
    }

    Ok(())
}

async fn append_to_tar_with_hash(
    tar: &mut Builder<GzipEncoder<tokio::io::BufWriter<tokio::io::DuplexStream>>>,
    hash: String,
    size: u64,
    stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>,
) -> Result<(), OxenError> {
    let mut header = tokio_tar::Header::new_gnu();
    header.set_size(size);
    if let Err(e) = header.set_path(&hash) {
        log::error!("Failed to set path for {hash}: {e}");
        return Err(OxenError::basic_str(format!(
            "Failed to set path for {hash}: {e}",
        )));
    }
    header.set_mode(0o644);
    header.set_uid(0);
    header.set_gid(0);
    header.set_cksum();

    // Add a buffer to avoid inconsistent chunk size returned in S3 stream causing panic in Windows
    let buffered_reader =
        tokio::io::BufReader::with_capacity(STREAM_BUFFER_SIZE, StreamReader::new(stream));

    if let Err(e) = tar.append(&header, buffered_reader).await {
        log::error!("Failed to append {hash} to tar: {e}");
        return Err(OxenError::IO(e));
    }

    log::info!("Successfully appended {hash} to tarball");
    Ok(())
}

async fn finish_tar(
    mut tar: Builder<GzipEncoder<tokio::io::BufWriter<tokio::io::DuplexStream>>>,
    error_tx: &tokio::sync::mpsc::UnboundedSender<OxenError>,
) -> Result<(), OxenError> {
    if let Err(e) = tar.finish().await {
        log::error!("Failed to finish tar: {e}");
        error_tx.send(OxenError::IO(e)).ok();
        return Err(OxenError::basic_str("Failed to finish tar"));
    }

    match tar.into_inner().await {
        Ok(mut enc) => {
            if let Err(e) = enc.shutdown().await {
                log::error!("Failed to shutdown gzip encoder: {e}");
                error_tx.send(OxenError::IO(e)).ok();
                return Err(OxenError::basic_str("Failed to shutdown encoder"));
            }

            let mut buffered_writer = enc.into_inner();

            if let Err(e) = buffered_writer.flush().await {
                log::error!("Failed to flush buffer: {e}");
                error_tx.send(OxenError::IO(e)).ok();
                return Err(OxenError::basic_str("Failed to flush buffer"));
            }

            if let Err(e) = buffered_writer.shutdown().await {
                log::error!("Failed to shutdown writer: {e}");
                error_tx.send(OxenError::IO(e)).ok();
                return Err(OxenError::basic_str("Failed to shutdown writer"));
            }

            log::info!("Successfully finished tarball");
            Ok(())
        }
        Err(e) => {
            log::error!("Failed to get encoder: {e}");
            error_tx.send(OxenError::IO(e)).ok();
            Err(OxenError::basic_str("Failed to get encoder"))
        }
    }
}

pub async fn stream_versions_zip(
    repo: &LocalRepository,
    files: Vec<FileWithHash>,
) -> Result<HttpResponse, OxenHttpError> {
    let version_store = repo.version_store()?;
    let (writer, reader) = tokio::io::duplex(DOWNLOAD_BUFFER_SIZE);

    let version_store_clone = version_store.clone();
    let files_clone = files.clone();

    let (error_tx, mut error_rx) = tokio::sync::mpsc::unbounded_channel();

    let writer_task = async move {
        let compat_writer = writer.compat_write();
        let mut zip_writer = ZipFileWriter::new(compat_writer);
        let mut had_error = false;

        for file in files_clone.iter() {
            let path = file.path.to_str();
            let path = match path.map(|s| s.to_string()) {
                Some(path) => path,
                None => {
                    let err = "Invalid UTF-8 in path".to_string();
                    error_tx.send(OxenError::basic_str(err)).ok();
                    had_error = true;
                    break;
                }
            };

            let hash = &file.hash;

            let file_size = match version_store_clone.get_version_size(hash).await {
                Ok(size) => size,
                Err(e) => {
                    log::error!("Failed to get file size for {hash}: {e}");
                    error_tx.send(e).ok();
                    had_error = true;
                    break;
                }
            };

            match version_store_clone.get_version_stream(hash).await {
                Ok(data) => {
                    let mut reader = StreamReader::new(data);

                    let compression = Compression::Deflate;
                    let zip_entry_builder =
                        async_zip::ZipEntryBuilder::new(path.into(), compression)
                            .uncompressed_size(file_size)
                            .unix_permissions(0o644);

                    let zip_entry = zip_entry_builder.build();

                    let entry_writer = match zip_writer.write_entry_stream(zip_entry).await {
                        Ok(writer) => writer,
                        Err(e) => {
                            log::error!("Failed to append {hash} to zip: {e}");
                            error_tx.send(OxenError::IO(std::io::Error::other(e))).ok();
                            had_error = true;
                            break;
                        }
                    };

                    let mut compat_writer = entry_writer.compat_write();
                    if let Err(e) = tokio::io::copy(&mut reader, &mut compat_writer).await {
                        log::error!("Failed to stream data for {hash} into zip: {e}");
                        error_tx.send(OxenError::IO(std::io::Error::other(e))).ok();
                        had_error = true;
                        break;
                    }

                    let entry_writer = compat_writer.into_inner();
                    if let Err(e) = entry_writer.close().await {
                        log::error!("Failed to close zip entry for {hash}: {e}");
                        error_tx.send(OxenError::IO(std::io::Error::other(e))).ok();
                        had_error = true;
                        break;
                    }

                    log::info!("Successfully appended data to zip for hash: {}", &hash);
                }
                Err(e) => {
                    log::error!("Failed to get version {hash}: {e}");
                    error_tx.send(OxenError::IO(std::io::Error::other(e))).ok();
                    had_error = true;
                    break;
                }
            }
        }

        if let Err(e) = zip_writer.close().await {
            log::error!("Failed to finish zip file: {e}");
            error_tx.send(OxenError::IO(std::io::Error::other(e))).ok();
            had_error = true;
        } else {
            log::info!("Successfully finished zip file");
        }

        // zip.close() typically handles the shutdown, but we check inner writer if needed
        if !had_error {
            log::info!("Streaming completed successfully");
        } else {
            log::warn!("Stream closed due to earlier error");
        }
    };

    tokio::spawn(writer_task);

    let stream = tokio_util::io::ReaderStream::new(reader).map(move |chunk| {
        if let Ok(err) = error_rx.try_recv() {
            log::error!("Stream error: {err}");
            return Err(OxenHttpError::from(err));
        }
        chunk.map_err(OxenHttpError::from)
    });

    Ok(HttpResponse::Ok()
        .content_type("application/zip")
        .streaming(stream))
}

#[derive(ToSchema)]
pub struct UploadVersionFile {
    #[schema(value_type = String, format = Binary)]
    pub file: Vec<u8>,
}

/// Batch upload version files
#[utoipa::path(
    post,
    path = "/{namespace}/{repo_name}/versions",
    tag = "Version Files",
    summary = "Batch upload files (Multipart)",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content_type = "multipart/form-data", 
        description = "Multipart upload of files. Each form field 'file[]' or 'file' should contain the file content (optionally gzip compressed), and the filename should be the content hash (e.g., 'file.jpg' is not used, the hash is the identifier).",
        content = UploadVersionFile,
    ),
    responses(
        (status = 200, description = "Files successfully uploaded (check err_files for failures)", body = ErrorFilesResponse),
        (status = 400, description = "Invalid multipart request"),
        (status = 404, description = "Repository not found"),
    )
)]
pub async fn batch_upload(
    req: HttpRequest,
    payload: Multipart,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let err_files = save_multiparts(payload, &repo).await?;
    log::debug!("batch upload complete with err_files: {}", err_files.len());

    Ok(HttpResponse::Ok().json(ErrorFilesResponse {
        status: StatusMessage::resource_created(),
        err_files,
    }))
}

pub async fn save_multiparts(
    mut payload: Multipart,
    repo: &LocalRepository,
) -> Result<Vec<ErrorFileInfo>, Error> {
    let version_store = repo.version_store().map_err(|oxen_err: OxenError| {
        log::error!("Failed to get version store: {oxen_err:?}");
        actix_web::error::ErrorInternalServerError(oxen_err.to_string())
    })?;
    let gzip_mime: mime::Mime = "application/gzip".parse().unwrap();

    let mut err_files: Vec<ErrorFileInfo> = vec![];

    // Parallel task
    let mut upload_tasks = Vec::new();
    // Higher concurrency for S3 as it has higher latency in storing version file than local
    let worker_num = version_store.concurrency() as usize;
    let semaphore = Arc::new(Semaphore::new(worker_num));

    while let Some(mut field) = payload.try_next().await? {
        let Some(content_disposition) = field.content_disposition().cloned() else {
            continue;
        };
        if let Some(name) = content_disposition.get_name() {
            if name == "file[]" || name == "file" {
                // The file hash is passed in as the filename. In version store, the file hash is the identifier.
                let upload_filehash = content_disposition.get_filename();
                let Some(upload_filehash) = upload_filehash else {
                    log::error!("Missing hash in multipart request");
                    record_error_file(
                        &mut err_files,
                        "".to_string(),
                        None,
                        "Missing hash in multipart request".to_string(),
                    );
                    continue;
                };

                // Get file size from header
                let raw_headers = field.headers();
                let file_size = raw_headers
                    .get("X-Oxen-File-Size")
                    .and_then(|val| val.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());

                let size = match file_size {
                    Some(size) => {
                        log::debug!("versions::save_multiparts got file_size from header: {size}");
                        size
                    }
                    None => {
                        log::error!("Failed to get file size for hash {upload_filehash}");
                        record_error_file(
                            &mut err_files,
                            upload_filehash.to_string(),
                            None,
                            "Failed to get file size".to_string(),
                        );
                        continue;
                    }
                };

                log::debug!("upload file_hash: {upload_filehash:?}");

                let is_gzipped = field
                    .content_type()
                    .map(|mime| {
                        mime.type_() == gzip_mime.type_() && mime.subtype() == gzip_mime.subtype()
                    })
                    .unwrap_or(false);

                let mut field_bytes = Vec::new();
                while let Some(chunk) = field.try_next().await? {
                    field_bytes.extend_from_slice(&chunk);
                }

                let version_store_clone = version_store.clone();
                let upload_filehash_owned = upload_filehash.to_string();
                let semaphore_clone = semaphore.clone();

                // Spawn tasks to avoid store version blocking multipart read
                let upload_task = tokio::spawn(async move {
                    let _permit = semaphore_clone.acquire().await.unwrap();

                    let reader: Box<dyn AsyncRead + Send + Sync + Unpin> = if is_gzipped {
                        let cursor = std::io::Cursor::new(field_bytes);
                        let buf_reader = BufReader::new(cursor);
                        Box::new(GzipDecoder::new(buf_reader))
                    } else {
                        let cursor = std::io::Cursor::new(field_bytes);
                        Box::new(cursor)
                    };

                    match version_store_clone
                        .store_version_from_reader_with_size(&upload_filehash_owned, reader, size)
                        .await
                    {
                        Ok(_) => {
                            log::info!(
                                "Successfully stored version for hash: {upload_filehash_owned}"
                            );
                            Ok(())
                        }
                        Err(e) => {
                            log::error!("Failed to store version: {e}");
                            Err((
                                upload_filehash_owned,
                                format!("Failed to store version: {e}"),
                            ))
                        }
                    }
                });

                upload_tasks.push(upload_task);
            }
        }
    }

    // Wait for all upload tasks finish
    for task in upload_tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err((hash, error_msg))) => {
                record_error_file(&mut err_files, hash, None, error_msg);
            }
            Err(e) => {
                log::error!("Upload task panicked: {e}");
                record_error_file(
                    &mut err_files,
                    "unknown".to_string(),
                    None,
                    format!("Upload task panicked: {e}"),
                );
            }
        }
    }

    Ok(err_files)
}

// // Read the payload files into memory and save to version store
// pub async fn save_multiparts(
//     mut payload: Multipart,
//     repo: &LocalRepository,
// ) -> Result<Vec<ErrorFileInfo>, Error> {
//     let version_store = repo.version_store().map_err(|oxen_err: OxenError| {
//         log::error!("Failed to get version store: {oxen_err:?}");
//         actix_web::error::ErrorInternalServerError(oxen_err.to_string())
//     })?;
//     let gzip_mime: mime::Mime = "application/gzip".parse().unwrap();

//     let mut err_files: Vec<ErrorFileInfo> = vec![];

//     while let Some(mut field) = payload.try_next().await? {
//         let Some(content_disposition) = field.content_disposition().cloned() else {
//             continue;
//         };
//         if let Some(name) = content_disposition.get_name() {
//             if name == "file[]" || name == "file" {
//                 // The file hash is passed in as the filename. In version store, the file hash is the identifier.
//                 let upload_filehash = content_disposition.get_filename();
//                 let Some(upload_filehash) = upload_filehash else {
//                     log::error!("Missing hash in multipart request");
//                     record_error_file(
//                         &mut err_files,
//                         "".to_string(),
//                         None,
//                         "Missing hash in multipart request".to_string(),
//                     );
//                     continue;
//                 };

//                 // Get file size from header
//                 let raw_headers = field.headers();
//                 let file_size = raw_headers
//                     .get("X-Oxen-File-Size")
//                     .and_then(|val| val.to_str().ok())
//                     .and_then(|s| s.parse::<u64>().ok());

//                 let size = match file_size {
//                     Some(size) => {
//                         log::debug!("versions::save_multiparts got file_size from header: {size}");
//                         size
//                     }
//                     None => {
//                         log::error!("Failed to get file size for hash {upload_filehash}");
//                         record_error_file(
//                             &mut err_files,
//                             upload_filehash.to_string(),
//                             None,
//                             "Failed to get file size".to_string(),
//                         );
//                         continue;
//                     }
//                 };

//                 log::debug!("upload file_hash: {upload_filehash:?}");

//                 let is_gzipped = field
//                     .content_type()
//                     .map(|mime| {
//                         mime.type_() == gzip_mime.type_() && mime.subtype() == gzip_mime.subtype()
//                     })
//                     .unwrap_or(false);

//                 // Read the bytes from the stream
//                 // TODO: Implement streaming here
//                 let mut field_bytes = Vec::new();
//                 while let Some(chunk) = field.try_next().await? {
//                     field_bytes.extend_from_slice(&chunk);
//                 }

//                 let reader: Box<dyn AsyncRead + Send + Sync + Unpin> = if is_gzipped {
//                     // async decompression
//                     let cursor = std::io::Cursor::new(field_bytes);
//                     let buf_reader = BufReader::new(cursor);
//                     Box::new(GzipDecoder::new(buf_reader))
//                 } else {
//                     let cursor = std::io::Cursor::new(field_bytes);
//                     Box::new(cursor)
//                 };

//                 match version_store
//                     .store_version_from_reader_with_size(upload_filehash, reader, size)
//                     .await
//                 {
//                     Ok(_) => {
//                         log::info!("Successfully stored version for hash: {upload_filehash}");
//                     }
//                     Err(e) => {
//                         log::error!("Failed to store version: {e}");
//                         record_error_file(
//                             &mut err_files,
//                             upload_filehash.to_string(),
//                             None,
//                             format!("Failed to store version: {e}"),
//                         );
//                         continue;
//                     }
//                 }
//             }
//         }
//     }

//     Ok(err_files)
// }

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

#[cfg(test)]
mod tests {
    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    use actix_web::{web, App};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::ErrorFilesResponse;
    use std::io::Write;

    #[actix_web::test]
    async fn test_controllers_versions_download() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        // create test file and commit
        util::fs::create_dir_all(repo.path.join("data"))?;
        let relative_path = "data/hello.txt";
        let hello_file = repo.path.join(relative_path);
        let file_content = "Hello";
        util::fs::write_to_path(&hello_file, file_content)?;
        repositories::add(&repo, &hello_file).await?;
        repositories::commit(&repo, "First commit")?;

        // test download
        let uri = format!("/oxen/{namespace}/{repo_name}/versions/main/{relative_path}");
        let req = actix_web::test::TestRequest::get()
            .uri(&uri)
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions/{resource:.*}",
                    web::get().to(controllers::versions::download),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        assert_eq!(bytes, "Hello");

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_versions_batch_upload() -> Result<(), OxenError> {
        test::init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        repositories::add(&repo, path).await?;
        repositories::commit(&repo, "first commit")?;

        let file_content = "Test Content";
        let file_hash = util::hasher::hash_str(file_content);

        // compress file content
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(file_content.as_bytes())?;
        let compressed_bytes = encoder.finish()?;
        let file_size = file_content.len();

        // Construct multipart request body
        let boundary = "----oxen-boundary";

        let mut body = Vec::new();

        // including self defined header x-oxen-file-size
        body.extend_from_slice(
            format!(
                "--{b}\r\n\
            Content-Disposition: form-data; name=\"file[]\"; filename=\"{file_hash}\"\r\n\
            Content-Type: application/gzip\r\n\
            X-Oxen-File-Size: {size}\r\n\
            \r\n",
                b = boundary,
                size = file_size,
                file_hash = file_hash,
            )
            .as_bytes(),
        );

        body.extend_from_slice(&compressed_bytes);

        body.extend_from_slice(format!("\r\n--{b}--\r\n", b = boundary).as_bytes());

        let uri = format!("/oxen/{namespace}/{repo_name}/versions");

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .insert_header((
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            ))
            .app_data(OxenAppData::new(sync_dir.to_path_buf()))
            .set_payload(body)
            .to_request();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/versions",
                    web::post().to(controllers::versions::batch_upload),
                ),
        )
        .await;

        let resp = actix_web::test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK);
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let response: ErrorFilesResponse = serde_json::from_slice(&bytes)?;
        assert_eq!(response.status.status, "success");
        assert!(response.err_files.is_empty());

        // verify file is stored correctly
        let version_store = repo.version_store()?;
        let stored_data = version_store.get_version(&file_hash).await?;
        assert_eq!(stored_data, file_content.as_bytes());

        // cleanup
        test::cleanup_sync_dir(&sync_dir)?;
        Ok(())
    }
}
