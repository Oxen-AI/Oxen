use crate::api;
use crate::api::client;
use crate::api::client::internal_types::LocalOrBase;
use crate::constants::{max_retries, AVG_CHUNK_SIZE};
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::{LocalRepository, MerkleHash, RemoteRepository};
use crate::util::{self, concurrency, hasher};
use crate::view::versions::{
    CleanCorruptedVersionsResponse, CompleteVersionUploadRequest, CompletedFileUpload,
    CreateVersionUploadRequest, MultipartLargeFileUpload, MultipartLargeFileUploadStatus,
    VersionFile, VersionFileResponse,
};
use crate::view::{ErrorFileInfo, ErrorFilesResponse, FileWithHash};

use crate::core::progress::push_progress::PushProgress;
use async_compression::tokio::bufread::GzipDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use http::header::CONTENT_LENGTH;
use http::Method;
use rand::{thread_rng, Rng};
use tokio_tar::Archive;
use tokio_util::codec::{BytesCodec, FramedRead};

use std::collections::{HashMap, HashSet};
use std::io::{SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::repositories;

// Multipart upload strategy, based off of AWS S3 Multipart Upload and huggingface hf_transfer
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
// https://github.com/huggingface/hf_transfer/blob/main/src/lib.rs#L104
const BASE_WAIT_TIME: usize = 300;
const MAX_WAIT_TIME: usize = 10_000;
const PARALLEL_FAILURES: usize = 63;

#[derive(Debug, Default)]
pub struct UploadResult {
    pub files_to_add: Vec<FileWithHash>,
    pub err_files: Vec<ErrorFileInfo>,
}

/// Check if a file exists in the remote repository by version id
pub async fn has_version(
    repository: &RemoteRepository,
    version_id: MerkleHash,
) -> Result<bool, OxenError> {
    Ok(get(repository, version_id).await?.is_some())
}

/// Get the size of a version
pub async fn get(
    repository: &RemoteRepository,
    version_id: MerkleHash,
) -> Result<Option<VersionFile>, OxenError> {
    let uri = format!("/versions/{version_id}/metadata");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("api::client::versions::get {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    if res.status() == 404 {
        return Ok(None);
    }

    let body = client::parse_json_body(&url, res).await?;
    let response: Result<VersionFileResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(version_file) => Ok(Some(version_file.version)),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::versions::get() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn clean(
    remote_repo: &RemoteRepository,
) -> Result<CleanCorruptedVersionsResponse, OxenError> {
    let uri = "/versions";
    let url = api::endpoint::url_from_repo(remote_repo, uri)?;
    log::debug!("api::client::versions::clean {url}");

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CleanCorruptedVersionsResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::versions::clean() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

/// Uploads a large file to the server in parallel and unpacks it in the versions directory
/// Returns the `MultipartLargeFileUpload` struct for the created upload
pub async fn parallel_large_file_upload(
    remote_repo: &RemoteRepository,
    file_path: impl AsRef<Path>,
    dst_dir: Option<impl AsRef<Path>>, // dst_dir is provided for workspace add workflow
    workspace_id: Option<String>,
    entry: Option<Entry>,                 // entry is provided for push workflow
    progress: Option<&Arc<PushProgress>>, // for push workflow
) -> Result<MultipartLargeFileUpload, OxenError> {
    log::debug!("multipart_large_file_upload path: {:?}", file_path.as_ref());

    let mut upload =
        create_multipart_large_file_upload(remote_repo, file_path, dst_dir, entry).await?;

    log::debug!("multipart_large_file_upload upload: {:?}", upload.hash);

    let max_retries = max_retries();
    let results = upload_chunks(
        remote_repo,
        &mut upload,
        AVG_CHUNK_SIZE,
        PARALLEL_FAILURES,
        max_retries,
        progress,
    )
    .await?;
    let num_chunks = results.len();
    log::debug!("multipart_large_file_upload num_chunks: {num_chunks:?}");
    complete_multipart_large_file_upload(remote_repo, upload, num_chunks, workspace_id).await
}

/// Creates a new multipart large file upload
/// Will reject the upload if the hash already exists on the server.
/// The rejection helps prevent duplicate uploads or parallel uploads of the same file.
/// Returns the `MultipartLargeFileUpload` struct for the created upload
async fn create_multipart_large_file_upload(
    remote_repo: &RemoteRepository,
    file_path: impl AsRef<Path>,
    dst_dir: Option<impl AsRef<Path>>,
    entry: Option<Entry>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    let file_path = file_path.as_ref();
    let dst_dir = dst_dir.as_ref();

    let (file_size, hash) = match entry {
        Some(entry) => (entry.num_bytes(), entry.hash()),
        None => {
            // Figure out how many parts we need to upload
            let Ok(metadata) = file_path.metadata() else {
                return Err(OxenError::path_does_not_exist(file_path));
            };
            let file_size = metadata.len();
            let hash = util::hasher::hash_file_contents(file_path)?;
            (file_size, hash)
        }
    };

    let uri = format!("/versions/{hash}/create");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;

    let body = CreateVersionUploadRequest {
        hash: hash.to_string(),
        file_name: file_path.file_name().unwrap().to_string_lossy().to_string(),
        size: file_size,
        dst_dir: dst_dir.map(|d| d.as_ref().to_path_buf()),
    };

    let body = serde_json::to_string(&body)?;
    let response = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(body)
        .send()
        .await?;
    response.error_for_status()?;

    Ok(MultipartLargeFileUpload {
        local_path: file_path.to_path_buf(),
        dst_dir: dst_dir.map(|d| d.as_ref().to_path_buf()),
        hash: hash.parse()?,
        size: file_size,
        status: MultipartLargeFileUploadStatus::Pending,
        reason: None,
    })
}

/// Batch download
pub async fn download_data_from_version_paths(
    remote_repo: &RemoteRepository,
    hashes: &[String],
    local_repo: &LocalRepository,
) -> Result<u64, OxenError> {
    let total_retries = max_retries().try_into().unwrap_or(max_retries() as u64);
    let mut num_retries = 0;

    while num_retries < total_retries {
        match try_download_data_from_version_paths(remote_repo, hashes, local_repo).await {
            Ok(val) => return Ok(val),
            Err(OxenError::Authentication(val)) => return Err(OxenError::Authentication(val)),
            Err(err) => {
                num_retries += 1;
                // Exponentially back off
                let sleep_time = num_retries * num_retries;
                log::warn!("Could not download content {err:?} sleeping {sleep_time}");
                tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;
            }
        }
    }

    let err = format!(
        "Err: Failed to download {} files after {} retries",
        hashes.len(),
        total_retries
    );
    Err(OxenError::basic_str(err))
}

pub async fn try_download_data_from_version_paths(
    remote_repo: &RemoteRepository,
    hashes: &[String],
    local_repo: &LocalRepository,
) -> Result<u64, OxenError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for hash in hashes.iter() {
        let line = format!("{hash}\n");
        // log::debug!("download_data_from_version_paths encoding line: {} path: {:?}", line, path);
        encoder.write_all(line.as_bytes())?;
    }
    let body = encoder.finish()?;
    log::debug!("download_data_from_version_paths body len: {}", body.len());

    let url = api::endpoint::url_from_repo(remote_repo, "/versions")?;
    let client = client::new_for_url(&url)?;
    let query_method = Method::from_bytes(b"QUERY").unwrap();
    if let Ok(res) = client.request(query_method, &url).body(body).send().await {
        if reqwest::StatusCode::UNAUTHORIZED == res.status() {
            let err = "Err: unauthorized request to download data".to_string();
            log::error!("{err}");
            return Err(OxenError::authentication(err));
        }

        let stream = res.bytes_stream();
        let reader = tokio_util::io::StreamReader::new(
            stream.map(|result| result.map_err(std::io::Error::other)),
        );
        let buf_reader = tokio::io::BufReader::new(reader);
        let decoder = GzipDecoder::new(buf_reader);
        let mut archive = Archive::new(decoder);

        let version_store = local_repo.version_store()?;
        let mut size: u64 = 0;

        // Iterate over archive entries and stream them to version store
        let mut entries = archive.entries()?;
        while let Some(file) = entries.next().await {
            let mut file = match file {
                Ok(file) => file,
                Err(err) => {
                    let err = format!("Could not unwrap file -> {err:?}");
                    return Err(OxenError::basic_str(err));
                }
            };

            let file_hash = file
                .path()
                .map_err(|e| OxenError::basic_str(format!("Failed to get entry path: {e}")))?
                .to_string_lossy()
                .to_string();

            // Get file size from tar entry header
            let file_size = file.header().size()?;
            size += file_size;

            // Stream the file content directly to version store without loading into memory
            match version_store
                .store_version_from_reader(&file_hash, &mut file)
                .await
            {
                Ok(_) => {
                    log::debug!(
                        "Successfully stored file {file_hash} ({file_size} bytes) to version store"
                    );
                }
                Err(err) => {
                    let err =
                        format!("Could not store file {file_hash} to version store -> {err:?}");
                    return Err(OxenError::basic_str(err));
                }
            }
        }

        Ok(size)
    } else {
        let err =
            format!("api::entries::download_data_from_version_paths Err request failed: {url}");
        Err(OxenError::basic_str(err))
    }
}

async fn upload_chunks(
    remote_repo: &RemoteRepository,
    upload: &mut MultipartLargeFileUpload,
    chunk_size: u64,
    parallel_failures: usize,
    max_retries: usize,
    progress: Option<&Arc<PushProgress>>,
) -> Result<Vec<HashMap<String, String>>, OxenError> {
    let client = Arc::new(api::client::builder_for_remote_repo(remote_repo)?.build()?);

    // Figure out how many parts we need to upload
    let file_size = upload.size;
    let num_chunks = file_size.div_ceil(chunk_size);

    let max_files = concurrency::num_threads_for_items(num_chunks as usize);
    let mut handles = FuturesUnordered::new();
    let semaphore = Arc::new(Semaphore::new(max_files));
    let parallel_failures_semaphore = Arc::new(Semaphore::new(parallel_failures));

    for chunk_number in 0..num_chunks {
        let remote_repo = remote_repo.clone();
        let upload = upload.clone();
        let client = Arc::clone(&client);

        let start = chunk_number * chunk_size;
        let semaphore = semaphore.clone();
        let parallel_failures_semaphore = parallel_failures_semaphore.clone();
        handles.push(tokio::spawn(async move {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|err| OxenError::basic_str(format!("Error acquiring semaphore: {err}")))?;
                    let mut chunk = upload_chunk(&client, &remote_repo, &upload, start, chunk_size).await;
                    let mut i = 0;
                    if parallel_failures > 0 {
                        while let Err(ul_err) = chunk {
                            if i >= max_retries {
                                return Err(OxenError::basic_str(format!(
                                    "Failed after too many retries ({max_retries}): {ul_err}"
                                )));
                            }

                            let parallel_failure_permit = parallel_failures_semaphore.clone().try_acquire_owned().map_err(|err| {
                                OxenError::basic_str(format!(
                                    "Failed too many failures in parallel ({parallel_failures}): {ul_err} ({err})"
                                ))
                            })?;

                            let wait_time = exponential_backoff(BASE_WAIT_TIME, i, MAX_WAIT_TIME);
                            sleep(Duration::from_millis(wait_time as u64)).await;

                            chunk = upload_chunk(&client, &remote_repo, &upload, start, chunk_size).await;
                            i += 1;
                            drop(parallel_failure_permit);
                        }
                    }
                    drop(permit);
                    chunk
                    .map_err(|e| OxenError::basic_str(format!("Upload error {e}")))
                    .map(|chunk| (chunk_number, chunk, chunk_size))
                }));
    }

    let mut results: Vec<HashMap<String, String>> = vec![HashMap::default(); num_chunks as usize];

    while let Some(result) = handles.next().await {
        match result {
            Ok(Ok((chunk_number, headers, size))) => {
                log::debug!("Uploaded part {chunk_number} with size {size}");
                results[chunk_number as usize] = headers;
                if let Some(p) = progress {
                    p.add_bytes(size);
                }
            }
            Ok(Err(py_err)) => {
                return Err(py_err);
            }
            Err(err) => {
                return Err(OxenError::basic_str(format!(
                    "Error occurred while uploading: {err}"
                )));
            }
        }
    }
    if let Some(p) = progress {
        p.add_files(1);
    }
    Ok(results)
}

async fn upload_chunk(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    upload: &MultipartLargeFileUpload,
    start: u64,
    chunk_size: u64,
) -> Result<HashMap<String, String>, OxenError> {
    let path = &upload.local_path;
    let mut options = OpenOptions::new();
    let mut file = options.read(true).open(path).await?;

    let file_size = upload.size;
    let bytes_transferred = std::cmp::min(file_size - start, chunk_size);

    file.seek(SeekFrom::Start(start)).await?;
    let chunk = file.take(chunk_size);

    let file_hash = &upload.hash.to_string();

    let uri = format!("/versions/{file_hash}/chunks?offset={start}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let response = client
        .put(url)
        .header(CONTENT_LENGTH, bytes_transferred)
        .body(reqwest::Body::wrap_stream(FramedRead::new(
            chunk,
            BytesCodec::new(),
        )))
        .send()
        .await?;
    let response = response.error_for_status()?;
    let mut headers = HashMap::new();
    for (name, value) in response.headers().into_iter() {
        headers.insert(
            name.to_string(),
            value
                .to_str()
                .map_err(|e| OxenError::basic_str(format!("Invalid header value: {e}")))?
                .to_owned(),
        );
    }
    Ok(headers)
}

async fn complete_multipart_large_file_upload(
    remote_repo: &RemoteRepository,
    upload: MultipartLargeFileUpload,
    num_chunks: usize,
    workspace_id: Option<String>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    let file_hash = &upload.hash.to_string();

    let uri = format!("/versions/{file_hash}/complete");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("complete_multipart_large_file_upload {url}");
    let client = client::new_for_url(&url)?;

    let body = CompleteVersionUploadRequest {
        files: vec![CompletedFileUpload {
            hash: file_hash.to_string(),
            file_name: upload
                .local_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            dst_dir: upload.dst_dir.clone(),
            num_chunks: Some(num_chunks),
            upload_results: None,
        }],
        workspace_id,
    };

    let body = serde_json::to_string(&body)?;
    let response = client.post(&url).body(body).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    log::debug!("complete_multipart_large_file_upload got body: {body}");
    Ok(upload)
}

/// Multipart batch upload with retry
/// Uploads a batch of small files to the server in parallel and retries on failure
/// Returns a list of files that failed to upload
pub async fn multipart_batch_upload_with_retry(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    chunk: &Vec<Entry>,
    client: &reqwest::Client,
) -> Result<(), OxenError> {
    let mut files_to_retry: Vec<ErrorFileInfo> = vec![];
    let mut first_try = true;
    let mut retry_count: usize = 0;
    let max_retries = max_retries();

    while (first_try || !files_to_retry.is_empty()) && retry_count < max_retries {
        first_try = false;
        retry_count += 1;

        files_to_retry =
            multipart_batch_upload(local_repo, remote_repo, chunk, client, files_to_retry).await?;

        if !files_to_retry.is_empty() {
            let wait_time = exponential_backoff(BASE_WAIT_TIME, retry_count, MAX_WAIT_TIME);
            sleep(Duration::from_millis(wait_time as u64)).await;
        }
    }
    if files_to_retry.is_empty() {
        Ok(())
    } else {
        Err(OxenError::basic_str(format!(
            "Failed to upload files: {files_to_retry:#?}"
        )))
    }
}

pub async fn multipart_batch_upload(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    chunk: &Vec<Entry>,
    client: &reqwest::Client,
    files_to_retry: Vec<ErrorFileInfo>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let version_store = local_repo.version_store()?;
    let mut form = reqwest::multipart::Form::new();
    let mut err_files: Vec<ErrorFileInfo> = vec![];

    // if it's the first try, we don't have any files to retry
    let retry_hashes: HashSet<String> = if files_to_retry.is_empty() {
        HashSet::new()
    } else {
        files_to_retry.iter().map(|f| f.hash.clone()).collect()
    };

    for entry in chunk {
        let file_hash = entry.hash();

        // if it's not the first try and the file is not in the retry list, skip
        if !files_to_retry.is_empty() && !retry_hashes.contains(&file_hash) {
            continue;
        }

        let data = version_store.get_version(&file_hash).await?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        std::io::copy(&mut data.as_slice(), &mut encoder)?;
        let compressed_bytes = match encoder.finish() {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Failed to finish gzip for file {}: {}", &file_hash, e);
                err_files.push(ErrorFileInfo {
                    hash: file_hash.clone(),
                    path: None,
                    error: format!("Failed to finish gzip for file {}: {}", &file_hash, e),
                });
                continue;
            }
        };

        let file_part = reqwest::multipart::Part::bytes(compressed_bytes)
            .file_name(entry.hash().to_string())
            .mime_str("application/gzip")?;
        form = form.part("file[]", file_part);
    }

    // If there are nodes to mark as synced, re-route API call
    let uri = ("/versions").to_string();

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    // Post the node hashes to sync on the first chunk upload

    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let response: ErrorFilesResponse = serde_json::from_str(&body)?;

    err_files.extend(response.err_files);

    Ok(err_files)
}

pub(crate) async fn workspace_multipart_batch_upload_versions(
    remote_repo: &RemoteRepository,
    local_or_base: Option<&LocalOrBase>,
    client: Arc<reqwest::Client>,
    paths: Vec<PathBuf>,
    result: UploadResult,
) -> Result<UploadResult, OxenError> {
    // save the errorred files info for retry
    let mut err_files: Vec<ErrorFileInfo> = vec![];
    // keep track of the files hash
    let mut files_to_add: Vec<FileWithHash> = vec![];

    // generate retry hashes if it's not the first try
    let retry_hashes: HashSet<String> = if result.err_files.is_empty() {
        HashSet::new()
    } else {
        result.err_files.iter().map(|f| f.hash.clone()).collect()
    };

    // generate a map of the file paths to hashes
    let path_to_hash: HashMap<PathBuf, String> = result
        .files_to_add
        .iter()
        .map(|f| (f.path.clone(), f.hash.clone()))
        .collect();

    let (repo_or_base_path, head_commit_local_repo_maybe) = match local_or_base {
        Some(LocalOrBase::Local(local_repo)) => {
            let head_commit_maybe = repositories::commits::head_commit_maybe(local_repo)?;
            (
                local_repo.path.clone(),
                head_commit_maybe.map(|head_commit| (head_commit, local_repo)),
            )
        }
        Some(LocalOrBase::Base(base_dir)) => (base_dir.clone(), None),
        None => (PathBuf::new(), None),
    };

    let form = {
        let mut form = reqwest::multipart::Form::new();

        for path in paths {
            let relative_path = util::fs::path_relative_to_dir(&path, &repo_or_base_path)?;
            // Skip adding files already present in tree
            if let Some((ref head_commit, local_repo)) = head_commit_local_repo_maybe {
                if let Some(file_node) =
                    repositories::tree::get_file_by_path(local_repo, head_commit, &relative_path)?
                {
                    if !util::fs::is_modified_from_node(&path, &file_node)? {
                        continue;
                    }
                }
            }

            // if it's not the first try
            if !result.err_files.is_empty() {
                // if the file doesn't have a hash it failed, so we need to retry it
                if let Some(hash) = path_to_hash.get(&path) {
                    // check if the file is in the retry list. if not, skip
                    if !retry_hashes.contains(hash) {
                        continue;
                    }
                }
            }

            let Some(_file_name) = path.file_name() else {
                return Err(OxenError::basic_str(format!("Invalid file path: {path:?}")));
            };

            let file = std::fs::read(&path).map_err(|e| {
                OxenError::basic_str(format!("Failed to read file '{path:?}': {e}"))
            })?;

            let hash = hasher::hash_buffer(&file);

            // Workspaces expect just the file name, while remote-mode repos expect the relative path
            // TODO: Refactor this into separate modules later, but for now, remote-mode repos will always have
            //       a local_repo, whereas workspaces will have local_repo be None
            files_to_add.push(FileWithHash {
                hash: hash.clone(),
                path: match local_or_base {
                    Some(_) => relative_path,
                    None => PathBuf::from(path.file_name().unwrap()),
                },
            });

            // gzip the file
            let compressed_bytes: Vec<u8> = {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                std::io::copy(&mut file.as_slice(), &mut encoder)?;
                match encoder.finish() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        log::error!("Failed to finish gzip for file {}: {}", &hash, e);
                        // When uploading to the version store, we use the hash as the file identifier. The path is not needed.
                        err_files.push(ErrorFileInfo {
                            hash: hash.clone(),
                            path: None,
                            error: format!("Failed to finish gzip for file {}: {}", &hash, e),
                        });
                        continue;
                    }
                }
            };

            let file_part = reqwest::multipart::Part::bytes(compressed_bytes)
                .file_name(hash)
                .mime_str("application/gzip")?;

            form = form.part("file[]", file_part);
        }

        form
    };

    let url = api::endpoint::url_from_repo(remote_repo, "/versions")?;
    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;

    let response: ErrorFilesResponse = serde_json::from_str(&body)?;
    log::debug!("workspace_multipart_batch_upload got response: {response:?}");
    err_files.extend(response.err_files);

    Ok(UploadResult {
        files_to_add,
        err_files,
    })
}

pub(crate) async fn workspace_multipart_batch_upload_parts_with_retry(
    remote_repo: &RemoteRepository,
    client: Arc<reqwest::Client>,
    form: reqwest::multipart::Form,
    files_to_retry: &mut Vec<FileWithHash>,
    local_or_base: Option<&LocalOrBase>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    log::debug!("Beginning workspace multipart batch upload");
    let uri = ("/versions").to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let mut retry_count: usize = 1;
    let max_retries = max_retries();

    // Upload the processed version files
    let response = client.post(&url).multipart(form).send().await?;
    let mut upload_result: UploadResult = match client::parse_json_body(&url, response).await {
        Ok(body) => {
            let result: ErrorFilesResponse = serde_json::from_str(&body)?;
            // Remove successfully uploaded files from files_to_retry
            let err_file_hashes: Vec<String> =
                result.err_files.iter().map(|f| f.hash.clone()).collect();
            files_to_retry.retain(|f| err_file_hashes.contains(&f.hash));

            UploadResult {
                files_to_add: vec![],
                err_files: result.err_files,
            }
        }
        Err(e) => {
            log::error!("failed to upload version files: {e:?}");
            UploadResult {
                files_to_add: files_to_retry.clone(),
                err_files: vec![],
            }
        }
    };

    // If files fail, rebuild parts for the err_files and retry
    while (!files_to_retry.is_empty()) && retry_count < max_retries {
        retry_count += 1;

        let paths = files_to_retry.iter().map(|f| f.path.clone()).collect();

        upload_result = match workspace_multipart_batch_upload_versions(
            remote_repo,
            local_or_base,
            client.clone(),
            paths,
            upload_result,
        )
        .await
        {
            Ok(upload_result) => {
                let err_file_hashes: Vec<String> = upload_result
                    .err_files
                    .iter()
                    .map(|f| f.hash.clone())
                    .collect();
                files_to_retry.retain(|f| err_file_hashes.contains(&f.hash));

                upload_result
            }
            Err(e) => {
                // TODO: Consider converting this to a debug log
                log::error!("failed to upload version files after {retry_count} retries: {e:?}");

                // Note: `files_to_add` and `err_files` aren't actually used by
                // workspace_multipart_batch_upload to process retry files differently
                // Hence, these fields can be empty
                UploadResult {
                    files_to_add: vec![],
                    err_files: vec![],
                }
            }
        };

        if !files_to_retry.is_empty() {
            let wait_time = exponential_backoff(BASE_WAIT_TIME, retry_count, MAX_WAIT_TIME);
            sleep(Duration::from_millis(wait_time as u64)).await;
        }
    }

    if !files_to_retry.is_empty() {
        return Err(OxenError::basic_str(format!(
            "Failed to upload version files after {max_retries} retries"
        )));
    }

    Ok(upload_result.err_files)
}

pub fn exponential_backoff(base_wait_time: usize, n: usize, max: usize) -> usize {
    log::debug!(
        "Exponential backoff got called with base_wait_time {base_wait_time}. n {n}, and max {max}"
    );
    (base_wait_time + n.pow(2) + jitter()).min(max)
}

fn jitter() -> usize {
    thread_rng().gen_range(0..=500)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::api;
    use crate::error::OxenError;
    use crate::test;

    #[tokio::test]
    async fn test_upload_large_file_in_chunks() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let path = test::test_30k_parquet();

            // Get original file size
            let metadata = path.metadata().unwrap();
            let original_file_size = metadata.len();

            // Just testing upload, not adding to workspace
            let workspace_id = None;
            let dst_dir: Option<PathBuf> = None;
            let result = api::client::versions::parallel_large_file_upload(
                &remote_repo,
                path,
                dst_dir,
                workspace_id,
                None,
                None,
            )
            .await;
            assert!(result.is_ok());

            let version = api::client::versions::get(&remote_repo, result.unwrap().hash).await?;
            assert!(version.is_some());
            assert_eq!(version.unwrap().size, original_file_size);

            Ok(remote_repo)
        })
        .await
    }
}
