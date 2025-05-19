use crate::api;
use crate::api::client;
use crate::constants::AVG_CHUNK_SIZE;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::{LocalRepository, MerkleHash, RemoteRepository};
use crate::view::versions::{
    CompleteVersionUploadRequest, CompletedFileUpload, MultipartLargeFileUpload,
    MultipartLargeFileUploadStatus, VersionFile, VersionFileResponse,
};
use crate::view::{ErrorFileInfo, FilesHashResponse};

use flate2::write::GzEncoder;
use flate2::Compression;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use http::header::CONTENT_LENGTH;
use rand::{thread_rng, Rng};
use tokio_util::codec::{BytesCodec, FramedRead};

use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;
use tokio::time::sleep;

use crate::util;

// Multipart upload strategy, based off of AWS S3 Multipart Upload and huggingface hf_transfer
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
// https://github.com/huggingface/hf_transfer/blob/main/src/lib.rs#L104
const BASE_WAIT_TIME: usize = 300;
const MAX_WAIT_TIME: usize = 10_000;
const MAX_FILES: usize = 64;
const PARALLEL_FAILURES: usize = 63;
const MAX_RETRIES: usize = 5;

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
    log::debug!("api::client::versions::get {}", url);

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

/// Uploads a large file to the server in parallel and unpacks it in the versions directory
/// Returns the `MultipartLargeFileUpload` struct for the created upload
pub async fn parallel_large_file_upload(
    remote_repo: &RemoteRepository,
    file_path: impl AsRef<Path>,
    dst_dir: Option<impl AsRef<Path>>,
    workspace_id: Option<String>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    log::debug!("multipart_large_file_upload path: {:?}", file_path.as_ref());
    let mut upload = create_multipart_large_file_upload(file_path, dst_dir).await?;
    log::debug!("multipart_large_file_upload upload: {:?}", upload.hash);
    let results = upload_chunks(
        remote_repo,
        &mut upload,
        AVG_CHUNK_SIZE,
        MAX_FILES,
        PARALLEL_FAILURES,
        MAX_RETRIES,
    )
    .await?;
    log::debug!(
        "multipart_large_file_upload results length: {:?}",
        results.len()
    );
    complete_multipart_large_file_upload(remote_repo, upload, results, workspace_id).await
}

/// Creates a new multipart large file upload
/// Will reject the upload if the hash already exists on the server.
/// The rejection helps prevent duplicate uploads or parallel uploads of the same file.
/// Returns the `MultipartLargeFileUpload` struct for the created upload
async fn create_multipart_large_file_upload(
    file_path: impl AsRef<Path>,
    dst_dir: Option<impl AsRef<Path>>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    let file_path = file_path.as_ref();
    let dst_dir = dst_dir.as_ref();

    // Figure out how many parts we need to upload
    let Ok(metadata) = file_path.metadata() else {
        return Err(OxenError::path_does_not_exist(file_path));
    };
    let file_size = metadata.len();
    let hash = MerkleHash::from_str(&util::hasher::hash_file_contents(file_path)?)?;

    Ok(MultipartLargeFileUpload {
        local_path: file_path.to_path_buf(),
        dst_dir: dst_dir.map(|d| d.as_ref().to_path_buf()),
        hash,
        size: file_size,
        status: MultipartLargeFileUploadStatus::Pending,
        reason: None,
    })
}

async fn upload_chunks(
    remote_repo: &RemoteRepository,
    upload: &mut MultipartLargeFileUpload,
    chunk_size: u64,
    max_files: usize,
    parallel_failures: usize,
    max_retries: usize,
) -> Result<Vec<HashMap<String, String>>, OxenError> {
    let file_path = &upload.local_path;
    let client = api::client::builder_for_remote_repo(remote_repo)?.build()?;

    let mut handles = FuturesUnordered::new();
    let semaphore = Arc::new(Semaphore::new(max_files));
    let parallel_failures_semaphore = Arc::new(Semaphore::new(parallel_failures));

    // Figure out how many parts we need to upload
    let Ok(metadata) = file_path.metadata() else {
        return Err(OxenError::path_does_not_exist(file_path));
    };
    let file_size = metadata.len();
    let num_chunks = file_size.div_ceil(chunk_size);

    for chunk_number in 0..num_chunks {
        let remote_repo = remote_repo.clone();
        let upload = upload.clone();
        let client = client.clone();

        let start = chunk_number * chunk_size;
        let semaphore = semaphore.clone();
        let parallel_failures_semaphore = parallel_failures_semaphore.clone();
        handles.push(tokio::spawn(async move {
                    let permit = semaphore
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|err| OxenError::basic_str(format!("Error acquiring semaphore: {err}")))?;
                    let mut chunk = upload_chunk(&client, &remote_repo, &upload, chunk_number, start, chunk_size).await;
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

                            chunk = upload_chunk(&client, &remote_repo, &upload, chunk_number, start, chunk_size).await;
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

    Ok(results)
}

async fn upload_chunk(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    upload: &MultipartLargeFileUpload,
    chunk_number: u64,
    start: u64,
    chunk_size: u64,
) -> Result<HashMap<String, String>, OxenError> {
    let path = &upload.local_path;
    let mut options = OpenOptions::new();
    let mut file = options.read(true).open(path).await?;
    let file_size = file.metadata().await?.len();
    let bytes_transferred = std::cmp::min(file_size - start, chunk_size);

    file.seek(SeekFrom::Start(start)).await?;
    let chunk = file.take(chunk_size);

    let file_hash = &upload.hash.to_string();

    let uri = format!("/versions/{file_hash}/chunks/{chunk_number}");
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
                .map_err(|e| OxenError::basic_str(format!("Invalid header value: {}", e)))?
                .to_owned(),
        );
    }
    Ok(headers)
}

async fn complete_multipart_large_file_upload(
    remote_repo: &RemoteRepository,
    upload: MultipartLargeFileUpload,
    results: Vec<HashMap<String, String>>,
    workspace_id: Option<String>,
) -> Result<MultipartLargeFileUpload, OxenError> {
    let file_hash = &upload.hash.to_string();

    let uri = format!("/versions/{file_hash}/complete");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("complete_multipart_large_file_upload {}", url);
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
            upload_results: results,
        }],
        workspace_id,
    };

    let body = serde_json::to_string(&body)?;
    let response = client.post(&url).body(body).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    log::debug!("complete_multipart_large_file_upload got body: {}", body);
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
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let mut files_to_retry: Vec<ErrorFileInfo> = vec![];
    let mut first_try = true;
    let mut retry_count: usize = 0;

    while (first_try || !files_to_retry.is_empty()) && retry_count < MAX_RETRIES {
        first_try = false;
        retry_count += 1;

        files_to_retry =
            multipart_batch_upload(local_repo, remote_repo, chunk, client, files_to_retry).await?;

        if !files_to_retry.is_empty() {
            let wait_time = exponential_backoff(BASE_WAIT_TIME, retry_count, MAX_WAIT_TIME);
            sleep(Duration::from_millis(wait_time as u64)).await;
        }
    }
    Ok(files_to_retry)
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
    let retry_hashes: std::collections::HashSet<String> = if files_to_retry.is_empty() {
        std::collections::HashSet::new()
    } else {
        files_to_retry.iter().map(|f| f.hash.clone()).collect()
    };

    for entry in chunk {
        let file_hash = entry.hash();

        // if it's not the first try and the file is not in the retry list, skip
        if !files_to_retry.is_empty() && !retry_hashes.contains(&file_hash) {
            continue;
        }

        let reader = version_store.open_version(&file_hash)?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        let mut buf_reader = std::io::BufReader::new(reader);
        std::io::copy(&mut buf_reader, &mut encoder)?;
        let compressed_bytes = match encoder.finish() {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Failed to finish gzip for file {}: {}", &file_hash, e);
                err_files.push(ErrorFileInfo {
                    hash: file_hash.clone(),
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
    let uri = ("/versions").to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let response = client.post(&url).multipart(form).send().await?;
    let body = client::parse_json_body(&url, response).await?;
    let response: FilesHashResponse = serde_json::from_str(&body)?;

    err_files.extend(response.err_files);

    Ok(err_files)
}

pub fn exponential_backoff(base_wait_time: usize, n: usize, max: usize) -> usize {
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
