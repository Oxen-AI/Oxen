use crate::api;
use crate::api::client;
use crate::constants::max_retries;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository, MerkleHash, RemoteRepository};
use crate::util::concurrency;
use crate::view::versions::{
    BeginVersionUploadRequest, CleanCorruptedVersionsResponse, VersionFile, VersionFileResponse,
    VersionUploadPartResponse, VersionUploadResponse, VersionUploadStart, WorkspaceDestination,
};
use crate::view::{ErrorFileInfo, ErrorFilesResponse};

use crate::core::progress::push_progress::PushProgress;
use async_compression::tokio::bufread::GzipDecoder;
use flate2::Compression;
use flate2::write::GzEncoder;
use futures_util::StreamExt;
use futures_util::stream;
use http::header::{CONTENT_LENGTH, RETRY_AFTER};
use http::{Method, StatusCode};
use rand::{RngExt, rng};
use tokio_tar::Archive;
use tokio_util::io::ReaderStream;

use std::collections::HashSet;
use std::io::{SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::Semaphore;
use tokio::time::sleep;

// Multipart upload strategy, based off of AWS S3 Multipart Upload and huggingface hf_transfer
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
// https://github.com/huggingface/hf_transfer/blob/main/src/lib.rs#L104
const BASE_WAIT_TIME: usize = 300;
const MAX_WAIT_TIME: usize = 10_000;

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

/// Uploads the large file at `file_path`, whose content hashes to `hash`, in parts, and stages it at
/// `destination` when one is given. A file the server already has is staged without sending it.
pub async fn parallel_large_file_upload(
    remote_repo: &RemoteRepository,
    file_path: &Path,
    hash: MerkleHash,
    destination: Option<WorkspaceDestination>,
    progress: Option<&Arc<PushProgress>>,
) -> Result<(), OxenError> {
    let size = file_path
        .metadata()
        .map_err(|_| OxenError::path_does_not_exist(file_path))?
        .len();
    let client = api::client::new_for_remote_repo(remote_repo)?;
    let url = api::endpoint::url_from_repo(remote_repo, "/versions/uploads")?;
    let request = BeginVersionUploadRequest {
        hash: hash.to_string(),
        size,
        destination,
    };

    // Reports bytes to `progress`, never more than the file's size, so the parts an upload begun
    // again sends a second time are not counted twice.
    let mut reported = 0;
    let mut report = |len: u64| {
        if let Some(progress) = progress {
            let len = len.min(size - reported);
            progress.add_bytes(len);
            reported += len;
        }
    };

    let attempts = max_retries() + 1;
    let mut completed = false;
    for _ in 0..attempts {
        let response =
            send_retrying(|| async { Ok(client.post(&url).json(&request).send().await?) }).await?;
        let body = client::parse_json_body(&url, response).await?;
        let begun: VersionUploadResponse = serde_json::from_str(&body)?;
        let (upload_id, part_size, num_parts, part_window) = match begun.file {
            VersionUploadStart::AwaitingParts {
                upload_id,
                part_size,
                num_parts,
                part_window,
            } => (upload_id, part_size, num_parts, part_window),
            VersionUploadStart::AlreadyStored => {
                completed = true;
                break;
            }
            VersionUploadStart::Unknown => {
                return Err(OxenError::internal_error(format!(
                    "{url} began an upload this client does not recognize: {body}"
                )));
            }
        };

        let parts_url = api::endpoint::url_from_repo(
            remote_repo,
            &format!("/versions/uploads/{upload_id}/parts"),
        )?;
        let requests = Semaphore::new(concurrency::num_threads_for_items(num_parts as usize));
        let mut parts = stream::iter(1..=num_parts)
            .map(|part_number| {
                let offset = (part_number - 1) * part_size;
                let len = part_size.min(size - offset);
                let url = format!("{parts_url}/{part_number}");
                let (client, requests) = (&client, &requests);
                async move {
                    let _request = requests.acquire().await.map_err(|err| {
                        OxenError::internal_error(format!("Failed to wait for a request: {err}"))
                    })?;
                    send_part(client, &url, file_path, offset, len)
                        .await
                        .map(|sent| (sent, len))
                }
            })
            // The server accepts a part at most `part_window` parts past the first it has not
            // hashed, which is never past the first part still in flight.
            .buffered(part_window.max(1) as usize);
        while let Some(sent) = parts.next().await {
            let (sent, len) = sent?;
            match sent {
                PartSent::Stored => report(len),
                PartSent::Completed => {
                    completed = true;
                    break;
                }
                PartSent::UploadGone => break,
            }
        }
        if completed {
            break;
        }
        // The server no longer has the upload: it completed without a response reaching us, or it
        // was aborted. Beginning again tells the two apart.
        log::debug!("Beginning the upload of {hash} again");
    }
    if !completed {
        return Err(OxenError::internal_error(format!(
            "{url} did not complete the upload of {hash} after {attempts} attempts"
        )));
    }
    report(size);
    if let Some(progress) = progress {
        progress.add_files(1);
    }
    Ok(())
}

/// What sending one part of an upload did.
#[derive(Debug, PartialEq, Eq)]
enum PartSent {
    /// The server stored the part, and waits for others.
    Stored,
    /// The part completed the upload.
    Completed,
    /// The server no longer has the upload.
    UploadGone,
}

/// Send the `len` bytes of `file_path` at `offset` as the part at `url`.
async fn send_part(
    client: &reqwest::Client,
    url: &str,
    file_path: &Path,
    offset: u64,
    len: u64,
) -> Result<PartSent, OxenError> {
    // A part sent again is ignored once the server stores it, so a part whose response is lost
    // partway through is sent again to learn what became of it.
    for _ in 0..=max_retries() {
        let response = send_retrying(|| async {
            let mut file = tokio::fs::File::open(file_path).await?;
            file.seek(SeekFrom::Start(offset)).await?;
            let body = reqwest::Body::wrap_stream(ReaderStream::with_capacity(
                file.take(len),
                1024 * 1024,
            ));
            Ok(client
                .put(url)
                .header(CONTENT_LENGTH, len)
                .body(body)
                .send()
                .await?)
        })
        .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(PartSent::UploadGone);
        }
        let body = match client::parse_json_body(url, response).await {
            Err(OxenError::HTTP(err)) if err.is_body() || err.is_decode() => {
                log::warn!("Lost the response for the part at {url}: {err}");
                continue;
            }
            body => body?,
        };
        let stored: VersionUploadPartResponse = serde_json::from_str(&body)?;
        return Ok(if stored.completed {
            PartSent::Completed
        } else {
            PartSent::Stored
        });
    }
    Err(OxenError::internal_error(format!(
        "Lost every response for the part at {url}"
    )))
}

/// Send the request `send` makes, and send it again when it fails to send or gets a response that
/// may clear up on its own (408, 425, 429, or a 5xx other than 501 and 505), up to `max_retries` times.
/// Each retry waits for the response's `Retry-After`, or else a backoff. Returns the last response.
async fn send_retrying<F, Fut>(mut send: F) -> Result<reqwest::Response, OxenError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<reqwest::Response, OxenError>>,
{
    let max_retries = max_retries();
    let transient = |status: StatusCode| match status {
        StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS => true,
        StatusCode::NOT_IMPLEMENTED | StatusCode::HTTP_VERSION_NOT_SUPPORTED => false,
        status => status.is_server_error(),
    };
    let mut attempt = 0;
    loop {
        let sent = send().await;
        let retry_after = match &sent {
            Ok(response) if !transient(response.status()) => return sent,
            Ok(response) => response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|seconds| seconds.parse().ok())
                .map(Duration::from_secs),
            Err(err) => {
                log::warn!("Failed to send a request, attempt {attempt}: {err}");
                None
            }
        };
        if attempt >= max_retries {
            return sent;
        }
        let backoff = || {
            Duration::from_millis(exponential_backoff(BASE_WAIT_TIME, attempt, MAX_WAIT_TIME) as u64)
        };
        sleep(retry_after.unwrap_or_else(backoff)).await;
        attempt += 1;
    }
}

/// Batch download.
///
/// `entries` is a list of `(content_hash, source_path)` pairs — only the hash is sent over the wire
/// (the bulk endpoint identifies blobs by hash); the path is preserved purely for diagnostic
/// surfaces (errors, logs) so end users can identify which file(s) failed.
#[tracing::instrument(skip_all)]
pub async fn download_data_from_version_paths(
    remote_repo: &RemoteRepository,
    entries: &[(String, PathBuf)],
    local_repo: &LocalRepository,
) -> Result<u64, OxenError> {
    let total_retries = max_retries().try_into().unwrap_or(max_retries() as u64);
    let mut num_retries = 0;
    let mut last_err: Option<OxenError> = None;

    while num_retries < total_retries {
        match try_download_data_from_version_paths(remote_repo, entries, local_repo).await {
            Ok(val) => return Ok(val),
            // Short-circuit on errors that won't change on retry (auth failures, 4xx
            // responses, server-confirmed missing blobs). Without this, a doomed pull
            // pays the full exponential backoff before surfacing the diagnostic.
            Err(err) if err.is_fatal_for_retry() => return Err(err),
            Err(err) => {
                num_retries += 1;
                // Exponentially back off
                let sleep_time = num_retries * num_retries;
                log::warn!("Could not download content {err:?} sleeping {sleep_time}");
                last_err = Some(err);
                tokio::time::sleep(std::time::Duration::from_secs(sleep_time)).await;
            }
        }
    }

    // Preserve the failing batch's entries (path + hash) and last underlying error so callers
    // and server logs can identify which file(s) the server couldn't serve. Without this, the
    // symptom surfaces as a bare "failed to download N files" with no way to map back to
    // specific files.
    let last_error = last_err
        .as_ref()
        .map(|e| format!("{e}"))
        .unwrap_or_else(|| "(no attempts made)".to_string());
    let formatted_entries = entries
        .iter()
        .map(|(h, p)| format!("{} (hash: {h})", p.display()))
        .collect::<Vec<_>>()
        .join(", ");
    log::error!(
        "Bulk download exhausted {} retries for {} entries: [{}]. Last error: {}",
        total_retries,
        entries.len(),
        formatted_entries,
        last_error,
    );
    Err(OxenError::DownloadBatchExhausted {
        num_files: entries.len(),
        num_retries: total_retries,
        entries: entries.to_vec(),
        last_error,
    })
}

#[tracing::instrument(skip_all)]
pub async fn try_download_data_from_version_paths(
    remote_repo: &RemoteRepository,
    entries: &[(String, PathBuf)],
    local_repo: &LocalRepository,
) -> Result<u64, OxenError> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for (hash, _path) in entries.iter() {
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
        let res = client::handle_non_json_response(&url, res).await?;

        let stream = res.bytes_stream();
        let reader = tokio_util::io::StreamReader::new(
            stream.map(|result| result.map_err(std::io::Error::other)),
        );
        let buf_reader = tokio::io::BufReader::new(reader);
        let decoder = GzipDecoder::new(buf_reader);
        let mut archive = Archive::new(decoder);

        let version_store = local_repo.version_store();
        let mut size: u64 = 0;

        // Iterate over archive entries and stream them to version store
        let mut entries = archive.entries()?;
        while let Some(file) = entries.next().await {
            let file = match file {
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
            let file_size = file.header().raw_file_size()?;
            size += file_size;

            // Stream the file content directly to version store without loading into memory
            match version_store
                .store_version_from_reader(&file_hash, Box::new(file), file_size)
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

/// Multipart batch upload with retry
/// Uploads a batch of small files to the server in parallel and retries on failure
/// Returns a list of files that failed to upload
pub async fn multipart_batch_upload_with_retry(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    chunk: &[CommitEntry],
    client: &reqwest::Client,
) -> Result<(), OxenError> {
    let mut files_to_retry: Vec<ErrorFileInfo> = vec![];
    let mut attempt: usize = 0;
    let max_retries = max_retries();
    // Held so an exhausted retry budget reports the transport failure that actually stopped us,
    // rather than an empty per-file list.
    let mut transport_err: Option<OxenError> = None;

    while attempt < max_retries {
        attempt += 1;

        match multipart_batch_upload(
            local_repo,
            remote_repo,
            chunk,
            client,
            files_to_retry.clone(),
        )
        .await
        {
            Ok(err_files) => {
                transport_err = None;
                files_to_retry = err_files;
                if files_to_retry.is_empty() {
                    return Ok(());
                }
            }
            // The batch never reached the server, or its response was lost. Re-post the same set:
            // version files are content-addressed, so the server treats a repeat as a no-op. Without
            // this the whole push dies on one dropped connection, and a push fans out to many
            // concurrent batches, so a small per-connection failure rate fails most pushes.
            Err(err) if !err.is_fatal_for_retry() => {
                log::warn!(
                    "versions batch upload attempt {attempt} of {max_retries} failed: {err}"
                );
                transport_err = Some(err);
            }
            Err(err) => return Err(err),
        }

        if attempt < max_retries {
            let wait_time = exponential_backoff(BASE_WAIT_TIME, attempt, MAX_WAIT_TIME);
            sleep(Duration::from_millis(wait_time as u64)).await;
        }
    }

    match transport_err {
        Some(err) => Err(err),
        None => Err(OxenError::basic_str(format!(
            "Failed to upload files: {files_to_retry:#?}"
        ))),
    }
}

pub async fn multipart_batch_upload(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    chunk: &[CommitEntry],
    client: &reqwest::Client,
    files_to_retry: Vec<ErrorFileInfo>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let version_store = local_repo.version_store();
    let mut form = reqwest::multipart::Form::new();
    let mut err_files: Vec<ErrorFileInfo> = vec![];

    // if it's the first try, we don't have any files to retry
    let retry_hashes: HashSet<String> = if files_to_retry.is_empty() {
        HashSet::new()
    } else {
        files_to_retry.iter().map(|f| f.hash.clone()).collect()
    };

    for commit_entry in chunk {
        let file_hash = &commit_entry.hash;

        // if it's not the first try and the file is not in the retry list, skip
        if !files_to_retry.is_empty() && !retry_hashes.contains(file_hash) {
            continue;
        }

        let data = version_store.get_version(file_hash).await?;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        std::io::copy(&mut data.as_slice(), &mut encoder)?;
        let compressed_bytes = match encoder.finish() {
            Ok(bytes) => bytes,
            Err(e) => {
                log::error!("Failed to finish gzip for file {}: {}", file_hash, e);
                err_files.push(ErrorFileInfo {
                    hash: file_hash.clone(),
                    path: None,
                    error: format!("Failed to finish gzip for file {}: {}", file_hash, e),
                });
                continue;
            }
        };

        let file_part = reqwest::multipart::Part::bytes(compressed_bytes)
            .file_name(commit_entry.hash.clone())
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

pub fn exponential_backoff(base_wait_time: usize, n: usize, max: usize) -> usize {
    log::debug!(
        "Exponential backoff got called with base_wait_time {base_wait_time}. n {n}, and max {max}"
    );
    (base_wait_time + n.pow(2) + jitter()).min(max)
}

fn jitter() -> usize {
    rng().random_range(0..=500)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::api;
    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::test;
    use crate::util::hasher::u128_hash_file_contents;

    use super::{PartSent, send_part};

    #[tokio::test]
    async fn test_send_part_waits_out_a_429_and_reports_a_gone_upload() -> Result<(), OxenError> {
        let mut server = mockito::Server::new_async().await;
        let deferred = server
            .mock("PUT", "/parts/1")
            .with_status(429)
            .with_header("retry-after", "0")
            .create_async()
            .await;
        let completed = server
            .mock("PUT", "/parts/1")
            .with_body(
                r#"{"status":"success","status_message":"resource_created","completed":true}"#,
            )
            .create_async()
            .await;
        let gone = server
            .mock("PUT", "/parts/2")
            .with_status(404)
            .create_async()
            .await;
        let cut_off = server
            .mock("PUT", "/parts/3")
            .with_chunked_body(|body| {
                body.write_all(br#"{"status":"success""#)?;
                Err(std::io::Error::other("the connection drops"))
            })
            .create_async()
            .await;
        let resent = server
            .mock("PUT", "/parts/3")
            .with_body(
                r#"{"status":"success","status_message":"resource_created","completed":false}"#,
            )
            .create_async()
            .await;
        // Reading the system's proxy settings takes most of a quarter second on macOS.
        let client = reqwest::Client::builder().no_proxy().build()?;
        let file = test::test_30k_parquet();

        let part_1 = format!("{}/parts/1", server.url());
        assert_eq!(
            send_part(&client, &part_1, &file, 0, 10).await?,
            PartSent::Completed,
            "a part refused with 429 is sent again after its Retry-After"
        );
        let part_2 = format!("{}/parts/2", server.url());
        assert_eq!(
            send_part(&client, &part_2, &file, 10, 10).await?,
            PartSent::UploadGone
        );
        let part_3 = format!("{}/parts/3", server.url());
        assert_eq!(
            send_part(&client, &part_3, &file, 20, 10).await?,
            PartSent::Stored,
            "a part whose response is cut off partway through is sent again"
        );
        deferred.assert_async().await;
        completed.assert_async().await;
        gone.assert_async().await;
        cut_off.assert_async().await;
        resent.assert_async().await;
        Ok(())
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_upload_in_parts_and_bulk_download() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let path = test::test_30k_parquet();
            let hash = MerkleHash::new(u128_hash_file_contents(&path)?);
            api::client::versions::parallel_large_file_upload(
                &remote_repo,
                &path,
                hash,
                None,
                None,
            )
            .await?;

            let version = api::client::versions::get(&remote_repo, hash).await?;
            assert_eq!(
                version.map(|version| version.size),
                Some(path.metadata()?.len()),
                "the server stores the whole file"
            );

            // Asked for a hash the server does not have, the bulk download's pre-flight check returns
            // a structured 404 and the client's retry loop stops rather than backing off and retrying.
            // A well-formed 32-char hex string that can't possibly exist on the server.
            let bogus_hash = "deadbeefdeadbeefdeadbeefdeadbeef".to_string();
            let entries = vec![(bogus_hash.clone(), PathBuf::from("does-not-exist.txt"))];

            let result = api::client::versions::download_data_from_version_paths(
                &remote_repo,
                &entries,
                &local_repo,
            )
            .await;

            let err = result.expect_err("expected error for missing hash");
            // The short-circuit returns the underlying fatal error directly — *not*
            // DownloadBatchExhausted, which is only emitted after the retry loop runs
            // out of attempts. Seeing DownloadBatchExhausted here would mean the loop
            // retried on a 4xx and slept its way through backoff.
            assert!(
                !matches!(&err, OxenError::DownloadBatchExhausted { .. }),
                "should have short-circuited on the 4xx instead of exhausting retries: {err:?}"
            );
            assert!(
                err.is_fatal_for_retry(),
                "missing-blob error should classify as fatal: {err:?}"
            );

            // The rendered error names the missing hash so the user can map it back to
            // the broken blob. We assert on the surface message rather than the variant
            // shape since the server's wire-format may evolve.
            let rendered = err.to_string();
            assert!(
                rendered.contains(&bogus_hash),
                "error should name the missing hash; got: {rendered}"
            );

            Ok(remote_repo)
        })
        .await
    }
}
