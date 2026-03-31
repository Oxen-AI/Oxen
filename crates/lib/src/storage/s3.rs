use crate::error::OxenError;
use async_tempfile::TempFile;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use bytes::Bytes;
use futures::TryStreamExt;
use futures::stream::StreamExt;
use log;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::OnceCell;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;

use super::version_store::{LocalFilePath, VersionStore};
use crate::constants::VERSION_FILE_NAME;
use crate::view::versions::CleanCorruptedVersionsResult;

/// S3 implementation of version storage
#[derive(Debug)]
pub struct S3VersionStore {
    client: OnceCell<Result<Arc<Client>, OxenError>>,
    bucket: String,
    prefix: String,
}

impl S3VersionStore {
    /// Create a new S3VersionStore
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `prefix` - Prefix for all objects in the bucket
    pub fn new(bucket: impl Into<String>, prefix: impl Into<String>) -> Self {
        Self {
            client: OnceCell::new(),
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    pub async fn init_client(&self) -> Result<Arc<Client>, OxenError> {
        let result_ref = self
            .client
            .get_or_init(|| async {
                // Create a temp client to get the bucket region
                let region_provider = RegionProviderChain::default_provider().or_else("us-west-1");

                let base_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(region_provider)
                    .load()
                    .await;
                let tmp_client = Client::new(&base_config);

                let detected_region = tmp_client
                    .get_bucket_location()
                    .bucket(&self.bucket)
                    .send()
                    .await
                    .map_err(|err| {
                        OxenError::basic_str(format!("Failed to get bucket location: {err:?}"))
                    })?
                    .location_constraint()
                    .map(|loc| loc.as_str().to_string())
                    .unwrap_or("us-east-1".to_string());

                // Construct the client with the detected bucket region
                let real_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(Region::new(detected_region))
                    .load()
                    .await;

                Ok::<Arc<Client>, OxenError>(Arc::new(Client::new(&real_config)))
            })
            .await;

        match result_ref {
            Ok(client) => Ok(client.clone()),
            Err(e) => Err(OxenError::basic_str(format!("{e:?}"))),
        }
    }

    /// Get the directory containing a version file
    fn version_dir(&self, hash: &str) -> String {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        format!("{}/{}/{}", self.prefix, topdir, subdir)
    }

    /// Get the full path for a version file
    fn generate_key(&self, hash: &str) -> String {
        format!("{}/{}", self.version_dir(hash), VERSION_FILE_NAME)
    }
}

#[async_trait]
impl VersionStore for S3VersionStore {
    async fn init(&self) -> Result<(), OxenError> {
        let client = self.init_client().await?;

        // Check permission to write to S3
        match client.head_bucket().bucket(&self.bucket).send().await {
            Ok(result) => {
                log::debug!("Successfully got S3 bucket {result:?}");
                let test_key = format!("{}/_permission_check", self.prefix);
                let body = ByteStream::from("permission-check".as_bytes().to_vec());

                match client
                    .put_object()
                    .bucket(&self.bucket)
                    .key(&test_key)
                    .body(body)
                    .send()
                    .await
                {
                    Ok(_) => {
                        client
                            .delete_object()
                            .bucket(&self.bucket)
                            .key(&test_key)
                            .send()
                            .await
                            .map_err(|err| {
                                OxenError::basic_str(format!(
                                    "Failed to delete _permission_check: {err}"
                                ))
                            })?;
                        Ok(())
                    }
                    // Surface the error from S3
                    Err(err) => Err(OxenError::basic_str(format!(
                        "S3 write permission check failed: {err}",
                    ))),
                }
            }
            Err(err) => Err(OxenError::basic_str(format!(
                "Cannot access S3 bucket '{}': {err}",
                self.bucket
            ))),
        }
    }

    async fn store_version_from_path(&self, hash: &str, file_path: &Path) -> Result<(), OxenError> {
        // get the client
        let client = self.init_client().await?;
        // get file content from the path
        let mut file = std::fs::File::open(file_path).map_err(|e| {
            OxenError::basic_str(format!("Failed to open file {}: {e}", file_path.display()))
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| {
            OxenError::basic_str(format!("Failed to read file {}: {e}", file_path.display()))
        })?;

        let key = self.generate_key(hash);
        let body = ByteStream::from(buffer);
        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|e| OxenError::basic_str(format!("Failed to store version in S3: {e}")))?;

        Ok(())
    }

    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        // S3 multipart upload requires 5MB minimum per part (except the last).
        // We use 8MB parts for a balance of memory usage and request count.
        const PART_SIZE: usize = 8 * 1024 * 1024;

        let mut reader = tokio::io::BufReader::new(reader);

        // Read the first part to determine if we need multipart upload
        let mut first_buf = vec![0u8; PART_SIZE];
        let first_n = read_full(&mut reader, &mut first_buf).await?;
        first_buf.truncate(first_n);

        if first_n < PART_SIZE {
            // Small file: single put_object
            let body = ByteStream::from(first_buf);
            client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(body)
                .send()
                .await
                .map_err(OxenError::aws_sdk_error)?;
            return Ok(());
        }

        // Large file: multipart upload
        let upload = client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(OxenError::aws_sdk_error)?;

        let upload_id = upload
            .upload_id()
            .ok_or_else(|| OxenError::upload("S3 multipart upload missing upload_id"))?
            .to_string();

        // Pipeline reading and uploading: read parts on this task and send them through a bounded
        // channel. A spawned task consumes the channel and uploads parts concurrently via
        // buffer_unordered. The bounded channel keeps at most MAX_PARTS_WAITING_FOR_UPLOAD parts in
        // memory.
        const MAX_CONCURRENT_UPLOADS: usize = 8;
        const MAX_PARTS_WAITING_FOR_UPLOAD: usize = 16;

        let (tx, rx) = tokio::sync::mpsc::channel::<(i32, Vec<u8>)>(MAX_PARTS_WAITING_FOR_UPLOAD);

        // Spawn the upload consumer on a separate tokio task. It starts executing immediately--you
        // don't have to call `.await` on the join handle first like you do with a regular future.
        //
        // This runs concurrently with the producer loop below, connected via the bounded channel.
        // Whatever the producer sends, the upload task will upload to S3, concurrently to the
        // producer because it's in a different task, and concurrently with up to
        // MAX_CONCURRENT_UPLOADS because buffer_unordered uses that many worker tasks.
        let upload_client = client.clone();
        let upload_bucket = self.bucket.clone();
        let upload_key = key.clone();
        let upload_id_clone = upload_id.clone();

        let upload_task_join_handle = tokio::spawn(async move {
            let results: Result<Vec<CompletedPart>, OxenError> = ReceiverStream::new(rx)
                .map(|(part_num, data)| {
                    let client = upload_client.clone();
                    let bucket = upload_bucket.clone();
                    let key = upload_key.clone();
                    let upload_id = upload_id_clone.clone();
                    async move {
                        let resp = client
                            .upload_part()
                            .bucket(bucket)
                            .key(key)
                            .upload_id(upload_id)
                            .part_number(part_num)
                            .body(ByteStream::from(data))
                            .send()
                            .await
                            .map_err(OxenError::aws_sdk_error)?;

                        let etag = resp.e_tag().map(|s| s.to_string()).ok_or_else(|| {
                            OxenError::upload("S3 upload_part response missing ETag")
                        })?;

                        Ok(CompletedPart::builder()
                            .part_number(part_num)
                            .e_tag(etag)
                            .build())
                    }
                })
                .buffer_unordered(MAX_CONCURRENT_UPLOADS)
                .try_collect()
                .await;
            results
        });

        // Producer: read parts and feed them into the channel. This loop happens on this task.
        let producer_result: Result<(), OxenError> = async {
            let mut part_num: i32 = 1;
            tx.send((part_num, first_buf)).await.map_err(|_| {
                OxenError::upload("Upload task terminated while sending first part")
            })?;
            part_num += 1;

            loop {
                let mut buf = vec![0u8; PART_SIZE];
                let n = read_full(&mut reader, &mut buf).await?;
                if n == 0 {
                    // We have reached EOF
                    break;
                }
                buf.truncate(n);
                tx.send((part_num, buf))
                    .await
                    .map_err(|_| OxenError::upload("Upload task terminated while sending parts"))?;
                part_num += 1;
            }

            Ok(())
        }
        .await;

        // Close the channel so the upload task finishes draining any buffered parts.
        drop(tx);

        let result = match producer_result {
            // The producer task completed reading the file and sent all parts to the upload task,
            // so wait for the upload task to finish next.
            Ok(()) => {
                match upload_task_join_handle
                    .await
                    .map_err(|e| OxenError::upload(&format!("Upload task panicked: {e}")))
                {
                    Ok(Ok(mut completed_parts)) => {
                        completed_parts.sort_by_key(|p| p.part_number);

                        let completed = CompletedMultipartUpload::builder()
                            .set_parts(Some(completed_parts))
                            .build();

                        client
                            .complete_multipart_upload()
                            .bucket(&self.bucket)
                            .key(&key)
                            .upload_id(&upload_id)
                            .multipart_upload(completed)
                            .send()
                            .await
                            .map_err(OxenError::aws_sdk_error)
                            .map(|_| ())
                    }
                    Ok(Err(e)) | Err(e) => Err(e),
                }
            }
            // The producer task wasn't able to successfully read the file and send the parts to the
            // upload task, so we ensure the upload task is cancelled and awaited so that no
            // in-flight upload_part calls race with abort_multipart_upload below.
            Err(producer_err) => {
                upload_task_join_handle.abort();
                let _ = upload_task_join_handle.await;
                Err(producer_err)
            }
        };

        if let Err(ref e) = result {
            log::error!("Multipart upload failed, aborting: {e}");
            let _ = client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
        }

        result
    }

    async fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        log::debug!("Storing version to S3");
        let key = self.generate_key(hash);

        let body = ByteStream::from(data.to_vec());
        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .map_err(|_| OxenError::Basic("failed to store version in S3".into()))?;

        Ok(())
    }

    async fn store_version_derived(
        &self,
        orig_hash: &str,
        derived_filename: &str,
        derived_data: &[u8],
    ) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let key = format!("{}/{}", self.version_dir(orig_hash), derived_filename);

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(derived_data.to_vec()))
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!("failed to store derived version file in S3: {e}"))
            })?;
        log::debug!("Saved derived version file {key}");

        Ok(())
    }

    async fn get_version_size(&self, hash: &str) -> Result<u64, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let resp = client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| OxenError::basic_str(format!("S3 head_object failed: {e}")))?;

        let size = resp
            .content_length()
            .ok_or_else(|| OxenError::basic_str("S3 object missing content_length"))?
            as u64;
        Ok(size)
    }

    async fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| OxenError::basic_str(format!("S3 get_object failed: {e}")))?;

        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| OxenError::basic_str(format!("S3 read body failed: {e}")))?
            .into_bytes()
            .to_vec();

        Ok(data)
    }

    async fn get_version_derived_size(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<u64, OxenError> {
        let client = self.init_client().await?;
        let key = format!("{}/{}", self.version_dir(orig_hash), derived_filename);

        let resp = client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| OxenError::basic_str(format!("S3 head_object failed: {e}")))?;

        let size = resp
            .content_length()
            .ok_or_else(|| OxenError::basic_str("S3 object missing content_length"))?
            as u64;
        Ok(size)
    }

    async fn get_version_stream(
        &self,
        hash: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| OxenError::basic_str(format!("S3 get_object failed: {e}")))?;

        let adapter = ByteStreamAdapter { inner: resp.body };

        Ok(Box::new(adapter) as Box<_>)
    }

    async fn get_version_derived_stream(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let client = self.init_client().await?;
        let key = format!("{}/{}", self.version_dir(orig_hash), derived_filename);

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| OxenError::basic_str(format!("S3 get_object failed: {e}")))?;

        let adapter = ByteStreamAdapter { inner: resp.body };
        Ok(Box::new(adapter) as Box<_>)
    }

    async fn derived_version_exists(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<bool, OxenError> {
        let client = self.init_client().await?;
        let key = format!("{}/{}", self.version_dir(orig_hash), derived_filename);

        match client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),

            Err(SdkError::ServiceError(err)) => match err.err() {
                HeadObjectError::NotFound(_) => Ok(false),
                err => Err(OxenError::basic_str(format!(
                    "derived_exists failed with S3 head_object error: {err:?}"
                ))),
            },

            Err(err) => Err(OxenError::basic_str(format!(
                "derived_exists failed with S3 head_object error: {err:?}"
            ))),
        }
    }

    async fn get_version_path(&self, hash: &str) -> Result<LocalFilePath, OxenError> {
        // TODO: This needs to be updated to handle large files that won't fit in memory
        let data = self.get_version(hash).await?;
        let mut tmp = TempFile::new()
            .await
            .map_err(|e| OxenError::basic_str(format!("Failed to create temp file: {e}")))?;
        tmp.write_all(&data)
            .await
            .map_err(|e| OxenError::basic_str(format!("Failed to write temp file: {e}")))?;
        Ok(LocalFilePath::Temp(tmp))
    }

    async fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError> {
        // TODO: This needs to be updated to handle large files that won't fit in memory
        let data = self.get_version(hash).await?;
        if let Some(parent) = dest_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| OxenError::basic_str(format!("Failed to create parent dirs: {e}")))?;
        }
        tokio::fs::write(dest_path, &data)
            .await
            .map_err(|e| OxenError::basic_str(format!("Failed to write file: {e}")))?;
        Ok(())
    }

    async fn store_version_chunk(
        &self,
        _hash: &str,
        _offset: u64,
        _data: &[u8],
    ) -> Result<(), OxenError> {
        // TODO: Implement S3 version chunk storage
        Err(OxenError::basic_str(
            "S3VersionStore store_version_chunk not yet implemented",
        ))
    }

    async fn get_version_chunk_writer(
        &self,
        _hash: &str,
        _offset: u64,
    ) -> Result<Box<dyn tokio::io::AsyncWrite + Send + Unpin>, OxenError> {
        // TODO: Implement S3 version chunk stream storage
        Err(OxenError::basic_str(
            "S3VersionStore get_version_chunk_writer not yet implemented",
        ))
    }

    async fn get_version_chunk(
        &self,
        _hash: &str,
        _offset: u64,
        _size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        // TODO: Implement S3 version chunk retrieval
        Err(OxenError::basic_str(
            "S3VersionStore get_version_chunk not yet implemented",
        ))
    }

    async fn list_version_chunks(&self, _hash: &str) -> Result<Vec<u64>, OxenError> {
        // TODO: Implement S3 version chunk listing
        Err(OxenError::basic_str(
            "S3VersionStore list_version_chunks not yet implemented",
        ))
    }

    async fn version_exists(&self, hash: &str) -> Result<bool, OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        match client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),

            Err(SdkError::ServiceError(err)) => match err.err() {
                HeadObjectError::NotFound(_) => Ok(false),
                err => Err(OxenError::basic_str(format!(
                    "version_exists failed with S3 head_object error: {err:?}"
                ))),
            },

            Err(err) => Err(OxenError::basic_str(format!(
                "version_exists failed with S3 head_object error: {err:?}"
            ))),
        }
    }

    async fn delete_version(&self, _hash: &str) -> Result<(), OxenError> {
        // TODO: Implement S3 version deletion
        Err(OxenError::basic_str(
            "S3VersionStore delete_version not yet implemented",
        ))
    }

    async fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        // TODO: Implement S3 version listing
        Err(OxenError::basic_str(
            "S3VersionStore list_versions not yet implemented",
        ))
    }

    async fn combine_version_chunks(
        &self,
        _hash: &str,
        _cleanup: bool,
    ) -> Result<PathBuf, OxenError> {
        // TODO: Implement S3 version chunk combination
        Err(OxenError::basic_str(
            "S3VersionStore combine_version_chunks not yet implemented",
        ))
    }

    async fn clean_corrupted_versions(
        &self,
        _dry_run: bool,
    ) -> Result<CleanCorruptedVersionsResult, OxenError> {
        // TODO: Implement S3 version chunk combination
        Err(OxenError::basic_str(
            "S3VersionStore clean_corrupted_versions not yet implemented",
        ))
    }

    fn storage_type(&self) -> &str {
        "s3"
    }

    fn storage_settings(&self) -> HashMap<String, String> {
        let mut settings = HashMap::new();
        settings.insert("bucket".to_string(), self.bucket.clone());
        settings.insert("prefix".to_string(), self.prefix.clone());
        settings
    }
}

/// Read from `reader` until `buf` is full or EOF, returning the number of
/// bytes read. Unlike a single `read()` call this won't return a short read
/// unless EOF is reached.
async fn read_full(
    reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    buf: &mut [u8],
) -> Result<usize, OxenError> {
    let mut offset = 0;
    while offset < buf.len() {
        let n = reader
            .read(&mut buf[offset..])
            .await
            .map_err(|e| OxenError::upload(&format!("Failed to read from reader: {e}")))?;
        if n == 0 {
            break;
        }
        offset += n;
    }
    Ok(offset)
}

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct ByteStreamAdapter {
    inner: ByteStream,
}

impl Stream for ByteStreamAdapter {
    type Item = Result<Bytes, io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(Some(Err(e))) => {
                let err = io::Error::other(format!("{e}"));
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
