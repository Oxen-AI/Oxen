use crate::error::OxenError;
use async_tempfile::TempFile;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use bytes::Bytes;
use futures::StreamExt;
use log;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::OnceCell;
use tokio_stream::Stream;

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
        reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        size: u64,
    ) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let key = self.generate_key(hash);

        const ONESHOT_SIZE: u64 = 100 * 1024 * 1024; // 100 MB
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB, S3 minimum
        const MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024; // 5 GB, S3 maximum
        const MAX_PARTS: usize = 10_000; // S3 maximum parts per upload
        const MAX_CONCURRENT_UPLOADS: usize = 16;

        let mut reader = tokio::io::BufReader::new(reader);

        // Files up to 100 MB: single put_object
        if size <= ONESHOT_SIZE {
            let mut buf = Vec::with_capacity(size as usize);
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf)
                .await
                .map_err(|e| OxenError::upload(&format!("Failed to read: {e}")))?;
            client
                .put_object() // AWS recommends switching to multipart uploads for > 100 MB
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::from(buf))
                .send()
                .await
                .map_err(OxenError::aws_sdk_error)?;
            return Ok(());
        }

        // Scale part size to fit within S3's 10,000 part limit for the file size.
        let part_size = ((size as usize).div_ceil(MAX_PARTS)).clamp(MIN_PART_SIZE, MAX_PART_SIZE);

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

        // Read parts sequentially and upload them concurrently via spawned tasks. Each upload
        // starts running immediately on the tokio runtime, so uploads proceed in the background
        // while we read the next part. FuturesUnordered holds the JoinHandles for collecting
        // results and enforcing backpressure.
        let mut uploads = futures::stream::FuturesUnordered::new();
        let mut completed_parts = Vec::new();
        let mut part_num = 1;

        let result: Result<(), OxenError> = async {
            loop {
                let mut buf = vec![0u8; part_size];
                let n = read_full(&mut reader, &mut buf).await?;
                if n == 0 {
                    break;
                }
                buf.truncate(n);
                uploads.push(tokio::spawn(upload_part(
                    client.clone(),
                    self.bucket.clone(),
                    key.clone(),
                    upload_id.clone(),
                    part_num,
                    buf,
                )));
                part_num += 1;

                // Stop reading until we have less than MAX_CONCURRENT_UPLOADS in flight
                while uploads.len() >= MAX_CONCURRENT_UPLOADS {
                    match uploads.next().await {
                        Some(Ok(result)) => completed_parts.push(result?),
                        Some(Err(e)) => {
                            return Err(OxenError::upload(&format!("Upload task panicked: {e}")));
                        }
                        None => break, // Shouldn't be possible since we check uploads.len() first
                    }
                }
            }

            // Wait for remaining uploads
            while let Some(join_result) = uploads.next().await {
                match join_result {
                    Ok(result) => completed_parts.push(result?),
                    Err(e) => return Err(OxenError::upload(&format!("Upload task panicked: {e}"))),
                }
            }
            Ok(())
        }
        .await;

        match result {
            // All parts uploaded successfully
            Ok(()) => {
                // Complete the multipart upload with the required special request
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
                    .map_err(OxenError::aws_sdk_error)?;
                Ok(())
            }
            // Upload failed
            Err(e) => {
                // cancel in-flight tasks
                for handle in uploads.iter() {
                    // we don't need to await aborted handles--they stop at the next await point automatically
                    handle.abort();
                }
                // abort the multipart upload
                let _ = client
                    .abort_multipart_upload()
                    .bucket(&self.bucket)
                    .key(&key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                Err(e)
            }
        }
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

async fn upload_part(
    client: Arc<Client>,
    bucket: String,
    key: String,
    upload_id: String,
    part_num: i32,
    data: Vec<u8>,
) -> Result<CompletedPart, OxenError> {
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

    let etag = resp
        .e_tag()
        .map(|s| s.to_string())
        .ok_or_else(|| OxenError::upload("S3 upload_part response missing ETag"))?;

    Ok(CompletedPart::builder()
        .part_number(part_num)
        .e_tag(etag)
        .build())
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
