use crate::error::OxenError;
use async_tempfile::TempFile;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use bytes::Bytes;
use futures::StreamExt;
use log;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{File, create_dir_all};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::OnceCell;
use tokio_stream::Stream;
use tokio_util::io::StreamReader;

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

    pub async fn client(&self) -> Result<Arc<Client>, OxenError> {
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

    #[cfg(test)]
    pub fn new_with_client(client: Arc<Client>, bucket: String, prefix: String) -> Self {
        let cell = OnceCell::new();
        cell.set(Ok(client)).expect("cell was just created");
        Self {
            client: cell,
            bucket,
            prefix,
        }
    }

    /// Get the directory containing a version file
    fn version_dir(&self, hash: &str) -> String {
        format!("{}/{}", self.prefix, hash)
    }

    /// Get the full path for a version file
    fn generate_key(&self, hash: &str) -> String {
        format!("{}/{}", self.version_dir(hash), VERSION_FILE_NAME)
    }

    /// Get the S3 key for a chunk at a specific offset
    fn chunk_key(&self, hash: &str, offset: u64) -> String {
        format!("{}/chunks/{}", self.version_dir(hash), offset)
    }

    /// List all S3 object keys under a given prefix, following continuation tokens.
    async fn list_objects_with_prefix(&self, prefix: &str) -> Result<Vec<String>, OxenError> {
        let client = self.client().await?;
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = client.list_objects_v2().bucket(&self.bucket).prefix(prefix);
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await?;

            if let Some(contents) = resp.contents {
                for obj in contents {
                    if let Some(key) = obj.key {
                        keys.push(key);
                    }
                }
            }

            if resp.is_truncated.unwrap_or(false) {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(keys)
    }

    /// Delete a set of S3 objects, batching into groups of 1000 (the S3 DeleteObjects limit).
    async fn delete_objects(&self, keys: Vec<String>) -> Result<(), OxenError> {
        let client = self.client().await?;

        for batch in keys.chunks(1000) {
            let objects: Vec<ObjectIdentifier> = batch
                .iter()
                .map(|k| ObjectIdentifier::builder().key(k).build())
                .collect::<Result<_, _>>()?;

            let delete = Delete::builder().set_objects(Some(objects)).build()?;

            let resp = client
                .delete_objects()
                .bucket(&self.bucket)
                .delete(delete)
                .send()
                .await?;

            if let Some(errors) = resp.errors
                && !errors.is_empty()
            {
                let key_failures = errors
                    .iter()
                    .map(|e| {
                        (
                            e.key.as_deref().unwrap_or("?").into(),
                            e.message.as_deref().unwrap_or("?").into(),
                        )
                    })
                    .collect::<Vec<_>>();
                return Err(OxenError::DeleteFailure(key_failures));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl VersionStore for S3VersionStore {
    async fn init(&self) -> Result<(), OxenError> {
        let client = self.client().await?;

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

    /// Streams file content to S3 without writing to disk.
    ///
    /// The caller must provide the file size up front. Files up to 100 MB are uploaded in a
    /// single PUT request (per AWS best-practice guidelines). Larger files are uploaded via
    /// multipart upload with a dynamically chosen part size and up to 16 concurrent part uploads.
    /// If any part fails, the multipart upload is cancelled so no orphaned parts are left behind.
    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        size: u64,
    ) -> Result<(), OxenError> {
        let client = self.client().await?;
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
                .await?;
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
            .await?;

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
                    .await?;
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
        let client = self.client().await?;
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
        let client = self.client().await?;
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
        let client = self.client().await?;
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
        let client = self.client().await?;
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
        let client = self.client().await?;
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
        let client = self.client().await?;
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
        let client = self.client().await?;
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
        let client = self.client().await?;
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

    /// Copy a version file to a destination path on the local filesystem, creating any necessary
    /// parent directories and overwriting any existing file at the destination path.
    // TODO: We should probably only allow writing to a specific set of path prefixes
    async fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError> {
        if let Some(parent) = dest_path.parent() {
            create_dir_all(parent)
                .await
                .map_err(|e| OxenError::basic_str(format!("Failed to create parent dirs: {e}")))?;
        }

        let file = File::create(dest_path)
            .await
            .map_err(|e| OxenError::basic_str(format!("Failed to create file: {e}")))?;
        let mut writer = tokio::io::BufWriter::with_capacity(10 * 1024 * 1024, file);

        let mut stream = StreamReader::new(self.get_version_stream(hash).await?);
        tokio::io::copy_buf(&mut stream, &mut writer)
            .await
            .map_err(|e| OxenError::basic_str(format!("Failed to copy S3 stream to file: {e}")))?;

        Ok(())
    }

    async fn store_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        data: Bytes,
    ) -> Result<(), OxenError> {
        let client = self.client().await?;
        let key = self.chunk_key(hash, offset);

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(data))
            .send()
            .await?;

        Ok(())
    }

    async fn get_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        if size == 0 {
            return Ok(Vec::new());
        }

        let client = self.client().await?;
        let key = self.generate_key(hash);

        // HTTP Range is inclusive on both ends: bytes=start-end means bytes [start..=end]
        let end = offset.checked_add(size - 1).ok_or_else(|| {
            OxenError::basic_str("get_version_chunk: offset + size overflows u64")
        })?;
        let range = format!("bytes={offset}-{end}");

        let resp = client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .range(&range)
            .send()
            .await?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!("get_version_chunk: body collect failed: {e}"))
            })?
            .into_bytes();

        Ok(bytes.to_vec())
    }

    async fn list_version_chunks(&self, _hash: &str) -> Result<Vec<u64>, OxenError> {
        // TODO: Implement S3 version chunk listing
        Err(OxenError::basic_str(
            "S3VersionStore list_version_chunks not yet implemented",
        ))
    }

    async fn version_exists(&self, hash: &str) -> Result<bool, OxenError> {
        let client = self.client().await?;
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

    async fn delete_version(&self, hash: &str) -> Result<(), OxenError> {
        let prefix = format!("{}/", self.version_dir(hash));
        let keys = self.list_objects_with_prefix(&prefix).await?;
        self.delete_objects(keys).await
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

/// Uploads a single part in an ongoing multipart S3 upload operation.
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
        .await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::version_store::VersionStore;

    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    async fn setup() -> (
        S3VersionStore,
        async_tempfile::TempDir,
        tokio::task::JoinHandle<()>,
    ) {
        let tmp = async_tempfile::TempDir::new().await.unwrap();
        let fs = s3s_fs::FileSystem::new(tmp.dir_path()).unwrap();

        let mut builder = s3s::service::S3ServiceBuilder::new(fs);
        builder.set_auth(s3s::auth::SimpleAuth::from_single("test", "test"));
        let service = builder.build();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(conn) => conn,
                    Err(_) => break,
                };
                let service = service.clone();
                tokio::spawn(async move {
                    let stream = hyper_util::rt::TokioIo::new(stream);
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(stream, service)
                        .await;
                });
            }
        });

        let client = build_test_client(addr);
        client
            .create_bucket()
            .bucket("test-bucket")
            .send()
            .await
            .unwrap();

        let store = S3VersionStore::new_with_client(
            Arc::new(client),
            "test-bucket".to_string(),
            "test-namespace/test-repo".to_string(),
        );

        (store, tmp, server_handle)
    }

    fn build_test_client(addr: SocketAddr) -> Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .endpoint_url(format!("http://{addr}"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .force_path_style(true)
            .build();
        Client::from_conf(config)
    }

    #[tokio::test]
    async fn test_store_and_get_small_version_from_reader() {
        let (store, _tmp, _server) = setup().await;
        let data = b"hello world";

        let cursor = std::io::Cursor::new(data.to_vec());
        store
            .store_version_from_reader("abcdef1234567890", Box::new(cursor), data.len() as u64)
            .await
            .unwrap();

        let retrieved = store.get_version("abcdef1234567890").await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_store_and_get_large_version_from_reader() {
        let (store, _tmp, _server) = setup().await;

        // 20MB -- forces multipart upload (> 8MB part size)
        let data = vec![42u8; 20 * 1024 * 1024];

        let cursor = std::io::Cursor::new(data.clone());
        store
            .store_version_from_reader("bbcdef1234567890", Box::new(cursor), data.len() as u64)
            .await
            .unwrap();

        let retrieved = store.get_version("bbcdef1234567890").await.unwrap();
        assert_eq!(retrieved.len(), data.len());
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_store_version_from_reader_exact_part_boundary() {
        let (store, _tmp, _server) = setup().await;

        // Exactly 16MB -- two full 8MB parts, no partial last part
        let data = vec![7u8; 16 * 1024 * 1024];

        let cursor = std::io::Cursor::new(data.clone());
        store
            .store_version_from_reader("cccdef1234567890", Box::new(cursor), data.len() as u64)
            .await
            .unwrap();

        let retrieved = store.get_version("cccdef1234567890").await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_store_version_from_reader_empty() {
        let (store, _tmp, _server) = setup().await;

        let cursor = std::io::Cursor::new(Vec::new());
        store
            .store_version_from_reader("ddddef1234567890", Box::new(cursor), 0)
            .await
            .unwrap();

        let retrieved = store.get_version("ddddef1234567890").await.unwrap();
        assert!(retrieved.is_empty());
    }

    #[tokio::test]
    async fn test_copy_version_to_path_streams_to_dest() {
        let (store, _tmp, _server) = setup().await;
        let data = b"streamed to destination";

        store.store_version("eeedef1234567890", data).await.unwrap();

        let dest_dir = async_tempfile::TempDir::new().await.unwrap();
        let dest_path = dest_dir.dir_path().join("subdir/output.bin");

        store
            .copy_version_to_path("eeedef1234567890", &dest_path)
            .await
            .unwrap();

        let contents = tokio::fs::read(&dest_path).await.unwrap();
        assert_eq!(contents, data);
    }

    #[tokio::test]
    async fn test_store_version_chunk() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";
        let chunk_data = Bytes::from_static(b"hello chunk");

        store
            .store_version_chunk(hash, 0, chunk_data.clone())
            .await
            .expect("store_version_chunk should succeed");

        // Verify by reading the object back directly from S3
        let client = store.client().await.expect("client should succeed");
        let resp = client
            .get_object()
            .bucket(&store.bucket)
            .key(store.chunk_key(hash, 0))
            .send()
            .await
            .expect("chunk object should exist");
        let body = resp
            .body
            .collect()
            .await
            .expect("body should collect")
            .into_bytes();
        assert_eq!(body, chunk_data);
    }

    #[tokio::test]
    async fn test_store_version_chunk_multiple_offsets() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";

        store
            .store_version_chunk(hash, 0, Bytes::from_static(b"chunk-0"))
            .await
            .expect("store chunk at offset 0 should succeed");
        store
            .store_version_chunk(hash, 1024, Bytes::from_static(b"chunk-1024"))
            .await
            .expect("store chunk at offset 1024 should succeed");

        // Verify each chunk stored independently
        let client = store.client().await.expect("client should succeed");

        let body0 = client
            .get_object()
            .bucket(&store.bucket)
            .key(store.chunk_key(hash, 0))
            .send()
            .await
            .expect("chunk at offset 0 should exist")
            .body
            .collect()
            .await
            .expect("body should collect")
            .into_bytes();
        assert_eq!(&body0[..], b"chunk-0");

        let body1024 = client
            .get_object()
            .bucket(&store.bucket)
            .key(store.chunk_key(hash, 1024))
            .send()
            .await
            .expect("chunk at offset 1024 should exist")
            .body
            .collect()
            .await
            .expect("body should collect")
            .into_bytes();
        assert_eq!(&body1024[..], b"chunk-1024");
    }

    #[tokio::test]
    async fn test_delete_version_removes_data_and_chunks() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";

        // Store main data + two chunks
        store.store_version(hash, b"main data").await.unwrap();
        store
            .store_version_chunk(hash, 0, Bytes::from_static(b"chunk-0"))
            .await
            .unwrap();
        store
            .store_version_chunk(hash, 1024, Bytes::from_static(b"chunk-1024"))
            .await
            .unwrap();

        assert!(store.version_exists(hash).await.unwrap());

        // Delete
        store
            .delete_version(hash)
            .await
            .expect("delete_version should succeed");

        // Verify nothing remains under the version prefix
        assert!(!store.version_exists(hash).await.unwrap());

        let prefix = format!("{}/", store.version_dir(hash));
        let remaining = store.list_objects_with_prefix(&prefix).await.unwrap();
        assert!(
            remaining.is_empty(),
            "expected no objects, found: {:?}",
            remaining
        );
    }

    #[tokio::test]
    async fn test_delete_version_missing_is_noop() {
        let (store, _tmp, _server) = setup().await;
        let hash = "deadbeefdeadbeefdeadbeefdeadbeef";

        // No objects exist; should not error
        store
            .delete_version(hash)
            .await
            .expect("delete of missing version should succeed");
    }

    #[tokio::test]
    async fn test_delete_version_isolates_by_hash() {
        let (store, _tmp, _server) = setup().await;
        let hash_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let hash_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        store.store_version(hash_a, b"a data").await.unwrap();
        store.store_version(hash_b, b"b data").await.unwrap();

        store.delete_version(hash_a).await.unwrap();

        assert!(!store.version_exists(hash_a).await.unwrap());
        assert!(store.version_exists(hash_b).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_version_chunk_mid_file() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";
        let data: Vec<u8> = (0..100u8).collect();
        store.store_version(hash, &data).await.unwrap();

        let chunk = store.get_version_chunk(hash, 10, 20).await.unwrap();
        assert_eq!(chunk, data[10..30]);
    }

    #[tokio::test]
    async fn test_get_version_chunk_from_start() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";
        let data = b"hello world!";
        store.store_version(hash, data).await.unwrap();

        let chunk = store.get_version_chunk(hash, 0, 5).await.unwrap();
        assert_eq!(&chunk[..], b"hello");
    }

    #[tokio::test]
    async fn test_get_version_chunk_zero_size() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";
        // Note: no store_version call — zero-size must not require the object to exist
        let chunk = store.get_version_chunk(hash, 0, 0).await.unwrap();
        assert!(chunk.is_empty());
    }

    #[tokio::test]
    async fn test_get_version_chunk_past_eof_errors() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";
        store.store_version(hash, b"small").await.unwrap();

        let result = store.get_version_chunk(hash, 1000, 10).await;
        assert!(
            result.is_err(),
            "expected error for offset past EOF, got {result:?}"
        );
    }
}
