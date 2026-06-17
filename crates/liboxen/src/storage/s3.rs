use crate::error::OxenError;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use bytes::Bytes;
use futures::StreamExt;
use log;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::AsyncReadExt;
use tokio::sync::OnceCell;
use tokio_stream::Stream;
use tokio_util::io::StreamReader;

use super::version_store::{VersionLocation, VersionStore};
use crate::constants::VERSION_FILE_NAME;
use crate::util::fs::AtomicFile;
use crate::util::hasher;
use crate::view::versions::CleanCorruptedVersionsResult;
use xxhash_rust::xxh3::Xxh3;

/// AWS recommends uploading to S3 in a single PUT if filesize is <= 100 MB.
const DEFAULT_ONESHOT_SIZE: u64 = 100 * 1024 * 1024;

/// Server-supplied S3 configuration carried separately from per-repo `StorageConfig`. The bucket is
/// a server-wide setting (the server can rotate it without rewriting every repo's config), so it
/// never appears in `.oxen/config.toml` — only in the server's TOML, then threaded through repo
/// construction. Clients of liboxen pass `None` for this when no S3 backend is server-enabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Opts {
    pub bucket: String,
}

/// S3 implementation of version storage
#[derive(Debug)]
pub struct S3VersionStore {
    client: OnceCell<Result<Arc<Client>, OxenError>>,
    bucket: String,
    prefix: String,
    /// Threshold (bytes) below which we upload with a single PUT rather than a multipart upload.
    oneshot_size: u64,
    /// Optional endpoint override (S3-compatible stores, the in-process s3s test fixture, MinIO,
    /// etc.). `None` selects the default AWS endpoint. Surfaced through `VersionLocation::S3` so
    /// cloud-aware readers (Polars `CloudOptions`, DuckDB `SET s3_endpoint=...`) hit the same
    /// host as the version store itself.
    endpoint_url: Option<String>,
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
            oneshot_size: DEFAULT_ONESHOT_SIZE,
            endpoint_url: None,
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
    pub fn new_with_client(
        client: Arc<Client>,
        bucket: String,
        prefix: String,
        endpoint_url: Option<String>,
    ) -> Self {
        let cell = OnceCell::new();
        cell.set(Ok(client)).expect("cell was just created");
        Self {
            client: cell,
            bucket,
            prefix,
            oneshot_size: DEFAULT_ONESHOT_SIZE,
            endpoint_url,
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

    /// Get the S3 key prefix for all chunks of a version
    fn chunks_prefix(&self, hash: &str) -> String {
        format!("{}/chunks/", self.version_dir(hash))
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
    ///
    /// Bytes are hashed (xxh3-128) as they stream from the reader and verified against the
    /// expected `hash` parameter. A mismatch aborts the upload before it is committed to S3, so
    /// no corrupt or misnamed object is ever visible.
    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        size: u64,
    ) -> Result<(), OxenError> {
        let client = self.client().await?;
        let key = self.generate_key(hash);

        const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB, S3 minimum
        const MAX_PART_SIZE: usize = 5 * 1024 * 1024 * 1024; // 5 GB, S3 maximum
        const MAX_PARTS: usize = 10_000; // S3 maximum parts per upload
        const MAX_CONCURRENT_UPLOADS: usize = 16;

        let mut reader = tokio::io::BufReader::new(reader);

        // Files at or below the oneshot threshold: single put_object. Hash the fully-read
        // buffer and verify before uploading so we never write corrupt or misnamed data to S3.
        if size <= self.oneshot_size {
            let mut buf = Vec::with_capacity(size as usize);
            tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut buf)
                .await
                .map_err(|e| OxenError::upload(&format!("Failed to read: {e}")))?;
            let computed = hasher::hash_buffer(&buf);
            if computed != hash {
                return Err(OxenError::upload(&format!(
                    "store_version_from_reader hash mismatch: expected {hash}, computed {computed}"
                )));
            }
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
        //
        // Bytes are fed into `hasher` synchronously before each part is moved into its upload
        // task, so the final digest covers every byte S3 will see. We check the digest before
        // calling complete_multipart_upload — on mismatch we fall into the Err(..) branch which
        // aborts the multipart upload, so no object is committed.
        let mut uploads = futures::stream::FuturesUnordered::new();
        let mut completed_parts = Vec::new();
        let mut part_num = 1;
        let mut hasher = Xxh3::new();

        let result: Result<(), OxenError> = async {
            loop {
                let mut buf = vec![0u8; part_size];
                let n = read_full(&mut reader, &mut buf).await?;
                if n == 0 {
                    break;
                }
                buf.truncate(n);
                hasher.update(&buf);
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

            // All parts uploaded successfully — verify the aggregate hash before committing.
            let computed = format!("{:x}", hasher.digest128());
            if computed != hash {
                return Err(OxenError::upload(&format!(
                    "store_version_from_reader hash mismatch: expected {hash}, computed {computed}"
                )));
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

    async fn store_version(&self, hash: &str, data: Bytes) -> Result<(), OxenError> {
        let client = self.client().await?;
        log::debug!("Storing version to S3");
        let key = self.generate_key(hash);

        let body = ByteStream::from(data);
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
        derived_data: Bytes,
    ) -> Result<(), OxenError> {
        let client = self.client().await?;
        let key = format!("{}/{}", self.version_dir(orig_hash), derived_filename);

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(derived_data))
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

    async fn version_location(&self, hash: &str) -> Result<VersionLocation, OxenError> {
        let client = self.client().await?;
        let region = client
            .config()
            .region()
            .map(|r| r.to_string())
            .unwrap_or_else(|| "us-east-1".to_string());
        Ok(VersionLocation::S3 {
            url: format!("s3://{}/{}", self.bucket, self.generate_key(hash)),
            region,
            endpoint_url: self.endpoint_url.clone(),
        })
    }

    /// Copy a version file to a destination path on the local filesystem, creating any necessary
    /// parent directories. The publish is atomic and the published file is stamped with `mtime`
    /// alongside the bytes — any error leaves the prior contents (if any) at `dest_path`
    /// untouched, never a torn or wrong-mtime file.
    // TODO: We should probably only allow writing to a specific set of path prefixes
    async fn copy_version_to_path(
        &self,
        hash: &str,
        dest_path: &Path,
        mtime: SystemTime,
    ) -> Result<(), OxenError> {
        let mut stream = StreamReader::new(self.get_version_stream(hash).await?);
        AtomicFile::new(dest_path)
            .with_mtime(mtime)
            .stream_async(&mut stream)
            .await
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

    async fn list_version_chunks(&self, hash: &str) -> Result<Vec<u64>, OxenError> {
        let prefix = self.chunks_prefix(hash);
        let keys = self.list_objects_with_prefix(&prefix).await?;

        let mut offsets = Vec::with_capacity(keys.len());
        for key in &keys {
            if let Some(offset_str) = key.strip_prefix(&prefix)
                && let Ok(offset) = offset_str.parse::<u64>()
            {
                offsets.push(offset);
            }
        }
        offsets.sort();

        Ok(offsets)
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
        // Each version is stored at `{self.prefix}/{hash}/...`. To enumerate the hashes
        // without reading every leaf object, ask S3 for the common prefixes under
        // `{self.prefix}/` with `/` as the delimiter — S3 collapses all keys sharing a
        // given `{hash}/` into one entry. Keys that sit directly under the prefix with no
        // further slash (e.g. the init-time `_permission_check`) come back as Contents
        // rather than CommonPrefixes, so they don't appear in the result.
        let client = self.client().await?;
        let base = format!("{}/", self.prefix);
        let mut hashes = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&base)
                .delimiter("/");
            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await?;

            if let Some(common_prefixes) = resp.common_prefixes {
                for cp in common_prefixes {
                    if let Some(hash) = cp
                        .prefix
                        .as_deref()
                        .and_then(|p| p.strip_prefix(&base))
                        .and_then(|s| s.strip_suffix('/'))
                    {
                        hashes.push(hash.to_string());
                    }
                }
            }

            if resp.is_truncated.unwrap_or(false) {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        // S3 already returns keys in UTF-8 byte order, so we don't need to explicitly sort hashes.
        Ok(hashes)
    }

    async fn combine_version_chunks(&self, hash: &str) -> Result<(), OxenError> {
        // 1. List chunk offsets (already sorted by list_version_chunks)
        let offsets = self.list_version_chunks(hash).await?;
        if offsets.is_empty() {
            return Ok(());
        }
        log::debug!("combine_version_chunks found {} chunks", offsets.len());

        let client = self.client().await?;
        let key = self.generate_key(hash);

        // 2. Create multipart upload
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

        // 3. Stream chunks and upload as multipart parts
        const MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5 MB, S3 minimum
        let mut part_buf: Vec<u8> = Vec::new();
        let mut part_num: i32 = 1;
        let mut completed_parts: Vec<CompletedPart> = Vec::new();

        let result: Result<(), OxenError> = async {
            for (i, offset) in offsets.iter().enumerate() {
                let is_last_chunk = i == offsets.len() - 1;

                // Download chunk from S3
                let resp = client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(self.chunk_key(hash, *offset))
                    .send()
                    .await?;
                let chunk_bytes = resp
                    .body
                    .collect()
                    .await
                    .map_err(|e| {
                        OxenError::basic_str(format!(
                            "Failed to read chunk body at offset {offset}: {e}"
                        ))
                    })?
                    .into_bytes();

                // Append to part buffer
                part_buf.extend_from_slice(&chunk_bytes);

                // Upload part when buffer is large enough (or on last chunk)
                while part_buf.len() >= MIN_PART_SIZE || (is_last_chunk && !part_buf.is_empty()) {
                    let drain_len = if part_buf.len() >= MIN_PART_SIZE && !is_last_chunk {
                        MIN_PART_SIZE
                    } else if is_last_chunk && part_buf.len() <= MIN_PART_SIZE {
                        // Last flush — drain everything
                        part_buf.len()
                    } else {
                        MIN_PART_SIZE
                    };

                    let part_data: Vec<u8> = part_buf.drain(..drain_len).collect();
                    let part = upload_part(
                        client.clone(),
                        self.bucket.clone(),
                        key.clone(),
                        upload_id.clone(),
                        part_num,
                        part_data,
                    )
                    .await?;
                    completed_parts.push(part);
                    part_num += 1;

                    // If this is the last chunk and buffer is empty, stop
                    if is_last_chunk && part_buf.is_empty() {
                        break;
                    }
                }
            }
            Ok(())
        }
        .await;

        // On failure, abort the multipart upload
        if let Err(e) = result {
            let _ = client
                .abort_multipart_upload()
                .bucket(&self.bucket)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            return Err(e);
        }

        // 4. Complete multipart upload
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

        // 5. Delete chunk objects
        let chunk_keys = self
            .list_objects_with_prefix(&self.chunks_prefix(hash))
            .await?;
        if !chunk_keys.is_empty() {
            self.delete_objects(chunk_keys).await?;
        }

        Ok(())
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

    fn storage_kind(&self) -> crate::storage::StorageKind {
        crate::storage::StorageKind::S3
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
            Some(format!("http://{addr}")),
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
    async fn test_version_location_returns_s3_variant_with_endpoint() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890";

        let location = store.version_location(hash).await.unwrap();
        match location {
            VersionLocation::S3 {
                url,
                region,
                endpoint_url,
            } => {
                assert_eq!(
                    url,
                    format!("s3://test-bucket/test-namespace/test-repo/{hash}/data")
                );
                assert_eq!(region, "us-east-1");
                let endpoint = endpoint_url.expect("test setup configures a loopback endpoint");
                assert!(
                    endpoint.starts_with("http://127.0.0.1:"),
                    "expected loopback endpoint url, got {endpoint:?}"
                );
            }
            other => panic!("expected S3 variant, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_store_and_get_small_version_from_reader() {
        let (store, _tmp, _server) = setup().await;
        let data = b"hello world";
        let hash = hasher::hash_buffer(data);

        let cursor = std::io::Cursor::new(data.to_vec());
        store
            .store_version_from_reader(&hash, Box::new(cursor), data.len() as u64)
            .await
            .unwrap();

        let retrieved = store.get_version(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_store_and_get_large_version_from_reader() {
        let (store, _tmp, _server) = setup().await;

        // 20MB -- forces multipart upload (> 8MB part size)
        let data = vec![42u8; 20 * 1024 * 1024];
        let hash = hasher::hash_buffer(&data);

        let cursor = std::io::Cursor::new(data.clone());
        store
            .store_version_from_reader(&hash, Box::new(cursor), data.len() as u64)
            .await
            .unwrap();

        let retrieved = store.get_version(&hash).await.unwrap();
        assert_eq!(retrieved.len(), data.len());
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_store_version_from_reader_exact_part_boundary() {
        let (store, _tmp, _server) = setup().await;

        // Exactly 16MB -- two full 8MB parts, no partial last part
        let data = vec![7u8; 16 * 1024 * 1024];
        let hash = hasher::hash_buffer(&data);

        let cursor = std::io::Cursor::new(data.clone());
        store
            .store_version_from_reader(&hash, Box::new(cursor), data.len() as u64)
            .await
            .unwrap();

        let retrieved = store.get_version(&hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_store_version_from_reader_empty() {
        let (store, _tmp, _server) = setup().await;
        let hash = hasher::hash_buffer(&[]);

        let cursor = std::io::Cursor::new(Vec::new());
        store
            .store_version_from_reader(&hash, Box::new(cursor), 0)
            .await
            .unwrap();

        let retrieved = store.get_version(&hash).await.unwrap();
        assert!(retrieved.is_empty());
    }

    #[tokio::test]
    async fn test_copy_version_to_path_streams_to_dest() {
        let (store, _tmp, _server) = setup().await;
        let data = b"streamed to destination";

        store
            .store_version("eeedef1234567890", Bytes::from_static(data))
            .await
            .unwrap();

        let dest_dir = async_tempfile::TempDir::new().await.unwrap();
        let dest_path = dest_dir.dir_path().join("subdir/output.bin");
        let mtime = SystemTime::UNIX_EPOCH + std::time::Duration::new(1_700_000_000, 123_456_789);

        store
            .copy_version_to_path("eeedef1234567890", &dest_path, mtime)
            .await
            .unwrap();

        let contents = tokio::fs::read(&dest_path).await.unwrap();
        assert_eq!(contents, data);
        let actual_mtime = std::fs::metadata(&dest_path).unwrap().modified().unwrap();
        assert_eq!(actual_mtime, mtime);
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
        store
            .store_version(hash, Bytes::from_static(b"main data"))
            .await
            .unwrap();
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
    async fn test_list_version_chunks() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";

        store
            .store_version_chunk(hash, 0, Bytes::from_static(b"chunk-0"))
            .await
            .unwrap();
        store
            .store_version_chunk(hash, 10240, Bytes::from_static(b"chunk-10240"))
            .await
            .unwrap();
        store
            .store_version_chunk(hash, 20480, Bytes::from_static(b"chunk-20480"))
            .await
            .unwrap();

        let offsets = store.list_version_chunks(hash).await.unwrap();
        assert_eq!(offsets, vec![0, 10240, 20480]);
    }

    #[tokio::test]
    async fn test_list_version_chunks_empty() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";

        let offsets = store.list_version_chunks(hash).await.unwrap();
        assert!(offsets.is_empty());
    }

    #[tokio::test]
    async fn test_list_version_chunks_isolates_by_hash() {
        let (store, _tmp, _server) = setup().await;
        let hash_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let hash_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        store
            .store_version_chunk(hash_a, 0, Bytes::from_static(b"a-chunk"))
            .await
            .unwrap();
        store
            .store_version_chunk(hash_b, 0, Bytes::from_static(b"b-chunk-0"))
            .await
            .unwrap();
        store
            .store_version_chunk(hash_b, 512, Bytes::from_static(b"b-chunk-512"))
            .await
            .unwrap();

        let offsets_a = store.list_version_chunks(hash_a).await.unwrap();
        assert_eq!(offsets_a, vec![0]);

        let offsets_b = store.list_version_chunks(hash_b).await.unwrap();
        assert_eq!(offsets_b, vec![0, 512]);
    }

    #[tokio::test]
    async fn test_delete_version_isolates_by_hash() {
        let (store, _tmp, _server) = setup().await;
        let hash_a = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let hash_b = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

        store
            .store_version(hash_a, Bytes::from_static(b"a data"))
            .await
            .unwrap();
        store
            .store_version(hash_b, Bytes::from_static(b"b data"))
            .await
            .unwrap();

        store.delete_version(hash_a).await.unwrap();

        assert!(!store.version_exists(hash_a).await.unwrap());
        assert!(store.version_exists(hash_b).await.unwrap());
    }

    #[tokio::test]
    async fn test_get_version_chunk_mid_file() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";
        let data: Vec<u8> = (0..100u8).collect();
        store
            .store_version(hash, Bytes::copy_from_slice(&data))
            .await
            .unwrap();

        let chunk = store.get_version_chunk(hash, 10, 20).await.unwrap();
        assert_eq!(chunk, data[10..30]);
    }

    #[tokio::test]
    async fn test_get_version_chunk_from_start() {
        let (store, _tmp, _server) = setup().await;
        let hash = "abcdef1234567890abcdef1234567890";
        let data = b"hello world!";
        store
            .store_version(hash, Bytes::from_static(data))
            .await
            .unwrap();

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
        store
            .store_version(hash, Bytes::from_static(b"small"))
            .await
            .unwrap();

        let result = store.get_version_chunk(hash, 1000, 10).await;
        assert!(
            result.is_err(),
            "expected error for offset past EOF, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_combine_version_chunks() {
        let (store, _tmp, _server) = setup().await;

        // Pre-compute the hash of the combined data
        let combined = b"chunk-0chunk-10240chunk-20480";
        let hash = hasher::hash_buffer(combined);

        // Store three chunks
        store
            .store_version_chunk(&hash, 0, Bytes::from_static(b"chunk-0"))
            .await
            .expect("store chunk 0");
        store
            .store_version_chunk(&hash, 10240, Bytes::from_static(b"chunk-10240"))
            .await
            .expect("store chunk 10240");
        store
            .store_version_chunk(&hash, 20480, Bytes::from_static(b"chunk-20480"))
            .await
            .expect("store chunk 20480");

        // Combine
        store
            .combine_version_chunks(&hash)
            .await
            .expect("combine_version_chunks should succeed");

        // Verify VERSION object has the correct content
        let client = store.client().await.expect("client");
        let resp = client
            .get_object()
            .bucket(&store.bucket)
            .key(store.generate_key(&hash))
            .send()
            .await
            .expect("VERSION object should exist");
        let body = resp.body.collect().await.expect("body").into_bytes();
        assert_eq!(&body[..], combined);

        // Verify chunk objects were deleted
        let chunk_keys = store
            .list_objects_with_prefix(&store.chunks_prefix(&hash))
            .await
            .expect("list chunks");
        assert!(
            chunk_keys.is_empty(),
            "chunks should be deleted after combine, found: {chunk_keys:?}"
        );
    }

    #[tokio::test]
    async fn test_store_version_from_reader_hash_mismatch_oneshot() {
        let (store, _tmp, _server) = setup().await;
        let data = b"hello world";
        // Pass a plausible-looking but incorrect hash (not the xxh3-128 of `data`).
        let wrong_hash = "deadbeefdeadbeefdeadbeefdeadbeef";

        let cursor = std::io::Cursor::new(data.to_vec());
        let result = store
            .store_version_from_reader(wrong_hash, Box::new(cursor), data.len() as u64)
            .await;

        assert!(
            matches!(result, Err(OxenError::Upload(_))),
            "expected Upload error for hash mismatch, got {result:?}"
        );

        // The oneshot path rejects before uploading, so no object should exist at the key.
        assert!(
            !store.version_exists(wrong_hash).await.unwrap(),
            "no object should exist at the mismatched hash's key"
        );
    }

    #[tokio::test]
    async fn test_store_version_from_reader_hash_mismatch_multipart() {
        let (mut store, _tmp, _server) = setup().await;
        // Drop the threshold below the data size so this exercises the multipart path. Any size
        // above the threshold works — the S3 5MB-minimum rule applies only to non-last parts,
        // so a single-part multipart upload is fine.
        store.oneshot_size = 512;

        let data = vec![42u8; 1024];
        let wrong_hash = "deadbeefdeadbeefdeadbeefdeadbeef";

        let cursor = std::io::Cursor::new(data.clone());
        let result = store
            .store_version_from_reader(wrong_hash, Box::new(cursor), data.len() as u64)
            .await;

        assert!(
            matches!(result, Err(OxenError::Upload(_))),
            "expected Upload error for hash mismatch, got {result:?}"
        );

        // Multipart upload should have been aborted before completion, so no object is visible
        // at the key.
        assert!(
            !store.version_exists(wrong_hash).await.unwrap(),
            "no object should exist at the mismatched hash's key"
        );
    }

    #[tokio::test]
    async fn test_list_versions() {
        let (store, _tmp, _server) = setup().await;

        // Insert out of order; list_versions is documented to return sorted results.
        store
            .store_version("cccc", Bytes::from_static(b"c data"))
            .await
            .unwrap();
        store
            .store_version("aaaa", Bytes::from_static(b"a data"))
            .await
            .unwrap();
        store
            .store_version("bbbb", Bytes::from_static(b"b data"))
            .await
            .unwrap();

        let versions = store.list_versions().await.unwrap();
        assert_eq!(versions, vec!["aaaa", "bbbb", "cccc"]);
    }

    #[tokio::test]
    async fn test_list_versions_empty() {
        let (store, _tmp, _server) = setup().await;

        let versions = store.list_versions().await.unwrap();
        assert!(versions.is_empty());
    }

    #[tokio::test]
    async fn test_list_versions_collapses_chunks_and_derived() {
        // A version's chunks and derived files live under the same `{prefix}/{hash}/` tree.
        // With common-prefix listing they should collapse to a single entry per hash.
        let (store, _tmp, _server) = setup().await;

        let hash = "abcdef1234567890abcdef1234567890";
        store
            .store_version(hash, Bytes::from_static(b"main"))
            .await
            .unwrap();
        store
            .store_version_chunk(hash, 0, Bytes::from_static(b"chunk-0"))
            .await
            .unwrap();
        store
            .store_version_chunk(hash, 1024, Bytes::from_static(b"chunk-1024"))
            .await
            .unwrap();
        store
            .store_version_derived(hash, "thumb.jpg", Bytes::from_static(b"thumbnail bytes"))
            .await
            .unwrap();

        let versions = store.list_versions().await.unwrap();
        assert_eq!(versions, vec![hash]);
    }

    /// Tabular reads served directly from S3 via `tabular::read_version_df`. Exercises the Polars
    /// cloud-scan paths (Parquet/CSV/TSV/JSONL/IPC), the in-memory JSON path, and head-sample CSV
    /// dialect sniffing — all against the s3s fixture.
    mod cloud_reads {
        use super::*;
        use crate::core::df::tabular;
        use crate::opts::DFOpts;
        use polars::prelude::*;

        /// Make the s3s fixture credentials discoverable by Polars' `from_env` cloud-scan path
        /// (used for the columnar formats). All s3s tests share these fixed creds, and nothing else
        /// in the test binary reads AWS credentials from the environment (the `S3VersionStore`
        /// client is injected), so a one-time set is safe.
        fn set_test_aws_env() {
            use std::sync::Once;
            static ONCE: Once = Once::new();
            ONCE.call_once(|| {
                // SAFETY: Set exactly once to constant values; no concurrent reader of these
                // variables exists in the test binary, so there is no data race on the environment.
                unsafe {
                    std::env::set_var("AWS_ACCESS_KEY_ID", "test");
                    std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
                    std::env::set_var("AWS_REGION", "us-east-1");
                }
            });
        }

        /// Build an s3s-backed store as a trait object, the form `read_version_df` consumes.
        async fn setup_dyn() -> (
            Arc<dyn VersionStore>,
            async_tempfile::TempDir,
            tokio::task::JoinHandle<()>,
        ) {
            set_test_aws_env();
            let (store, tmp, server) = setup().await;
            (Arc::new(store), tmp, server)
        }

        /// Store `bytes` as a version file and return its hash so the reader can resolve it.
        async fn upload(store: &Arc<dyn VersionStore>, bytes: Vec<u8>) -> String {
            let hash = hasher::hash_buffer(&bytes);
            let len = bytes.len() as u64;
            store
                .store_version_from_reader(&hash, Box::new(std::io::Cursor::new(bytes)), len)
                .await
                .unwrap();
            hash
        }

        async fn read(store: &Arc<dyn VersionStore>, hash: &str, ext: &str) -> DataFrame {
            tabular::read_version_df(store, hash, ext, &DFOpts::empty())
                .await
                .unwrap()
        }

        fn sample_df() -> DataFrame {
            df! { "a" => &[1i64, 2, 3], "b" => &["x", "y", "z"] }.unwrap()
        }

        /// Assert a round-tripped frame matches `sample_df` by value, not just shape — catches
        /// header-as-data, column swaps, and dtype/null coercion that a shape-only check misses.
        fn assert_sample_df(df: &DataFrame) {
            assert_eq!((df.height(), df.width()), (3, 2));
            assert_eq!(df.column("a").unwrap().i64().unwrap().get(0), Some(1));
            assert_eq!(df.column("b").unwrap().str().unwrap().get(2), Some("z"));
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_read_s3_parquet() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload_sample_parquet(&store).await;

            let df = read(&store, &hash, "parquet").await;
            assert_sample_df(&df);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_read_s3_ipc() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload_sample_ipc(&store).await;

            let df = read(&store, &hash, "arrow").await;
            assert_sample_df(&df);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_read_s3_csv() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload(&store, b"a,b\n1,x\n2,y\n3,z\n".to_vec()).await;

            let df = read(&store, &hash, "csv").await;
            assert_sample_df(&df);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_read_s3_tsv() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload(&store, b"a\tb\n1\tx\n2\ty\n3\tz\n".to_vec()).await;

            let df = read(&store, &hash, "tsv").await;
            assert_sample_df(&df);
        }

        /// A `.csv` file that is actually semicolon-delimited must still parse into two columns:
        /// proves the head-sample dialect sniff runs against the S3 object, matching local behavior.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_read_s3_csv_sniffs_non_default_delimiter() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload(&store, b"a;b\n1;x\n2;y\n3;z\n".to_vec()).await;

            // Value-level: a mis-sniff (defaulting to comma) would yield width 1, not the (3, 2)
            // sample, so this asserts the semicolon was sniffed from the S3 head sample.
            let df = read(&store, &hash, "csv").await;
            assert_sample_df(&df);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_read_s3_jsonl() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload(
                &store,
                b"{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n{\"a\":3,\"b\":\"z\"}\n".to_vec(),
            )
            .await;

            let df = read(&store, &hash, "jsonl").await;
            assert_sample_df(&df);
        }

        /// Non-line-delimited JSON has no Polars cloud reader, so this exercises the fetch-bytes
        /// fallback path (`get` the whole object, then `JsonReader`).
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_read_s3_json_array() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload(
                &store,
                b"[{\"a\":1,\"b\":\"x\"},{\"a\":2,\"b\":\"y\"},{\"a\":3,\"b\":\"z\"}]".to_vec(),
            )
            .await;

            let df = read(&store, &hash, "json").await;
            assert_sample_df(&df);
        }

        // The following three tests use the default `#[tokio::test]` (current-thread) runtime to
        // mirror the actix server. A transform that drives the cloud `LazyFrame` (collect or schema
        // resolution) must run off the async worker via spawn_blocking, or Polars' `block_in_place`
        // panics ("can call blocking only when running on the multi-threaded runtime").
        async fn upload_sample_parquet(store: &Arc<dyn VersionStore>) -> String {
            let mut buf = Vec::new();
            ParquetWriter::new(&mut buf)
                .finish(&mut sample_df())
                .unwrap();
            upload(store, buf).await
        }

        async fn upload_sample_ipc(store: &Arc<dyn VersionStore>) -> String {
            let mut buf = Vec::new();
            IpcWriter::new(&mut buf).finish(&mut sample_df()).unwrap();
            upload(store, buf).await
        }

        #[tokio::test]
        async fn test_read_s3_with_take_transform_current_thread() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload_sample_parquet(&store).await;
            let mut opts = DFOpts::empty();
            opts.take = Some("0,2".to_string());
            let df = tabular::read_version_df(&store, &hash, "parquet", &opts)
                .await
                .unwrap();
            assert_eq!((df.height(), df.width()), (2, 2));
            let a = df.column("a").unwrap().i64().unwrap();
            assert_eq!((a.get(0), a.get(1)), (Some(1), Some(3)));
        }

        #[tokio::test]
        async fn test_read_s3_with_column_at_transform_current_thread() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload_sample_parquet(&store).await;
            let mut opts = DFOpts::empty();
            opts.item = Some("b:2".to_string());
            let df = tabular::read_version_df(&store, &hash, "parquet", &opts)
                .await
                .unwrap();
            assert_eq!(df.get(0).unwrap()[0], AnyValue::String("z"));
        }

        #[tokio::test]
        async fn test_read_s3_with_filter_transform_current_thread() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload_sample_parquet(&store).await;
            let mut opts = DFOpts::empty();
            opts.filter = Some("a == 2".to_string());
            let df = tabular::read_version_df(&store, &hash, "parquet", &opts)
                .await
                .unwrap();
            assert_eq!((df.height(), df.width()), (1, 2));
            assert_eq!(df.column("b").unwrap().str().unwrap().get(0), Some("y"));
        }

        // Non-parquet variant: the transform fix is format-agnostic, but the cloud-scan root differs
        // per format, so exercise a filter (which resolves schema) over a JSONL cloud scan too.
        #[tokio::test]
        async fn test_read_s3_jsonl_with_filter_transform_current_thread() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload(
                &store,
                b"{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n{\"a\":3,\"b\":\"z\"}\n".to_vec(),
            )
            .await;
            let mut opts = DFOpts::empty();
            opts.filter = Some("a == 2".to_string());
            let df = tabular::read_version_df(&store, &hash, "jsonl", &opts)
                .await
                .unwrap();
            assert_eq!((df.height(), df.width()), (1, 2));
            assert_eq!(df.column("b").unwrap().str().unwrap().get(0), Some("y"));
        }

        // A transform whose collect hits an S3 error (object never stored) must return a clean
        // error, not panic via `unwrap` — and on the current-thread runtime it must not
        // `block_in_place`-panic on the way there either.
        #[tokio::test]
        async fn test_read_s3_column_at_missing_object_errors_not_panics() {
            let (store, _tmp, _server) = setup_dyn().await;
            let mut opts = DFOpts::empty();
            opts.item = Some("b:0".to_string());
            let result =
                tabular::read_version_df(&store, "deadbeefdeadbeef", "parquet", &opts).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_read_s3_arrow_with_sql_is_rejected() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload_sample_ipc(&store).await;
            let mut opts = DFOpts::empty();
            opts.sql = Some("SELECT * FROM df".to_string());
            let result = tabular::read_version_df(&store, &hash, "arrow", &opts).await;
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_read_s3_unknown_extension_errors() {
            let (store, _tmp, _server) = setup_dyn().await;
            let hash = upload(&store, b"not a data frame".to_vec()).await;
            let result = tabular::read_version_df(&store, &hash, "xlsx", &DFOpts::empty()).await;
            assert!(result.is_err());
        }

        /// End-to-end: index a workspace data frame whose version file lives in S3.
        ///
        /// Drives 16c's S3 branch of `workspaces::data_frames::index`: `version_location` returns
        /// `S3`, so the bytes are streamed to a temp file via `copy_version_to_path` and DuckDB
        /// reads that local path. The DuckDB database itself stays on local disk; only the version
        /// file is sourced from the s3s fixture, exercising the path that has no local version
        /// file to fall back on.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn test_index_workspace_data_frame_from_s3() -> Result<(), crate::error::OxenError> {
            // DuckDB indexing is skipped on Windows across the workspace tests; mirror that.
            if std::env::consts::OS == "windows" {
                return Ok(());
            }

            use crate::config::UserConfig;
            use crate::constants::TABLE_NAME;
            use crate::core::db::data_frames::df_db;
            use crate::core::db::data_frames::df_db::with_df_db_manager;
            use crate::model::{LocalRepository, Workspace};
            use crate::repositories;
            use std::path::Path;

            let (s3_store, _tmp, _server) = setup_dyn().await;

            crate::test::run_training_data_repo_test_fully_committed_async(|repo| async move {
                let file_path = Path::new("annotations")
                    .join("train")
                    .join("bounding_box.csv");
                let commit = repositories::commits::head_commit(&repo)?;

                // Mirror the committed file's version bytes into the S3 store under the same hash
                // so the S3-backed index path has them to fetch.
                let node =
                    repositories::tree::get_node_by_path_with_children(&repo, &commit, &file_path)?
                        .expect("committed file should resolve in the merkle tree");
                let hash = node.hash.to_string();
                let bytes = repo.version_store().get_version(&hash).await?;
                s3_store.store_version(&hash, bytes.into()).await?;

                // Build a workspace, then point its base repo at the S3-backed store.
                let workspace_id = UserConfig::identifier()?;
                let workspace =
                    repositories::workspaces::create(&repo, &commit, workspace_id, true)?;
                let s3_base = LocalRepository::new_for_testing(&workspace.base_repo, s3_store);
                let s3_workspace = Workspace {
                    base_repo: s3_base,
                    ..workspace
                };

                // version_location -> S3 -> copy_version_to_path(temp) -> DuckDB read of the temp.
                repositories::workspaces::data_frames::index(
                    &s3_workspace.base_repo,
                    &s3_workspace,
                    &file_path,
                )
                .await?;

                // The indexed table should be populated from the S3-sourced bytes.
                let db_path =
                    repositories::workspaces::data_frames::duckdb_path(&s3_workspace, &file_path);
                let count = with_df_db_manager(&db_path, |manager| {
                    manager.with_conn(|conn| {
                        let n = df_db::count(conn, TABLE_NAME)?;
                        Ok(n)
                    })
                })?;
                assert!(count > 0, "expected the S3-indexed table to have rows");

                Ok(())
            })
            .await
        }
    }
}
