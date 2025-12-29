use crate::error::OxenError;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, primitives::ByteStream, Client};
use bytes::Bytes;
use image::DynamicImage;
use std::collections::HashMap;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio_stream::Stream;

use super::version_store::VersionStore;
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
                    .unwrap_or("us-west-1".to_string());

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
        _hash: &str,
        _reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> Result<(), OxenError> {
        // TODO: Implement S3 version storage from reader
        Err(OxenError::basic_str(
            "S3VersionStore store_version_from_reader not yet implemented",
        ))
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
        _derived_image: DynamicImage,
        image_buf: Vec<u8>,
        derived_path: &Path,
    ) -> Result<(), OxenError> {
        let client = self.init_client().await?;
        let key = derived_path.to_string_lossy().into_owned();

        client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(image_buf))
            .send()
            .await
            .map_err(|e| {
                OxenError::basic_str(format!("failed to store derived version file in S3: {e}"))
            })?;
        log::debug!("Saved derived version file {derived_path:?}");

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

        let size = resp.content_length().unwrap_or(0) as u64;
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
        derived_path: &Path,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let client = self.init_client().await?;
        let key = derived_path.to_string_lossy().into_owned();

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

    fn get_version_path(&self, hash: &str) -> Result<PathBuf, OxenError> {
        let key = self.generate_key(hash);
        Ok(PathBuf::from(key))
    }

    async fn copy_version_to_path(&self, _hash: &str, _dest_path: &Path) -> Result<(), OxenError> {
        // TODO: Implement S3 version copying to path
        Err(OxenError::basic_str(
            "S3VersionStore copy_version_to_path not yet implemented",
        ))
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

    async fn get_version_chunk_stream(
        &self,
        _hash: &str,
        _offset: u64,
        _size: u64,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        // TODO: Implement S3 version chunk stream retrieval
        Err(OxenError::basic_str(
            "S3VersionStore get_version_chunk_stream not yet implemented",
        ))
    }

    async fn list_version_chunks(&self, _hash: &str) -> Result<Vec<u64>, OxenError> {
        // TODO: Implement S3 version chunk listing
        Err(OxenError::basic_str(
            "S3VersionStore list_version_chunks not yet implemented",
        ))
    }

    fn version_exists(&self, _hash: &str) -> Result<bool, OxenError> {
        // TODO: Implement S3 version existence check
        Err(OxenError::basic_str(
            "S3VersionStore version_exists not yet implemented",
        ))
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

    async fn clean_corrupted_versions(&self) -> Result<CleanCorruptedVersionsResult, OxenError> {
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
