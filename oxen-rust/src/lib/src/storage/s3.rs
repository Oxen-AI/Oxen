use crate::error::OxenError;
use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{primitives::ByteStream, Client};
use bytes::Bytes;
use std::collections::HashMap;
use std::fs::Metadata;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio_stream::Stream;

use super::version_store::VersionStore;
use crate::constants::VERSION_FILE_NAME;
use crate::storage::version_store::ReadSeek;

/// S3 implementation of version storage
#[derive(Debug)]
pub struct S3VersionStore {
    client: Arc<OnceCell<Client>>,
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
            client: Arc::new(OnceCell::new()),
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    async fn init_client(&self) -> Client {
        self.client
            .get_or_init(|| async {
                let region_provider = RegionProviderChain::default_provider().or_else("us-west-1");
                let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .region(region_provider)
                    .load()
                    .await;
                Client::new(&config)
            })
            .await
            .clone()
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
        let client = self.init_client().await;

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
                        let _ = client
                            .delete_object()
                            .bucket(&self.bucket)
                            .key(&test_key)
                            .send()
                            .await;
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
        let client = self.init_client().await;
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
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError> {
        let client = self.init_client().await;
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

    fn store_version_blocking(&self, _hash: &str, _data: &[u8]) -> Result<(), OxenError> {
        // TODO: Implement S3 version storage
        // This method may not be necessary for S3
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    fn open_version(
        &self,
        _hash: &str,
    ) -> Result<Box<dyn ReadSeek + Send + Sync + 'static>, OxenError> {
        // TODO: Implement S3 version opening
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn get_version_metadata(&self, _hash: &str) -> Result<Metadata, OxenError> {
        // TODO: Implement S3 version retrieval
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn get_version(&self, _hash: &str) -> Result<Vec<u8>, OxenError> {
        // TODO: Implement S3 version retrieval
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn get_version_stream(
        &self,
        _hash: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        // TODO: Implement S3 version stream retrieval
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    fn get_version_path(&self, _hash: &str) -> Result<PathBuf, OxenError> {
        // TODO: Implement S3 version path retrieval
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn copy_version_to_path(&self, _hash: &str, _dest_path: &Path) -> Result<(), OxenError> {
        // TODO: Implement S3 version copying to path
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn store_version_chunk(
        &self,
        _hash: &str,
        _offset: u64,
        _data: &[u8],
    ) -> Result<(), OxenError> {
        // TODO: Implement S3 version chunk storage
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn get_version_chunk_writer(
        &self,
        _hash: &str,
        _offset: u64,
    ) -> Result<Box<dyn tokio::io::AsyncWrite + Send + Unpin>, OxenError> {
        // TODO: Implement S3 version chunk stream storage
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn get_version_chunk(
        &self,
        _hash: &str,
        _offset: u64,
        _size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        // TODO: Implement S3 version chunk retrieval
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn get_version_chunk_stream(
        &self,
        _hash: &str,
        _offset: u64,
        _size: u64,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        // TODO: Implement S3 version chunk stream retrieval
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn list_version_chunks(&self, _hash: &str) -> Result<Vec<u64>, OxenError> {
        // TODO: Implement S3 version chunk listing
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    fn version_exists(&self, _hash: &str) -> Result<bool, OxenError> {
        // TODO: Implement S3 version existence check
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn delete_version(&self, _hash: &str) -> Result<(), OxenError> {
        // TODO: Implement S3 version deletion
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        // TODO: Implement S3 version listing
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
    }

    async fn combine_version_chunks(
        &self,
        _hash: &str,
        _cleanup: bool,
    ) -> Result<PathBuf, OxenError> {
        // TODO: Implement S3 version chunk combination
        Err(OxenError::basic_str("S3VersionStore not yet implemented"))
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
