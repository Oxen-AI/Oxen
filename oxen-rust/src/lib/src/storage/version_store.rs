use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Seek};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use image::DynamicImage;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite};
use tokio_stream::Stream;

use crate::constants;
use crate::error::OxenError;
use crate::opts::StorageOpts;
use crate::storage::{LocalVersionStore, S3VersionStore};
use crate::util;
use crate::view::versions::CleanCorruptedVersionsResult;

/// Configuration for version storage backend
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StorageConfig {
    /// Storage type: "local" or "s3"
    #[serde(rename = "type")]
    pub type_: String,
    /// Backend-specific settings
    #[serde(default)]
    pub settings: HashMap<String, String>,
}

/// Trait for async read and seek operations
pub trait AsyncReadSeek: AsyncRead + AsyncSeek + Send + Sync + Unpin {}

/// Implement AsyncReadSeek for any type that implements both AsyncRead and AsyncSeek
impl<T: AsyncRead + AsyncSeek + Send + Sync + Unpin> AsyncReadSeek for T {}

/// Trait for async write operations
pub trait AsyncWriteSeek: AsyncWrite + AsyncSeek + Send + Sync + Unpin {}

/// Implement AsyncWriteSeek for any type that implements both AsyncWrite and AsyncSeek
impl<T: AsyncWrite + AsyncSeek + Send + Sync + Unpin> AsyncWriteSeek for T {}

/// Trait for sync read and seek operations
pub trait ReadSeek: Read + Seek + Send + Sync {}

/// Implement ReadSeek for any type that implements both Read and Seek
impl<T: Read + Seek + Send + Sync> ReadSeek for T {}

/// Trait defining operations for version file storage backends
#[async_trait]
pub trait VersionStore: Debug + Send + Sync + 'static {
    /// Initialize the storage backend
    async fn init(&self) -> Result<(), OxenError>;

    /// Store a version file from a file path
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `file_path` - Path to the file to store
    async fn store_version_from_path(&self, hash: &str, file_path: &Path) -> Result<(), OxenError>;

    /// Store a version file from an async reader
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `reader` - Any type that implements Read trait
    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), OxenError>;

    /// Store a version file from bytes
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `data` - The raw bytes to store
    async fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError>;

    /// Store a chunk of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `offset` - The starting byte position of the chunk
    /// * `data` - The raw bytes to store
    async fn store_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<(), OxenError>;

    /// Store a derived version file (resized, video thumbnail etc.)
    ///
    /// # Arguments
    /// * `derived_image` - The derived image to store, used for local processing
    /// * `image_buf` - The raw bytes to store, used for S3
    /// * `derived_path` - Path/key to the derived version file
    async fn store_version_derived(
        &self,
        derived_image: DynamicImage,
        image_buf: Vec<u8>,
        derived_path: &Path,
    ) -> Result<(), OxenError>;

    /// Get a writer for a chunk of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `offset` - The starting byte position of the chunk
    async fn get_version_chunk_writer(
        &self,
        hash: &str,
        offset: u64,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>, OxenError>;

    /// Retrieve a chunk of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `offset` - The starting byte position of the chunk
    /// * `size` - The chunk size
    async fn get_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OxenError>;

    /// Get a chunk of a version file as a stream of bytes
    ///    
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    /// * `offset` - The starting byte position of the chunk
    /// * `size` - The chunk size
    async fn get_version_chunk_stream(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>;

    /// List all chunks for a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    async fn list_version_chunks(&self, hash: &str) -> Result<Vec<u64>, OxenError>;

    /// Combine all the chunks for a version file into a single file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `cleanup` - Whether to delete the chunks after combining. If false, the chunks will be left in place.
    ///   May be helpful for debugging or chunk-level deduplication.
    async fn combine_version_chunks(&self, hash: &str, cleanup: bool)
        -> Result<PathBuf, OxenError>;

    /// Get metadata of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    async fn get_version_size(&self, hash: &str) -> Result<u64, OxenError>;

    /// Retrieve a version file's contents as bytes (less efficient for large files)
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    async fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError>;

    /// Get a version file as a stream of bytes
    ///    
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    async fn get_version_stream(
        &self,
        hash: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>;

    /// Get stream of a derived version file (resized, video thumbnail etc.)
    ///
    /// # Arguments
    /// * `derived_path` - Path/key to the derived version file
    async fn get_version_derived_stream(
        &self,
        derived_path: &Path,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>;

    /// Get the path to a version file (sync operation)
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    fn get_version_path(&self, hash: &str) -> Result<PathBuf, OxenError>;

    /// Copy a version to a destination path
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    /// * `dest_path` - Destination path to copy the file to
    async fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError>;

    /// Check if a version exists
    ///
    /// # Arguments
    /// * `hash` - The content hash to check
    async fn version_exists(&self, hash: &str) -> Result<bool, OxenError>;

    /// Delete a version
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to delete
    async fn delete_version(&self, hash: &str) -> Result<(), OxenError>;

    /// List all versions
    async fn list_versions(&self) -> Result<Vec<String>, OxenError>;

    /// Clean corrupted version files
    /// If `dry_run` is true, only scan and report without deleting
    async fn clean_corrupted_versions(
        &self,
        dry_run: bool,
    ) -> Result<CleanCorruptedVersionsResult, OxenError>;

    /// Get the storage type identifier (e.g., "local", "s3")
    fn storage_type(&self) -> &str;

    /// Get the storage-specific settings
    fn storage_settings(&self) -> HashMap<String, String>;
}

// This only creates a version store struct, it does not initialize it
pub fn create_version_store(
    repo_dir: &Path,
    storage_opts: &StorageOpts,
) -> Result<Arc<dyn VersionStore>, OxenError> {
    match storage_opts.type_.as_str() {
        "local" => {
            let Some(ref local_storage_opts) = storage_opts.local_storage_opts else {
                return Err(OxenError::basic_str("local storage opts not found"));
            };

            let versions_dir = if let Some(path) = &local_storage_opts.path {
                if path.starts_with(".oxen") {
                    // if the path is relative, convert to absolute path
                    repo_dir.join(path)
                } else {
                    path.clone()
                }
            } else {
                util::fs::oxen_hidden_dir(repo_dir)
                    .join(constants::VERSIONS_DIR)
                    .join(constants::FILES_DIR)
            };

            let store = LocalVersionStore::new(versions_dir);

            Ok(Arc::new(store))
        }
        "s3" => {
            let Some(ref s3_opts) = storage_opts.s3_opts else {
                return Err(OxenError::basic_str("s3 storage opts not found"));
            };

            let bucket = s3_opts.bucket.clone();
            let prefix = s3_opts.prefix.clone().unwrap_or("versions".to_string());
            let store = S3VersionStore::new(bucket, prefix);

            Ok(Arc::new(store))
        }
        _ => Err(OxenError::basic_str(format!(
            "Unsupported async storage type: {}",
            storage_opts.type_
        ))),
    }
}
