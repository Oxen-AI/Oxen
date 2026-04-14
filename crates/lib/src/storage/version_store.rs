use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncRead;
use tokio_stream::Stream;

use crate::constants;
use crate::error::OxenError;
use crate::opts::StorageOpts;
use crate::storage::{LocalVersionStore, S3VersionStore};
use crate::util;
use crate::view::versions::CleanCorruptedVersionsResult;

/// A local filesystem path to a version file.
///
/// The path is guaranteed to be readable on the local filesystem, but callers must NOT assume it is
/// stable: non-local backends (e.g. S3) materialize the file into a temporary location that is
/// cleaned up when this value is dropped.
///
/// - Implements `Deref<Target = Path>` so that `&LocalFilePath` can be passed directly to any
///   function that accepts `&Path`.
/// - Implements `AsRef<Path>` so that `LocalFilePath` can be passed directly to any function that
///   accepts `impl AsRef<Path>`.
///
/// TODO: See how many of our own functions can be updated to accept LocalFilePath directly. Perhaps we can remove the need for `AsRef<Path>`.
pub enum LocalFilePath {
    /// A stable path (e.g. from `LocalVersionStore`) that outlives this value.
    Stable(PathBuf),
    /// A temporary file that is deleted when this value is dropped.
    Temp(async_tempfile::TempFile),
}

impl Deref for LocalFilePath {
    type Target = Path;

    fn deref(&self) -> &Path {
        match self {
            LocalFilePath::Stable(p) => p.as_path(),
            LocalFilePath::Temp(t) => t.file_path(),
        }
    }
}

impl AsRef<Path> for LocalFilePath {
    fn as_ref(&self) -> &Path {
        self.deref()
    }
}

impl std::fmt::Debug for LocalFilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LocalFilePath::Stable(p) => write!(f, "Stable({p:?})"),
            LocalFilePath::Temp(t) => write!(f, "Temp({:?})", t.file_path()),
        }
    }
}

impl std::fmt::Display for LocalFilePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.deref().display())
    }
}

impl LocalFilePath {
    pub fn to_pathbuf(&self) -> PathBuf {
        self.deref().to_path_buf()
    }
}

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

/// Trait defining operations for version file storage backends
#[async_trait]
pub trait VersionStore: Debug + Send + Sync + 'static {
    /// Initialize the storage backend
    async fn init(&self) -> Result<(), OxenError>;

    /// Store a version file from an async reader
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `reader` - An owned async reader
    /// * `size` - Total size in bytes, used to tune upload chunk sizes
    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        size: u64,
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
        data: Bytes,
    ) -> Result<(), OxenError>;

    /// Store a derived file (resized image, video thumbnail, etc.) corresponding to a file version
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact (e.g. "200x300.jpg")
    /// * `derived_data` - The raw bytes of the derived artifact to store
    async fn store_version_derived(
        &self,
        orig_hash: &str,
        derived_filename: &str,
        derived_data: &[u8],
    ) -> Result<(), OxenError>;

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

    /// Get the size in bytes of a derived file (e.g., resized image, video thumbnail).
    ///
    /// Returns the content length of the derived artifact on disk.
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact
    ///
    /// # Errors
    /// Returns `OxenError` if the derived file does not exist or cannot be read.
    async fn get_version_derived_size(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<u64, OxenError>;

    /// Get a stream of a derived file (resized, video thumbnail, etc.)
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact
    async fn get_version_derived_stream(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>;

    /// Check if a derived file exists
    ///
    /// # Arguments
    /// * `orig_hash` - The content hash of the parent version
    /// * `derived_filename` - Filename for the derived artifact
    async fn derived_version_exists(
        &self,
        orig_hash: &str,
        derived_filename: &str,
    ) -> Result<bool, OxenError>;

    /// Get a local filesystem path to a version file.
    ///
    /// The returned `LocalFilePath` is guaranteed to be readable on the local
    /// filesystem. For local backends the path points into the version store
    /// directly; for remote backends (e.g. S3) the file is downloaded to a
    /// temporary location that is cleaned up when the `LocalFilePath` is dropped.
    ///
    /// **Callers must keep the returned value alive for as long as they use the
    /// path.**
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version file to retrieve
    async fn get_version_path(&self, hash: &str) -> Result<LocalFilePath, OxenError>;

    /// Copy a versioned file from the version store to a destination path on the local filesystem
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

/// This only creates a version store struct, it does not initialize it
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
