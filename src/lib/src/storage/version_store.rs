use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Read, Seek};
use std::panic::RefUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::constants;
use crate::error::OxenError;
use crate::storage::{LocalVersionStore, S3VersionStore};
use crate::util;

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

/// Trait for types that implement Read and Seek
pub trait ReadSeek: Read + Seek + Send + Sync {}

/// Implement ReadSeek for any type that implements both Read and Seek
impl<T: Read + Seek + Send + Sync> ReadSeek for T {}

/// Trait defining operations for version file storage backends
pub trait VersionStore: Debug + Send + Sync + RefUnwindSafe + 'static {
    /// Initialize the storage backend
    fn init(&self) -> Result<(), OxenError>;

    /// Store a version file from a file path
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `file_path` - Path to the file to store
    fn store_version_from_path(&self, hash: &str, file_path: &Path) -> Result<(), OxenError>;

    /// Store a version file from a reader
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `reader` - Any type that implements Read trait
    fn store_version_from_reader(&self, hash: &str, reader: &mut dyn Read)
        -> Result<(), OxenError>;

    /// Store a version file from bytes (less efficient for large files)
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `data` - The raw bytes to store
    fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError>;

    /// Store a chunk of a version file, to be combined into a full version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `chunk_number` - The chunk number to store
    /// * `data` - The raw bytes to store
    fn store_version_chunk(
        &self,
        hash: &str,
        chunk_number: u32,
        data: &[u8],
    ) -> Result<(), OxenError>;

    /// Retrieve a chunk of a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `offset` - The starting byte position of the chunk
    /// * `size` - The chunk size
    fn get_version_chunk(&self, hash: &str, offset: u64, size: u64) -> Result<Vec<u8>, OxenError>;

    /// List all chunks for a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    fn list_version_chunks(&self, hash: &str) -> Result<Vec<u32>, OxenError>;

    /// Combine all the chunks for a version file into a single file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `cleanup` - Whether to delete the chunks after combining. If false, the chunks will be left in place.
    ///   May be helpful for debugging or chunk-level deduplication.
    fn combine_version_chunks(&self, hash: &str, cleanup: bool) -> Result<PathBuf, OxenError>;

    /// Open a version file for reading
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    fn open_version(&self, hash: &str) -> Result<Box<dyn ReadSeek>, OxenError>;

    /// Retrieve a version file's contents as bytes (less efficient for large files)
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError>;

    /// Get the path to a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    fn get_version_path(&self, hash: &str) -> Result<PathBuf, OxenError>;

    /// Copy a version to a destination path
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    /// * `dest_path` - Destination path to copy the file to
    fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError>;

    /// Check if a version exists
    ///
    /// # Arguments
    /// * `hash` - The content hash to check
    fn version_exists(&self, hash: &str) -> Result<bool, OxenError>;

    /// Delete a version
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to delete
    fn delete_version(&self, hash: &str) -> Result<(), OxenError>;

    /// List all versions
    fn list_versions(&self) -> Result<Vec<String>, OxenError>;

    /// Get the storage type identifier (e.g., "local", "s3")
    fn storage_type(&self) -> &str;

    /// Get the storage-specific settings
    fn storage_settings(&self) -> HashMap<String, String>;
}

/// Factory method to create the appropriate version store
pub fn create_version_store(
    path: impl AsRef<Path>,
    storage_config: Option<&StorageConfig>,
) -> Result<Arc<dyn VersionStore>, OxenError> {
    let path = path.as_ref();
    match storage_config {
        Some(config) => match config.type_.as_str() {
            "local" => {
                let versions_dir = util::fs::oxen_hidden_dir(path)
                    .join(constants::VERSIONS_DIR)
                    .join(constants::FILES_DIR);
                let store = LocalVersionStore::new(versions_dir);
                store.init()?;
                Ok(Arc::new(store))
            }
            "s3" => {
                let bucket = config
                    .settings
                    .get("bucket")
                    .ok_or_else(|| OxenError::basic_str("S3 bucket not specified"))?;
                let prefix = config
                    .settings
                    .get("prefix")
                    .cloned()
                    .unwrap_or_else(|| String::from("versions"));
                let store = S3VersionStore::new(bucket, prefix);
                store.init()?;
                Ok(Arc::new(store))
            }
            _ => Err(OxenError::basic_str(format!(
                "Unsupported storage type: {}",
                config.type_
            ))),
        },
        None => {
            // Default to local storage
            let versions_dir = util::fs::oxen_hidden_dir(path)
                .join(constants::VERSIONS_DIR)
                .join(constants::FILES_DIR);
            let store = LocalVersionStore::new(versions_dir);
            store.init()?;
            Ok(Arc::new(store))
        }
    }
}
