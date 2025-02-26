use crate::error::OxenError;
use std::collections::HashMap;
use std::fmt::Debug;

/// Trait defining operations for version file storage backends
pub trait VersionStore: Debug + Send + Sync + 'static {
    /// Initialize the storage backend
    fn init(&self) -> Result<(), OxenError>;

    /// Store a version file
    ///
    /// # Arguments
    /// * `hash` - The content hash that identifies this version
    /// * `data` - The raw bytes to store
    fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError>;

    /// Retrieve a version file's contents
    ///
    /// # Arguments
    /// * `hash` - The content hash of the version to retrieve
    fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError>;

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
