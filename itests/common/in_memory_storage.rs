use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{self, Cursor, Read, Seek};
use std::panic::RefUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use liboxen::error::OxenError;
use liboxen::storage::version_store::{ReadSeek, VersionStore};
use liboxen::util;
use tokio::io::{AsyncRead, AsyncReadExt};

/// In-memory implementation of VersionStore for fast integration tests
/// Stores all data in HashMaps instead of the filesystem
#[derive(Debug)]
pub struct InMemoryVersionStore {
    /// Main storage for version data: hash -> file content
    storage: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// Chunk storage: hash -> (chunk_number -> chunk_data)
    chunks: Arc<Mutex<HashMap<String, HashMap<u32, Vec<u8>>>>>,
    /// Storage type identifier
    storage_type: String,
}

impl InMemoryVersionStore {
    /// Create a new in-memory version store
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
            chunks: Arc::new(Mutex::new(HashMap::new())),
            storage_type: "memory".to_string(),
        }
    }

    /// Preload data into the store for testing
    pub fn preload(&self, hash: &str, data: Vec<u8>) -> Result<(), OxenError> {
        self.storage
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?
            .insert(hash.to_string(), data);
        Ok(())
    }

    /// Clear all stored data (useful for test cleanup)
    pub fn clear(&self) -> Result<(), OxenError> {
        {
            let mut storage = self.storage.lock().map_err(|e| {
                OxenError::basic_str(format!("Failed to acquire storage lock: {}", e))
            })?;
            storage.clear();
        }
        {
            let mut chunks = self.chunks.lock().map_err(|e| {
                OxenError::basic_str(format!("Failed to acquire chunks lock: {}", e))
            })?;
            chunks.clear();
        }
        Ok(())
    }
}

impl Default for InMemoryVersionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// A wrapper around Cursor<Vec<u8>> that implements ReadSeek
#[derive(Debug)]
struct MemoryReader {
    cursor: Cursor<Vec<u8>>,
}

impl MemoryReader {
    fn new(data: Vec<u8>) -> Self {
        Self {
            cursor: Cursor::new(data),
        }
    }
}

impl Read for MemoryReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        std::io::Read::read(&mut self.cursor, buf)
    }
}

impl Seek for MemoryReader {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.cursor.seek(pos)
    }
}

// ReadSeek is automatically implemented for MemoryReader since it implements Read + Seek + Send + Sync

#[async_trait]
impl VersionStore for InMemoryVersionStore {
    async fn init(&self) -> Result<(), OxenError> {
        // Nothing to initialize for in-memory storage
        Ok(())
    }

    async fn store_version_from_path(&self, hash: &str, file_path: &Path) -> Result<(), OxenError> {
        let data = std::fs::read(file_path).map_err(|e| OxenError::IO(e))?;
        self.store_version(hash, &data).await
    }

    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: &mut (dyn AsyncRead + Send + Unpin),
    ) -> Result<(), OxenError> {
        let mut data = Vec::new();
        reader
            .read_to_end(&mut data)
            .await
            .map_err(|e| OxenError::IO(e))?;
        self.store_version(hash, &data).await
    }

    async fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError> {
        self.storage
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?
            .insert(hash.to_string(), data.to_vec());
        Ok(())
    }

    async fn store_version_chunk(
        &self,
        hash: &str,
        chunk_number: u32,
        data: &[u8],
    ) -> Result<(), OxenError> {
        let mut chunks = self
            .chunks
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;

        let file_chunks = chunks.entry(hash.to_string()).or_insert_with(HashMap::new);
        file_chunks.insert(chunk_number, data.to_vec());
        Ok(())
    }

    async fn get_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;

        let data = storage
            .get(hash)
            .ok_or_else(|| OxenError::basic_str(format!("Version not found: {}", hash)))?;

        let start = offset as usize;
        let end = std::cmp::min(start + size as usize, data.len());

        if start >= data.len() {
            return Ok(Vec::new());
        }

        Ok(data[start..end].to_vec())
    }

    async fn list_version_chunks(&self, hash: &str) -> Result<Vec<u32>, OxenError> {
        let chunks = self
            .chunks
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;

        if let Some(file_chunks) = chunks.get(hash) {
            let mut chunk_numbers: Vec<u32> = file_chunks.keys().cloned().collect();
            chunk_numbers.sort();
            Ok(chunk_numbers)
        } else {
            Ok(Vec::new())
        }
    }

    async fn combine_version_chunks(
        &self,
        hash: &str,
        _cleanup: bool,
    ) -> Result<PathBuf, OxenError> {
        // First, collect all chunk data without holding the lock across await
        let combined_data = {
            let chunks = self
                .chunks
                .lock()
                .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;

            let file_chunks = chunks.get(hash).ok_or_else(|| {
                OxenError::basic_str(format!("No chunks found for version: {}", hash))
            })?;

            // Combine chunks in order
            let mut chunk_numbers: Vec<u32> = file_chunks.keys().cloned().collect();
            chunk_numbers.sort();

            let mut combined_data = Vec::new();
            for chunk_number in chunk_numbers {
                if let Some(chunk_data) = file_chunks.get(&chunk_number) {
                    combined_data.extend_from_slice(chunk_data);
                }
            }
            combined_data
        }; // Lock is released here

        // Store the combined data as a regular version
        self.store_version(hash, &combined_data).await?;

        // Return a mock path (in real usage, this would be a temporary file)
        Ok(PathBuf::from(format!("/memory/{}", hash)))
    }

    fn open_version(&self, hash: &str) -> Result<Box<dyn ReadSeek + Send + Sync>, OxenError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;

        let data = storage
            .get(hash)
            .ok_or_else(|| OxenError::basic_str(format!("Version not found: {}", hash)))?
            .clone();

        Ok(Box::new(MemoryReader::new(data)))
    }

    async fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;

        storage
            .get(hash)
            .cloned()
            .ok_or_else(|| OxenError::basic_str(format!("Version not found: {}", hash)))
    }

    fn get_version_path(&self, hash: &str) -> Result<PathBuf, OxenError> {
        // For in-memory storage, we return a mock path
        // In practice, this might create a temporary file if needed
        if self.version_exists(hash)? {
            Ok(PathBuf::from(format!("/memory/{}", hash)))
        } else {
            Err(OxenError::basic_str(format!("Version not found: {}", hash)))
        }
    }

    async fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError> {
        let data = self.get_version(hash).await?;
        util::fs::write(dest_path, data)?;
        Ok(())
    }

    fn version_exists(&self, hash: &str) -> Result<bool, OxenError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;
        Ok(storage.contains_key(hash))
    }

    async fn delete_version(&self, hash: &str) -> Result<(), OxenError> {
        {
            let mut storage = self.storage.lock().map_err(|e| {
                OxenError::basic_str(format!("Failed to acquire storage lock: {}", e))
            })?;
            storage.remove(hash);
        }
        {
            let mut chunks = self.chunks.lock().map_err(|e| {
                OxenError::basic_str(format!("Failed to acquire chunks lock: {}", e))
            })?;
            chunks.remove(hash);
        }
        Ok(())
    }

    async fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| OxenError::basic_str(format!("Failed to acquire lock: {}", e)))?;
        Ok(storage.keys().cloned().collect())
    }

    fn storage_type(&self) -> &str {
        &self.storage_type
    }

    fn storage_settings(&self) -> HashMap<String, String> {
        let mut settings = HashMap::new();
        settings.insert("type".to_string(), self.storage_type.clone());
        settings.insert("location".to_string(), "memory".to_string());
        settings
    }
}

// Ensure the type is thread-safe
impl RefUnwindSafe for InMemoryVersionStore {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_storage_operations() {
        let store = InMemoryVersionStore::new();

        // Test initialization
        assert!(store.init().await.is_ok());

        // Test storing and retrieving data
        let hash = "test_hash";
        let data = b"Hello, world!";

        assert!(store.store_version(hash, data).await.is_ok());
        assert!(store.version_exists(hash).unwrap());

        let retrieved = store.get_version(hash).await.unwrap();
        assert_eq!(retrieved, data);

        // Test list versions
        let versions = store.list_versions().await.unwrap();
        assert!(versions.contains(&hash.to_string()));

        // Test deletion
        assert!(store.delete_version(hash).await.is_ok());
        assert!(!store.version_exists(hash).unwrap());
    }

    #[tokio::test]
    async fn test_chunk_operations() {
        let store = InMemoryVersionStore::new();
        let hash = "chunked_file";

        // Store chunks
        assert!(store.store_version_chunk(hash, 0, b"Hello, ").await.is_ok());
        assert!(store.store_version_chunk(hash, 1, b"world!").await.is_ok());

        // List chunks
        let chunks = store.list_version_chunks(hash).await.unwrap();
        assert_eq!(chunks, vec![0, 1]);

        // Combine chunks
        let _path = store.combine_version_chunks(hash, false).await.unwrap();

        // Verify combined data
        let combined = store.get_version(hash).await.unwrap();
        assert_eq!(combined, b"Hello, world!");
    }

    #[tokio::test]
    async fn test_reader_operations() {
        let store = InMemoryVersionStore::new();
        let hash = "reader_test";
        let data = b"Test data for reader";

        store.store_version(hash, data).await.unwrap();

        let mut reader = store.open_version(hash).unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).unwrap();

        assert_eq!(buffer, data);
    }

    #[tokio::test]
    async fn test_preload_functionality() {
        let store = InMemoryVersionStore::new();
        let hash = "preloaded";
        let data = b"Preloaded data".to_vec();

        assert!(store.preload(hash, data.clone()).is_ok());
        assert!(store.version_exists(hash).unwrap());

        let retrieved = store.get_version(hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_clear_functionality() {
        let store = InMemoryVersionStore::new();

        // Add some data
        store.store_version("hash1", b"data1").await.unwrap();
        store.store_version("hash2", b"data2").await.unwrap();

        assert_eq!(store.list_versions().await.unwrap().len(), 2);

        // Clear and verify
        assert!(store.clear().is_ok());
        assert_eq!(store.list_versions().await.unwrap().len(), 0);
    }
}
