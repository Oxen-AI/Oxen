use std::collections::HashMap;
use std::io::{self};
use std::path::{Path, PathBuf};

use crate::constants::{VERSION_CHUNKS_DIR, VERSION_CHUNK_FILE_NAME, VERSION_FILE_NAME};
use crate::error::OxenError;
use crate::storage::version_store::{ReadSeek, VersionStore};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;
use tokio_stream::Stream;
use tokio_util::io::ReaderStream;

/// Local filesystem implementation of version storage
#[derive(Debug)]
pub struct LocalVersionStore {
    /// Root path where versions are stored
    root_path: PathBuf,
}

impl LocalVersionStore {
    /// Create a new LocalVersionStore
    ///
    /// # Arguments
    /// * `root_path` - Base directory for version storage
    pub fn new(root_path: impl AsRef<Path>) -> Self {
        Self {
            root_path: root_path.as_ref().to_path_buf(),
        }
    }

    /// Get the directory containing a version file
    fn version_dir(&self, hash: &str) -> PathBuf {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        self.root_path.join(topdir).join(subdir)
    }

    /// Get the full path for a version file
    fn version_path(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_FILE_NAME)
    }

    /// Get the directory containing all the chunks for a version file
    fn version_chunks_dir(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_CHUNKS_DIR)
    }

    /// Get the directory containing a version file
    /// .oxen/versions/{hash}/chunks/{chunk_number}
    fn version_chunk_dir(&self, hash: &str, chunk_number: u32) -> PathBuf {
        self.version_chunks_dir(hash).join(chunk_number.to_string())
    }

    /// Get the directory containing a version file
    fn version_chunk_file(&self, hash: &str, chunk_number: u32) -> PathBuf {
        self.version_chunk_dir(hash, chunk_number)
            .join(VERSION_CHUNK_FILE_NAME)
    }
}

#[async_trait]
impl VersionStore for LocalVersionStore {
    async fn init(&self) -> Result<(), OxenError> {
        if !self.root_path.exists() {
            fs::create_dir_all(&self.root_path).await?;
        }
        Ok(())
    }

    async fn store_version_from_path(&self, hash: &str, file_path: &Path) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        fs::create_dir_all(&version_dir).await?;

        let version_path = self.version_path(hash);
        if !version_path.exists() {
            fs::copy(file_path, &version_path).await?;
        }
        Ok(())
    }

    async fn store_version_from_reader(
        &self,
        hash: &str,
        reader: &mut (dyn tokio::io::AsyncRead + Send + Unpin),
    ) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        fs::create_dir_all(&version_dir).await?;

        let version_path = self.version_path(hash);

        if !version_path.exists() {
            let mut file = File::create(&version_path).await?;
            tokio::io::copy(reader, &mut file).await?;
        }

        Ok(())
    }

    async fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        fs::create_dir_all(&version_dir).await?;

        let version_path = self.version_path(hash);

        if !version_path.exists() {
            fs::write(&version_path, data).await?;
        }

        Ok(())
    }

    fn open_version(
        &self,
        hash: &str,
    ) -> Result<Box<dyn ReadSeek + Send + Sync + 'static>, OxenError> {
        let path = self.version_path(hash);
        let file = std::fs::File::open(&path)?;
        Ok(Box::new(file))
    }

    async fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError> {
        let path = self.version_path(hash);
        let data = fs::read(&path).await?;
        Ok(data)
    }

    async fn get_version_stream(
        &self,
        hash: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let path = self.version_path(hash);
        let file = File::open(&path).await?;
        let reader = BufReader::new(file);
        let stream = ReaderStream::new(reader);

        Ok(Box::new(stream))
    }

    fn get_version_path(&self, hash: &str) -> Result<PathBuf, OxenError> {
        Ok(self.version_path(hash))
    }

    async fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError> {
        let version_path = self.version_path(hash);
        fs::copy(&version_path, dest_path).await?;
        Ok(())
    }

    fn version_exists(&self, hash: &str) -> Result<bool, OxenError> {
        Ok(self.version_path(hash).exists())
    }

    async fn delete_version(&self, hash: &str) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        if version_dir.exists() {
            fs::remove_dir_all(&version_dir).await?;
        }
        Ok(())
    }

    async fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        let mut versions = Vec::new();

        // Walk through the two-level directory structure
        let mut top_entries = fs::read_dir(&self.root_path).await?;
        while let Some(top_entry) = top_entries.next_entry().await? {
            let file_type = top_entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }

            let top_name = top_entry.file_name();
            let mut sub_entries = fs::read_dir(top_entry.path()).await?;
            while let Some(sub_entry) = sub_entries.next_entry().await? {
                let file_type = sub_entry.file_type().await?;
                if !file_type.is_dir() {
                    continue;
                }

                let sub_name = sub_entry.file_name();
                let hash = format!(
                    "{}{}",
                    top_name.to_string_lossy(),
                    sub_name.to_string_lossy()
                );
                versions.push(hash);
            }
        }

        Ok(versions)
    }

    async fn store_version_chunk(
        &self,
        hash: &str,
        chunk_number: u32,
        data: &[u8],
    ) -> Result<(), OxenError> {
        let chunk_dir = self.version_chunk_dir(hash, chunk_number);
        fs::create_dir_all(&chunk_dir).await?;

        let chunk_path = self.version_chunk_file(hash, chunk_number);

        if !chunk_path.exists() {
            fs::write(&chunk_path, data).await?;
        }

        Ok(())
    }

    async fn get_version_chunk(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        let version_file_path = self.version_path(hash);

        let mut file = File::open(&version_file_path).await?;
        let metadata = file.metadata().await?;
        let file_len = metadata.len();

        if offset >= file_len || offset + size > file_len {
            return Err(OxenError::IO(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "beyond end of file",
            )));
        }

        let read_len = std::cmp::min(size, file_len - offset);
        if read_len > usize::MAX as u64 {
            return Err(OxenError::basic_str("requested chunk too large"));
        }

        use tokio::io::{AsyncSeekExt, SeekFrom};
        file.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; read_len as usize];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    async fn get_version_chunk_stream(
        &self,
        hash: &str,
        offset: u64,
        size: u64,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let version_file_path = self.version_path(hash);

        let mut file = File::open(&version_file_path).await?;
        let metadata = file.metadata().await?;
        let file_len = metadata.len();

        if offset >= file_len || offset + size > file_len {
            return Err(OxenError::IO(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "beyond end of file",
            )));
        }

        let read_len = std::cmp::min(size, file_len - offset);
        if read_len > usize::MAX as u64 {
            return Err(OxenError::basic_str("requested chunk too large"));
        }

        use tokio::io::{AsyncSeekExt, SeekFrom};
        file.seek(SeekFrom::Start(offset)).await?;

        // Create a limited reader that only reads the specified range
        let limited_reader = file.take(read_len);
        let reader = BufReader::new(limited_reader);
        let stream = ReaderStream::new(reader);

        Ok(Box::new(stream))
    }

    async fn list_version_chunks(&self, hash: &str) -> Result<Vec<u32>, OxenError> {
        let chunk_dir = self.version_chunks_dir(hash);
        let mut chunks = Vec::new();

        let mut entries = fs::read_dir(&chunk_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                if let Ok(chunk_number) = entry.file_name().to_string_lossy().parse::<u32>() {
                    chunks.push(chunk_number);
                }
            }
        }
        Ok(chunks)
    }

    async fn combine_version_chunks(
        &self,
        hash: &str,
        cleanup: bool,
    ) -> Result<PathBuf, OxenError> {
        let version_path = self.version_path(hash);
        let mut output_file = File::create(&version_path).await?;

        // Get list of chunks and sort them to ensure correct order
        let mut chunks = self.list_version_chunks(hash).await?;
        chunks.sort();

        // Process each chunk
        for chunk_number in chunks {
            let chunk_path = self.version_chunk_file(hash, chunk_number);
            let mut chunk_file = File::open(&chunk_path).await?;
            tokio::io::copy(&mut chunk_file, &mut output_file).await?;

            // Cleanup chunk if requested
            if cleanup {
                let chunk_dir = self.version_chunk_dir(hash, chunk_number);
                fs::remove_dir_all(&chunk_dir).await?;
            }
        }

        // Cleanup the chunks directory if requested
        if cleanup {
            let chunks_dir = self.version_chunks_dir(hash);
            if chunks_dir.exists() {
                fs::remove_dir_all(&chunks_dir).await?;
            }
        }

        Ok(version_path)
    }

    fn storage_type(&self) -> &str {
        "local"
    }

    fn storage_settings(&self) -> HashMap<String, String> {
        // Local storage doesn't need any special settings
        HashMap::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::TempDir;

    async fn setup() -> (TempDir, LocalVersionStore) {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalVersionStore::new(temp_dir.path());
        store.init().await.unwrap();
        (temp_dir, store)
    }

    #[tokio::test]
    async fn test_init() {
        let (_temp_dir, store) = setup().await;
        assert!(store.root_path.exists());
        assert!(store.root_path.is_dir());
    }

    #[tokio::test]
    async fn test_store_and_get_version() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";
        let data = b"test data";

        // Store the version
        store.store_version(hash, data).await.unwrap();

        // Verify the file exists with correct structure
        let version_path = store.version_path(hash);
        assert!(version_path.exists());
        assert_eq!(version_path.parent().unwrap(), store.version_dir(hash));

        // Get and verify the data
        let retrieved = store.get_version(hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_store_from_reader() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";
        let data = b"test data from reader";

        // Create a cursor with the test data
        let mut cursor = Cursor::new(data.to_vec());

        // Store using the reader
        store
            .store_version_from_reader(hash, &mut cursor)
            .await
            .unwrap();

        // Verify the file exists
        let version_path = store.version_path(hash);
        assert!(version_path.exists());

        // Get and verify the data
        let retrieved = store.get_version(hash).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_open_version() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567892";
        let data = b"test data for open";

        // Store the version
        store.store_version(hash, data).await.unwrap();

        // Open the version as a reader
        let mut reader = store.open_version(hash).unwrap();

        // Read the data
        let mut retrieved = Vec::new();
        reader.read_to_end(&mut retrieved).unwrap();

        // Verify the data
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_version_exists() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";
        let data = b"test data";

        // Check non-existent version
        assert!(!store.version_exists(hash).unwrap());

        // Store and check again
        store.store_version(hash, data).await.unwrap();
        assert!(store.version_exists(hash).unwrap());
    }

    #[tokio::test]
    async fn test_delete_version() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";
        let data = b"test data";

        // Store and verify
        store.store_version(hash, data).await.unwrap();
        assert!(store.version_exists(hash).unwrap());

        // Delete and verify
        store.delete_version(hash).await.unwrap();
        assert!(!store.version_exists(hash).unwrap());
        assert!(!store.version_dir(hash).exists());
    }

    #[tokio::test]
    async fn test_list_versions() {
        let (_temp_dir, store) = setup().await;
        let hashes = vec!["abcdef1234567890", "bbcdef1234567890", "cbcdef1234567890"];
        let data = b"test data";

        // Store multiple versions
        for hash in &hashes {
            store.store_version(hash, data).await.unwrap();
        }

        // List and verify
        let mut versions = store.list_versions().await.unwrap();
        versions.sort();
        assert_eq!(versions.len(), hashes.len());

        let mut expected = hashes.clone();
        expected.sort();
        assert_eq!(versions, expected);
    }

    #[tokio::test]
    async fn test_get_nonexistent_version() {
        let (_temp_dir, store) = setup().await;
        let hash = "nonexistent";

        match store.get_version(hash).await {
            Ok(_) => panic!("Expected error when getting non-existent version"),
            Err(OxenError::IO(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
            Err(e) => {
                panic!(
                    "Unexpected error when getting non-existent version: {:?}",
                    e
                );
            }
        }
    }

    #[tokio::test]
    async fn test_delete_nonexistent_version() {
        let (_temp_dir, store) = setup().await;
        let hash = "nonexistent";

        // Should not error when deleting non-existent version
        store.delete_version(hash).await.unwrap();
    }

    #[tokio::test]
    async fn test_store_and_get_version_chunk() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";
        let offset = 0;
        let data = b"test chunk data";
        let size = data.len() as u64;

        // Store the chunk
        store.store_version(hash, data).await.unwrap();

        // Verify the file exists with correct structure
        let file_path = store.version_path(hash);
        assert!(file_path.exists());
        assert_eq!(file_path.parent().unwrap(), store.version_dir(hash));

        // Get and verify the data
        let retrieved = store.get_version_chunk(hash, offset, size).await.unwrap();
        assert_eq!(retrieved, data);
    }

    #[tokio::test]
    async fn test_get_nonexistent_chunk() {
        let (_temp_dir, store) = setup().await;
        let hash = "abcdef1234567890";
        let offset = 0;
        let size = 100;

        match store.get_version_chunk(hash, offset, size).await {
            Ok(_) => panic!("Expected error when getting non-existent chunk"),
            Err(OxenError::IO(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
            Err(e) => {
                panic!("Unexpected error when getting non-existent chunk: {:?}", e);
            }
        }
    }
}
