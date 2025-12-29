use std;
use std::collections::HashMap;
use std::io::{self};
use std::path::{Path, PathBuf};

use crate::constants::{VERSION_CHUNKS_DIR, VERSION_CHUNK_FILE_NAME, VERSION_FILE_NAME};
use crate::error::OxenError;
use crate::storage::version_store::VersionStore;
use crate::util::{self, concurrency, hasher};
use crate::view::versions::CleanCorruptedVersionsResult;

use async_trait::async_trait;
use bytes::Bytes;
use image::{self, DynamicImage};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::io::{BufReader, BufWriter};
use tokio::sync::Semaphore;
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
    /// .oxen/versions/{hash}/chunks
    fn version_chunk_dir(&self, hash: &str, offset: u64) -> PathBuf {
        self.version_chunks_dir(hash).join(offset.to_string())
    }

    /// Get the directory containing a version file
    fn version_chunk_file(&self, hash: &str, offset: u64) -> PathBuf {
        self.version_chunk_dir(hash, offset)
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

    async fn store_version_derived(
        &self,
        derived_image: DynamicImage,
        _image_buf: Vec<u8>,
        derived_path: &Path,
    ) -> Result<(), OxenError> {
        let path = PathBuf::from(derived_path);
        let derived_parent = path.parent().unwrap_or(Path::new(""));
        if !derived_parent.exists() {
            util::fs::create_dir_all(derived_parent)?;
        }
        derived_image.save(derived_path)?;
        log::debug!("Saved derived version file {derived_path:?}");

        Ok(())
    }

    async fn get_version_size(&self, hash: &str) -> Result<u64, OxenError> {
        let path = self.version_path(hash);
        let metadata = fs::metadata(&path).await?;
        Ok(metadata.len())
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

    async fn get_version_derived_stream(
        &self,
        derived_path: &Path,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        let file = File::open(derived_path).await?;
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
        offset: u64,
        data: &[u8],
    ) -> Result<(), OxenError> {
        let chunk_dir = self.version_chunk_dir(hash, offset);
        fs::create_dir_all(&chunk_dir).await?;

        let chunk_path = self.version_chunk_file(hash, offset);

        if !chunk_path.exists() {
            fs::write(&chunk_path, data).await?;
        }

        Ok(())
    }

    async fn get_version_chunk_writer(
        &self,
        hash: &str,
        offset: u64,
    ) -> Result<Box<dyn tokio::io::AsyncWrite + Send + Unpin>, OxenError> {
        let chunk_dir = self.version_chunk_dir(hash, offset);
        fs::create_dir_all(&chunk_dir).await?;

        let chunk_path = self.version_chunk_file(hash, offset);
        let file = File::create(&chunk_path).await?;
        let writer = BufWriter::new(file);

        Ok(Box::new(writer))
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

    async fn list_version_chunks(&self, hash: &str) -> Result<Vec<u64>, OxenError> {
        let chunk_dir = self.version_chunks_dir(hash);
        let mut chunks = Vec::new();

        let mut entries = fs::read_dir(&chunk_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                if let Ok(chunk_offset) = entry.file_name().to_string_lossy().parse::<u64>() {
                    chunks.push(chunk_offset);
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
        log::debug!("combine_version_chunks found {:?} chunks", chunks.len());
        chunks.sort();

        // Process each chunk
        for chunk_offset in chunks {
            let chunk_path = self.version_chunk_file(hash, chunk_offset);
            let mut chunk_file = File::open(&chunk_path).await?;
            tokio::io::copy(&mut chunk_file, &mut output_file).await?;
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

    // It's left here for a quick fix. TODO: Move the business logic to versions controller.
    async fn clean_corrupted_versions(&self) -> Result<CleanCorruptedVersionsResult, OxenError> {
        // Keep record of the stats
        #[derive(Default)]
        struct Stats {
            scanned_objects: AtomicUsize,
            corrupted_objects: AtomicUsize,
            io_errors: AtomicUsize,
            deleted_objects: AtomicUsize,
        }

        impl Stats {
            fn incr_scanned(&self) {
                self.scanned_objects.fetch_add(1, Ordering::Relaxed);
            }
            fn incr_corrupted(&self) {
                self.corrupted_objects.fetch_add(1, Ordering::Relaxed);
            }
            fn incr_io_error(&self) {
                self.io_errors.fetch_add(1, Ordering::Relaxed);
            }
            fn incr_deleted(&self) {
                self.deleted_objects.fetch_add(1, Ordering::Relaxed);
            }

            fn snapshot(&self) -> (usize, usize, usize, usize) {
                (
                    self.scanned_objects.load(Ordering::Relaxed),
                    self.corrupted_objects.load(Ordering::Relaxed),
                    self.io_errors.load(Ordering::Relaxed),
                    self.deleted_objects.load(Ordering::Relaxed),
                )
            }
        }

        let start = std::time::Instant::now();
        let stats = Arc::new(Stats::default());

        // Limit concurrency
        let concurrency = concurrency::default_num_threads();
        let semaphore = Arc::new(Semaphore::new(concurrency));

        // Read prefix dirs
        let mut prefix_rd = fs::read_dir(&self.root_path).await?;
        let mut prefix_paths: Vec<PathBuf> = Vec::new();
        while let Some(entry) = prefix_rd.next_entry().await? {
            match entry.file_type().await {
                Ok(ft) if ft.is_dir() => {
                    prefix_paths.push(entry.path());
                }
                _ => {
                    stats.incr_io_error();
                }
            }
        }

        // Spawn one task per prefix (concurrent)
        let mut handles = Vec::with_capacity(prefix_paths.len());

        for prefix_path in prefix_paths {
            let semaphore_cl = semaphore.clone();
            let stats_cl = stats.clone();
            let handle = tokio::spawn(async move {
                let mut suffix_rd = match fs::read_dir(&prefix_path).await {
                    Ok(rd) => rd,
                    Err(_) => {
                        stats_cl.incr_io_error();
                        return;
                    }
                };

                // Process suffix directories sequentially
                while let Ok(Some(entry)) = suffix_rd.next_entry().await {
                    // Only process directories
                    let file_type = match entry.file_type().await {
                        Ok(ft) => ft,
                        Err(_) => {
                            stats_cl.incr_io_error();
                            continue;
                        }
                    };
                    if !file_type.is_dir() {
                        continue;
                    }

                    let suffix_path = entry.path();

                    // hash = prefix+suffix
                    let prefix_name = match prefix_path.file_name().and_then(|s| s.to_str()) {
                        Some(p) => p.to_string(),
                        None => {
                            stats_cl.incr_io_error();
                            continue;
                        }
                    };
                    let suffix_name = match suffix_path.file_name().and_then(|s| s.to_str()) {
                        Some(s) => s.to_string(),
                        None => {
                            stats_cl.incr_io_error();
                            continue;
                        }
                    };
                    let expected_hash = format!("{prefix_name}{suffix_name}");

                    // Acquire concurrency permit for heavy operations (I/O, hashing)
                    let permit = semaphore_cl.clone().acquire_owned().await.unwrap();

                    {
                        // First read the data async
                        let data_path = suffix_path.join("data");
                        let data = match fs::read(&data_path).await {
                            Ok(b) => b,
                            Err(_) => {
                                // cannot read data - treat as corrupted: delete dir
                                stats_cl.incr_io_error();

                                if fs::remove_dir_all(&suffix_path).await.is_ok() {
                                    stats_cl.incr_deleted();
                                }
                                drop(permit);
                                continue;
                            }
                        };

                        // increment scanned count
                        stats_cl.incr_scanned();

                        // Compute hash in blocking thread
                        let actual_hash =
                            match tokio::task::spawn_blocking(move || hasher::hash_buffer(&data))
                                .await
                            {
                                Ok(h) => h,
                                Err(_) => {
                                    log::debug!(
                                        "Failed to compute hash for version file {expected_hash}"
                                    );
                                    stats_cl.incr_io_error();

                                    if fs::remove_dir_all(&suffix_path).await.is_ok() {
                                        stats_cl.incr_deleted();
                                    }
                                    drop(permit);
                                    continue;
                                }
                            };

                        if actual_hash != expected_hash {
                            log::debug!("version file {actual_hash} is corrupted!");
                            // mismatch - delete the corrupted version dir
                            stats_cl.incr_corrupted();
                            match fs::remove_dir_all(&suffix_path).await {
                                Ok(_) => {
                                    stats_cl.incr_deleted();
                                    log::debug!("Corrupted version file {actual_hash} deleted");
                                }
                                Err(_) => {
                                    stats_cl.incr_io_error();
                                    log::debug!(
                                        "Failed to delete corrupted version file {actual_hash}"
                                    );
                                }
                            }
                        }

                        // release permit
                        drop(permit);
                    }
                }
            });

            handles.push(handle);
        }

        // Await all prefix tasks
        for h in handles {
            let _ = h.await;
        }

        // Gather stats and return result
        let (scanned, corrupted, io_errors, deleted) = stats.snapshot();
        let elapsed = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);
        let result = CleanCorruptedVersionsResult {
            scanned: scanned as u64,
            corrupted: corrupted as u64,
            cleaned: deleted as u64,
            errors: io_errors as u64,
            elapsed,
        };

        Ok(result)
    }

    fn storage_type(&self) -> &str {
        "local"
    }

    fn storage_settings(&self) -> HashMap<String, String> {
        let mut settings = HashMap::new();

        let root_path_str = self.root_path.to_str().unwrap_or("").to_string();
        settings.insert("path".to_string(), root_path_str);

        settings
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
                panic!("Unexpected error when getting non-existent version: {e:?}");
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
                panic!("Unexpected error when getting non-existent chunk: {e:?}");
            }
        }
    }
}
