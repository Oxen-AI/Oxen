use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::constants::VERSION_FILE_NAME;
use crate::error::OxenError;
use crate::storage::version_store::ReadSeek;
use crate::util;

use super::version_store::VersionStore;

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

    /// Get the full path for a version file
    fn version_path(&self, hash: &str) -> PathBuf {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        self.root_path
            .join(topdir)
            .join(subdir)
            .join(VERSION_FILE_NAME)
    }

    /// Get the directory containing a version file
    fn version_dir(&self, hash: &str) -> PathBuf {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        self.root_path.join(topdir).join(subdir)
    }
}

impl VersionStore for LocalVersionStore {
    fn init(&self) -> Result<(), OxenError> {
        util::fs::create_dir_all(&self.root_path)
    }

    fn store_version_from_path(&self, hash: &str, file_path: &Path) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        util::fs::create_dir_all(&version_dir)?;

        let version_path = self.version_path(hash);
        fs::copy(file_path, &version_path)?;

        Ok(())
    }

    fn store_version_from_reader(
        &self,
        hash: &str,
        reader: &mut dyn Read,
    ) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        util::fs::create_dir_all(&version_dir)?;

        let version_path = self.version_path(hash);
        let mut file = File::create(&version_path)?;
        io::copy(reader, &mut file)?;

        Ok(())
    }

    fn store_version(&self, hash: &str, data: &[u8]) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        util::fs::create_dir_all(&version_dir)?;

        let version_path = self.version_path(hash);
        let mut file = File::create(&version_path)?;
        file.write_all(data)?;

        Ok(())
    }

    fn open_version(&self, hash: &str) -> Result<Box<dyn ReadSeek>, OxenError> {
        let path = self.version_path(hash);
        let file = File::open(&path)?;
        Ok(Box::new(file))
    }

    fn get_version(&self, hash: &str) -> Result<Vec<u8>, OxenError> {
        let path = self.version_path(hash);
        Ok(fs::read(&path)?)
    }

    fn copy_version_to_path(&self, hash: &str, dest_path: &Path) -> Result<(), OxenError> {
        let version_path = self.version_path(hash);
        fs::copy(&version_path, dest_path)?;
        Ok(())
    }

    fn version_exists(&self, hash: &str) -> Result<bool, OxenError> {
        Ok(self.version_path(hash).exists())
    }

    fn delete_version(&self, hash: &str) -> Result<(), OxenError> {
        let version_dir = self.version_dir(hash);
        if version_dir.exists() {
            util::fs::remove_dir_all(&version_dir)?;
        }
        Ok(())
    }

    fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        let mut versions = Vec::new();

        // Walk through the two-level directory structure
        for top_entry in fs::read_dir(&self.root_path)? {
            let top_entry = top_entry?;
            if !top_entry.file_type()?.is_dir() {
                continue;
            }

            let top_name = top_entry.file_name();
            for sub_entry in fs::read_dir(top_entry.path())? {
                let sub_entry = sub_entry?;
                if !sub_entry.file_type()?.is_dir() {
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

    fn setup() -> (TempDir, LocalVersionStore) {
        let temp_dir = TempDir::new().unwrap();
        let store = LocalVersionStore::new(temp_dir.path());
        store.init().unwrap();
        (temp_dir, store)
    }

    #[test]
    fn test_init() {
        let (_temp_dir, store) = setup();
        assert!(store.root_path.exists());
        assert!(store.root_path.is_dir());
    }

    #[test]
    fn test_store_and_get_version() {
        let (_temp_dir, store) = setup();
        let hash = "abcdef1234567890";
        let data = b"test data";

        // Store the version
        store.store_version(hash, data).unwrap();

        // Verify the file exists with correct structure
        let version_path = store.version_path(hash);
        assert!(version_path.exists());
        assert_eq!(version_path.parent().unwrap(), store.version_dir(hash));

        // Get and verify the data
        let retrieved = store.get_version(hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_store_from_reader() {
        let (_temp_dir, store) = setup();
        let hash = "abcdef1234567890";
        let data = b"test data from reader";

        // Create a cursor with the test data
        let mut cursor = Cursor::new(data.to_vec());

        // Store using the reader
        store.store_version_from_reader(hash, &mut cursor).unwrap();

        // Verify the file exists
        let version_path = store.version_path(hash);
        assert!(version_path.exists());

        // Get and verify the data
        let retrieved = store.get_version(hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_open_version() {
        let (_temp_dir, store) = setup();
        let hash = "abcdef1234567892";
        let data = b"test data for open";

        // Store the version
        store.store_version(hash, data).unwrap();

        // Open the version as a reader
        let mut reader = store.open_version(hash).unwrap();

        // Read the data
        let mut retrieved = Vec::new();
        reader.read_to_end(&mut retrieved).unwrap();

        // Verify the data
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_version_exists() {
        let (_temp_dir, store) = setup();
        let hash = "abcdef1234567890";
        let data = b"test data";

        // Check non-existent version
        assert!(!store.version_exists(hash).unwrap());

        // Store and check again
        store.store_version(hash, data).unwrap();
        assert!(store.version_exists(hash).unwrap());
    }

    #[test]
    fn test_delete_version() {
        let (_temp_dir, store) = setup();
        let hash = "abcdef1234567890";
        let data = b"test data";

        // Store and verify
        store.store_version(hash, data).unwrap();
        assert!(store.version_exists(hash).unwrap());

        // Delete and verify
        store.delete_version(hash).unwrap();
        assert!(!store.version_exists(hash).unwrap());
        assert!(!store.version_dir(hash).exists());
    }

    #[test]
    fn test_list_versions() {
        let (_temp_dir, store) = setup();
        let hashes = vec!["abcdef1234567890", "bbcdef1234567890", "cbcdef1234567890"];
        let data = b"test data";

        // Store multiple versions
        for hash in &hashes {
            store.store_version(hash, data).unwrap();
        }

        // List and verify
        let mut versions = store.list_versions().unwrap();
        versions.sort();
        assert_eq!(versions.len(), hashes.len());

        let mut expected = hashes.clone();
        expected.sort();
        assert_eq!(versions, expected);
    }

    #[test]
    fn test_get_nonexistent_version() {
        let (_temp_dir, store) = setup();
        let hash = "nonexistent";

        match store.get_version(hash) {
            Ok(_) => panic!("Expected error when getting non-existent version"),
            Err(e) => {
                assert!(e.to_string().contains("No such file or directory"));
            }
        }
    }

    #[test]
    fn test_delete_nonexistent_version() {
        let (_temp_dir, store) = setup();
        let hash = "nonexistent";

        // Should not error when deleting non-existent version
        store.delete_version(hash).unwrap();
    }
}
