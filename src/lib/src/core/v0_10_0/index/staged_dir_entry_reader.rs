//! # StagedDirEntryReader
//!
//! Facade around the StagedDirEntryDB
//! Faster for lookups since it does not allow writing, hence no locking
//!

use crate::core::db::key_val::path_db;
use crate::core::v0_10_0::index::StagedDirEntryDB;
use crate::error::OxenError;
use crate::model::{LocalRepository, StagedEntry};

use indicatif::{ProgressBar, ProgressStyle};
use rocksdb::MultiThreaded;
use std::path::{Path, PathBuf};

pub struct StagedDirEntryReader {
    // https://docs.rs/rocksdb/latest/rocksdb/type.DB.html
    // SingleThreaded does not have the RwLock overhead inside the DB
    // Even with SingleThreaded, almost all of RocksDB operations is
    // multi-threaded unless the underlying RocksDB
    // instance is specifically configured otherwise
    pub db: StagedDirEntryDB<MultiThreaded>,
}

impl StagedDirEntryReader {
    /// # Create new staged dir reader
    pub fn new(
        repository: &LocalRepository,
        dir: &Path,
    ) -> Result<StagedDirEntryReader, OxenError> {
        let db = StagedDirEntryDB::new_read_only(repository, dir)?;
        Ok(StagedDirEntryReader { db })
    }

    /// # Checks if the file exists in this directory
    /// More efficient than get_entry since it does not actual deserialize the entry
    pub fn has_entry<P: AsRef<Path>>(&self, path: P) -> bool {
        self.db.has_entry(path)
    }

    /// # Get the staged entry object from the file path
    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<StagedEntry>, OxenError> {
        self.db.get_entry(path)
    }

    /// # List the file paths in the staged dir
    /// More efficient than list_added_path_entries since it does not deserialize the entries
    pub fn list_added_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        self.db.list_added_paths()
    }

    /// # List file names and attached entries
    pub fn list_added_path_entries(&self) -> Result<Vec<(PathBuf, StagedEntry)>, OxenError> {
        self.db.list_added_path_entries()
    }

    /// # Count the number of files in the staged dir
    pub fn count_added_files(&self, progress: bool) -> Result<usize, OxenError> {
        if progress {
            log::debug!("Counting staged files with progress");
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .unwrap(),
            );
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            pb.set_message("🐂 Counting staged files...".to_string());
            path_db::count(&self.db.db, Some(pb))
        } else {
            path_db::count(&self.db.db, None)
        }
    }
}
