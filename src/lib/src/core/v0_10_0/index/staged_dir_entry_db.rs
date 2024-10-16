//! # StagedDirEntryDB
//!
//! This module helps read and write to the staging area on `oxen add`
//! It takes a directory and creates a db for that directory allowing fast
//! querying for files per directory that is staged
//!

use crate::constants::STAGED_DIR;
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository, StagedEntry, StagedEntryStatus};
use crate::util;

use rocksdb::{DBWithThreadMode, ThreadMode};
use std::path::{Path, PathBuf};

/// # StagedDirEntryDB
/// We keep a list of all the staged files in a directory
/// for more efficient lookup per directory
pub struct StagedDirEntryDB<T: ThreadMode> {
    pub db: DBWithThreadMode<T>,
    dir: PathBuf,
    pub repository: LocalRepository,
}

pub fn staging_dir(repo: &LocalRepository, dir: &Path) -> PathBuf {
    // log::debug!("StagedDirEntryDB got repo path {:?}", repo.path);
    util::fs::oxen_hidden_dir(&repo.path)
        .join(Path::new(STAGED_DIR))
        .join("files")
        .join(dir)
}

impl<T: ThreadMode> StagedDirEntryDB<T> {
    /// # Create new staged dir
    /// Contains all the staged files within that dir,
    /// for faster filtering during `oxen status`
    pub fn new(repository: &LocalRepository, dir: &Path) -> Result<StagedDirEntryDB<T>, OxenError> {
        let read_only = false;
        StagedDirEntryDB::p_new(repository, dir, read_only)
    }

    /// # Create read only version
    pub fn new_read_only(
        repository: &LocalRepository,
        dir: &Path,
    ) -> Result<StagedDirEntryDB<T>, OxenError> {
        let read_only = true;
        StagedDirEntryDB::p_new(repository, dir, read_only)
    }

    pub fn p_new(
        repository: &LocalRepository,
        dir: &Path,
        read_only: bool,
    ) -> Result<StagedDirEntryDB<T>, OxenError> {
        log::debug!(
            "StagedDirEntryDB p_new {:?} read_only: {:?}",
            dir,
            read_only
        );
        let db_path = staging_dir(repository, dir);

        log::debug!("StagedDirEntryDB db_path {:?}", db_path);
        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }
        let opts = db::key_val::opts::default();
        let db = if read_only {
            // Before opening for read only, we need to make sure the db is instantiated on disk
            if !db_path.join("CURRENT").exists() {
                if let Err(err) = std::fs::create_dir_all(&db_path) {
                    log::error!(
                        "StagedDirEntryDB could not create dir {:?}\nerr: {:?}",
                        db_path,
                        err
                    );
                }
                // open it then lose scope to close it
                let _db: DBWithThreadMode<T> =
                    DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
            }

            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), false)?
        } else {
            DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?
        };
        Ok(StagedDirEntryDB {
            db,
            dir: dir.to_owned(),
            repository: repository.clone(),
        })
    }

    /// # Stages a file that has been removed from the index
    /// Flags it as removed, so we know when committing that we have to remove it from the commit db
    pub fn add_removed_file<P: AsRef<Path>>(
        &self,
        path: P,
        entry: &CommitEntry,
    ) -> Result<StagedEntry, OxenError> {
        let path = path.as_ref();
        let entry = StagedEntry {
            hash: entry.hash.clone(),
            status: StagedEntryStatus::Removed,
        };

        log::debug!(
            "staged_dir_entry_db {:?} add_removed_file {:?} -> {:?}",
            self.dir,
            path,
            entry.status
        );
        path_db::put(&self.db, path, &entry)?;

        Ok(entry)
    }

    /// # Checks if the file exists in this directory
    /// More efficient than get_entry since it does not actual deserialize the entry
    pub fn has_entry<P: AsRef<Path>>(&self, path: P) -> bool {
        path_db::has_entry(&self.db, path)
    }

    /// # Get the staged entry object from the file path
    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<StagedEntry>, OxenError> {
        path_db::get_entry(&self.db, path)
    }

    /// # Serializes the entry to json and writes to db
    pub fn add_staged_entry_to_db<P: AsRef<Path>>(
        &self,
        path: P,
        staged_entry: &StagedEntry,
    ) -> Result<(), OxenError> {
        path_db::put(&self.db, path, staged_entry)
    }

    /// # List the file paths in the staged dir
    /// More efficient than list_added_path_entries since it does not deserialize the entries
    pub fn list_added_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        path_db::list_paths(&self.db, &self.dir)
    }

    /// # List file names and attached entries
    pub fn list_added_path_entries(&self) -> Result<Vec<(PathBuf, StagedEntry)>, OxenError> {
        path_db::list_path_entries(&self.db, &self.dir)
    }

    /// Remove a specifc file from the staged idx
    pub fn remove_path<P: AsRef<Path>>(&self, path: P) -> Result<(), OxenError> {
        path_db::delete(&self.db, path)
    }

    /// Clear all the entries from being staged
    pub fn unstage(&self) -> Result<(), OxenError> {
        path_db::clear(&self.db)
    }
}
