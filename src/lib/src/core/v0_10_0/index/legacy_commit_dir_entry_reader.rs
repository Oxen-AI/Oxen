use crate::constants::{FILES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};
use std::str;
/// # CommitDirEntryReader
/// We keep a list of all the committed files in a subdirectory directory for fast lookup
pub struct LegacyCommitDirEntryReader {
    db: DBWithThreadMode<MultiThreaded>,
}

impl LegacyCommitDirEntryReader {
    /// .oxen/history/commit_id/files/path/to/dir
    pub fn db_dir(base_path: &Path, commit_id: &str, dir: &Path) -> PathBuf {
        if dir == Path::new("") {
            return util::fs::oxen_hidden_dir(base_path)
                .join(HISTORY_DIR)
                .join(commit_id)
                .join(FILES_DIR);
        }

        util::fs::oxen_hidden_dir(base_path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(FILES_DIR)
            .join(dir)
    }

    pub fn db_exists(base_path: &Path, commit_id: &str, dir: &Path) -> bool {
        // Must check the CURRENT file since the .oxen/history/COMMIT_ID/files/ path
        // may have already been created by a deeper object
        let db_path = LegacyCommitDirEntryReader::db_dir(base_path, commit_id, dir);
        db_path.join("CURRENT").exists()
    }

    /// # Create new commit dir
    /// Contains all the committed files within that dir, for faster filtering per dir
    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
    ) -> Result<LegacyCommitDirEntryReader, OxenError> {
        LegacyCommitDirEntryReader::new_from_path(&repository.path, commit_id, dir)
    }

    pub fn new_from_path(
        base_path: &Path,
        commit_id: &str,
        dir: &Path,
    ) -> Result<LegacyCommitDirEntryReader, OxenError> {
        let db_path = LegacyCommitDirEntryReader::db_dir(base_path, commit_id, dir);
        log::debug!(
            "CommitDirEntryReader::new() dir {:?} db_path {:?}",
            dir,
            db_path
        );

        let opts = db::key_val::opts::default();
        if !LegacyCommitDirEntryReader::db_exists(base_path, commit_id, dir) {
            if let Err(err) = std::fs::create_dir_all(&db_path) {
                log::error!("CommitDirEntryReader could not create dir {db_path:?}\nErr: {err:?}");
            }
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
        }

        Ok(LegacyCommitDirEntryReader {
            db: DBWithThreadMode::open_for_read_only(&opts, &db_path, true)?,
        })
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        path_db::list_entries(&self.db)
    }
}
