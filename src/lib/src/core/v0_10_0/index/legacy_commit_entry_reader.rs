// Only necessary for facilitating migrations from old commit storage formats to new ones
use crate::constants::{DIRS_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::v0_10_0::index::LegacyCommitDirEntryReader;
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry};
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::{Path, PathBuf};

use crate::core::db::key_val::path_db;
use crate::model::LocalRepository;

pub struct LegacyCommitEntryReader {
    base_path: PathBuf,
    dir_db: DBWithThreadMode<MultiThreaded>,
    pub commit_id: String,
}

impl LegacyCommitEntryReader {
    pub fn db_path(base_path: impl AsRef<Path>, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(&base_path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(DIRS_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<LegacyCommitEntryReader, OxenError> {
        log::debug!("CommitEntryReader::new() commit_id: {}", commit.id);
        LegacyCommitEntryReader::new_from_commit_id(repository, &commit.id)
    }

    pub fn new_from_commit_id(
        repository: &LocalRepository,
        commit_id: &str,
    ) -> Result<LegacyCommitEntryReader, OxenError> {
        LegacyCommitEntryReader::new_from_path(&repository.path, commit_id)
    }

    pub fn new_from_path(
        base_path: impl AsRef<Path>,
        commit_id: &str,
    ) -> Result<LegacyCommitEntryReader, OxenError> {
        let path = Self::db_path(&base_path, commit_id);
        let opts = db::key_val::opts::default();
        log::debug!(
            "CommitEntryReader::new_from_path() commit_id: {} path: {:?}",
            commit_id,
            path
        );

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
        }

        Ok(LegacyCommitEntryReader {
            base_path: base_path.as_ref().to_owned(),
            dir_db: DBWithThreadMode::open_for_read_only(&opts, &path, true)?,
            commit_id: commit_id.to_owned(),
        })
    }

    /// Lists all the directories in the commit
    pub fn list_dirs(&self) -> Result<Vec<PathBuf>, OxenError> {
        let root = PathBuf::from("");
        let mut paths = path_db::list_paths(&self.dir_db, &root)?;
        if !paths.contains(&root) {
            paths.push(root);
        }
        paths.sort();
        Ok(paths)
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        for dir in self.list_dirs()? {
            let commit_dir =
                LegacyCommitDirEntryReader::new_from_path(&self.base_path, &self.commit_id, &dir)?;
            let mut files = commit_dir.list_entries()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }
}
