use crate::constants::COMMITS_DIR;
use crate::core::db;
use crate::core::v0_10_0::index::CommitDBReader;
use crate::error::OxenError;
use crate::model::Commit;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::str;

use crate::model::LocalRepository;

pub struct CommitReader {
    repository: LocalRepository,
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitReader {
    pub fn db_path(repository: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repository.path).join(COMMITS_DIR)
    }

    /// Create a new reader that can find commits, list history, etc
    pub fn new(repository: &LocalRepository) -> Result<CommitReader, OxenError> {
        let path = Self::db_path(repository);
        let opts = db::key_val::opts::default();

        log::debug!("CommitReader::new path: {:?}", path);

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
        }

        Ok(CommitReader {
            repository: repository.clone(),
            db: DBWithThreadMode::open_for_read_only(&opts, &path, false)?,
        })
    }

    /// Returns all the commit objects in a repo, in no particular order
    pub fn list_all(&self) -> Result<HashSet<Commit>, OxenError> {
        CommitDBReader::list_all(&self.db)
    }

    /// Returns all the commit objects ordered by timestamp
    pub fn list_all_sorted_by_timestamp(&self) -> Result<Vec<Commit>, OxenError> {
        let all = CommitDBReader::list_all(&self.db)?;
        let mut all_vec: Vec<Commit> = Vec::from_iter(all);
        all_vec.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));
        Ok(all_vec)
    }

    /// Return the latest commit by timestamp
    pub fn latest_commit(&self) -> Result<Commit, OxenError> {
        CommitDBReader::latest_commit(&self.db)
    }

    /// Return the head commit
    pub fn head_commit(&self) -> Result<Commit, OxenError> {
        CommitDBReader::head_commit(&self.repository, &self.db)
    }

    /// Get the head commit if it exists
    pub fn head_commit_maybe(&self) -> Result<Option<Commit>, OxenError> {
        CommitDBReader::head_commit_maybe(&self.repository, &self.db)
    }

    /// Get the root commit of the db
    pub fn root_commit(&self) -> Result<Commit, OxenError> {
        // Traverse db to find root commit
        CommitDBReader::root_commit(&self.repository, &self.db)
    }

    /// List the commit history starting at a commit id
    pub fn history_from_commit_id(&self, commit_id: &str) -> Result<Vec<Commit>, OxenError> {
        let mut commits: HashSet<Commit> = HashSet::new();
        CommitDBReader::history_from_commit_id(&self.db, commit_id, &mut commits)?;
        let mut commits: Vec<Commit> = commits.into_iter().collect();
        commits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(commits)
    }

    pub fn history_from_base_to_head(
        &self,
        base_commit_id: &str,
        head_commit_id: &str,
    ) -> Result<Vec<Commit>, OxenError> {
        let mut commits: HashSet<Commit> = HashSet::new();
        CommitDBReader::history_from_base_to_head(
            &self.db,
            base_commit_id,
            head_commit_id,
            &mut commits,
        )?;

        let mut commits: Vec<Commit> = commits.into_iter().collect();
        commits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(commits)
    }

    /// List the commit history from the HEAD commit
    pub fn history_from_head(&self) -> Result<Vec<Commit>, OxenError> {
        if self.repository.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
        }

        let head_commit = self.head_commit()?;
        let mut commits: Vec<Commit> = CommitDBReader::history_from_commit(&self.db, &head_commit)?
            .into_iter()
            .collect();
        commits.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        Ok(commits)
    }

    /// List the commit history from a commit keeping track of depth along the way
    pub fn history_with_depth_from_commit(
        &self,
        commit: &Commit,
    ) -> Result<HashMap<Commit, usize>, OxenError> {
        CommitDBReader::history_with_depth_from_commit(&self.db, commit)
    }

    /// List the commit history from a commit keeping track of depth along the way
    pub fn history_with_depth_from_head(&self) -> Result<HashMap<Commit, usize>, OxenError> {
        let head = self.head_commit()?;
        CommitDBReader::history_with_depth_from_commit(&self.db, &head)
    }

    /// See if a commit id exists
    pub fn commit_id_exists(&self, commit_id: &str) -> bool {
        CommitDBReader::commit_id_exists(&self.db, commit_id)
    }

    /// Get a commit object from an ID
    pub fn get_commit_by_id(
        &self,
        commit_id: impl AsRef<str>,
    ) -> Result<Option<Commit>, OxenError> {
        CommitDBReader::get_commit_by_id(&self.db, commit_id.as_ref())
    }
}

#[cfg(test)]
mod tests {

    use crate::constants::INITIAL_COMMIT_MSG;
    use crate::core::v0_10_0::index::CommitReader;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::test;

    #[test]
    fn test_get_root_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_w_version(
            MinOxenVersion::V0_10_0,
            |repo| {
                let commit_reader = CommitReader::new(&repo)?;
                let root_commit = commit_reader.root_commit()?;

                assert_eq!(root_commit.message, INITIAL_COMMIT_MSG);

                Ok(())
            },
        )
    }
}
