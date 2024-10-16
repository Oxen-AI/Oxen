use crate::core::db::key_val::str_json_db;
use crate::core::refs::RefReader;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::str;

pub struct CommitDBReader {}

impl CommitDBReader {
    /// Return the latest commit by timestamp
    pub fn latest_commit(db: &DBWithThreadMode<MultiThreaded>) -> Result<Commit, OxenError> {
        let commits: Vec<Commit> = str_json_db::list_vals(db)?;
        // log::debug!("Looking for latest commit in path: {:?}", db.path());
        // log::debug!("Finding latest commit from {} commits", commits.len());
        let mut latest_commit: Option<Commit> = None;
        for commit in commits.iter() {
            if latest_commit.is_none() {
                latest_commit = Some(commit.clone());
            } else {
                let latest_commit_timestamp = latest_commit.as_ref().unwrap().timestamp;
                if commit.timestamp > latest_commit_timestamp {
                    latest_commit = Some(commit.clone());
                }
            }
        }

        if latest_commit.is_none() {
            return Err(OxenError::no_commits_found());
        }

        Ok(latest_commit.unwrap())
    }

    /// List all the commits for the repo
    pub fn list_all(db: &DBWithThreadMode<MultiThreaded>) -> Result<HashSet<Commit>, OxenError> {
        let commits: Vec<Commit> = str_json_db::list_vals(db)?;
        Ok(commits.into_iter().collect())
    }

    pub fn head_commit(
        repo: &LocalRepository,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<Commit, OxenError> {
        let ref_reader = RefReader::new(repo)?;
        match ref_reader.head_commit_id() {
            Ok(Some(commit_id)) => {
                let commit = CommitDBReader::get_commit_by_id(db, &commit_id)?
                    .ok_or_else(|| OxenError::commit_db_corrupted(commit_id))?;
                Ok(commit)
            }
            Ok(None) => Err(OxenError::head_not_found()),
            Err(err) => Err(err),
        }
    }

    pub fn head_commit_maybe(
        repo: &LocalRepository,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<Option<Commit>, OxenError> {
        let ref_reader = RefReader::new(repo)?;
        match ref_reader.head_commit_id() {
            Ok(Some(commit_id)) => CommitDBReader::get_commit_by_id(db, &commit_id),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn root_commit(
        repo: &LocalRepository,
        db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<Commit, OxenError> {
        let head_commit = CommitDBReader::head_commit(repo, db)?;

        let commit = CommitDBReader::rget_root_commit(db, &head_commit.id)?;
        if let Some(root) = commit {
            Ok(root)
        } else {
            log::error!("could not find root....");
            Err(OxenError::commit_db_corrupted(head_commit.id))
        }
    }

    fn rget_root_commit(
        db: &DBWithThreadMode<MultiThreaded>,
        commit_id: &str,
    ) -> Result<Option<Commit>, OxenError> {
        let commit = CommitDBReader::get_commit_by_id(db, commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(commit_id))?;

        if commit.parent_ids.is_empty() {
            return Ok(Some(commit));
        }

        for parent_id in commit.parent_ids.iter() {
            // Recursive call to this module
            match CommitDBReader::rget_root_commit(db, parent_id) {
                Ok(commit) => {
                    return Ok(commit);
                }
                Err(err) => {
                    log::error!("rget_root_commit cannot get root: {}", err);
                }
            }
        }
        Ok(None)
    }

    pub fn get_commit_by_id(
        db: &DBWithThreadMode<MultiThreaded>,
        commit_id: &str,
    ) -> Result<Option<Commit>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match db.get(key) {
            Ok(Some(value)) => {
                let commit: Commit = serde_json::from_str(str::from_utf8(&value)?)?;
                Ok(Some(commit))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("Error commits_db to find commit_id {commit_id:?}\nErr: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn commit_id_exists(db: &DBWithThreadMode<MultiThreaded>, commit_id: &str) -> bool {
        match CommitDBReader::get_commit_by_id(db, commit_id) {
            Ok(Some(_commit)) => true,
            Ok(None) => false,
            Err(err) => {
                log::error!("commit_id_exists err: {:?}", err);
                false
            }
        }
    }

    pub fn history_from_commit(
        db: &DBWithThreadMode<MultiThreaded>,
        commit: &Commit,
    ) -> Result<HashSet<Commit>, OxenError> {
        let mut commit_msgs: HashSet<Commit> = HashSet::new();
        CommitDBReader::history_from_commit_id(db, &commit.id, &mut commit_msgs)?;
        Ok(commit_msgs)
    }

    pub fn history_with_depth_from_commit(
        db: &DBWithThreadMode<MultiThreaded>,
        commit: &Commit,
    ) -> Result<HashMap<Commit, usize>, OxenError> {
        let mut commit_msgs: HashMap<Commit, usize> = HashMap::new();
        let initial_depth: usize = 0;
        CommitDBReader::history_with_depth_from_commit_id(
            db,
            &commit.id,
            &mut commit_msgs,
            initial_depth,
        )?;
        Ok(commit_msgs)
    }

    pub fn history_from_commit_id(
        db: &DBWithThreadMode<MultiThreaded>,
        commit_id: &str,
        commits: &mut HashSet<Commit>,
    ) -> Result<(), OxenError> {
        match CommitDBReader::get_commit_by_id(db, commit_id) {
            Ok(Some(commit)) => {
                commits.insert(commit.to_owned());
                for parent_id in commit.parent_ids.iter() {
                    CommitDBReader::history_from_commit_id(db, parent_id, commits)?;
                }
                Ok(())
            }
            Ok(None) => Err(OxenError::commit_id_does_not_exist(commit_id)),
            Err(e) => Err(e),
        }
    }

    pub fn history_from_base_to_head(
        db: &DBWithThreadMode<MultiThreaded>,
        base_commit_id: &str,
        head_commit_id: &str,
        commits: &mut HashSet<Commit>,
    ) -> Result<(), OxenError> {
        // TODO: This way of calculating has some issues for divergent / merge commits.
        // in those cases, the "base=head" base case is insufficient and it will trace
        // all the way back to root commit. The parent set logic below covers some of these cases,
        // but might need to rethink how this is calculated for ease of use in push

        // End recursion, we found all the commits from base to head
        log::debug!(
            "history_from_base_to_head searching bewteen {} and {}",
            base_commit_id,
            head_commit_id
        );
        if base_commit_id == head_commit_id {
            return Ok(());
        }

        let base_commit = CommitDBReader::get_commit_by_id(db, base_commit_id)?
            .ok_or_else(|| OxenError::commit_id_does_not_exist(base_commit_id))?;

        match CommitDBReader::get_commit_by_id(db, head_commit_id) {
            Ok(Some(commit)) => {
                commits.insert(commit.to_owned());

                let base_set: HashSet<_> = base_commit.parent_ids.iter().cloned().collect();
                let head_set: HashSet<_> = commit.parent_ids.iter().cloned().collect();
                if base_set == head_set {
                    return Ok(());
                }

                for parent_id in commit.parent_ids.iter() {
                    CommitDBReader::history_from_base_to_head(
                        db,
                        base_commit_id,
                        parent_id,
                        commits,
                    )?;
                }
                Ok(())
            }
            Ok(None) => Err(OxenError::commit_id_does_not_exist(head_commit_id)),
            Err(e) => Err(e),
        }
    }

    pub fn history_with_depth_from_commit_id(
        db: &DBWithThreadMode<MultiThreaded>,
        commit_id: &str,
        commits: &mut HashMap<Commit, usize>,
        depth: usize,
    ) -> Result<(), OxenError> {
        if let Some(commit) = CommitDBReader::get_commit_by_id(db, commit_id)? {
            commits.insert(commit.clone(), depth);
            for parent_id in commit.parent_ids.iter() {
                CommitDBReader::history_with_depth_from_commit_id(
                    db,
                    parent_id,
                    commits,
                    depth + 1,
                )?;
            }
        }
        Ok(())
    }
}
