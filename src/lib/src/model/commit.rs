use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};
use time::OffsetDateTime;

use super::{Branch, MerkleHash, User};
use crate::core::v0_10_0::index::CommitReader;
use crate::error::OxenError;
use crate::view::workspaces::WorkspaceCommit;
use core::convert::Into;

use std::str::FromStr;

/// NewCommitBody is used to parse the json into a Commit from the API
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NewCommitBody {
    pub message: String,
    pub author: String,
    pub email: String,
}

/// NewCommit is to be used when creating a new Commit, but we don't know the id yet because we need to hash the contents
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NewCommit {
    pub parent_ids: Vec<String>,
    pub message: String,
    pub author: String,
    pub email: String,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}

impl NewCommit {
    pub fn from_commit(commit: &Commit) -> NewCommit {
        NewCommit {
            parent_ids: commit.parent_ids.to_owned(),
            message: commit.message.to_owned(),
            author: commit.author.to_owned(),
            email: commit.email.to_owned(),
            timestamp: commit.timestamp.to_owned(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Commit {
    pub id: String,
    pub parent_ids: Vec<String>,
    pub message: String,
    pub author: String,
    pub email: String,
    pub root_hash: Option<String>, // Option for now to facilitate migration from older stored commits
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
}

impl From<Commit> for WorkspaceCommit {
    fn from(val: Commit) -> Self {
        WorkspaceCommit {
            id: val.id,
            message: val.message,
            author: val.author,
            email: val.email,
            timestamp: val.timestamp,
        }
    }
}

impl fmt::Display for Commit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} -> '{}'", self.id, self.message)
    }
}

// TODO: is there a way to derive all these values...and just add one new?
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommitWithSize {
    pub id: String,
    pub parent_ids: Vec<String>,
    pub message: String,
    pub author: String,
    pub email: String,
    pub root_hash: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub size: u64,
}

// TODO: is there a way to derive all these values...and just add one new?
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommitWithBranchName {
    pub id: String,
    pub parent_ids: Vec<String>,
    pub message: String,
    pub author: String,
    pub email: String,
    pub root_hash: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub timestamp: OffsetDateTime,
    pub size: u64,
    pub branch_name: String,
}

// Hash on the id field so we can quickly look up
impl PartialEq for Commit {
    fn eq(&self, other: &Commit) -> bool {
        self.id == other.id
    }
}
impl Eq for Commit {}
impl Hash for Commit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl std::error::Error for Commit {}

impl Commit {
    pub fn from_new_and_id(new_commit: &NewCommit, id: String) -> Commit {
        Commit {
            id,
            parent_ids: new_commit.parent_ids.to_owned(),
            message: new_commit.message.to_owned(),
            author: new_commit.author.to_owned(),
            email: new_commit.email.to_owned(),
            timestamp: new_commit.timestamp.to_owned(),
            root_hash: None,
        }
    }

    pub fn from_new_and_hash(new_commit: &NewCommit, hash: u128) -> Commit {
        Commit {
            id: format!("{hash:x}"),
            parent_ids: new_commit.parent_ids.to_owned(),
            message: new_commit.message.to_owned(),
            author: new_commit.author.to_owned(),
            email: new_commit.email.to_owned(),
            timestamp: new_commit.timestamp.to_owned(),
            root_hash: None,
        }
    }

    pub fn has_ancestor(
        &self,
        parent_id: &str,
        commit_reader: &CommitReader,
    ) -> Result<bool, OxenError> {
        if self.id == parent_id {
            return Ok(true);
        }
        for parent in &self.parent_ids {
            if parent == parent_id {
                return Ok(true);
            }

            // Recurse
            let commit = commit_reader.get_commit_by_id(parent)?;
            if let Some(commit) = commit {
                if commit.has_ancestor(parent_id, commit_reader)? {
                    return Ok(true);
                }
            } else {
                return Err(OxenError::local_parent_link_broken(parent));
            }
        }
        Ok(false)
    }

    pub fn update_root_hash(&mut self, root_hash: String) {
        self.root_hash = Some(root_hash);
    }

    pub fn from_with_size(commit: &CommitWithSize) -> Commit {
        Commit {
            id: commit.id.to_owned(),
            parent_ids: commit.parent_ids.to_owned(),
            message: commit.message.to_owned(),
            author: commit.author.to_owned(),
            email: commit.email.to_owned(),
            timestamp: commit.timestamp.to_owned(),
            root_hash: commit.root_hash.to_owned(),
        }
    }

    pub fn from_with_branch_name(commit: &CommitWithBranchName) -> Commit {
        Commit {
            id: commit.id.to_owned(),
            parent_ids: commit.parent_ids.to_owned(),
            message: commit.message.to_owned(),
            author: commit.author.to_owned(),
            email: commit.email.to_owned(),
            timestamp: commit.timestamp.to_owned(),
            root_hash: commit.root_hash.to_owned(),
        }
    }

    pub fn from_branch(commit_reader: &CommitReader, branch: &Branch) -> Result<Commit, OxenError> {
        commit_reader
            .get_commit_by_id(&branch.commit_id)?
            .ok_or(OxenError::revision_not_found(
                branch.commit_id.to_string().into(),
            ))
    }

    pub fn hash(&self) -> Result<MerkleHash, OxenError> {
        MerkleHash::from_str(&self.id)
    }

    pub fn to_uri_encoded(&self) -> String {
        serde_url_params::to_string(&self).unwrap()
    }

    pub fn get_user(&self) -> User {
        User {
            name: self.author.to_owned(),
            email: self.email.to_owned(),
        }
    }
}

impl CommitWithSize {
    pub fn from_commit(commit: &Commit, size: u64) -> CommitWithSize {
        CommitWithSize {
            id: commit.id.to_owned(),
            parent_ids: commit.parent_ids.to_owned(),
            message: commit.message.to_owned(),
            author: commit.author.to_owned(),
            email: commit.email.to_owned(),
            timestamp: commit.timestamp.to_owned(),
            root_hash: commit.root_hash.to_owned(),
            size,
        }
    }
}

impl CommitWithBranchName {
    pub fn from_commit(commit: &Commit, size: u64, branch_name: String) -> CommitWithBranchName {
        CommitWithBranchName {
            id: commit.id.to_owned(),
            parent_ids: commit.parent_ids.to_owned(),
            message: commit.message.to_owned(),
            author: commit.author.to_owned(),
            email: commit.email.to_owned(),
            timestamp: commit.timestamp.to_owned(),
            root_hash: commit.root_hash.to_owned(),
            size,
            branch_name,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitStats {
    pub commit: Commit,
    pub num_entries: usize, // this is how many entries are in our commit db
    pub num_synced_files: usize, // this is how many files are actually synced (in case we killed)
}

impl CommitStats {
    pub fn is_synced(&self) -> bool {
        self.num_entries == self.num_synced_files
    }
}
