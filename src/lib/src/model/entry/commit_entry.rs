use crate::constants::VERSION_FILE_NAME;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::{Commit, ContentHashable, LocalRepository, RemoteEntry, Schema};
use crate::util;

use filetime::FileTime;
use serde::{Deserialize, Serialize};
use std::env;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub enum Entry {
    CommitEntry(CommitEntry),
    SchemaEntry(SchemaEntry),
}

impl Entry {
    pub fn commit_id(&self) -> String {
        match self {
            Entry::CommitEntry(entry) => entry.commit_id.clone(),
            Entry::SchemaEntry(entry) => entry.commit_id.clone(),
        }
    }

    pub fn path(&self) -> PathBuf {
        match self {
            Entry::CommitEntry(entry) => entry.path.clone(),
            Entry::SchemaEntry(entry) => entry.path.clone(),
        }
    }

    pub fn hash(&self) -> String {
        match self {
            Entry::CommitEntry(entry) => entry.hash.clone(),
            Entry::SchemaEntry(entry) => entry.hash.clone(),
        }
    }

    pub fn num_bytes(&self) -> u64 {
        match self {
            Entry::CommitEntry(entry) => entry.num_bytes,
            Entry::SchemaEntry(entry) => entry.num_bytes,
        }
    }
    pub fn extension(&self) -> String {
        match self {
            Entry::CommitEntry(entry) => entry.extension(),
            Entry::SchemaEntry(_entry) => "".to_string(),
        }
    }
}

// get a From for entry
impl From<CommitEntry> for Entry {
    fn from(entry: CommitEntry) -> Self {
        Entry::CommitEntry(entry)
    }
}

impl From<SchemaEntry> for Entry {
    fn from(entry: SchemaEntry) -> Self {
        Entry::SchemaEntry(entry)
    }
}

impl From<Entry> for CommitEntry {
    fn from(entry: Entry) -> Self {
        match entry {
            Entry::CommitEntry(entry) => entry,
            _ => panic!("Cannot convert Entry to CommitEntry"),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitPath {
    pub commit: Option<Commit>,
    pub path: PathBuf,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CommitEntry {
    pub commit_id: String,
    pub path: PathBuf,
    pub hash: String,
    pub num_bytes: u64,
    pub last_modified_seconds: i64,
    pub last_modified_nanoseconds: u32,
}

// TODONOW - maybe rename or reorg, this isn't an "entry" as such
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct CompareEntry {
    pub commit_entry: Option<CommitEntry>,
    pub path: PathBuf,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SchemaEntry {
    pub commit_id: String,
    pub path: PathBuf,
    pub hash: String,
    pub num_bytes: u64,
}

impl SchemaEntry {
    pub fn new(commit_id: String, path: PathBuf, schema: Schema) -> SchemaEntry {
        SchemaEntry {
            commit_id,
            path,
            hash: schema.hash.clone(),
            num_bytes: schema.num_bytes(),
        }
    }
}

impl ContentHashable for CommitEntry {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}

// Hash on the path field so we can quickly look up
impl PartialEq for CommitEntry {
    fn eq(&self, other: &CommitEntry) -> bool {
        self.path == other.path
    }
}

impl Eq for CommitEntry {}
impl Hash for CommitEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl CommitEntry {
    // For HashSet search purposes
    pub fn from_path<T: AsRef<Path>>(path: T) -> CommitEntry {
        CommitEntry {
            commit_id: String::from(""),
            path: path.as_ref().to_path_buf(),
            hash: String::from(""),
            num_bytes: 0,
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
        }
    }

    pub fn from_file_node(file_node: &FileNode) -> CommitEntry {
        CommitEntry {
            commit_id: file_node.last_commit_id.to_string(),
            path: PathBuf::from(file_node.name.clone()),
            hash: file_node.hash.to_string(),
            num_bytes: file_node.num_bytes,
            last_modified_seconds: file_node.last_modified_seconds,
            last_modified_nanoseconds: file_node.last_modified_nanoseconds,
        }
    }

    pub fn from_dir_node(dir_node: &DirNode) -> CommitEntry {
        CommitEntry {
            commit_id: dir_node.last_commit_id.to_string(),
            path: PathBuf::from(dir_node.name.clone()),
            hash: dir_node.hash.to_string(),
            num_bytes: dir_node.num_bytes,
            last_modified_seconds: dir_node.last_modified_seconds,
            last_modified_nanoseconds: dir_node.last_modified_nanoseconds,
        }
    }

    pub fn version_file(&self) -> PathBuf {
        let current_dir = env::current_dir().unwrap();
        let repo_dir = util::fs::get_repo_root(&current_dir).expect("Oxen repo not found.");
        let repo = LocalRepository::from_dir(&repo_dir).unwrap();
        util::fs::version_path(&repo, self)
    }

    // <= 0.8.4:
    pub fn deprecated_filename(&self) -> PathBuf {
        PathBuf::from(format!("{}.{}", self.commit_id, self.extension()))
    }

    pub fn filename(&self) -> PathBuf {
        if self.extension() == "" {
            PathBuf::from(VERSION_FILE_NAME)
        } else {
            PathBuf::from(format!("{}.{}", VERSION_FILE_NAME, self.extension()))
        }
    }

    pub fn filename_from_commit_id(&self, commit_id: &str) -> PathBuf {
        PathBuf::from(format!("{}.{}", commit_id, self.extension()))
    }

    pub fn extension(&self) -> String {
        if let Some(ext) = self.path.extension() {
            String::from(ext.to_str().unwrap_or(""))
        } else {
            String::from("")
        }
    }

    pub fn to_synced(&self) -> CommitEntry {
        CommitEntry {
            commit_id: self.commit_id.to_owned(),
            path: self.path.to_owned(),
            hash: self.hash.to_owned(),
            num_bytes: self.num_bytes,
            last_modified_seconds: self.last_modified_seconds,
            last_modified_nanoseconds: self.last_modified_nanoseconds,
        }
    }

    pub fn to_remote(&self) -> RemoteEntry {
        RemoteEntry {
            filename: self.path.to_str().unwrap_or("").to_string(),
            hash: self.hash.to_owned(),
        }
    }

    pub fn to_uri_encoded(&self) -> String {
        serde_url_params::to_string(&self).unwrap()
    }

    pub fn has_different_modification_time(&self, time: &FileTime) -> bool {
        self.last_modified_nanoseconds != time.nanoseconds()
            || self.last_modified_seconds != time.unix_seconds()
    }
}
