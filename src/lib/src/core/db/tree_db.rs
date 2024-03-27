use crate::constants::{
    OBJECTS_DIR, OBJECT_DIRS_DIR, OBJECT_FILES_DIR, OBJECT_SCHEMAS_DIR, OBJECT_VNODES_DIR,
    OXEN_HIDDEN_DIR, SCHEMAS_TREE_PREFIX,
};
use crate::error::OxenError;
use crate::model::{LocalRepository, StagedDirStats, StagedEntryStatus, StagedSchema};
use crate::{core::db, model::CommitEntry};
use core::panic;

use os_path::OsPath;
use rocksdb::{DBWithThreadMode, ThreadMode};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub struct TreeDB<T: ThreadMode> {
    pub db: DBWithThreadMode<T>,
}

impl<T: ThreadMode> TreeDB<T> {
    pub fn new(db_path: &Path) -> Result<TreeDB<T>, OxenError> {
        let read_only = false;
        TreeDB::p_new(db_path, read_only)
    }

    pub fn new_read_only(db_path: &Path) -> Result<TreeDB<T>, OxenError> {
        let read_only = true;
        TreeDB::p_new(db_path, read_only)
    }

    pub fn p_new(db_path: &Path, read_only: bool) -> Result<TreeDB<T>, OxenError> {
        if !db_path.exists() {
            std::fs::create_dir_all(db_path)?;
        }
        let opts = db::opts::default();
        let db = if read_only {
            if !db_path.join("CURRENT").exists() {
                if let Err(err) = std::fs::create_dir_all(db_path) {
                    log::error!("Could not create staging dir {:?}\nErr: {}", db_path, err);
                }
                let _db: DBWithThreadMode<T> =
                    DBWithThreadMode::open(&opts, dunce::simplified(db_path))?;
            }

            DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(db_path), false)?
        } else {
            DBWithThreadMode::open(&opts, dunce::simplified(db_path))?
        };
        Ok(TreeDB { db })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TreeObjectChildWithStatus {
    pub child: TreeObjectChild,
    pub status: StagedEntryStatus,
}

impl TreeObjectChildWithStatus {
    pub fn from_staged_schema(
        path: PathBuf,
        staged_schema: &StagedSchema,
    ) -> TreeObjectChildWithStatus {
        TreeObjectChildWithStatus {
            child: TreeObjectChild::Schema {
                path: PathBuf::from(SCHEMAS_TREE_PREFIX).join(path),
                hash: staged_schema.schema.hash.clone(),
            },
            status: staged_schema.status.clone(),
        }
    }

    pub fn from_staged_dir(dir_stats: &StagedDirStats) -> TreeObjectChildWithStatus {
        TreeObjectChildWithStatus {
            child: TreeObjectChild::Dir {
                path: dir_stats.path.clone(),
                hash: "".to_string(),
            },
            status: dir_stats.status.clone(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TreeObjectChild {
    File { path: PathBuf, hash: String },
    Schema { path: PathBuf, hash: String },
    Dir { path: PathBuf, hash: String },
    VNode { path: PathBuf, hash: String },
}

impl TreeObjectChild {
    pub fn hash(&self) -> &String {
        match self {
            TreeObjectChild::File { hash, .. } => hash,
            TreeObjectChild::Schema { hash, .. } => hash,
            TreeObjectChild::Dir { hash, .. } => hash,
            TreeObjectChild::VNode { hash, .. } => hash,
        }
    }

    pub fn path(&self) -> &PathBuf {
        match self {
            TreeObjectChild::File { path, .. } => path,
            TreeObjectChild::Schema { path, .. } => path,
            TreeObjectChild::Dir { path, .. } => path,
            TreeObjectChild::VNode { path, .. } => path,
        }
    }

    pub fn path_as_str(&self) -> &str {
        match self {
            TreeObjectChild::File { path, .. } => path.to_str().unwrap(),
            TreeObjectChild::Schema { path, .. } => path.to_str().unwrap(),
            TreeObjectChild::Dir { path, .. } => path.to_str().unwrap(),
            TreeObjectChild::VNode { path, .. } => path.to_str().unwrap(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum TreeObject {
    File {
        hash: String,
        num_bytes: u64,
        last_modified_seconds: i64,
        last_modified_nanoseconds: u32,
    },
    Schema {
        hash: String,
        num_bytes: u64,
    },
    Dir {
        children: Vec<TreeObjectChild>,
        hash: String,
    },
    VNode {
        children: Vec<TreeObjectChild>,
        hash: String,
        name: String,
    },
}

impl TreeObject {
    pub fn hash(&self) -> &String {
        match self {
            TreeObject::File { hash, .. } => hash,
            TreeObject::Schema { hash, .. } => hash,
            TreeObject::Dir { hash, .. } => hash,
            TreeObject::VNode { hash, .. } => hash,
        }
    }
    pub fn from_entry(commit_entry: &CommitEntry) -> TreeObject {
        TreeObject::File {
            hash: commit_entry.hash.clone(),
            num_bytes: commit_entry.num_bytes,
            last_modified_seconds: commit_entry.last_modified_seconds,
            last_modified_nanoseconds: commit_entry.last_modified_nanoseconds,
        }
    }

    pub fn children(&self) -> &Vec<TreeObjectChild> {
        match self {
            TreeObject::File { .. } => panic!("File does not have children"),
            TreeObject::Schema { .. } => panic!("Schema does not have children"),
            TreeObject::Dir { children, .. } => children,
            TreeObject::VNode { children, .. } => children,
        }
    }

    pub fn set_children(&mut self, new_children: Vec<TreeObjectChild>) {
        match self {
            TreeObject::File { .. } => panic!("File does not have children"),
            TreeObject::Schema { .. } => panic!("Schema does not have children"),
            TreeObject::Dir { children, .. } => *children = new_children,
            TreeObject::VNode { children, .. } => *children = new_children,
        }
    }

    pub fn set_hash(&mut self, new_hash: String) {
        match self {
            TreeObject::File { hash, .. } => *hash = new_hash,
            TreeObject::Schema { hash, .. } => *hash = new_hash,
            TreeObject::Dir { hash, .. } => *hash = new_hash,
            TreeObject::VNode { hash, .. } => *hash = new_hash,
        }
    }

    pub fn name(&self) -> &String {
        match self {
            TreeObject::File { .. } => panic!("File does not have a name"),
            TreeObject::Schema { .. } => panic!("Schema does not have a name"),
            TreeObject::Dir { .. } => panic!("Dir does not have a name"),
            TreeObject::VNode { name, .. } => name,
        }
    }

    pub fn num_bytes(&self) -> u64 {
        match self {
            TreeObject::File { num_bytes, .. } => *num_bytes,
            TreeObject::Schema { num_bytes, .. } => *num_bytes,
            TreeObject::Dir { .. } => panic!("num_bytes not defined for dir object"),
            TreeObject::VNode { .. } => panic!("num_bytes not defined for vnode object"),
        }
    }

    pub fn object_path(&self, repo: &LocalRepository) -> PathBuf {
        let objects_dir = repo.path.join(OXEN_HIDDEN_DIR).join(OBJECTS_DIR);
        let top_hash = &self.hash()[..2];
        let bottom_hash = &self.hash()[2..];
        let base_path = match self {
            TreeObject::File { .. } => objects_dir.join(OBJECT_FILES_DIR),
            TreeObject::Schema { .. } => objects_dir.join(OBJECT_SCHEMAS_DIR),
            TreeObject::Dir { .. } => objects_dir.join(OBJECT_DIRS_DIR),
            TreeObject::VNode { .. } => objects_dir.join(OBJECT_VNODES_DIR),
        };
        base_path.join(top_hash).join(bottom_hash)
    }

    pub fn write(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        let path = self.object_path(repo);
        std::fs::create_dir_all(path.parent().unwrap())?; // Will always have a parent

        let mut file = std::fs::File::create(path)?;

        let bytes = serde_json::to_vec(self)?;
        file.write_all(&bytes)?;
        Ok(())
    }

    pub fn read(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        let path = self.object_path(repo);
        let mut file = std::fs::File::open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let _tree_object: TreeObject = serde_json::from_slice(&bytes)?;
        Ok(())
    }

    pub fn binary_search_on_path(
        &self,
        path: &PathBuf,
    ) -> Result<Option<TreeObjectChild>, OxenError> {
        match self {
            TreeObject::File { .. } => panic!("File does not have children"),
            TreeObject::Schema { .. } => panic!("Schema does not have children"),
            TreeObject::Dir { children, .. } => {
                let result = children.binary_search_by(|probe| {
                    let probe_path = probe.path();
                    let os_path = OsPath::from(probe_path);
                    let new_path = os_path.to_pathbuf();

                    log::debug!("tree_db::binary_search_by dir {:?} -> {:?}", new_path, path);

                    new_path.cmp(path)
                });

                match result {
                    Ok(index) => Ok(Some(children[index].clone())),
                    Err(_) => Ok(None),
                }
            }
            TreeObject::VNode { children, .. } => {
                let mut new_c: Vec<TreeObjectChild> = vec![];
                for c in children {
                    let os_path = OsPath::from(c.path());
                    let new_path = os_path.to_pathbuf();
                    let n = TreeObjectChild::File {
                        path: new_path,
                        hash: c.hash().to_owned(),
                    };
                    new_c.push(n);
                }

                let result = new_c.binary_search_by(|probe| {
                    let probe_path = probe.path();
                    // log::debug!(
                    //     "tree_db::binary_search_by vnode {:?} -> {:?}",
                    //     probe_path,
                    //     path
                    // );
                    probe_path.cmp(path)
                });

                match result {
                    Ok(index) => Ok(Some(children[index].clone())),
                    Err(_) => Ok(None),
                }
            }
        }
    }

    pub fn to_commit_entry(&self, path: &PathBuf, commit_id: &str) -> CommitEntry {
        match self {
            TreeObject::File {
                hash,
                num_bytes,
                last_modified_seconds,
                last_modified_nanoseconds,
            } => CommitEntry {
                commit_id: commit_id.to_string(),
                path: path.to_owned(),
                hash: hash.to_owned(),
                num_bytes: *num_bytes,
                last_modified_seconds: *last_modified_seconds,
                last_modified_nanoseconds: *last_modified_nanoseconds,
            },
            _ => panic!("Cannot convert non-file object to CommitEntry"),
        }
    }
}
