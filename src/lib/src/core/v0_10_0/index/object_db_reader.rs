use crate::constants::{self};
use crate::core::db::key_val::tree_db::{TreeObject, TreeObjectChild};
use crate::core::db::{self, key_val::path_db, key_val::tree_db};

use crate::error::OxenError;

use crate::model::LocalRepository;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};

use lazy_static::lazy_static;
use lru::LruCache;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use super::CommitEntryWriter;

lazy_static! {
    pub static ref OBJECT_READER_LRU: Arc<RwLock<LruCache<String, Arc<ObjectDBReader>>>> = Arc::new(
        RwLock::new(LruCache::new(std::num::NonZeroUsize::new(16).unwrap()),)
    );
}

pub fn get_object_reader(
    repo: &LocalRepository,
    commit_id: &str,
) -> Result<Arc<ObjectDBReader>, OxenError> {
    let key = format!("{:?}_{}", repo.path, commit_id,);
    // log::debug!("get_object_reader LRU key {}", key);

    let mut cache = OBJECT_READER_LRU.write().unwrap();

    // log::debug!("get_object_reader LRU cache size {:?}", cache.len());

    if let Some(cder) = cache.get(&key) {
        // log::debug!("get_object_reader found in LRU {}", key);
        Ok(cder.clone())
    } else {
        // log::debug!("get_object_reader not found in LRU {}", key);
        let cder = ObjectDBReader::new(repo, commit_id)?;
        // log::debug!("get_object_reader looking up entry {}", key);
        cache.put(key, cder.clone());
        Ok(cder)
    }
}

pub struct ObjectDBReader {
    pub commit_id: String,
    files_db: DBWithThreadMode<MultiThreaded>,
    schemas_db: DBWithThreadMode<MultiThreaded>,
    dirs_db: DBWithThreadMode<MultiThreaded>,
    dir_hashes_db: DBWithThreadMode<MultiThreaded>,
    vnodes_db: DBWithThreadMode<MultiThreaded>,
}

impl ObjectDBReader {
    pub fn objects_dir(path: impl AsRef<Path>) -> PathBuf {
        util::fs::oxen_hidden_dir(path.as_ref()).join(Path::new(constants::OBJECTS_DIR))
    }

    pub fn files_db_dir(path: impl AsRef<Path>) -> PathBuf {
        util::fs::oxen_hidden_dir(path.as_ref())
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_FILES_DIR)
    }

    pub fn schemas_db_dir(path: impl AsRef<Path>) -> PathBuf {
        util::fs::oxen_hidden_dir(path.as_ref())
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_SCHEMAS_DIR)
    }

    pub fn dirs_db_dir(path: impl AsRef<Path>) -> PathBuf {
        util::fs::oxen_hidden_dir(path.as_ref())
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_DIRS_DIR)
    }

    pub fn vnodes_db_dir(path: impl AsRef<Path>) -> PathBuf {
        util::fs::oxen_hidden_dir(path.as_ref())
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_VNODES_DIR)
    }

    pub fn dir_hashes_db_dir(path: impl AsRef<Path>, commit_id: impl AsRef<str>) -> PathBuf {
        CommitEntryWriter::commit_dir(path.as_ref(), commit_id.as_ref())
            .join(constants::DIR_HASHES_DIR)
    }

    pub fn new_from_path(
        path: PathBuf,
        commit_id: impl AsRef<str>,
    ) -> Result<Arc<ObjectDBReader>, OxenError> {
        let files_db_path = ObjectDBReader::files_db_dir(&path);
        let schemas_db_path = ObjectDBReader::schemas_db_dir(&path);
        let dirs_db_path = ObjectDBReader::dirs_db_dir(&path);
        let dir_hashes_db_path = ObjectDBReader::dir_hashes_db_dir(&path, &commit_id);
        let vnodes_db_path = ObjectDBReader::vnodes_db_dir(path.clone());

        log::debug!("ObjectDBReader::new_from_path: {:?}", path);

        let opts = db::key_val::opts::default();
        for path in &[
            &files_db_path,
            &schemas_db_path,
            &dirs_db_path,
            &dir_hashes_db_path,
            &vnodes_db_path,
        ] {
            if !path.exists() {
                // Create the db
                util::fs::create_dir_all(path)?;
                let _db: DBWithThreadMode<MultiThreaded> =
                    DBWithThreadMode::open(&opts, dunce::simplified(path))?;
            }
        }

        Ok(Arc::new(ObjectDBReader {
            commit_id: commit_id.as_ref().to_string(),
            files_db: DBWithThreadMode::open_for_read_only(
                &opts,
                dunce::simplified(&files_db_path),
                false,
            )?,
            schemas_db: DBWithThreadMode::open_for_read_only(
                &opts,
                dunce::simplified(&schemas_db_path),
                false,
            )?,
            dirs_db: DBWithThreadMode::open_for_read_only(
                &opts,
                dunce::simplified(&dirs_db_path),
                false,
            )?,
            dir_hashes_db: DBWithThreadMode::open_for_read_only(
                &opts,
                dunce::simplified(&dir_hashes_db_path),
                false,
            )?,
            vnodes_db: DBWithThreadMode::open_for_read_only(
                &opts,
                dunce::simplified(&vnodes_db_path),
                false,
            )?,
        }))
    }

    pub fn new(
        repo: &LocalRepository,
        commit_id: impl AsRef<str>,
    ) -> Result<Arc<ObjectDBReader>, OxenError> {
        ObjectDBReader::new_from_path(repo.path.clone(), commit_id)
    }

    pub fn get_node_from_child(
        &self,
        child: &TreeObjectChild,
    ) -> Result<Option<TreeObject>, OxenError> {
        match child {
            TreeObjectChild::File { hash, .. } => tree_db::get_tree_object(&self.files_db, hash),
            TreeObjectChild::Dir { hash, .. } => tree_db::get_tree_object(&self.dirs_db, hash),
            TreeObjectChild::VNode { hash, .. } => tree_db::get_tree_object(&self.vnodes_db, hash),
            TreeObjectChild::Schema { hash, .. } => {
                tree_db::get_tree_object(&self.schemas_db, hash)
            }
        }
    }

    pub fn get_dir_hash(&self, path: impl AsRef<Path>) -> Result<Option<String>, OxenError> {
        // log::debug!(
        //     "get_dir_hash path: {:?} in db: {:?}",
        //     path.as_ref(),
        //     self.dir_hashes_db.path()
        // );
        let dir_hash = path_db::get_entry(&self.dir_hashes_db, path)?;
        Ok(dir_hash)
    }

    pub fn get_dir(&self, hash: &str) -> Result<Option<TreeObject>, OxenError> {
        tree_db::get_tree_object(&self.dirs_db, hash)
    }

    pub fn get_file(&self, hash: &str) -> Result<Option<TreeObject>, OxenError> {
        tree_db::get_tree_object(&self.files_db, hash)
    }

    pub fn get_vnode(&self, hash: &str) -> Result<Option<TreeObject>, OxenError> {
        tree_db::get_tree_object(&self.vnodes_db, hash)
    }

    pub fn get_schema(&self, hash: &str) -> Result<Option<TreeObject>, OxenError> {
        tree_db::get_tree_object(&self.schemas_db, hash)
    }
}
