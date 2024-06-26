use crate::constants::{self};
use crate::core::db::tree_db::{TreeObject, TreeObjectChild};
use crate::core::db::{self, tree_db};

use crate::error::OxenError;

use crate::model::LocalRepository;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::CommitEntryWriter;

pub struct ObjectDBReader {
    files_db: DBWithThreadMode<MultiThreaded>,
    schemas_db: DBWithThreadMode<MultiThreaded>,
    dirs_db: DBWithThreadMode<MultiThreaded>,
    vnodes_db: DBWithThreadMode<MultiThreaded>,
}

impl ObjectDBReader {
    pub fn objects_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(constants::OBJECTS_DIR))
    }

    pub fn files_db_dir(path: PathBuf) -> PathBuf {
        util::fs::oxen_hidden_dir(path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_FILES_DIR)
    }

    pub fn schemas_db_dir(path: PathBuf) -> PathBuf {
        util::fs::oxen_hidden_dir(path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_SCHEMAS_DIR)
    }

    pub fn dirs_db_dir(path: PathBuf) -> PathBuf {
        util::fs::oxen_hidden_dir(path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_DIRS_DIR)
    }

    pub fn vnodes_db_dir(path: PathBuf) -> PathBuf {
        util::fs::oxen_hidden_dir(path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_VNODES_DIR)
    }

    pub fn commit_dir_hash_db(path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(path, commit_id).join(constants::DIR_HASHES_DIR)
    }

    pub fn new_from_path(path: PathBuf) -> Result<Arc<ObjectDBReader>, OxenError> {
        let files_db_path = ObjectDBReader::files_db_dir(path.clone());
        let schemas_db_path = ObjectDBReader::schemas_db_dir(path.clone());
        let dirs_db_path = ObjectDBReader::dirs_db_dir(path.clone());
        let vnodes_db_path = ObjectDBReader::vnodes_db_dir(path.clone());

        log::debug!("ObjectDBReader::new_from_path: {:?}", path);

        for path in &[
            &files_db_path,
            &schemas_db_path,
            &dirs_db_path,
            &vnodes_db_path,
        ] {
            if !path.exists() {
                util::fs::create_dir_all(path)?;
            }
        }

        let opts = db::opts::default();

        Ok(Arc::new(ObjectDBReader {
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
            vnodes_db: DBWithThreadMode::open_for_read_only(
                &opts,
                dunce::simplified(&vnodes_db_path),
                false,
            )?,
        }))
    }

    pub fn new(repo: &LocalRepository) -> Result<Arc<ObjectDBReader>, OxenError> {
        ObjectDBReader::new_from_path(repo.path.clone())
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
