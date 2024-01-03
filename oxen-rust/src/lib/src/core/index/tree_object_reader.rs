use crate::api;
use crate::constants::{
    self, DEFAULT_BRANCH_NAME, HISTORY_DIR, SCHEMAS_TREE_PREFIX, TMP_DIR, VERSIONS_DIR,
};
use crate::core::db;
use crate::core::db::tree_db::{
    TreeChild, TreeNode, TreeObject, TreeObjectChild, TreeObjectChildWithStatus,
};
use crate::core::db::{kv_db, path_db};
use crate::core::index::{CommitDirEntryWriter, RefWriter, SchemaReader, SchemaWriter};
use crate::error::OxenError;
use crate::model::diff::dir_diff;
use crate::model::schema::staged_schema::StagedSchemaStatus;
use crate::model::{
    Commit, CommitEntry, LocalRepository, Schema, StagedData, StagedEntry, StagedEntryStatus,
    StagedSchema,
};
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::view::schema::SchemaWithPath;

use filetime::FileTime;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::{CommitDirEntryReader, CommitEntryReader, CommitEntryWriter, TreeDBReader};

pub struct TreeObjectReader {
    repository: LocalRepository,
    files_db: DBWithThreadMode<MultiThreaded>,
    schemas_db: DBWithThreadMode<MultiThreaded>,
    dirs_db: DBWithThreadMode<MultiThreaded>,
    vnodes_db: DBWithThreadMode<MultiThreaded>,
    dir_hashes_db: DBWithThreadMode<MultiThreaded>,
}

impl TreeObjectReader {
    pub fn objects_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(constants::OBJECTS_DIR))
    }

    pub fn files_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_FILES_DIR)
    }

    pub fn schemas_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_SCHEMAS_DIR)
    }

    pub fn dirs_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_DIRS_DIR)
    }

    pub fn vnodes_db_dir(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(constants::OBJECTS_DIR)
            .join(constants::OBJECT_VNODES_DIR)
    }

    // pub fn temp_commit_hashes_db_dir(repo: &LocalRepository) -> PathBuf {
    //     util::fs::oxen_hidden_dir(&repo.path)
    //         .join(constants::OBJECTS_DIR)
    //         .join("commit-hashes")
    // }

    pub fn commit_dir_hash_db(path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(path, commit_id).join(constants::DIR_HASHES_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<TreeObjectReader, OxenError> {
        let files_db_path = TreeObjectReader::files_db_dir(&repository);
        let schemas_db_path = TreeObjectReader::schemas_db_dir(&repository);
        let dirs_db_path = TreeObjectReader::dirs_db_dir(&repository);
        let vnodes_db_path = TreeObjectReader::vnodes_db_dir(&repository);
        // let temp_commit_hashes_db_path = TreeObjectReader::temp_commit_hashes_db_dir(&repository);

        for path in &[
            &files_db_path,
            &schemas_db_path,
            &dirs_db_path,
            &vnodes_db_path,
            // &temp_commit_hashes_db_path,
        ] {
            if !path.exists() {
                util::fs::create_dir_all(&path)?;
            }
        }

        let opts = db::opts::default();

        Ok(TreeObjectReader {
            repository: repository.clone(),
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
            dir_hashes_db: DBWithThreadMode::open_for_read_only(
                &opts,
                dunce::simplified(&TreeObjectReader::commit_dir_hash_db(
                    &repository.path,
                    &commit.id,
                )),
                false,
            )?,
        })
    }
    pub fn get_node_from_child(
        &self,
        child: &TreeObjectChild,
    ) -> Result<Option<TreeObject>, OxenError> {
        match child {
            TreeObjectChild::File { hash, .. } => path_db::get_entry(&self.files_db, hash),
            TreeObjectChild::Dir { hash, .. } => path_db::get_entry(&self.dirs_db, hash),
            TreeObjectChild::VNode { hash, .. } => path_db::get_entry(&self.vnodes_db, hash),
            TreeObjectChild::Schema { hash, .. } => path_db::get_entry(&self.schemas_db, hash),
        }
    }

    // TODONOW: This should be retooled to take in a commit - take the
    // commit out of the objectreader struct
    pub fn get_root_node(&self) -> Result<Option<TreeObject>, OxenError> {
        let root_hash: String = path_db::get_entry(&self.dir_hashes_db, "")?.unwrap();
        path_db::get_entry(&self.dirs_db, &root_hash)
    }
}
