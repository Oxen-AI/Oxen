use crate::constants::{FILES_DIR, HISTORY_DIR, SCHEMAS_DIR, SCHEMAS_TREE_PREFIX};
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::core::db::key_val::tree_db::{TreeObject, TreeObjectChild};
use crate::core::index::CommitEntryWriter;
use crate::core::index::{CommitReader, ObjectDBReader};
use crate::error::OxenError;
use crate::model::entry::commit_entry::SchemaEntry;
use crate::model::{LocalRepository, Schema};
use crate::util;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

pub struct ObjectsSchemaReader {
    object_reader: Arc<ObjectDBReader>,
    dir_hashes_db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
    commit_id: String,
}

impl ObjectsSchemaReader {
    pub fn schemas_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR)
            .join(SCHEMAS_DIR)
    }

    pub fn schema_files_db_dir(repo: &LocalRepository, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(SCHEMAS_DIR)
            .join(FILES_DIR)
    }

    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
    ) -> Result<ObjectsSchemaReader, OxenError> {

        let object_schema_reader = with_dir_hash_db_manager(repository, commit_id, |dir_hashes_db| {
            let object_reader = ObjectDBReader::new(repository, commit_id)?;

            Ok(ObjectsSchemaReader {
                dir_hashes_db,
                object_reader,
                repository: repository.clone(),
                commit_id: commit_id.to_owned(),
            })
        })?;

        Ok(object_schema_reader)
    }

    pub fn new_from_head(repository: &LocalRepository) -> Result<ObjectsSchemaReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        ObjectsSchemaReader::new(repository, &commit.id)
    }

    pub fn list_schemas(&self) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        log::debug!("calling list schemas");
        let root_hash: String = path_db::get_entry(&self.dir_hashes_db, "")?.unwrap();
        let root_node: TreeObject = self.object_reader.get_dir(&root_hash)?.unwrap();
        let mut path_vals: HashMap<PathBuf, Schema> = HashMap::new();

        self.r_list_schemas(root_node, &mut path_vals)?;

        Ok(path_vals)
    }

    fn r_list_schemas(
        &self,
        dir_node: TreeObject,
        path_vals: &mut HashMap<PathBuf, Schema>,
    ) -> Result<(), OxenError> {
        for vnode in dir_node.children() {
            let vnode = self.object_reader.get_vnode(vnode.hash())?.unwrap();
            for child in vnode.children() {
                match child {
                    TreeObjectChild::Dir { hash, .. } => {
                        let dir_node = self.object_reader.get_dir(hash)?.unwrap();
                        self.r_list_schemas(dir_node, path_vals)?;
                    }
                    TreeObjectChild::Schema { path, hash, .. } => {
                        log::debug!("r_list_schemas() got schema path {:?} hash", path);
                        let stripped_path = path.strip_prefix(SCHEMAS_TREE_PREFIX).unwrap();
                        let found_schema = self.get_schema_by_hash(hash)?;
                        path_vals.insert(stripped_path.to_path_buf(), found_schema);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub fn list_schema_entries(&self) -> Result<Vec<SchemaEntry>, OxenError> {
        let commit_reader = CommitReader::new(&self.repository)?;
        let commit =
            commit_reader
                .get_commit_by_id(&self.commit_id)?
                .ok_or(OxenError::basic_str(format!(
                    "Could not find commit {}",
                    self.commit_id
                )))?;

        let root_hash = commit
            .root_hash
            .ok_or(format!("Root hash not found for commit {}", self.commit_id))?;

        let root_node: TreeObject =
            self.object_reader
                .get_dir(&root_hash)?
                .ok_or(OxenError::basic_str(
                    "Could not find root node in object db",
                ))?;

        let mut entries: Vec<SchemaEntry> = Vec::new();

        self.r_list_schema_entries(root_node, &mut entries)?;

        Ok(entries)
    }

    fn r_list_schema_entries(
        &self,
        dir_node: TreeObject,
        entries: &mut Vec<SchemaEntry>,
    ) -> Result<(), OxenError> {
        for vnode in dir_node.children() {
            let vnode = self.object_reader.get_vnode(vnode.hash())?.unwrap();
            for child in vnode.children() {
                match child {
                    TreeObjectChild::Dir { hash, .. } => {
                        let dir_node = self.object_reader.get_dir(hash)?.unwrap();
                        self.r_list_schema_entries(dir_node, entries)?;
                    }
                    TreeObjectChild::Schema { path, hash, .. } => {
                        let stripped_path = path.strip_prefix(SCHEMAS_TREE_PREFIX).unwrap();
                        log::debug!("got stripped path {:?} and hash {:?}", stripped_path, hash);
                        let found_schema = self.object_reader.get_schema(hash)?.unwrap();
                        log::debug!("got found schema {:?}", found_schema);
                        let found_entry = SchemaEntry {
                            commit_id: self.commit_id.clone(),
                            path: stripped_path.to_path_buf(),
                            hash: found_schema.hash().clone(),
                            num_bytes: found_schema.num_bytes(),
                        };
                        entries.push(found_entry);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    pub fn list_schemas_for_ref(
        &self,
        schema_ref: impl AsRef<str>,
    ) -> Result<HashMap<PathBuf, Schema>, OxenError> {
        let all_schemas = self.list_schemas()?;
        log::debug!("list_schemas_for_ref all schemas {}", all_schemas.len());

        let mut found_schemas: HashMap<PathBuf, Schema> = HashMap::new();
        for (path, schema) in all_schemas.iter() {
            log::debug!("list_schemas_for_ref path {:?}", path);
            log::debug!("list_schemas_for_ref schema {:?}", schema);
            if path.to_string_lossy() == schema_ref.as_ref()
                || schema.hash == schema_ref.as_ref()
                || schema.name == Some(schema_ref.as_ref().to_string())
            {
                found_schemas.insert(path.clone(), schema.clone());
            }
        }
        Ok(found_schemas)
    }
}
