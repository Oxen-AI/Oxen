//! # oxen schemas
//!
//! Interact with schemas
//!

use std::collections::HashMap;
use std::path::PathBuf;

use rmp_serde::Serializer;
use rocksdb::DBWithThreadMode;
use rocksdb::IteratorMode;
use rocksdb::MultiThreaded;
use serde::Serialize;
use std::str;

use crate::constants;
use crate::core::db;

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::MerkleHash;
use crate::model::StagedEntryStatus;
use crate::model::{Commit, LocalRepository, Schema};
use crate::repositories;
use crate::util;

use std::path::Path;

pub fn list(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let tree = CommitMerkleTree::from_commit(repo, commit)?;
    let mut schemas = HashMap::new();
    r_list_schemas(repo, tree.root, PathBuf::new(), &mut schemas)?;
    Ok(schemas)
}

fn r_list_schemas(
    _repo: &LocalRepository,
    node: MerkleTreeNode,
    current_path: impl AsRef<Path>,
    schemas: &mut HashMap<PathBuf, Schema>,
) -> Result<(), OxenError> {
    for child in node.children {
        match &child.node {
            EMerkleTreeNode::VNode(_) => {
                let child_path = current_path.as_ref();
                r_list_schemas(_repo, child, child_path, schemas)?;
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let child_path = current_path.as_ref().join(&dir_node.name);
                r_list_schemas(_repo, child, child_path, schemas)?;
            }
            EMerkleTreeNode::File(file_node) => {
                if let Some(GenericMetadata::MetadataTabular(metadata)) = &file_node.metadata {
                    let child_path = current_path.as_ref().join(&file_node.name);
                    schemas.insert(child_path, metadata.tabular.schema.clone());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn get_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    let path = path.as_ref();
    let node = repositories::tree::get_file_by_path(repo, commit, path)?;
    let Some(node) = node else {
        return Err(OxenError::path_does_not_exist(path));
    };

    let Some(GenericMetadata::MetadataTabular(metadata)) = &node.metadata else {
        return Err(OxenError::path_does_not_exist(path));
    };

    Ok(Some(metadata.tabular.schema.clone()))
}

/// Get a staged schema
pub fn get_staged(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    let path = path.as_ref();
    let path = util::fs::path_relative_to_dir(path, &repo.path)?;
    let key = path.to_string_lossy();
    let db = get_staged_db(repo)?;
    log::debug!("get_staged({:?}) from db {:?}", key, db.path());
    let bytes = key.as_bytes();
    match db.get(bytes) {
        Ok(Some(value)) => {
            let schema = db_val_to_schema(&value)?;
            Ok(Some(schema))
        }
        _ => {
            log::debug!("could not get staged schema");
            Ok(None)
        }
    }
}

/// List all the staged schemas
pub fn list_staged(repo: &LocalRepository) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let db = get_staged_db(repo)?;
    let mut results = HashMap::new();

    let iter = db.iterator(IteratorMode::Start);
    for item in iter {
        match item {
            Ok((key, value)) => {
                let key = str::from_utf8(&key)?;
                // try deserialize
                let schema = db_val_to_schema(&value)?;
                results.insert(PathBuf::from(key), schema);
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }

    Ok(results)
}

fn db_val_to_schema(data: &[u8]) -> Result<Schema, OxenError> {
    let val: Result<StagedMerkleTreeNode, rmp_serde::decode::Error> = rmp_serde::from_slice(data);
    match val {
        Ok(val) => match &val.node.node {
            EMerkleTreeNode::File(file_node) => match &file_node.metadata {
                Some(GenericMetadata::MetadataTabular(m)) => {
                    return Ok(m.tabular.schema.to_owned());
                }
                _ => {
                    log::error!("File node metadata must be tabular.");
                }
            },
            _ => {
                log::error!("Merkle tree node type must be file.");
            }
        },
        Err(err) => {
            log::error!("Error deserializing tabular metadata: {:?}", err);
        }
    }
    Err(OxenError::basic_str("Cannot get schema"))
}

/// Remove a schema override from the staging area, TODO: Currently undefined behavior for non-staged schemas
pub fn rm(repo: &LocalRepository, path: impl AsRef<Path>, staged: bool) -> Result<(), OxenError> {
    if !staged {
        panic!("Undefined behavior for non-staged schemas")
    }

    let path = path.as_ref();
    let db = get_staged_db(repo)?;
    let key = path.to_string_lossy();
    db.delete(key.as_bytes())?;

    Ok(())
}

/// Add metadata to the schema
pub fn add_schema_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let path = path.as_ref();
    let db = get_staged_db(repo)?;

    let key = path.to_string_lossy();

    let staged_merkle_tree_node = db.get(key.as_bytes())?;
    let mut staged_nodes: HashMap<PathBuf, StagedMerkleTreeNode> = HashMap::new();

    let mut file_node = if let Some(staged_merkle_tree_node) = staged_merkle_tree_node {
        let staged_merkle_tree_node: StagedMerkleTreeNode =
            rmp_serde::from_slice(&staged_merkle_tree_node)
                .map_err(|_| OxenError::basic_str("Could not read staged merkle tree node"))?;
        staged_merkle_tree_node.node.file()?
    } else {
        // Get the FileNode from the CommitMerkleTree
        let Some(commit) = repositories::commits::head_commit_maybe(repo)? else {
            return Err(OxenError::basic_str(
                "Cannot add metadata, no commits found.",
            ));
        };
        let Some(file_node) = repositories::tree::get_file_by_path(repo, &commit, path)? else {
            return Err(OxenError::path_does_not_exist(path));
        };
        let node = repositories::tree::get_node_by_path(repo, &commit, path)?.unwrap();
        let mut parent_id = node.parent_id;
        let mut dir_path = path.to_path_buf();
        while let Some(current_parent_id) = parent_id {
            if current_parent_id == MerkleHash::new(0) {
                break;
            }
            let mut parent_node = MerkleTreeNode::from_hash(repo, &current_parent_id)?;
            parent_id = parent_node.parent_id;
            let EMerkleTreeNode::Directory(mut dir_node) = parent_node.node.clone() else {
                continue;
            };
            dir_path = dir_path.parent().unwrap().to_path_buf();
            dir_node.name = dir_path.to_string_lossy().to_string();
            parent_node.node = EMerkleTreeNode::Directory(dir_node);
            let staged_parent_node = StagedMerkleTreeNode {
                status: StagedEntryStatus::Modified,
                node: parent_node,
            };
            staged_nodes.insert(dir_path.clone(), staged_parent_node);
        }
        file_node
    };

    // Update the metadata
    match &mut file_node.metadata {
        Some(GenericMetadata::MetadataTabular(m)) => {
            m.tabular.schema.metadata = Some(metadata.to_owned());
        }
        _ => {
            return Err(OxenError::path_does_not_exist(path));
        }
    }

    let staged_entry_node = MerkleTreeNode::from_file(file_node.clone());
    let mut staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Modified,
        node: staged_entry_node.clone(),
    };

    for (path, staged_node) in staged_nodes.iter() {
        let key = path.to_string_lossy();
        let mut buf = Vec::new();
        staged_node
            .serialize(&mut Serializer::new(&mut buf))
            .unwrap();
        db.put(key.as_bytes(), &buf)?;
    }

    let oxen_metadata = &file_node.metadata;
    let oxen_metadata_hash = util::hasher::get_metadata_hash(oxen_metadata)?;
    let combined_hash =
        util::hasher::get_combined_hash(Some(oxen_metadata_hash), file_node.hash.to_u128())?;

    let mut file_node = staged_entry.node.file()?;

    file_node.name = path.to_str().unwrap().to_string();
    file_node.metadata_hash = Some(MerkleHash::new(oxen_metadata_hash));
    file_node.combined_hash = MerkleHash::new(combined_hash);

    staged_entry.node = MerkleTreeNode::from_file(file_node);

    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();
    db.put(key.as_bytes(), &buf)?;
    Ok(HashMap::new())
}

/// Add metadata to a specific column
pub fn add_column_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let db = get_staged_db(repo)?;
    let path = path.as_ref();
    let path = util::fs::path_relative_to_dir(path, &repo.path)?;
    let column = column.as_ref();

    let key = path.to_string_lossy();

    let staged_merkle_tree_node = db.get(key.as_bytes())?;
    let mut staged_nodes: HashMap<PathBuf, StagedMerkleTreeNode> = HashMap::new();

    let mut file_node = if let Some(staged_merkle_tree_node) = staged_merkle_tree_node {
        let staged_merkle_tree_node: StagedMerkleTreeNode =
            rmp_serde::from_slice(&staged_merkle_tree_node)
                .map_err(|_| OxenError::basic_str("Could not read staged merkle tree node"))?;
        staged_merkle_tree_node.node.file()?
    } else {
        // Get the FileNode from the CommitMerkleTree
        let Some(commit) = repositories::commits::head_commit_maybe(repo)? else {
            return Err(OxenError::basic_str(
                "Cannot add metadata, no commits found.",
            ));
        };
        let node = repositories::tree::get_node_by_path(repo, &commit, &path)?.unwrap();
        let mut parent_id = node.parent_id;
        let mut dir_path = path.clone();
        while let Some(current_parent_id) = parent_id {
            if current_parent_id == MerkleHash::new(0) {
                break;
            }
            let mut parent_node = MerkleTreeNode::from_hash(repo, &current_parent_id)?;
            parent_id = parent_node.parent_id;
            let EMerkleTreeNode::Directory(mut dir_node) = parent_node.node.clone() else {
                continue;
            };
            dir_path = dir_path.parent().unwrap().to_path_buf();
            dir_node.name = dir_path.to_string_lossy().to_string();
            parent_node.node = EMerkleTreeNode::Directory(dir_node);
            let staged_parent_node = StagedMerkleTreeNode {
                status: StagedEntryStatus::Modified,
                node: parent_node,
            };
            staged_nodes.insert(dir_path.clone(), staged_parent_node);
        }

        let Some(file_node) = repositories::tree::get_file_by_path(repo, &commit, &path)? else {
            return Err(OxenError::path_does_not_exist(&path));
        };
        file_node
    };

    // Update the column metadata
    let mut results = HashMap::new();
    match &mut file_node.metadata {
        Some(GenericMetadata::MetadataTabular(m)) => {
            log::debug!("add_column_metadata: {m:?}");
            for f in m.tabular.schema.fields.iter_mut() {
                log::debug!("add_column_metadata: checking column {f:?} == {column}");

                if f.name == column {
                    log::debug!("add_column_metadata: found column {f:?}");
                    f.metadata = Some(metadata.to_owned());
                }
            }
            results.insert(path.clone(), m.tabular.schema.clone());
        }
        _ => {
            return Err(OxenError::path_does_not_exist(path));
        }
    }

    let mut staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Modified,
        node: MerkleTreeNode::from_file(file_node.clone()),
    };

    for (path, staged_node) in staged_nodes.iter() {
        let key = path.to_string_lossy();
        let mut buf = Vec::new();
        staged_node
            .serialize(&mut Serializer::new(&mut buf))
            .unwrap();
        db.put(key.as_bytes(), &buf)?;
    }

    let oxen_metadata = &file_node.metadata;
    let oxen_metadata_hash = util::hasher::get_metadata_hash(oxen_metadata)?;
    let combined_hash =
        util::hasher::get_combined_hash(Some(oxen_metadata_hash), file_node.hash.to_u128())?;

    let mut file_node = staged_entry.node.file()?;

    file_node.name = path.to_str().unwrap().to_string();
    file_node.combined_hash = MerkleHash::new(combined_hash);
    file_node.metadata_hash = Some(MerkleHash::new(oxen_metadata_hash));

    staged_entry.node = MerkleTreeNode::from_file(file_node);

    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();
    db.put(key.as_bytes(), &buf)?;

    Ok(results)
}

fn get_staged_db(repo: &LocalRepository) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
    let path = staged_db_path(&repo.path)?;
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
    Ok(db)
}

pub fn staged_db_path(path: &Path) -> Result<PathBuf, OxenError> {
    let path = util::fs::oxen_hidden_dir(path).join(Path::new(constants::STAGED_DIR));
    log::debug!("staged_db_path {:?}", path);
    if !path.exists() {
        std::fs::create_dir_all(&path)?;
    }
    Ok(path)
}
