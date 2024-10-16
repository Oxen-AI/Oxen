use crate::constants::{self, HASH_FILE};
use crate::core::db::key_val::tree_db::TreeObjectChild;
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::CommitEntryReader;
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, ContentHashable, LocalRepository, NewCommit};
use crate::repositories;
use crate::util;
use std::sync::Arc;

use super::ObjectDBReader;

#[derive(Debug)]
struct SimpleHash {
    hash: String,
}

impl ContentHashable for SimpleHash {
    fn content_hash(&self) -> String {
        self.hash.clone()
    }
}

pub fn validate_tree_hash(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    let is_valid: Result<bool, OxenError> = if commit.parent_ids.is_empty() {
        validate_complete_merkle_tree(repository, commit)
    } else {
        let parent = repositories::commits::get_by_id(repository, &commit.parent_ids[0])?.ok_or(
            OxenError::basic_str(format!("parent commit not found for commit {}", commit.id)),
        )?;
        validate_changed_parts_of_merkle_tree(repository, commit, &parent)
    };

    match is_valid {
        Ok(is_valid) => {
            log::debug!("got is_valid {:?} for commit {:?}", is_valid, commit);
            Ok(is_valid)
        }
        Err(e) => {
            log::debug!("got is_valid error {:?} for commit {:?}", e, commit);
            Err(e)
        }
    }
}

pub fn compute_commit_content_hash(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<String, OxenError> {
    let commit_entry_reader = CommitEntryReader::new(repository, commit)?;
    let entries = commit_entry_reader.list_entries()?;
    let n_commit = NewCommit::from_commit(commit); // need this to pass in metadata about commit
    let content_hash = compute_versions_hash(repository, &n_commit, &entries)?;
    Ok(content_hash)
}

fn compute_versions_hash(
    repository: &LocalRepository,
    commit: &NewCommit,
    entries: &[CommitEntry],
) -> Result<String, OxenError> {
    // log::debug!("Computing commit hash for {} entries", entries.len());
    let mut hashes: Vec<SimpleHash> = vec![];
    for entry in entries.iter() {
        // Sometimes we have pre computed the HASH, so that we don't have to fully hash contents again to
        // check if data is synced (I guess this is already in the file path...should we just grab it from there instead?)
        // I think the extra hash computation on the server is nice so that you know the actual contents was saved to disk
        let version_path = util::fs::version_path(repository, entry);
        let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
        // log::debug!("Versions hash Entry [{}]: {:?}", i, entry.path);
        if maybe_hash_file.exists() {
            let hash = util::fs::read_from_path(&maybe_hash_file)?;
            // log::debug!(
            //     "compute_versions_hash cached hash [{i}] {hash} => {:?}",
            //     entry.path
            // );
            hashes.push(SimpleHash { hash });
            continue;
        }

        let hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
        // log::debug!("Got hash: {:?} -> {}", entry.path, hash);

        hashes.push(SimpleHash { hash })
    }

    let content_id = util::hasher::compute_commit_hash(commit, &hashes);
    Ok(content_id)
}

fn validate_complete_merkle_tree(
    repository: &LocalRepository,
    commit: &Commit,
) -> Result<bool, OxenError> {
    let object_reader = get_object_reader(repository, &commit.id)?;
    let root_hash = commit.root_hash.clone().unwrap();
    log::debug!("got root_hash {:?}", root_hash);
    let root_node = object_reader.get_dir(&root_hash)?.unwrap();
    for child in root_node.children() {
        if !new_r_validate_complete_merkle_node(repository, commit, object_reader.clone(), child)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn new_r_validate_complete_merkle_node(
    repository: &LocalRepository,
    commit: &Commit,
    object_reader: Arc<ObjectDBReader>,
    child_node: &TreeObjectChild,
) -> Result<bool, OxenError> {
    // Any missing node in the objects dir = invalid commit, we don't have the data
    match child_node {
        TreeObjectChild::File { path, hash } => {
            let version_path = util::fs::version_path_from_hash_and_file(
                &repository.path,
                hash.clone(),
                path.clone(),
            );

            let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
            if maybe_hash_file.exists() {
                let disk_hash = util::fs::read_from_path(&maybe_hash_file)?;
                if &disk_hash != hash {
                    return Ok(false);
                }
                Ok(true)
            } else {
                let disk_hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
                if hash != &disk_hash {
                    log::debug!("found file issue for file {:?}", path);
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
        TreeObjectChild::Schema { path, hash } => {
            let schema_path = path
                .strip_prefix(constants::SCHEMAS_TREE_PREFIX)?
                .to_path_buf();

            let maybe_schema = repositories::data_frames::schemas::get_by_path(
                repository,
                commit,
                schema_path.clone(),
            )?;

            log::debug!(
                "got maybe_schema {:#?} for path {:?} with provided hash {:?}",
                maybe_schema,
                path,
                hash,
            );

            match maybe_schema {
                Some(schema) => {
                    if &schema.hash != hash {
                        log::debug!("found schema issue for schema {:?}", schema_path);
                        return Ok(false);
                    }
                }
                None => {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        TreeObjectChild::Dir { path: _, hash } => {
            let node = object_reader.get_dir(hash)?;
            if node.is_none() {
                return Ok(false);
            }
            let node = node.unwrap();
            for child in node.children() {
                if !new_r_validate_complete_merkle_node(
                    repository,
                    commit,
                    object_reader.clone(),
                    child,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        TreeObjectChild::VNode { path: _, hash } => {
            let node = object_reader.get_vnode(hash)?;
            if node.is_none() {
                return Ok(false);
            }
            let node = node.unwrap();
            for child in node.children() {
                if !new_r_validate_complete_merkle_node(
                    repository,
                    commit,
                    object_reader.clone(),
                    child,
                )? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}

fn validate_changed_parts_of_merkle_tree(
    repository: &LocalRepository,
    commit: &Commit,
    parent: &Commit,
) -> Result<bool, OxenError> {
    log::debug!(
        "validate_changed_parts_of_merkle_tree for commit {:?} and parent {:?}",
        commit,
        parent
    );

    log::debug!("commit.root_hash {:?}", commit.root_hash);
    log::debug!("parent.root_hash {:?}", parent.root_hash);

    let object_reader = get_object_reader(repository, &commit.id)?;
    let root_hash = commit
        .root_hash
        .clone()
        .ok_or(OxenError::basic_str(format!(
            "root_hash is None for commit {}",
            commit.id
        )))?;
    let parent_root_hash = parent
        .root_hash
        .clone()
        .ok_or(OxenError::basic_str(format!(
            "root_hash is None for parent {}",
            parent.id
        )))?;

    let root_node = object_reader
        .get_dir(&root_hash)?
        .ok_or(OxenError::basic_str(format!(
            "root_node is None for commit {}",
            commit.id
        )))?;
    let parent_root_node =
        object_reader
            .get_dir(&parent_root_hash)?
            .ok_or(OxenError::basic_str(format!(
                "root_node is None for parent {}",
                parent.id
            )))?;

    for child in root_node.children() {
        // Search in the parent root node for the same child
        let parent_child = parent_root_node.binary_search_on_path(child.path())?;

        if let Some(parent_child) = parent_child {
            if parent_child.hash() == child.hash() {
                continue;
            } else if !r_validate_changed_parts_of_merkle_node(
                repository,
                commit,
                // parent,
                &object_reader,
                child,
                &Some(parent_child),
            )? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

fn r_validate_changed_parts_of_merkle_node(
    repository: &LocalRepository,
    commit: &Commit,
    // parent_commit: &Commit,
    object_reader: &ObjectDBReader,
    child_node: &TreeObjectChild,
    maybe_parent_node: &Option<TreeObjectChild>,
) -> Result<bool, OxenError> {
    match child_node {
        TreeObjectChild::Dir { path: _, hash } => {
            let node = object_reader.get_dir(hash)?;
            if maybe_parent_node.is_none() {
                for child in node.unwrap().children() {
                    if !r_validate_changed_parts_of_merkle_node(
                        repository,
                        commit,
                        // parent_commit,
                        object_reader,
                        child,
                        &None,
                    )? {
                        return Ok(false);
                    }
                }
            } else {
                let parent_node = object_reader
                    .get_dir(maybe_parent_node.clone().unwrap().hash())?
                    .unwrap();
                for child in node.unwrap().children() {
                    let maybe_parent_child = parent_node.binary_search_on_path(child.path())?;

                    if let Some(parent_child) = maybe_parent_child {
                        if parent_child.hash() == child.hash() {
                            continue;
                        } else if !r_validate_changed_parts_of_merkle_node(
                            repository,
                            commit,
                            // parent_commit,
                            object_reader,
                            child,
                            &Some(parent_child),
                        )? {
                            return Ok(false);
                        }
                    }
                }
            }
            Ok(true)
        }
        TreeObjectChild::VNode { path: _, hash } => {
            let node = object_reader.get_vnode(hash)?;
            if maybe_parent_node.is_none() {
                for child in node.unwrap().children() {
                    if !r_validate_changed_parts_of_merkle_node(
                        repository,
                        commit,
                        // parent_commit,
                        object_reader,
                        child,
                        &None,
                    )? {
                        return Ok(false);
                    }
                }
            } else {
                let parent_node = object_reader
                    .get_vnode(maybe_parent_node.clone().unwrap().hash())?
                    .unwrap();
                for child in node.unwrap().children() {
                    let maybe_parent_child = parent_node.binary_search_on_path(child.path())?;
                    if let Some(parent_child) = maybe_parent_child {
                        if parent_child.hash() == child.hash() {
                            continue;
                        } else if !r_validate_changed_parts_of_merkle_node(
                            repository,
                            commit,
                            // parent_commit,
                            object_reader,
                            child,
                            &Some(parent_child),
                        )? {
                            return Ok(false);
                        }
                    }
                }
            }
            Ok(true)
        }
        TreeObjectChild::File { path, hash } => {
            let version_path = util::fs::version_path_from_hash_and_file(
                &repository.path,
                hash.clone(),
                path.clone(),
            );

            let maybe_hash_file = version_path.parent().unwrap().join(HASH_FILE);
            if maybe_hash_file.exists() {
                let disk_hash = util::fs::read_from_path(&maybe_hash_file)?;
                if &disk_hash != hash {
                    return Ok(false);
                }
                Ok(true)
            } else {
                let disk_hash = util::hasher::hash_file_contents_with_retry(&version_path)?;
                if hash != &disk_hash {
                    log::debug!("found file issue for file {:?}", path);
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
        TreeObjectChild::Schema { path, hash } => {
            let schema_path = path
                .strip_prefix(constants::SCHEMAS_TREE_PREFIX)?
                .to_path_buf();

            let maybe_schema = repositories::data_frames::schemas::get_by_path(
                repository,
                commit,
                schema_path.clone(),
            )?;

            log::debug!(
                "got maybe_schema {:#?} for path {:?} with provided hash {:?}",
                maybe_schema,
                path,
                hash,
            );

            match maybe_schema {
                Some(schema) => {
                    if &schema.hash != hash {
                        log::debug!("found schema issue for schema {:?}", schema_path);
                        return Ok(false);
                    }
                }
                None => {
                    return Ok(false);
                }
            }
            Ok(true)
        }
    }
}
