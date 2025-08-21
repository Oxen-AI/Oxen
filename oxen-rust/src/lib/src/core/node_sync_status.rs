use crate::constants;
use crate::core::db::merkle_node::merkle_node_db::node_db_prefix;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::MerkleHash;
use crate::util;
use std::path::PathBuf;

pub fn node_is_synced(repo: &LocalRepository, node_hash: &MerkleHash) -> bool {
    let is_synced_path = node_is_synced_file_path(repo, node_hash);
    log::debug!("Checking if node is synced: {is_synced_path:?}");
    match std::fs::read_to_string(&is_synced_path) {
        Ok(value) => {
            log::debug!("Is synced value: {value}");
            "true" == value
        }
        Err(err) => {
            log::debug!("Could not read is_synced file {is_synced_path:?}: {}", err);
            false
        }
    }
}

pub fn mark_node_as_synced(
    repo: &LocalRepository,
    node_hash: &MerkleHash,
) -> Result<(), OxenError> {
    let is_synced_path = node_is_synced_file_path(repo, node_hash);
    if let Some(parent) = is_synced_path.parent() {
        log::debug!("Creating parent directory: {parent:?}");
        util::fs::create_dir_all(parent)?;
    }

    log::debug!("Writing is synced: {is_synced_path:?}");

    match std::fs::write(&is_synced_path, "true") {
        Ok(_) => {
            log::debug!("Wrote is synced file: {is_synced_path:?}");
            Ok(())
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "Could not write is_synced file: {}",
            err
        ))),
    }
}

fn node_is_synced_file_path(repo: &LocalRepository, node_hash: &MerkleHash) -> PathBuf {
    let dir_prefix = node_db_prefix(node_hash);
    repo.path
        .join(constants::OXEN_HIDDEN_DIR)
        .join(constants::TREE_DIR)
        .join(constants::SYNC_STATUS_DIR)
        .join(constants::NODES_DIR)
        .join(dir_prefix)
        .join(constants::IS_SYNCED)
}
