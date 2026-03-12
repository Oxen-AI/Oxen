//! Helper functions to get metadata from the local filesystem.
//!

use crate::error::OxenError;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use crate::util;

use std::path::Path;

use super::index::CommitMerkleTree;

/// Returns metadata with latest commit information. Less efficient than get().
pub fn get_cli(
    repo: &LocalRepository,
    entry_path: impl AsRef<Path>,
    data_path: impl AsRef<Path>,
) -> Result<CLIMetadataEntry, OxenError> {
    let path = data_path.as_ref();
    let entry_path = entry_path.as_ref();
    let base_name = entry_path
        .file_name()
        .ok_or(OxenError::file_has_no_name(path))?;
    let size = repositories::metadata::get_file_size(path)?;
    let hash = util::hasher::hash_file_contents(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);

    let mut last_updated: Option<Commit> = None;
    if let Ok(head_commit) = repositories::commits::head_commit(repo) {
        let tree = CommitMerkleTree::from_commit(repo, &head_commit)?;
        if let Some(node) = tree.get_by_path(entry_path)? {
            if let EMerkleTreeNode::File(file_node) = &node.node {
                let last_commit_id = file_node.last_commit_id().to_string();
                // this commit is guaranteed to exist because we are iterating through the tree
                let commit = repositories::commits::get_by_id(repo, &last_commit_id)?.unwrap();
                last_updated = Some(commit);
            }
            if let EMerkleTreeNode::Directory(dir_node) = &node.node {
                let last_commit_id = dir_node.last_commit_id().to_string();
                let commit = repositories::commits::get_by_id(repo, &last_commit_id)?.unwrap();
                last_updated = Some(commit);
            }
        }
    }

    Ok(CLIMetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        last_updated,
        hash,
        size,
        data_type,
        mime_type,
        extension,
    })
}
