use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::{
    Commit, EntryDataType, LocalRepository, MerkleHash, MetadataEntry, ParsedResource,
};
use std::collections::HashMap;
use std::path::Path;

use crate::core::v_old::v0_19_0::index::CommitMerkleTree;
use crate::repositories;

pub fn get_file(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    let Some(file_node) = get_file_merkle_tree_node(repo, commit, path)? else {
        return Ok(None);
    };

    if let EMerkleTreeNode::File(file_node) = file_node.node {
        Ok(Some(file_node))
    } else {
        Ok(None)
    }
}

pub fn get_file_merkle_tree_node(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let parent = path.as_ref().parent().unwrap_or(Path::new(""));
    let parent_node = CommitMerkleTree::dir_with_children(repo, commit, parent)?;
    let Some(parent_node) = parent_node else {
        log::debug!("path has no parent: {:?}", path.as_ref());
        return Ok(None);
    };

    let Some(file_name) = path.as_ref().file_name() else {
        log::debug!("path has no file name: {:?}", path.as_ref());
        return Ok(None);
    };

    let file_node = parent_node.get_by_path(file_name)?;
    Ok(file_node)
}

pub fn get_meta_entry(
    repo: &LocalRepository,
    parsed_resource: &ParsedResource,
    path: &Path,
) -> Result<MetadataEntry, OxenError> {
    let commit = parsed_resource
        .commit
        .clone()
        .ok_or(OxenError::parsed_resource_not_found(
            parsed_resource.clone(),
        ))?;
    log::debug!("get_meta_entry path: {:?} commit: {}", path, commit);
    let node = repositories::tree::get_dir_without_children(repo, &commit, path)?;
    log::debug!("get_meta_entry node: {:?}", node);

    if let Some(node) = node {
        log::debug!(
            "get_meta_entry dir path found: {:?}",
            path.to_str().unwrap()
        );
        let metadata =
            dir_node_to_metadata_entry(repo, &node, parsed_resource, &mut HashMap::new(), false)?;
        Ok(metadata.unwrap())
    } else {
        log::debug!("get_meta_entry file path: {:?}", path.to_str().unwrap());
        let file_node = get_file_merkle_tree_node(repo, &commit, path)?;
        if let Some(file_node) = file_node {
            // log::debug!("get_meta_entry file node found: {:?}", file_node);
            let metadata = file_node_to_metadata_entry(
                repo,
                &file_node,
                parsed_resource,
                &mut HashMap::new(),
            )?;
            Ok(metadata.unwrap())
        } else {
            log::debug!(
                "get_meta_entry path not found: {:?}",
                path.to_str().unwrap()
            );
            Err(OxenError::resource_not_found(path.to_str().unwrap()))
        }
    }
}

fn dir_node_to_metadata_entry(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Commit>,
    // Should append resource is because at the top level we don't want to append the resource
    // but when we recurse we do
    should_append_resource: bool,
) -> Result<Option<MetadataEntry>, OxenError> {
    let EMerkleTreeNode::Directory(dir_node) = &node.node else {
        return Ok(None);
    };

    if let std::collections::hash_map::Entry::Vacant(e) =
        found_commits.entry(*dir_node.last_commit_id())
    {
        let commit = repositories::commits::get_by_hash(repo, dir_node.last_commit_id())?.ok_or(
            OxenError::commit_id_does_not_exist(dir_node.last_commit_id().to_string()),
        )?;
        e.insert(commit);
    }

    let commit = found_commits.get(dir_node.last_commit_id()).unwrap();
    let mut parsed_resource = parsed_resource.clone();
    if should_append_resource {
        parsed_resource.resource = parsed_resource.resource.join(dir_node.name());
        parsed_resource.path = parsed_resource.path.join(dir_node.name());
    }

    Ok(Some(MetadataEntry {
        filename: dir_node.name().to_string(),
        hash: dir_node.hash().to_string(),
        is_dir: true,
        latest_commit: Some(commit.clone()),
        resource: Some(parsed_resource.clone()),
        size: dir_node.num_bytes(),
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: "".to_string(),
        metadata: Some(GenericMetadata::MetadataDir(MetadataDir::new(
            dir_node.data_types(),
        ))),
        is_queryable: None,
        status: None,
    }))
}

fn file_node_to_metadata_entry(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Commit>,
) -> Result<Option<MetadataEntry>, OxenError> {
    let EMerkleTreeNode::File(file_node) = &node.node else {
        return Ok(None);
    };

    if let std::collections::hash_map::Entry::Vacant(e) =
        found_commits.entry(*file_node.last_commit_id())
    {
        let commit = repositories::commits::get_by_hash(repo, file_node.last_commit_id())?.ok_or(
            OxenError::commit_id_does_not_exist(file_node.last_commit_id().to_string()),
        )?;
        e.insert(commit);
    }

    let commit = found_commits.get(file_node.last_commit_id()).unwrap();
    let data_type = file_node.data_type();

    let mut parsed_resource = parsed_resource.clone();
    // HACK for not knowing if we have the full path or just the dir path
    // so we just add the file name to the end of the path if it's not already there
    let mut file_path = parsed_resource.path.clone();
    if !file_path.ends_with(file_node.name()) {
        file_path = file_path.join(file_node.name());
        parsed_resource.resource = parsed_resource.resource.join(file_node.name());
        parsed_resource.path = parsed_resource.path.join(file_node.name());
    }

    let is_indexed = if *data_type == EntryDataType::Tabular {
        Some(
            crate::core::v_latest::workspaces::data_frames::is_queryable_data_frame_indexed_from_file_node(
                repo, file_node, &file_path,
            )?,
        )
    } else {
        None
    };

    Ok(Some(MetadataEntry {
        filename: file_node.name().to_string(),
        hash: file_node.hash().to_string(),
        is_dir: false,
        latest_commit: Some(commit.clone()),
        resource: Some(parsed_resource.clone()),
        size: file_node.num_bytes(),
        data_type: file_node.data_type().clone(),
        mime_type: file_node.mime_type().to_string(),
        extension: file_node.extension().to_string(),
        metadata: file_node.metadata(),
        is_queryable: is_indexed,
        status: None,
    }))
}
