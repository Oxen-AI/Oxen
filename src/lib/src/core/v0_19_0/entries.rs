use crate::error::OxenError;
use crate::model::merkle_tree::node::{DirNode, EMerkleTreeNode, FileNode, MerkleTreeNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::{
    Commit, CommitEntry, EntryDataType, LocalRepository, MerkleHash, MetadataEntry, ParsedResource,
};
use crate::opts::PaginateOpts;
use crate::repositories;
use crate::util;
use crate::view::entries::ResourceVersion;
use crate::view::PaginatedDirEntries;
use std::collections::HashMap;
use std::path::Path;

use super::index::CommitMerkleTree;

pub fn get_directory(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<DirNode>, OxenError> {
    let node = CommitMerkleTree::dir_without_children(repo, commit, path)?;
    let Some(node) = node else {
        return Ok(None);
    };
    Ok(Some(node.dir()?))
}

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

pub fn list_directory(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    parsed_resource: &ParsedResource,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    let directory = directory.as_ref();
    let revision = parsed_resource.version.to_str().unwrap_or("").to_string();
    let page = paginate_opts.page_num;
    let page_size = paginate_opts.page_size;

    let resource = Some(ResourceVersion {
        path: directory.to_str().unwrap().to_string(),
        version: revision.clone(),
    });

    log::debug!(
        "list_directory directory {:?} revision {:?}",
        directory,
        revision
    );

    let commit = parsed_resource
        .commit
        .clone()
        .ok_or(OxenError::revision_not_found(revision.into()))?;

    log::debug!("list_directory commit {}", commit);

    let dir = repositories::tree::get_dir_with_children(repo, &commit, directory)?
        .ok_or(OxenError::resource_not_found(directory.to_str().unwrap()))?;

    log::debug!("list_directory dir {}", dir);

    let EMerkleTreeNode::Directory(dir_node) = &dir.node else {
        return Err(OxenError::resource_not_found(directory.to_str().unwrap()));
    };

    log::debug!("list_directory dir_node {}", dir_node);

    // Found commits is used to cache the commits so that we don't have
    // to read them from disk again while looping over entries
    let mut found_commits: HashMap<MerkleHash, Commit> = HashMap::new();
    let dir_entry =
        dir_node_to_metadata_entry(repo, &dir, parsed_resource, &mut found_commits, false)?;
    log::debug!("list_directory dir_entry {:?}", dir_entry);
    let entries: Vec<MetadataEntry> =
        dir_entries(repo, &dir, directory, parsed_resource, &mut found_commits)?;
    log::debug!("list_directory got {} entries", entries.len());

    let (entries, pagination) = util::paginate(entries, page, page_size);
    let metadata: Option<MetadataDir> = Some(MetadataDir::new(dir_node.data_types()));

    Ok(PaginatedDirEntries {
        dir: dir_entry,
        entries,
        resource,
        metadata,
        page_size,
        page_number: page,
        total_pages: pagination.total_pages,
        total_entries: pagination.total_entries,
    })
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
            log::debug!("get_meta_entry file node found: {:?}", file_node);
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

pub fn dir_entries(
    repo: &LocalRepository,
    dir: &MerkleTreeNode,
    search_directory: impl AsRef<Path>,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Commit>,
) -> Result<Vec<MetadataEntry>, OxenError> {
    log::debug!(
        "dir_entries search_directory {:?} dir {}",
        search_directory.as_ref(),
        dir
    );
    let mut entries: Vec<MetadataEntry> = Vec::new();
    let current_directory = search_directory.as_ref();
    p_dir_entries(
        repo,
        dir,
        &search_directory,
        current_directory,
        parsed_resource,
        found_commits,
        &mut entries,
    )?;
    log::debug!("dir_entries got {} entries", entries.len());
    Ok(entries)
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
        found_commits.entry(dir_node.last_commit_id)
    {
        let commit = repositories::commits::get_by_hash(repo, &dir_node.last_commit_id)?.ok_or(
            OxenError::commit_id_does_not_exist(dir_node.last_commit_id.to_string()),
        )?;
        e.insert(commit);
    }

    let commit = found_commits.get(&dir_node.last_commit_id).unwrap();
    let mut parsed_resource = parsed_resource.clone();
    if should_append_resource {
        parsed_resource.resource = parsed_resource.resource.join(&dir_node.name);
        parsed_resource.path = parsed_resource.path.join(&dir_node.name);
    }

    Ok(Some(MetadataEntry {
        filename: dir_node.name.clone(),
        hash: dir_node.hash.to_string(),
        is_dir: true,
        latest_commit: Some(commit.clone()),
        resource: Some(parsed_resource.clone()),
        size: dir_node.num_bytes,
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: "".to_string(),
        metadata: Some(GenericMetadata::MetadataDir(MetadataDir::new(
            dir_node.data_types(),
        ))),
        is_queryable: None,
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
        found_commits.entry(file_node.last_commit_id)
    {
        let commit = repositories::commits::get_by_hash(repo, &file_node.last_commit_id)?.ok_or(
            OxenError::commit_id_does_not_exist(file_node.last_commit_id.to_string()),
        )?;
        e.insert(commit);
    }

    let commit = found_commits.get(&file_node.last_commit_id).unwrap();
    let data_type = &file_node.data_type;

    let mut parsed_resource = parsed_resource.clone();
    // HACK for not knowing if we have the full path or just the dir path
    // so we just add the file name to the end of the path if it's not already there
    let mut file_path = parsed_resource.path.clone();
    if !file_path.ends_with(&file_node.name) {
        file_path = file_path.join(&file_node.name);
        parsed_resource.resource = parsed_resource.resource.join(&file_node.name);
        parsed_resource.path = parsed_resource.path.join(&file_node.name);
    }

    let is_indexed = if *data_type == EntryDataType::Tabular {
        Some(
            repositories::workspaces::data_frames::is_queryable_data_frame_indexed(
                repo, &file_path, commit,
            )?,
        )
    } else {
        None
    };

    Ok(Some(MetadataEntry {
        filename: file_node.name.clone(),
        hash: file_node.hash.to_string(),
        is_dir: false,
        latest_commit: Some(commit.clone()),
        resource: Some(parsed_resource.clone()),
        size: file_node.num_bytes,
        data_type: file_node.data_type.clone(),
        mime_type: file_node.mime_type.clone(),
        extension: file_node.extension.clone(),
        metadata: file_node.metadata.clone(),
        is_queryable: is_indexed,
    }))
}

fn p_dir_entries(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    search_directory: impl AsRef<Path>,
    current_directory: impl AsRef<Path>,
    parsed_resource: &ParsedResource,
    found_commits: &mut HashMap<MerkleHash, Commit>,
    entries: &mut Vec<MetadataEntry>,
) -> Result<(), OxenError> {
    let search_directory = search_directory.as_ref();
    let current_directory = current_directory.as_ref();
    log::debug!(
        "p_dir_entries current_directory {:?} search_directory {:?} node {}",
        current_directory,
        search_directory,
        node
    );
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::VNode(_) => {
                log::debug!(
                    "p_dir_entries got vnode {:?} search_directory {:?}",
                    current_directory,
                    search_directory
                );
                p_dir_entries(
                    repo,
                    child,
                    search_directory,
                    current_directory,
                    parsed_resource,
                    found_commits,
                    entries,
                )?;
            }
            EMerkleTreeNode::Directory(child_dir) => {
                log::debug!(
                    "p_dir_entries current_directory {:?} search_directory {:?} child_dir {:?}",
                    current_directory,
                    search_directory,
                    child_dir.name
                );
                if current_directory == search_directory && !child_dir.name.is_empty() {
                    log::debug!(
                        "p_dir_entries adding dir entry current_directory {:?}",
                        current_directory
                    );
                    let metadata = dir_node_to_metadata_entry(
                        repo,
                        child,
                        parsed_resource,
                        found_commits,
                        true,
                    )?;
                    log::debug!("p_dir_entries added dir entry {:?}", metadata);
                    entries.push(metadata.unwrap());
                }
                let current_directory = current_directory.join(&child_dir.name);
                p_dir_entries(
                    repo,
                    child,
                    search_directory,
                    current_directory,
                    parsed_resource,
                    found_commits,
                    entries,
                )?;
            }
            EMerkleTreeNode::File(child_file) => {
                log::debug!(
                    "p_dir_entries current_directory {:?} search_directory {:?} child_file {:?}",
                    current_directory,
                    search_directory,
                    child_file.name
                );

                if current_directory == search_directory {
                    log::debug!(
                        "p_dir_entries adding file entry current_directory {:?} file_name {:?}",
                        current_directory,
                        child_file.name
                    );
                    let metadata =
                        file_node_to_metadata_entry(repo, child, parsed_resource, found_commits)?;
                    log::debug!(
                        "p_dir_entries added file entry {:?} file_name {:?}",
                        metadata,
                        child_file.name
                    );
                    entries.push(metadata.unwrap());
                }
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn list_tabular_files_in_repo(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<MetadataEntry>, OxenError> {
    let entries = repositories::tree::list_tabular_files_in_repo(repo, commit)?;
    let entries: Vec<FileNode> = entries.into_iter().collect();
    let entries: Vec<MetadataEntry> = entries
        .into_iter()
        .map(|node| MetadataEntry::from_file_node(repo, Some(node), commit).unwrap())
        .collect();
    Ok(entries)
}

pub fn count_for_commit(repo: &LocalRepository, commit: &Commit) -> Result<usize, OxenError> {
    let tree = CommitMerkleTree::from_commit(repo, commit)?;
    let (entries, _) = repositories::tree::list_files_and_dirs(&tree)?;
    Ok(entries.len())
}

pub fn list_for_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    let tree = CommitMerkleTree::from_commit(repo, commit)?;
    let (entries, _) = repositories::tree::list_files_and_dirs(&tree)?;
    Ok(entries
        .into_iter()
        .map(|entry| CommitEntry::from_file_node(&entry.file_node))
        .collect())
}
