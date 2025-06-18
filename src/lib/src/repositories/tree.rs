use bytesize::ByteSize;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str;
use std::str::FromStr;
use tar::Archive;

use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR, NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::commit_sync_status;
use crate::core::db;
use crate::core::db::merkle_node::merkle_node_db::{node_db_path, node_db_prefix};
use crate::core::db::merkle_node::MerkleNodeDB;
use crate::core::v_latest::index::CommitMerkleTree as CommitMerkleTreeLatest;
use crate::core::v_old::v0_19_0::index::CommitMerkleTree as CommitMerkleTreeV0_19_0;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{
    CommitNode, DirNodeWithPath, EMerkleTreeNode, FileNode, FileNodeWithDir, MerkleTreeNode,
};
use crate::model::{
    Commit, EntryDataType, LocalRepository, MerkleHash, MerkleTreeNodeType, TMerkleTreeNode,
};
use crate::{repositories, util};

/// This will return the MerkleTreeNode with type CommitNode if the Commit exists
/// Otherwise it will return None
/// The node will not have any children, so is fast to look up
/// if you want the root with children, use `get_root_with_children``
pub fn get_root(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::root_without_children(repo, commit),
        _ => CommitMerkleTreeLatest::root_without_children(repo, commit),
    }
}

/// This will return the MerkleTreeNode with type CommitNode if the Commit exists
/// Otherwise it will return None
/// The node will load all children from disk, so is slower than `get_root`
pub fn get_root_with_children(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::root_with_children(repo, commit),
        _ => CommitMerkleTreeLatest::root_with_children(repo, commit),
    }
}

/// If passed in a commit node, will return the root directory node
/// Will error if the node is not a commit node, because only CommitNodes have a root directory
pub fn get_root_dir(node: &MerkleTreeNode) -> Result<&MerkleTreeNode, OxenError> {
    if node.node.node_type() != MerkleTreeNodeType::Commit {
        return Err(OxenError::basic_str(format!(
            "Expected a commit node, but got: '{:?}'",
            node.node.node_type()
        )));
    }

    // A commit node should have exactly one child, which is the root directory
    if node.children.len() != 1 {
        return Err(OxenError::basic_str(format!(
            "Commit node should have exactly one child (root directory) but got: {} from {}",
            node.children.len(),
            node
        )));
    }

    let root_dir = &node.children[0];
    if root_dir.node.node_type() != MerkleTreeNodeType::Dir {
        return Err(OxenError::basic_str(format!(
            "The child of a commit node should be a directory, but got: '{:?}'",
            root_dir.node.node_type()
        )));
    }

    Ok(root_dir)
}

pub fn get_node_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = false;
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::read_node(repo, hash, load_recursive),
        _ => CommitMerkleTreeLatest::read_node(repo, hash, load_recursive),
    }
}

pub fn get_node_by_id_with_children(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = true;
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::read_node(repo, hash, load_recursive),
        _ => CommitMerkleTreeLatest::read_node(repo, hash, load_recursive),
    }
}

pub fn get_commit_node_version(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<MinOxenVersion, OxenError> {
    let commit_id = MerkleHash::from_str(&commit.id)?;
    let Some(commit_node) = repositories::tree::get_node_by_id(repo, &commit_id)? else {
        return Err(OxenError::commit_id_does_not_exist(&commit.id));
    };

    let EMerkleTreeNode::Commit(commit_node) = &commit_node.node else {
        // This should never happen
        log::error!("Commit node is not a commit node");
        return Err(OxenError::commit_id_does_not_exist(&commit.id));
    };
    Ok(commit_node.version())
}

pub fn has_dir(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let dir_hashes = repositories::tree::dir_hashes(repo, commit)?;
    Ok(dir_hashes.contains_key(path.as_ref()))
}

pub fn has_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let dir_hashes = repositories::tree::dir_hashes(repo, commit)?;
    match dir_hashes.get(path) {
        Some(dir_hash) => {
            let node = get_node_by_id_with_children(repo, dir_hash)?.unwrap();
            Ok(node.get_by_path(path)?.is_some())
        }
        None => {
            let parent = path.parent().unwrap();
            if let Some(parent_hash) = dir_hashes.get(parent) {
                let node = get_node_by_id_with_children(repo, parent_hash)?.unwrap();
                Ok(node.get_by_path(path)?.is_some())
            } else {
                Ok(false)
            }
        }
    }
}

pub fn get_node_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = false;
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            match CommitMerkleTreeV0_19_0::from_path(repo, commit, path, load_recursive) {
                Ok(tree) => Ok(Some(tree.root)),
                Err(e) => {
                    log::warn!("Error getting node by path: {:?}", e);
                    Ok(None)
                }
            }
        }
        _ => match CommitMerkleTreeLatest::from_path(repo, commit, path, load_recursive) {
            Ok(tree) => Ok(Some(tree.root)),
            Err(e) => {
                log::warn!("Error getting node by path: {:?}", e);
                Ok(None)
            }
        },
    }
}

pub fn get_node_by_path_with_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = true;
    let node = match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::from_path(repo, commit, path, load_recursive)?.root
        }
        _ => CommitMerkleTreeLatest::from_path(repo, commit, path, load_recursive)?.root,
    };
    Ok(Some(node))
}

pub fn get_file_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    let Some(root) = get_node_by_path(repo, commit, &path)? else {
        return Ok(None);
    };
    match root.node {
        EMerkleTreeNode::File(file_node) => Ok(Some(file_node.clone())),
        _ => Ok(None),
    }
}

pub fn get_dir_with_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::dir_with_children(repo, commit, path),
        _ => CommitMerkleTreeLatest::dir_with_children(repo, commit, path),
    }
}

pub fn get_dir_without_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::dir_without_children(repo, commit, path)
        }
        _ => CommitMerkleTreeLatest::dir_without_children(repo, commit, path),
    }
}

pub fn get_dir_with_children_recursive(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::dir_with_children_recursive(repo, commit, path)
        }
        _ => CommitMerkleTreeLatest::dir_with_children_recursive(repo, commit, path),
    }
}

/// Helper function where you can pass in Optional depth and Optional path and get a tree
/// If depth is None, it will default to -1 which means the entire subtree
/// If path is None, it will default to the root
/// Otherwise it will get the subtree at the given path with the given depth
pub fn get_subtree_by_depth(
    repo: &LocalRepository,
    commit: &Commit,
    maybe_subtree: &Option<PathBuf>,
    maybe_depth: &Option<i32>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match (maybe_subtree, maybe_depth) {
        (Some(subtree), Some(depth)) => {
            log::debug!(
                "Getting subtree {:?} with depth {} for commit {}",
                subtree,
                depth,
                commit
            );
            get_subtree(repo, commit, subtree, *depth)
        }
        (Some(subtree), None) => {
            // If the depth is not provided, we default to -1 which means the entire subtree
            log::debug!(
                "Getting subtree {:?} for commit {} with depth -1",
                subtree,
                commit
            );
            get_subtree(repo, commit, subtree, -1)
        }
        (None, Some(depth)) => {
            log::debug!(
                "Getting tree from root with depth {} for commit {}",
                depth,
                commit
            );
            get_subtree(repo, commit, PathBuf::from("."), *depth)
        }
        _ => {
            log::debug!("Getting full tree for commit {}", commit);
            get_root_with_children(repo, commit)
        }
    }
}

pub fn get_subtree(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    depth: i32,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::from_path_depth(repo, commit, path, depth)
        }
        _ => CommitMerkleTreeLatest::from_path_depth(repo, commit, path, depth),
    }
}

/// Given a set of paths, will return a map of the path to the FileNode or DirNode
pub fn list_nodes_from_paths(
    repo: &LocalRepository,
    commit: &Commit,
    paths: &[PathBuf],
) -> Result<HashMap<PathBuf, MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::read_nodes(repo, commit, paths),
        _ => CommitMerkleTreeLatest::read_nodes(repo, commit, paths),
    }
}

/// List the files and folders given a directory node
pub fn list_files_and_folders(node: &MerkleTreeNode) -> Result<Vec<MerkleTreeNode>, OxenError> {
    if MerkleTreeNodeType::Dir != node.node.node_type() {
        return Err(OxenError::basic_str(format!(
            "list_files_and_folders Merkle tree node is not a directory: '{:?}'",
            node.node.node_type()
        )));
    }

    // The dir node will have vnode children
    let mut children = Vec::new();
    for child in &node.children {
        if let EMerkleTreeNode::VNode(_) = &child.node {
            children.extend(child.children.iter().cloned());
        }
    }
    Ok(children)
}

/// List the files and folders given a directory node in a HashSet
pub fn list_files_and_folders_set(
    node: &MerkleTreeNode,
) -> Result<HashSet<MerkleTreeNode>, OxenError> {
    if MerkleTreeNodeType::Dir != node.node.node_type() {
        return Err(OxenError::basic_str(format!(
            "list_files_and_folders Merkle tree node is not a directory: '{:?}'",
            node.node.node_type()
        )));
    }

    // The dir node will have vnode children
    let mut children = HashSet::new();
    for child in &node.children {
        if let EMerkleTreeNode::VNode(_) = &child.node {
            children.extend(child.children.iter().cloned());
        }
    }
    Ok(children)
}

/// List the files and folders given a directory node in a HashMap
pub fn list_files_and_folders_map(
    node: &MerkleTreeNode,
) -> Result<HashMap<PathBuf, MerkleTreeNode>, OxenError> {
    if MerkleTreeNodeType::Dir != node.node.node_type() {
        return Err(OxenError::basic_str(format!(
            "list_files_and_folders_map Merkle tree node is not a directory: '{:?}'",
            node.node.node_type()
        )));
    }

    // The dir node will have vnode children
    let mut children = HashMap::new();
    for child in &node.children {
        if let EMerkleTreeNode::VNode(_) = &child.node {
            for child in &child.children {
                match &child.node {
                    EMerkleTreeNode::File(file_node) => {
                        children.insert(PathBuf::from(file_node.name()), child.clone());
                    }
                    EMerkleTreeNode::Directory(dir_node) => {
                        children.insert(PathBuf::from(dir_node.name()), child.clone());
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(children)
}

/// Will traverse the given paths and return the node hashes in the `hashes` HashSet<MerkleHash>
/// If starting_node_hashes is provided, it will only add nodes that are not in the starting_node_hashes
pub fn collect_nodes_along_path(
    repo: &LocalRepository,
    commit: &Commit,
    paths: Vec<PathBuf>,
    starting_node_hashes: &Option<HashSet<MerkleHash>>,
    hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    // Grab the first path or error if empty
    let root_path = paths
        .first()
        .ok_or(OxenError::basic_str("No paths provided"))?;
    let node = get_node_by_path_with_children(repo, commit, root_path)?
        .ok_or(OxenError::basic_str("Node not found"))?;

    let (_root_node, nodes) = node.get_nodes_along_paths(paths)?;

    if let Some(starting_node_hashes) = starting_node_hashes {
        for node in nodes {
            if !starting_node_hashes.contains(&node.hash) {
                hashes.insert(node.hash);
            }
        }
    } else {
        for node in nodes {
            hashes.insert(node.hash);
        }
    }
    Ok(())
}

pub fn list_missing_file_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<HashSet<MerkleHash>, OxenError> {
    if repo.min_version() == MinOxenVersion::V0_19_0 {
        let Some(node) = CommitMerkleTreeV0_19_0::read_depth(repo, hash, 1)? else {
            return Err(OxenError::basic_str(format!("Node {} not found", hash)));
        };
        node.list_missing_file_hashes(repo)
    } else {
        let Some(node) = CommitMerkleTreeLatest::read_depth(repo, hash, 1)? else {
            return Err(OxenError::basic_str(format!("Node {} not found", hash)));
        };
        node.list_missing_file_hashes(repo)
    }
}

/// Subtree in this context means we cloned a directory that was not the root of the repo
pub fn from_commit_or_subtree(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    // This debug log is to help make sure we don't load the tree too many times
    // if you see it in the logs being called too much, it could be why the code is slow.
    log::debug!(
        "Load tree from commit: {} in repo: {:?} with subtree_paths: {:?}",
        commit,
        repo.path,
        repo.subtree_paths()
    );

    let node_hash = MerkleHash::from_str(&commit.id)?;
    // If we have a subtree path, we need to load the tree from that path
    let root = match (repo.subtree_paths(), repo.depth()) {
        (Some(subtree_paths), Some(depth)) => {
            // Get it working with the first path for now, we might want to clone recursively to the root
            // or have multiple roots
            get_subtree(repo, commit, &subtree_paths[0], depth)?
        }
        _ => get_node_by_id_with_children(repo, &node_hash)?,
    };

    Ok(root)
}

/// Given a set of commit ids, return the hashes that are missing from the tree
pub fn list_missing_file_hashes_from_commits(
    repo: &LocalRepository,
    commit_ids: &HashSet<MerkleHash>,
    subtree_paths: &Option<Vec<PathBuf>>,
    depth: &Option<i32>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    log::debug!(
        "list_missing_file_hashes_from_commits checking {} commit ids, subtree paths: {:?}, depth: {:?}",
        commit_ids.len(),
        subtree_paths,
        depth
    );
    let mut candidate_hashes: HashSet<MerkleHash> = HashSet::new();
    for commit_id in commit_ids {
        let commit_id_str = commit_id.to_string();
        let Some(commit) = repositories::commits::get_by_id(repo, &commit_id_str)? else {
            log::error!(
                "list_missing_file_hashes_from_commits Commit {} not found",
                commit_id_str
            );
            return Err(OxenError::revision_not_found(commit_id_str.into()));
        };
        // Handle the case where we are given a list of subtrees to check
        // It is much faster to check the subtree directly than to walk the entire tree
        if let Some(subtree_paths) = subtree_paths {
            // Compute all the parents of the subtrees
            let mut all_parent_paths: HashSet<PathBuf> = HashSet::new();
            for path in subtree_paths.clone() {
                let mut path = path.clone();
                all_parent_paths.insert(path.clone());
                while let Some(parent) = path.parent() {
                    all_parent_paths.insert(parent.to_path_buf());
                    path = parent.to_path_buf();
                }
            }
            log::debug!(
                "list_missing_file_hashes_from_commits all_parent_paths: {:?}",
                all_parent_paths
            );

            for path in all_parent_paths {
                let Some(tree) =
                    repositories::tree::get_subtree_by_depth(repo, &commit, &Some(path), depth)?
                else {
                    log::warn!("list_missing_file_hashes_from_commits subtree not found for path");
                    continue;
                };
                tree.walk_tree(|node| {
                    if node.is_file() {
                        candidate_hashes.insert(node.hash);
                    }
                });
            }
        } else {
            let Some(tree) = get_root_with_children(repo, &commit)? else {
                log::warn!(
                    "list_missing_file_hashes_from_commits root not found for commit: {:?}",
                    commit
                );
                continue;
            };
            tree.walk_tree(|node| {
                if node.is_file() {
                    candidate_hashes.insert(node.hash);
                }
            });
        }
    }
    log::debug!(
        "list_missing_file_hashes_from_commits candidate_hashes count: {}",
        candidate_hashes.len()
    );
    list_missing_file_hashes_from_hashes(repo, &candidate_hashes)
}

pub fn dir_entries_with_paths(
    node: &MerkleTreeNode,
    base_path: &PathBuf,
) -> Result<HashSet<(FileNode, PathBuf)>, OxenError> {
    let mut entries = HashSet::new();

    match &node.node {
        EMerkleTreeNode::Directory(_) | EMerkleTreeNode::VNode(_) | EMerkleTreeNode::Commit(_) => {
            for child in &node.children {
                match &child.node {
                    EMerkleTreeNode::File(file_node) => {
                        let file_path = base_path.join(file_node.name());
                        entries.insert((file_node.clone(), file_path));
                    }
                    EMerkleTreeNode::Directory(dir_node) => {
                        let new_base_path = base_path.join(dir_node.name());
                        entries.extend(dir_entries_with_paths(child, &new_base_path)?);
                    }
                    EMerkleTreeNode::VNode(_vnode) => {
                        entries.extend(dir_entries_with_paths(child, base_path)?);
                    }
                    _ => {}
                }
            }
        }
        EMerkleTreeNode::File(_) => {}
        _ => {
            return Err(OxenError::basic_str(format!(
                "Unexpected node type: {:?}",
                node.node.node_type()
            )))
        }
    }

    Ok(entries)
}

// Get HashMap of all entries that aren't present in shared_hashes
pub fn unique_dir_entries(
    base_path: &PathBuf,
    node: &MerkleTreeNode,
    shared_hashes: &HashSet<MerkleHash>,
) -> Result<HashMap<PathBuf, FileNode>, OxenError> {
    let mut entries = HashMap::new();
    match &node.node {
        EMerkleTreeNode::Directory(_) | EMerkleTreeNode::VNode(_) | EMerkleTreeNode::Commit(_) => {
            if !shared_hashes.contains(&node.hash) {
                for child in &node.children {
                    match &child.node {
                        EMerkleTreeNode::File(file_node) => {
                            let file_path = base_path.join(file_node.name());
                            entries.insert(file_path, file_node.clone());
                        }
                        EMerkleTreeNode::Directory(dir_node) => {
                            let new_base_path = base_path.join(dir_node.name());
                            entries.extend(unique_dir_entries(
                                &new_base_path,
                                child,
                                shared_hashes,
                            )?);
                        }
                        EMerkleTreeNode::VNode(_vnode) => {
                            entries.extend(unique_dir_entries(base_path, child, shared_hashes)?);
                        }
                        _ => {}
                    }
                }
            }
        }
        EMerkleTreeNode::File(_) => {}
        _ => {
            return Err(OxenError::basic_str(format!(
                "Unexpected node type: {:?}",
                node.node.node_type()
            )))
        }
    }

    Ok(entries)
}

// Given a set of hashes, return the hashes that are missing from the tree
pub fn list_missing_node_hashes(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let mut results = HashSet::new();
    for hash in hashes {
        let dir_prefix = node_db_path(repo, hash);

        if !(commit_sync_status::commit_is_synced(repo, hash)
            && dir_prefix.join("node").exists()
            && dir_prefix.join("children").exists())
        {
            results.insert(*hash);
        }
    }

    Ok(results)
}

fn list_missing_file_hashes_from_hashes(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let mut results = HashSet::new();
    let version_store = repo.version_store()?;
    for hash in hashes {
        if !version_store.version_exists(&hash.to_string())? {
            results.insert(*hash);
        }
    }
    Ok(results)
}

pub fn list_all_files(
    node: &MerkleTreeNode,
    subtree_path: &PathBuf,
) -> Result<HashSet<FileNodeWithDir>, OxenError> {
    let mut file_nodes = HashSet::new();
    r_list_all_files(node, subtree_path, &mut file_nodes)?;
    Ok(file_nodes)
}

fn r_list_all_files(
    node: &MerkleTreeNode,
    traversed_path: impl AsRef<Path>,
    file_nodes: &mut HashSet<FileNodeWithDir>,
) -> Result<(), OxenError> {
    let traversed_path = traversed_path.as_ref();
    for child in &node.children {
        // log::debug!("Found child: {child}");
        match &child.node {
            EMerkleTreeNode::File(file_node) => {
                file_nodes.insert(FileNodeWithDir {
                    file_node: file_node.to_owned(),
                    dir: traversed_path.to_owned(),
                });
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let new_path = traversed_path.join(dir_node.name());
                r_list_all_files(child, new_path, file_nodes)?;
            }
            EMerkleTreeNode::VNode(_) => {
                r_list_all_files(child, traversed_path, file_nodes)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Collect MerkleTree into Directories
pub fn list_all_dirs(node: &MerkleTreeNode) -> Result<HashSet<DirNodeWithPath>, OxenError> {
    let mut dir_nodes = HashSet::new();
    r_list_all_dirs(node, PathBuf::from(""), &mut dir_nodes)?;
    Ok(dir_nodes)
}

fn r_list_all_dirs(
    node: &MerkleTreeNode,
    traversed_path: impl AsRef<Path>,
    dir_nodes: &mut HashSet<DirNodeWithPath>,
) -> Result<(), OxenError> {
    let traversed_path = traversed_path.as_ref();
    for child in &node.children {
        // log::debug!("Found child: {child}");
        match &child.node {
            EMerkleTreeNode::Directory(dir_node) => {
                let new_path = traversed_path.join(dir_node.name());
                dir_nodes.insert(DirNodeWithPath {
                    dir_node: dir_node.to_owned(),
                    path: new_path.to_owned(),
                });
                r_list_all_dirs(child, new_path, dir_nodes)?;
            }
            EMerkleTreeNode::VNode(_) => {
                r_list_all_dirs(child, traversed_path, dir_nodes)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Collect MerkleTree into Directories and Files
pub fn list_files_and_dirs(
    root: &MerkleTreeNode,
) -> Result<(HashSet<FileNodeWithDir>, HashSet<DirNodeWithPath>), OxenError> {
    let mut file_nodes = HashSet::new();
    let mut dir_nodes = HashSet::new();
    r_list_files_and_dirs(root, PathBuf::new(), &mut file_nodes, &mut dir_nodes)?;
    Ok((file_nodes, dir_nodes))
}

fn r_list_files_and_dirs(
    node: &MerkleTreeNode,
    traversed_path: impl AsRef<Path>,
    file_nodes: &mut HashSet<FileNodeWithDir>,
    dir_nodes: &mut HashSet<DirNodeWithPath>,
) -> Result<(), OxenError> {
    let traversed_path = traversed_path.as_ref();
    for child in &node.children {
        // log::debug!("Found child: {child}");
        match &child.node {
            EMerkleTreeNode::File(file_node) => {
                file_nodes.insert(FileNodeWithDir {
                    file_node: file_node.to_owned(),
                    dir: traversed_path.to_owned(),
                });
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let new_path = traversed_path.join(dir_node.name());
                if new_path != PathBuf::from("") {
                    dir_nodes.insert(DirNodeWithPath {
                        dir_node: dir_node.to_owned(),
                        path: new_path.to_owned(),
                    });
                }
                r_list_files_and_dirs(child, new_path, file_nodes, dir_nodes)?;
            }
            EMerkleTreeNode::VNode(_) => {
                r_list_files_and_dirs(child, traversed_path, file_nodes, dir_nodes)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn list_tabular_files_in_repo(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashSet<FileNode>, OxenError> {
    let entries = list_files_by_type(repo, commit, &EntryDataType::Tabular)?;
    Ok(entries)
}

pub fn list_files_by_type(
    repo: &LocalRepository,
    commit: &Commit,
    data_type: &EntryDataType,
) -> Result<HashSet<FileNode>, OxenError> {
    let mut file_nodes = HashSet::new();
    let Some(tree) = get_root_with_children(repo, commit)? else {
        log::warn!(
            "get_root_with_children returned None for commit: {:?}",
            commit
        );
        return Ok(file_nodes);
    };
    r_list_files_by_type(&tree, data_type, &mut file_nodes, PathBuf::new())?;
    Ok(file_nodes)
}

fn r_list_files_by_type(
    node: &MerkleTreeNode,
    data_type: &EntryDataType,
    file_nodes: &mut HashSet<FileNode>,
    traversed_path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let traversed_path = traversed_path.as_ref();
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::File(file_node) => {
                if file_node.data_type() == data_type {
                    let mut file_node = file_node.to_owned();
                    let full_path = traversed_path.join(file_node.name());
                    file_node.set_name(&full_path.to_string_lossy());
                    file_nodes.insert(file_node);
                }
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let full_path = traversed_path.join(dir_node.name());
                r_list_files_by_type(child, data_type, file_nodes, full_path)?;
            }
            EMerkleTreeNode::VNode(_) => {
                r_list_files_by_type(child, data_type, file_nodes, traversed_path)?;
            }
            _ => {}
        }
    }
    Ok(())
}

pub fn cp_dir_hashes_to(
    repo: &LocalRepository,
    original_commit_id: &MerkleHash,
    new_commit_id: &MerkleHash,
) -> Result<(), OxenError> {
    let original_dir_hashes_path = dir_hash_db_path_from_commit_id(repo, original_commit_id);
    let new_dir_hashes_path = dir_hash_db_path_from_commit_id(repo, new_commit_id);
    util::fs::copy_dir_all(original_dir_hashes_path, new_dir_hashes_path)?;
    Ok(())
}

pub fn compress_tree(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);
    compress_full_tree(repository, &mut tar)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);

    log::debug!("Compressed entire tree size is {}", ByteSize::b(total_size));

    Ok(buffer)
}

pub fn compress_full_tree(
    repository: &LocalRepository,
    tar: &mut tar::Builder<GzEncoder<Vec<u8>>>,
) -> Result<(), OxenError> {
    // This will be the subdir within the tarball,
    // so when we untar it, all the subdirs will be extracted to
    // tree/nodes/...
    let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR);
    let nodes_dir = repository
        .path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);

    log::debug!("Compressing tree in dir {:?}", nodes_dir);

    if nodes_dir.exists() {
        tar.append_dir_all(&tar_subdir, nodes_dir)?;
    }

    Ok(())
}

pub fn compress_nodes(
    repository: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<Vec<u8>, OxenError> {
    // zip up the node directories for each commit tree
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    log::debug!("Compressing {} unique nodes...", hashes.len());
    for hash in hashes {
        // This will be the subdir within the tarball
        // so when we untar it, all the subdirs will be extracted to
        // tree/nodes/...
        let dir_prefix = node_db_prefix(hash);
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

        let node_dir = node_db_path(repository, hash);
        // log::debug!("Compressing node from dir {:?}", node_dir);
        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        }
    }
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    Ok(buffer)
}

pub fn compress_node(
    repository: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Vec<u8>, OxenError> {
    // This will be the subdir within the tarball
    // so when we untar it, all the subdirs will be extracted to
    // tree/nodes/...
    let dir_prefix = node_db_prefix(hash);
    let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

    // zip up the node directory
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);
    let node_dir = node_db_path(repository, hash);

    // log::debug!("Compressing node {} from dir {:?}", hash, node_dir);
    if node_dir.exists() {
        tar.append_dir_all(&tar_subdir, node_dir)?;
    }
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed node {} size is {}",
        hash,
        ByteSize::b(total_size)
    );

    Ok(buffer)
}

pub fn compress_commits(
    repository: &LocalRepository,
    commits: &Vec<Commit>,
) -> Result<Vec<u8>, OxenError> {
    // zip up the node directory
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    for commit in commits {
        let hash = commit.hash()?;
        // This will be the subdir within the tarball
        // so when we untar it, all the subdirs will be extracted to
        // tree/nodes/...
        let dir_prefix = node_db_prefix(&hash);
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

        let node_dir = node_db_path(repository, &hash);
        log::debug!("Compressing commit from dir {:?}", node_dir);
        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        }
    }
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    Ok(buffer)
}

pub fn unpack_nodes(
    repository: &LocalRepository,
    buffer: &[u8],
) -> Result<HashSet<MerkleHash>, OxenError> {
    let mut hashes: HashSet<MerkleHash> = HashSet::new();
    log::debug!("Unpacking nodes from buffer...");
    let decoder = GzDecoder::new(buffer);
    log::debug!("Decoder created");
    let mut archive = Archive::new(decoder);
    log::debug!("Archive created");
    let Ok(entries) = archive.entries() else {
        return Err(OxenError::basic_str(
            "Could not unpack tree database from archive",
        ));
    };
    log::debug!("Extracting entries...");
    for file in entries {
        let Ok(mut file) = file else {
            log::error!("Could not unpack file in archive...");
            continue;
        };
        let path = file.path().unwrap();
        let oxen_hidden_path = repository.path.join(OXEN_HIDDEN_DIR);
        let dst_path = oxen_hidden_path.join(TREE_DIR).join(NODES_DIR).join(path);

        if let Some(parent) = dst_path.parent() {
            util::fs::create_dir_all(parent).expect("Could not create parent dir");
        }
        // log::debug!("create_node writing {:?}", dst_path);
        file.unpack(&dst_path).unwrap();

        // the hash is the last two path components combined
        if !dst_path.ends_with("node") && !dst_path.ends_with("children") {
            let id = dst_path
                .components()
                .rev()
                .take(2)
                .map(|c| c.as_os_str().to_str().unwrap())
                .collect::<Vec<&str>>()
                .into_iter()
                .rev()
                .collect::<String>();
            hashes.insert(MerkleHash::from_str(&id)?);
        }
    }
    Ok(hashes)
}

/// Write a node to disk
pub fn write_tree(repo: &LocalRepository, node: &MerkleTreeNode) -> Result<(), OxenError> {
    let EMerkleTreeNode::Commit(commit_node) = &node.node else {
        return Err(OxenError::basic_str("Expected commit node"));
    };
    let commit_node = CommitNode::new(repo, commit_node.get_opts())?;
    p_write_tree(repo, node, &commit_node)?;
    Ok(())
}

fn p_write_tree(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    node_impl: &impl TMerkleTreeNode,
) -> Result<(), OxenError> {
    let parent_id = node.parent_id;

    let mut db = MerkleNodeDB::open_read_write(repo, node_impl, parent_id)?;
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::VNode(ref vnode) => {
                db.add_child(vnode)?;
                p_write_tree(repo, child, vnode)?;
            }
            EMerkleTreeNode::Directory(ref dir_node) => {
                db.add_child(dir_node)?;
                p_write_tree(repo, child, dir_node)?;
            }
            EMerkleTreeNode::File(ref file_node) => {
                db.add_child(file_node)?;
            }
            node => {
                panic!("p_write_tree Unexpected node type: {:?}", node);
            }
        }
    }
    db.close()?;
    Ok(())
}

/// The dir hashes allow you to skip to a directory in the tree
pub fn dir_hashes(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashMap<PathBuf, MerkleHash>, OxenError> {
    let node_db_dir = repositories::tree::dir_hash_db_path(repo, commit);
    log::debug!("loading dir_hashes from: {:?}", node_db_dir);
    let opts = db::key_val::opts::default();
    let node_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, node_db_dir, false)?;
    let mut dir_hashes = HashMap::new();
    let iterator = node_db.iterator(IteratorMode::Start);
    for item in iterator {
        match item {
            Ok((key, value)) => {
                let key = str::from_utf8(&key)?;
                let value = str::from_utf8(&value)?;
                let hash = MerkleHash::from_str(value)?;
                dir_hashes.insert(PathBuf::from(key), hash);
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    // log::debug!("read dir_hashes: {:?}", dir_hashes);
    Ok(dir_hashes)
}

/// Collect all the node hashes for the given commits
pub fn get_all_node_hashes_for_commits(
    repository: &LocalRepository,
    commits: &[Commit],
    maybe_subtrees: &Option<Vec<PathBuf>>,
    maybe_depth: &Option<i32>,
    is_download: bool,
) -> Result<HashSet<MerkleHash>, OxenError> {
    log::debug!(
        "Getting ALL node hashes for {} commits... and subtree paths {:?}",
        commits.len(),
        maybe_subtrees
    );

    let mut all_node_hashes: HashSet<MerkleHash> = HashSet::new();

    for commit in commits {
        get_node_hashes_for_commit(
            repository,
            commit,
            maybe_subtrees,
            maybe_depth,
            is_download,
            &None,
            &mut all_node_hashes,
        )?;
    }
    log::debug!("All node hashes: {}", all_node_hashes.len());

    Ok(all_node_hashes)
}

/// Collect the new node hashes between the first and last commits in the given list
/// If only one commit is provided, then it will return all the node hashes for that commit.
pub fn get_node_hashes_between_commits(
    repository: &LocalRepository,
    commits: &[Commit],
    maybe_subtrees: &Option<Vec<PathBuf>>,
    maybe_depth: &Option<i32>,
    is_download: bool,
) -> Result<HashSet<MerkleHash>, OxenError> {
    // (the nodes that are NOT in the base commit, but ARE in any of the later commits)
    // There will be duplicate nodes across commits, hence the need to dedup
    log::debug!(
        "Getting new node hashes for {} commits... and subtree paths {:?}",
        commits.len(),
        maybe_subtrees
    );
    let (first_commit, new_commits) = commits
        .split_first()
        .ok_or(OxenError::basic_str("Must provide at least one commit"))?;

    let mut starting_node_hashes: HashSet<MerkleHash> = HashSet::new();
    get_node_hashes_for_commit(
        repository,
        first_commit,
        maybe_subtrees,
        maybe_depth,
        is_download,
        &None,
        &mut starting_node_hashes,
    )?;

    if new_commits.is_empty() {
        // If there are no new commits, then we just return the node hashes for the first commit
        return Ok(starting_node_hashes);
    }

    let mut new_node_hashes: HashSet<MerkleHash> = HashSet::new();

    let maybe_starting_node_hashes = Some(starting_node_hashes);
    for commit in new_commits {
        get_node_hashes_for_commit(
            repository,
            commit,
            maybe_subtrees,
            maybe_depth,
            is_download,
            &maybe_starting_node_hashes,
            &mut new_node_hashes,
        )?;
    }
    log::debug!("New node hashes: {}", new_node_hashes.len());

    Ok(new_node_hashes)
}

pub fn get_node_hashes_for_commit(
    repository: &LocalRepository,
    commit: &Commit,
    maybe_subtrees: &Option<Vec<PathBuf>>,
    maybe_depth: &Option<i32>,
    is_download: bool,
    starting_node_hashes: &Option<HashSet<MerkleHash>>,
    new_node_hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    if let Some(subtrees) = maybe_subtrees {
        // Traverse up the tree to get all the parent directories
        let mut all_parent_paths: Vec<PathBuf> = Vec::new();
        for subtree_path in subtrees {
            let path = subtree_path.clone();
            let mut current_path = path.clone();

            // Add the original subtree path first
            all_parent_paths.push(current_path.clone());

            // Traverse up the tree to add parent paths
            while let Some(parent) = current_path.parent() {
                all_parent_paths.push(parent.to_path_buf());
                current_path = parent.to_path_buf();
            }
            all_parent_paths.reverse();
        }

        for subtree in subtrees {
            get_node_hashes_for_subtree(
                repository,
                commit,
                &Some(subtree.clone()),
                maybe_depth,
                starting_node_hashes,
                new_node_hashes,
            )?;
        }

        if !is_download {
            collect_nodes_along_path(
                repository,
                commit,
                all_parent_paths,
                starting_node_hashes,
                new_node_hashes,
            )?;
        }
    } else {
        get_node_hashes_for_subtree(
            repository,
            commit,
            &None,
            maybe_depth,
            starting_node_hashes,
            new_node_hashes,
        )?;
    }
    // Add the commit hash itself
    new_node_hashes.insert(commit.hash()?);
    Ok(())
}

fn get_node_hashes_for_subtree(
    repository: &LocalRepository,
    commit: &Commit,
    subtree_path: &Option<PathBuf>,
    depth: &Option<i32>,
    starting_node_hashes: &Option<HashSet<MerkleHash>>,
    new_node_hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    let Ok(Some(tree)) = get_subtree_by_depth(repository, commit, subtree_path, depth) else {
        // If the subtree is not found, then we don't need to add any nodes to the unique node hashes
        return Ok(());
    };

    if let Some(starting_node_hashes) = starting_node_hashes {
        // Only add nodes that are NOT in the starting hashes
        tree.walk_tree_without_leaves(|node| {
            if !starting_node_hashes.contains(&node.hash) {
                new_node_hashes.insert(node.hash);
            }
        });
    } else {
        tree.walk_tree_without_leaves(|node| {
            new_node_hashes.insert(node.hash);
        });
    }

    Ok(())
}

// Commit db is the directories per commit
// This helps us skip to a directory in the tree
// .oxen/history/{COMMIT_ID}/dir_hashes
pub fn dir_hash_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(Path::new(HISTORY_DIR))
        .join(&commit.id)
        .join(DIR_HASHES_DIR)
}

pub fn dir_hash_db_path_from_commit_id(repo: &LocalRepository, commit_id: &MerkleHash) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(Path::new(HISTORY_DIR))
        .join(commit_id.to_string())
        .join(DIR_HASHES_DIR)
}

pub fn print_tree(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    let tree = get_root_with_children(repo, commit)?.unwrap();
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::print_node(&tree);
        }
        _ => {
            CommitMerkleTreeLatest::print_node(&tree);
        }
    }
    Ok(())
}

pub fn print_tree_depth_subtree(
    repo: &LocalRepository,
    commit: &Commit,
    depth: i32,
    subtree: &PathBuf,
) -> Result<(), OxenError> {
    let tree = get_subtree(repo, commit, subtree, depth)?.unwrap();
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::print_node_depth(&tree, depth);
        }
        _ => {
            CommitMerkleTreeLatest::print_node_depth(&tree, depth);
        }
    }
    Ok(())
}

pub fn print_tree_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<(), OxenError> {
    let tree = get_node_by_path_with_children(repo, commit, path)?.unwrap();
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::print_node(&tree);
        }
        _ => {
            CommitMerkleTreeLatest::print_node(&tree);
        }
    }
    Ok(())
}

pub fn print_tree_depth(
    repo: &LocalRepository,
    commit: &Commit,
    depth: i32,
) -> Result<(), OxenError> {
    let tree = get_root_with_children(repo, commit)?.unwrap();
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::print_node_depth(&tree, depth);
        }
        _ => {
            CommitMerkleTreeLatest::print_node_depth(&tree, depth);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use std::path::PathBuf;

    #[test]
    fn test_list_tabular_files_in_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create a deeply nested directory
            let dir_path = repo
                .path
                .join("data")
                .join("train")
                .join("images")
                .join("cats");
            util::fs::create_dir_all(&dir_path)?;

            // Add two tabular files to it
            let filename = "cats.tsv";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            let filename = "dogs.csv";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "1,2,3\nhello,world,sup\n")?;

            // And write a file in the same dir that is not tabular
            let filename = "README.md";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // And write a tabular file to the root dir
            let filename = "labels.tsv";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // And write a non tabular file to the root dir
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // Add and commit all
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            // List files
            let files = repositories::tree::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(files.len(), 3);

            // Add another tabular file
            let filename = "dogs.tsv";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // Add and commit all
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding additional file")?;

            let files = repositories::tree::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(files.len(), 4);

            // Remove the deeply nested dir
            util::fs::remove_dir_all(&dir_path)?;

            let mut opts = RmOpts::from_path(dir_path);
            opts.recursive = true;
            repositories::rm(&repo, &opts)?;
            let commit = repositories::commit(&repo, "Removing dir")?;

            let files = repositories::tree::list_tabular_files_in_repo(&repo, &commit)?;
            assert_eq!(files.len(), 2);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_merkle_two_files_same_hash() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|local_repo| async move {
            let p1 = "hi.txt";
            let p2 = "bye.txt";
            let path_1 = local_repo.path.join(p1);
            let path_2 = local_repo.path.join(p2);

            let common_contents = "the same file";

            test::write_txt_file_to_path(&path_1, common_contents)?;
            test::write_txt_file_to_path(&path_2, common_contents)?;

            repositories::add(&local_repo, &path_1)?;
            repositories::add(&local_repo, &path_2)?;

            let status = repositories::status(&local_repo)?;

            log::debug!("staged files here are {:?}", status.staged_files);

            assert_eq!(status.staged_files.len(), 2);

            assert!(status.staged_files.contains_key(&PathBuf::from(p1)));
            assert!(status.staged_files.contains_key(&PathBuf::from(p2)));

            let commit = repositories::commit(&local_repo, "add two files")?;

            assert!(repositories::tree::has_path(
                &local_repo,
                &commit,
                PathBuf::from(p1)
            )?);
            assert!(repositories::tree::has_path(
                &local_repo,
                &commit,
                PathBuf::from(p2)
            )?);

            Ok(())
        })
        .await
    }

    #[test]
    fn test_get_node_hashes_between_commits() -> Result<(), OxenError> {
        test::run_local_repo_training_data_committed(|repo| {
            // Get the initial commit from training data to use as baseline
            let starting_commit = repositories::commits::head_commit(&repo)?;

            // Add some new files and make first new commit
            let new_file1 = repo.path.join("new_file1.txt");
            let new_file2 = repo.path.join("new_dir1").join("new_file2.txt");
            util::fs::create_dir_all(new_file2.parent().unwrap())?;

            util::fs::write(&new_file1, "This is new file 1 content")?;
            util::fs::write(&new_file2, "This is new file 2 content")?;

            repositories::add(&repo, &new_file1)?;
            repositories::add(&repo, &new_file2)?;
            let commit1 = repositories::commit(&repo, "Add first batch of new files")?;

            // Add more files and make second new commit
            let new_file3 = repo.path.join("new_dir2").join("new_file3.csv");
            util::fs::create_dir_all(new_file3.parent().unwrap())?;
            util::fs::write(&new_file3, "col1,col2,col3\nval1,val2,val3\n")?;

            repositories::add(&repo, &new_file3)?;
            let commit2 = repositories::commit(&repo, "Add second batch of new files")?;

            // Test get_unique_node_hashes with all commits (baseline + new commits)
            let commit_list = vec![starting_commit, commit1.clone(), commit2.clone()];
            let new_hashes = super::get_node_hashes_between_commits(
                &repo,
                &commit_list,
                &None, // no subtree filter
                &None, // no depth limit
                false, // not a download operation
            )?;

            // Verify that we got the correct number of unique hashes
            // commit1, root dir (v1), root vnode (v1), new_dir1, new_dir1 vnode
            // commit2, root dir (v2), root vnode (v2), new_dir2, new_dir2 vnode
            // = 10 nodes
            assert!(
                new_hashes.len() == 10,
                "Should have found 10 unique node hashes for new commits, got {}",
                new_hashes.len()
            );

            // Verify that the unique hashes include the commit hashes themselves
            assert!(
                new_hashes.contains(&commit1.hash()?),
                "Should include first new commit hash"
            );
            assert!(
                new_hashes.contains(&commit2.hash()?),
                "Should include second new commit hash"
            );

            // Test with subtree filter
            let subtree_path = PathBuf::from("new_dir1");
            let subtree_hashes = super::get_node_hashes_between_commits(
                &repo,
                &commit_list,
                &Some(vec![subtree_path]),
                &None,
                false,
            )?;

            // Subtree filter should return fewer hashes since it's only looking at new_dir1
            // commit1, root dir (v1), root vnode (v1), new_dir1, new_dir1 vnode
            // commit2, root dir (v2), root vnode (v2)
            // = 8 nodes
            assert!(
                subtree_hashes.len() == 8,
                "Subtree filter should return 8 hashes, got {}",
                subtree_hashes.len()
            );

            // Test with depth limit
            let depth_limited_hashes = super::get_node_hashes_between_commits(
                &repo,
                &commit_list,
                &None,
                &Some(0), // depth of 0
                false,
            )?;

            // Depth filter should return fewer hashes
            // commit1, root dir (v1), root vnode (v1)
            // commit2, root dir (v2), root vnode (v2)
            // = 6 nodes
            assert!(
                depth_limited_hashes.len() == 6,
                "Depth filter should return 6 hashes, got {}",
                depth_limited_hashes.len()
            );

            Ok(())
        })
    }
}
