use bytesize::ByteSize;
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::str;
use tar::Archive;

use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::db::merkle_node::MerkleNodeDB;
use crate::core::db::merkle_node::merkle_node_db::{MerkleDbError, node_db_path};
use crate::core::node_sync_status;
use crate::core::v_latest::index::CommitMerkleTree as CommitMerkleTreeLatest;
use crate::core::v_latest::index::CommitMerkleTree;
use crate::core::v_old::v0_19_0::index::CommitMerkleTree as CommitMerkleTreeV0_19_0;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merkle_tree::merkle_transport::{PackOptions, UnpackOptions};
use crate::model::merkle_tree::node::{
    CommitNode, DirNodeWithPath, EMerkleTreeNode, FileNode, FileNodeWithDir, MerkleTreeNode,
};
use crate::model::{
    Commit, EntryDataType, LocalRepository, MerkleHash, MerkleTreeNodeType, PartialNode,
    TMerkleTreeNode,
};
use crate::util::fs::AtomicFile;
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

/// This will return the MerkleTreeNode with type CommitNode if the Commit exists
/// Otherwise it will return None
/// Also gathers all loaded dir and vnode hashes into `shared_hashes`
pub fn get_root_with_children_and_node_hashes(
    repo: &LocalRepository,
    commit: &Commit,
    base_hashes: Option<&HashSet<MerkleHash>>,
    unique_hashes: Option<&mut HashSet<MerkleHash>>,
    shared_hashes: Option<&mut HashSet<MerkleHash>>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::root_with_children(repo, commit),
        _ => CommitMerkleTreeLatest::root_with_children_and_node_hashes(
            repo,
            commit,
            base_hashes,
            unique_hashes,
            shared_hashes,
        ),
    }
}

/// This will return the MerkleTreeNode with type CommitNode if the Commit exists
/// Otherwise it will return None
/// Also gathers all loaded dir and vnode hashes into `node_hashes`, and all loaded file nodes as partial nodes
pub fn get_root_with_children_and_partial_nodes(
    repo: &LocalRepository,
    commit: &Commit,
    base_hashes: Option<&HashSet<MerkleHash>>,
    unique_hashes: Option<&mut HashSet<MerkleHash>>,
    shared_hashes: Option<&mut HashSet<MerkleHash>>,
    partial_nodes: &mut HashMap<PathBuf, PartialNode>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::root_with_children(repo, commit),
        _ => CommitMerkleTreeLatest::root_with_children_and_partial_nodes(
            repo,
            commit,
            base_hashes,
            unique_hashes,
            shared_hashes,
            partial_nodes,
        ),
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
    let commit_id = commit.id.parse()?;
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
    let dir_hashes = CommitMerkleTreeLatest::dir_hashes(repo, commit)?;
    Ok(dir_hashes.contains_key(path.as_ref()))
}

pub fn has_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let dir_hashes = CommitMerkleTreeLatest::dir_hashes(repo, commit)?;
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
                    log::warn!("Error getting node by path: {e:?}");
                    Ok(None)
                }
            }
        }
        _ => match CommitMerkleTreeLatest::read_from_path(repo, commit, path, load_recursive) {
            Ok(node) => Ok(node),
            Err(e) => {
                log::warn!("Error getting node by path: {e:?}");
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
            Some(CommitMerkleTreeV0_19_0::from_path(repo, commit, path, load_recursive)?.root)
        }
        _ => CommitMerkleTreeLatest::read_from_path(repo, commit, path, load_recursive)?,
    };

    Ok(node)
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
    dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let _perf = crate::perf_guard!("tree::get_dir_with_children");

    match repo.min_version() {
        MinOxenVersion::V0_19_0 => CommitMerkleTreeV0_19_0::dir_with_children(repo, commit, path),
        _ => CommitMerkleTreeLatest::dir_with_children(repo, commit, path, dir_hashes),
    }
}

pub fn get_dir_without_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::dir_without_children(repo, commit, path)
        }
        _ => CommitMerkleTreeLatest::dir_without_children(repo, commit, path, dir_hashes),
    }
}

pub fn get_dir_with_children_recursive(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::dir_with_children_recursive(repo, commit, path)
        }
        _ => CommitMerkleTreeLatest::dir_with_children_recursive(repo, commit, path, dir_hashes),
    }
}

/// Helper function where you can pass in Optional depth and Optional path and get a tree
/// If depth is None, it will default to -1 which means the entire subtree
/// If path is None, it will default to the root
/// Otherwise it will get the subtree at the given path with the given depth
pub fn get_subtree(
    repo: &LocalRepository,
    commit: &Commit,
    maybe_subtree: &Option<PathBuf>,
    maybe_depth: &Option<i32>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match (maybe_subtree, maybe_depth) {
        (Some(subtree), Some(depth)) => {
            log::debug!("Getting subtree {subtree:?} with depth {depth} for commit {commit}");
            get_subtree_by_depth(repo, commit, subtree, *depth)
        }
        (Some(subtree), None) => {
            // If the depth is not provided, we default to -1 which means the entire subtree
            log::debug!("Getting subtree {subtree:?} for commit {commit} with depth -1");
            get_subtree_by_depth(repo, commit, subtree, -1)
        }
        (None, Some(depth)) => {
            log::debug!("Getting tree from root with depth {depth} for commit {commit}");
            get_subtree_by_depth(repo, commit, PathBuf::from("."), *depth)
        }
        _ => {
            log::debug!("Getting full tree for commit {commit}");
            get_root_with_children(repo, commit)
        }
    }
}

pub fn get_subtree_by_depth(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    depth: i32,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::from_path_depth(repo, commit, path, depth)
        }
        _ => CommitMerkleTreeLatest::read_depth_from_path(repo, commit, path, depth),
    }
}

pub fn get_subtree_by_depth_with_unique_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    base_hashes: Option<&HashSet<MerkleHash>>,
    unique_hashes: Option<&mut HashSet<MerkleHash>>,
    shared_hashes: Option<&mut HashSet<MerkleHash>>,
    depth: i32,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_19_0 => {
            CommitMerkleTreeV0_19_0::from_path_depth(repo, commit, path, depth)
        }
        _ => CommitMerkleTreeLatest::read_depth_from_path_and_collect_hashes(
            repo,
            commit,
            path,
            base_hashes,
            unique_hashes,
            shared_hashes,
            depth,
        ),
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
    starting_node_hashes: &mut HashSet<MerkleHash>,
    hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    // Grab the first path or error if empty
    let root_path = paths
        .first()
        .ok_or_else(|| OxenError::basic_str("No paths provided"))?;
    let node = get_node_by_path_with_children(repo, commit, root_path)?
        .ok_or_else(|| OxenError::basic_str("Node not found"))?;

    let (_root_node, nodes) = node.get_nodes_along_paths(paths)?;

    for node in nodes {
        if !starting_node_hashes.contains(&node.hash) {
            hashes.insert(node.hash);
        }
    }

    Ok(())
}

pub fn list_missing_node_hashes(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let mut results = HashSet::new();
    for hash in hashes {
        if !node_sync_status::node_is_synced(repo, hash) {
            results.insert(*hash);
        }
    }

    Ok(results)
}

// TODO: Deduplicate functionality with model::MerkleTreeNode
pub async fn list_missing_file_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<HashSet<MerkleHash>, OxenError> {
    if repo.min_version() == MinOxenVersion::V0_19_0 {
        let Some(node) = CommitMerkleTreeV0_19_0::read_depth(repo, hash, 1)? else {
            return Err(OxenError::basic_str(format!("Node {hash} not found")));
        };
        node.list_missing_file_hashes(repo).await
    } else {
        let Some(node) = CommitMerkleTreeLatest::read_depth(repo, hash, 1)? else {
            return Err(OxenError::basic_str(format!("Node {hash} not found")));
        };
        node.list_missing_file_hashes(repo).await
    }
}

/// Given a set of commit ids, return the hashes that are missing from the tree
pub async fn list_missing_file_hashes_from_commits(
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
            log::error!("list_missing_file_hashes_from_commits Commit {commit_id_str} not found");
            return Err(OxenError::RevisionNotFound(commit_id_str.into()));
        };
        // Handle the case where we are given a list of subtrees to check
        // It is much faster to check the subtree directly than to walk the entire tree
        if let Some(subtree_paths) = subtree_paths {
            for path in subtree_paths {
                // TODO: Use the partial load
                let Some(tree) =
                    repositories::tree::get_subtree(repo, &commit, &Some(path.clone()), depth)?
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
                    "list_missing_file_hashes_from_commits root not found for commit: {commit:?}"
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
    list_missing_file_hashes_from_hashes(repo, &candidate_hashes).await
}

/// Files and directories collected from a merkle subtree walk. `oxen restore <dir>` needs
/// both: file entries to call `restore_file` on, and directory paths so it can `mkdir -p`
/// every tracked directory (including empty ones, which are first-class in Oxen — see
/// CLAUDE.md "How Oxen Differs from Git"). Empty dirs have no files to drive
/// `restore_file`'s `create_dir_all(parent)` side-effect, so without an explicit dir list
/// they'd be silently skipped (ENG-1003).
#[derive(Debug, Default)]
pub struct DirEntries {
    pub files: HashSet<(FileNode, PathBuf)>,
    pub dirs: HashSet<PathBuf>,
}

/// Walk `node`'s merkle subtree and collect every File entry and every Directory path,
/// with `base_path` interpreted as the path of `node` itself. Infallible: unexpected node
/// types contribute nothing.
pub fn dir_entries_with_paths(node: &MerkleTreeNode, base_path: &Path) -> DirEntries {
    let mut out = DirEntries::default();

    if matches!(&node.node, EMerkleTreeNode::Directory(_)) {
        out.dirs.insert(base_path.to_path_buf());
    }

    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::File(file_node) => {
                let file_path = base_path.join(file_node.name());
                out.files.insert((file_node.clone(), file_path));
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let new_base_path = base_path.join(dir_node.name());
                let sub = dir_entries_with_paths(child, &new_base_path);
                out.files.extend(sub.files);
                out.dirs.extend(sub.dirs);
            }
            EMerkleTreeNode::VNode(_) => {
                let sub = dir_entries_with_paths(child, base_path);
                out.files.extend(sub.files);
                out.dirs.extend(sub.dirs);
            }
            _ => {}
        }
    }

    out
}

/// Get HashMap of all entries that aren't present in shared_hashes
pub fn unique_dir_entries(
    base_path: &Path,
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
            )));
        }
    }

    Ok(entries)
}

async fn list_missing_file_hashes_from_hashes(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let mut results = HashSet::new();
    let version_store = repo.version_store();
    // Todo: Parallelize for S3
    for hash in hashes {
        if !version_store.version_exists(&hash.to_string()).await? {
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

    if let EMerkleTreeNode::File(file_node) = &node.node {
        file_nodes.insert(FileNodeWithDir {
            file_node: file_node.to_owned(),
            dir: traversed_path.to_owned(),
        });
        return Ok(());
    }

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
                if new_path != Path::new("") {
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
        log::warn!("get_root_with_children returned None for commit: {commit:?}");
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
            EMerkleTreeNode::File(file_node) if file_node.data_type() == data_type => {
                let mut file_node = file_node.to_owned();
                let full_path = traversed_path.join(file_node.name());
                file_node.set_name(&full_path.to_string_lossy());
                file_nodes.insert(file_node);
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
    let original_dir_hashes_path =
        CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, &original_commit_id.to_string());
    let new_dir_hashes_path =
        CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, &new_commit_id.to_string());
    util::fs::copy_dir_all(original_dir_hashes_path, new_dir_hashes_path)?;

    Ok(())
}

pub fn compress_tree(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
    let mut buffer = Vec::new();
    write_all_tar(&repository.path, &mut buffer, true)?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("Compressed entire tree size is {}", ByteSize::b(total_size));
    Ok(buffer)
}

pub fn compress_nodes(
    repository: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<Vec<u8>, OxenError> {
    log::debug!("Compressing {} unique nodes...", hashes.len());
    let mut buffer = Vec::new();
    write_hashes_tar(
        &repository.path,
        hashes,
        PackOptions::ServerCanonical,
        &mut buffer,
        true,
    )?;
    Ok(buffer)
}

pub fn compress_node(
    repository: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Vec<u8>, OxenError> {
    let buffer = compress_nodes(repository, &HashSet::from([*hash]))?;
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
    commits: &[Commit],
) -> Result<Vec<u8>, OxenError> {
    let hashes = commits
        .iter()
        .map(|commit| commit.hash())
        .collect::<Result<HashSet<MerkleHash>, _>>()?;
    compress_nodes(repository, &hashes)
}

pub fn unpack_nodes(
    repository: &LocalRepository,
    buffer: &[u8],
) -> Result<HashSet<MerkleHash>, OxenError> {
    // The server-side upload consumer skips entries that already exist on disk.
    let mut reader: &[u8] = buffer;
    let oxen_hidden = repository.path.join(OXEN_HIDDEN_DIR);
    extract_tar_under(&mut reader, &oxen_hidden, false).map_err(OxenError::from)
}

/// Pack the tar-gz wire format for a set of merkle hashes into `out`, using the
/// in-tar layout and gzip compression level selected by `opts`.
///
/// Hashes whose node directory is missing on disk are silently skipped iff
/// `skip_missing_node_dir` is `true`: this matches the existing oxen behavior
/// of `compress_nodes` / `compress_node` / `compress_commits`. If `false`,
/// missing node directories result in an `Err(MerkleDbError::MissingNodeDir)`.
fn write_hashes_tar<W: Write>(
    repo_path: &Path,
    hashes: &HashSet<MerkleHash>,
    opts: PackOptions,
    out: W,
    skip_missing_node_dir: bool,
) -> Result<(), MerkleDbError> {
    let enc = GzEncoder::new(out, pack_options_compression(opts));
    let mut tar = tar::Builder::new(enc);
    for hash in hashes {
        let dir_prefix = hash.to_hex_hash().node_db_prefix();
        let tar_subdir: PathBuf = match opts {
            PackOptions::ServerCanonical => Path::new(TREE_DIR).join(NODES_DIR).join(&dir_prefix),
            PackOptions::LegacyClientPush => PathBuf::from(&dir_prefix),
        };
        let node_dir = node_db_path(repo_path, hash);
        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        } else if !skip_missing_node_dir {
            return Err(MerkleDbError::MissingNodeDir(*hash));
        } else {
            log::warn!("Skipping missing node dir for hash {}", hash.to_hex_hash());
        }
    }
    tar.finish()?;
    tar.into_inner()?.finish()?;
    Ok(())
}

/// Map a [`PackOptions`] to the gzip compression level historically used for that
/// layout. `Compression::fast` for the server-canonical download bytes;
/// `Compression::default` for the legacy client-push upload bytes.
fn pack_options_compression(opts: PackOptions) -> Compression {
    match opts {
        PackOptions::ServerCanonical => Compression::fast(),
        PackOptions::LegacyClientPush => Compression::default(),
    }
}

/// Pack the tar-gz wire format for every node in the store into `out`.
/// If `skip_missing_node_dir` is `false`, missing node directories result in
/// an `Err(MerkleDbError::MissingTreeNodesDir)`. Otherwise, missing node dirs
/// are skipped and logged as a warning.
fn write_all_tar<W: Write>(
    repo_path: &Path,
    out: W,
    skip_missing_node_dir: bool,
) -> Result<(), MerkleDbError> {
    let enc = GzEncoder::new(out, Compression::fast());
    let mut tar = tar::Builder::new(enc);
    let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR);
    let nodes_dir = repo_path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);
    if nodes_dir.exists() {
        tar.append_dir_all(&tar_subdir, nodes_dir)?;
    } else if !skip_missing_node_dir {
        return Err(MerkleDbError::MissingTreeNodesDir);
    } else {
        log::warn!("Missing oxen tree/nodes dir in this repository: resulting in empty tarball");
    }
    tar.finish()?;
    tar.into_inner()?.finish()?;
    Ok(())
}

/// Unpack a tar-gz wire stream into `oxen_hidden` (the repository's `.oxen/` directory).
///
/// Tolerates two historical tarball layouts so that either a new or legacy client can talk
/// to the on-disk node store:
///   - **Server-style** (emitted by [`write_hashes_tar`] / [`write_all_tar`] and the old
///     `compress_*` helpers): entries carry the full `tree/nodes/{prefix}/{suffix}/{node,children}`
///     prefix. Joined directly under `oxen_hidden`.
///   - **Legacy client-push style** (emitted by the old `api::client::tree::create_nodes`):
///     entries start at `{prefix}/{suffix}/{node,children}` with no `tree/nodes/` prefix.
///     Prepended under `oxen_hidden/tree/nodes/`.
///
/// Returns the set of hashes parsed from the tarball.
///
/// Behaviour controls (to provide a backwards-compatible format that older clients & servers speak):
/// - `overwrite_existing == true` overwrites entries whose destination already exists;
///   matches `util::fs::unpack_async_tar_archive`'s download-path behaviour.
/// - `overwrite_existing == false` skips them; matches
///   `repositories::tree::unpack_nodes`'s upload-consumer behaviour.
///
/// File-entry payloads are written through [`AtomicFile::stream`] (the write-temp-then-
/// rename pattern). A crash or cancellation mid-write leaves either the prior
/// contents (or nothing) at the destination — never a half-written file. Directory
/// entries are created with `std::fs::create_dir_all`; tar's per-entry mtime/perms are
/// intentionally not preserved (the merkle store doesn't rely on either, and dropping
/// them lets us avoid the full `tar::Entry::unpack` path).
///
/// Errors:
/// - Entries that aren't regular files or directories return [`MerkleDbError::UnsupportedTarEntry`].
/// - Entries whose path contains a `..` component return [`MerkleDbError::PathTraversal`].
///
/// Both checks mirror `util::fs::unpack_async_tar_archive`.
fn extract_tar_under<R: Read>(
    reader: R,
    oxen_hidden: &Path,
    overwrite_existing: bool,
) -> Result<HashSet<MerkleHash>, MerkleDbError> {
    let mut hashes: HashSet<MerkleHash> = HashSet::new();
    let decoder = GzDecoder::new(reader);
    let mut archive = Archive::new(decoder);
    let entries = archive.entries().map_err(MerkleDbError::CannotReadMerkle)?;

    let tree_nodes_prefix = Path::new(TREE_DIR).join(NODES_DIR);

    for entry in entries {
        let Ok(mut file) = entry else {
            log::error!("Could not unpack file in merkle tar archive");
            // TODO: raise this error to the caller instead!?
            continue;
        };
        let path = file.path()?.into_owned();
        // Path-traversal guard: refuse any entry whose path resolves above its container.
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            return Err(MerkleDbError::PathTraversal(path.display().to_string()));
        }
        // Entry-type validation: only regular files and directories are allowed.
        let entry_type = file.header().entry_type();
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(MerkleDbError::UnsupportedTarEntry {
                path: path.display().to_string(),
            });
        }
        // Server-style entries already contain `tree/nodes/...`; join directly.
        // Legacy client-push entries begin at `{prefix}/{suffix}/...`; prepend `tree/nodes/`.
        let dst_path = if path.starts_with(&tree_nodes_prefix) {
            oxen_hidden.join(&path)
        } else {
            oxen_hidden.join(&tree_nodes_prefix).join(&path)
        };
        if dst_path.exists() && !overwrite_existing {
            log::info!("Node already exists at {dst_path:?}, skipping");
            continue;
        }
        if entry_type.is_dir() {
            std::fs::create_dir_all(&dst_path)?;
        } else {
            AtomicFile::new(&dst_path)
                .stream(&mut file)
                .map_err(|err| MerkleDbError::FsTransport(Box::new(err)))?;
        }

        // Extract the merkle hash from this entry's path, if it identifies one.
        //
        // After the path-resolution above, `dst_path` is of the form
        // `<oxen_hidden>/tree/nodes/<rest>`. We classify entries by the SHAPE
        // of `<rest>`, never by whether components happen to be hex. We assume that
        // we have the hex-encoded hash as the `{prefix}/{suffix}` dirs.
        if let Some(hash) = extract_hash_from_entry_path(&dst_path, oxen_hidden)? {
            hashes.insert(hash);
        }
        // If we can't extract a path, it's because we're looking at a node or children file.
        // We will have already obtained the hash from the directory, so this is ok!
    }
    Ok(hashes)
}

/// Inspect a fully-resolved tar entry destination path and, if it identifies a Merkle
/// node, return that node's hash.
///
/// `dst_path` must be inside `<oxen_hidden>/tree/nodes/`. The path's segments after
/// that prefix determine the entry kind:
///
/// | Segments after `tree/nodes/` | Entry kind | Result |
/// |---|---|---|
/// | 0 (the `tree/nodes` dir itself) | intermediate dir | `Ok(None)` |
/// | 1 (the `{prefix}` dir) | intermediate dir | `Ok(None)` |
/// | 2 (`{prefix}/{suffix}` dir) | hash-bearing dir | `Ok(Some(hash))` (hash parsed from `{prefix}{suffix}` as hex u128) |
/// | 3 (`{prefix}/{suffix}/node` or `.../children`) | leaf file | `Ok(None)` (hash already produced from the parent dir entry) |
///
/// Anything else returns [`MerkleDbError::InvalidTarStructure`]. A `{prefix}/{suffix}`
/// dir whose `{prefix}` & `{suffix}` don't hex-parse as a `u128` value returns a Err of
/// [`MerkleDbError::InvalidNodeIdHex`]. Any non-UTF-8 path components in the tarball return
/// an Err of [`MerkleDbError::InvalidTarStructure`]: the merkle layout should never produce
/// a non-UTF-8 segment name.
fn extract_hash_from_entry_path(
    dst_path: &Path,
    oxen_hidden: &Path,
) -> Result<Option<MerkleHash>, MerkleDbError> {
    let tree_nodes_prefix = Path::new(TREE_DIR).join(NODES_DIR);

    // make a new InvalidTarStructure MerkleDbError instance
    let invalid_structure = |reason: &str| MerkleDbError::InvalidTarStructure {
        entry_path: dst_path.display().to_string(),
        reason: reason.to_string(),
    };

    let rel = dst_path.strip_prefix(oxen_hidden).map_err(|_| {
        invalid_structure("entry resolved outside of the repo's `.oxen/` directory")
    })?;
    let under_tree_nodes = rel
        .strip_prefix(&tree_nodes_prefix)
        .map_err(|_| invalid_structure("entry path is not under `tree/nodes/`"))?;

    // Collect normal path components as `&str`. Reject non-UTF-8 components
    // up front (they can't appear in a well-formed merkle tar archive).
    let mut components: Vec<&str> = Vec::new();
    for component in under_tree_nodes.components() {
        let Component::Normal(segment) = component else {
            return Err(invalid_structure(
                "entry path contains a non-`Normal` component",
            ));
        };
        let Some(s) = segment.to_str() else {
            return Err(invalid_structure("entry path contains a non-UTF-8 segment"));
        };
        components.push(s);
    }

    match components.as_slice() {
        // `tree/nodes` itself, or `tree/nodes/{prefix}` — intermediate dirs
        // produced by `pack_all`. No hash to record.
        [] | [_] => Ok(None),
        // `tree/nodes/{prefix}/{suffix}` — the hash-bearing dir. Parse
        // unconditionally; failure is a structured error.
        [prefix, suffix] => {
            let id = format!("{prefix}{suffix}");
            let hash_value = u128::from_str_radix(&id, 16)
                .map_err(|source| MerkleDbError::InvalidNodeIdHex { id, source })?;
            Ok(Some(MerkleHash::new(hash_value)))
        }
        // `tree/nodes/{prefix}/{suffix}/{node|children}` — leaf files. The
        // hash is recorded when the parent dir entry is processed.
        [_, _, leaf] if *leaf == "node" || *leaf == "children" => Ok(None),
        [_, _, other] => Err(invalid_structure(&format!(
            "leaf file under `tree/nodes/{{prefix}}/{{suffix}}/` must be `node` or `children`, got `{other}`"
        ))),
        _ => Err(invalid_structure(
            "entry has more components than `tree/nodes/{prefix}/{suffix}/[node|children]`",
        )),
    }
}

/// Pack the given node hashes into `out` as a tar-gz stream, using the layout
/// and compression level selected by `opts`. Hashes absent from the store are
/// silently skipped.
pub(crate) fn pack_nodes(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
    opts: PackOptions,
    out: &mut dyn Write,
) -> Result<(), OxenError> {
    write_hashes_tar(&repo.path, hashes, opts, out, true).map_err(OxenError::from)
}

/// Pack every node the store holds into `out` as a tar-gz stream.
/// Always emits the server-canonical layout. Only the f5 wire-compat tests pack the whole
/// tree through this path; the server's full-tree download goes through
/// `repositories::tree::compress_full_tree`.
#[cfg(test)]
pub(crate) fn pack_all(repo: &LocalRepository, out: &mut dyn Write) -> Result<(), OxenError> {
    write_all_tar(&repo.path, out, true)?;
    Ok(())
}

/// Estimate the **uncompressed** packed node tar payload.
///
/// The estimate sums each present node directory's file content sizes plus tar's
/// fixed-size overhead (one 512-byte header per file/directory entry, file content
/// padded to 512-byte multiples). It ignores the gzip ratio because the merkle
/// `node` and `children` files contain mostly random-looking hash bytes, which
/// compress to ~1.0× — so this uncompressed total is a tight upper bound on the
/// post-gzip bytes that will flow over the wire.
///
/// Hashes whose node directory is missing on disk contribute 0, matching the
/// silent-skip semantics of [`pack_nodes`].
pub(crate) fn pack_nodes_byte_estimate(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> u64 {
    const TAR_HEADER_BYTES: u64 = 512;
    const TAR_BLOCK_SIZE: u64 = 512;

    let mut total: u64 = 0;
    for hash in hashes {
        let node_dir = node_db_path(&repo.path, hash);
        if !node_dir.exists() {
            continue;
        }
        // The directory entry itself.
        total = total.saturating_add(TAR_HEADER_BYTES);
        let Ok(entries) = std::fs::read_dir(&node_dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if meta.is_file() {
                let len = meta.len();
                let padded = len.div_ceil(TAR_BLOCK_SIZE).saturating_mul(TAR_BLOCK_SIZE);
                total = total.saturating_add(TAR_HEADER_BYTES.saturating_add(padded));
            } else if meta.is_dir() {
                total = total.saturating_add(TAR_HEADER_BYTES);
            }
        }
    }
    total
}

/// Unpack a tar-gz wire stream into the repo's `.oxen/`, applying `opts`'s existing-file
/// policy. File-entry payloads are written through [`AtomicFile::stream`]
/// (write-temp-then-rename), so a crash mid-write never leaves a half-written file.
pub(crate) fn unpack(
    repo: &LocalRepository,
    reader: &mut dyn Read,
    opts: UnpackOptions,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let overwrite_existing = matches!(opts, UnpackOptions::Overwrite);
    let oxen_hidden = repo.path.join(OXEN_HIDDEN_DIR);
    extract_tar_under(reader, &oxen_hidden, overwrite_existing).map_err(OxenError::from)
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

/// Write the entire merkle tree, starting from the node (`node_impl`) to the local repository.
///
/// Recursively writes the node and all its children to disk. To write a full tree, the node
/// (`node_impl`) **MUST** be the root of the tree -- i.e. a `Commit` node.
fn p_write_tree(
    repo: &LocalRepository,
    node: &MerkleTreeNode,
    node_impl: &impl TMerkleTreeNode,
) -> Result<(), OxenError> {
    let parent_id = node.parent_id;

    let mut db = MerkleNodeDB::open_read_write(repo.merkle_node_store(), node_impl, parent_id)?;
    for child in &node.children {
        match &child.node {
            EMerkleTreeNode::VNode(vnode) => {
                db.add_child(vnode)?;
                p_write_tree(repo, child, vnode)?;
            }
            EMerkleTreeNode::Directory(dir_node) => {
                db.add_child(dir_node)?;
                p_write_tree(repo, child, dir_node)?;
            }
            EMerkleTreeNode::File(file_node) => {
                db.add_child(file_node)?;
            }
            node => {
                return Err(OxenError::basic_str(format!(
                    "p_write_tree unexpected node type: {node:?}"
                )));
            }
        }
    }
    db.close()?;
    Ok(())
}

// TODO: Refactor to remove 'is_download' var here
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
    let mut starting_node_hashes = HashSet::new();

    for commit in commits {
        get_node_hashes_for_commit(
            repository,
            commit,
            maybe_subtrees,
            maybe_depth,
            is_download,
            &mut starting_node_hashes,
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
        .ok_or_else(|| OxenError::basic_str("Must provide at least one commit"))?;

    let mut starting_node_hashes = HashSet::new();

    if let Some(subtrees) = maybe_subtrees {
        for subtree in subtrees {
            populate_starting_hashes(
                repository,
                first_commit,
                &Some(subtree.to_path_buf()),
                maybe_depth,
                &mut starting_node_hashes,
            )?;
        }
    } else {
        populate_starting_hashes(
            repository,
            first_commit,
            &None,
            maybe_depth,
            &mut starting_node_hashes,
        )?;
    }

    if new_commits.is_empty() {
        // If there are no new commits, then we just return the node hashes for the first commit
        return Ok(starting_node_hashes);
    }

    let mut new_node_hashes: HashSet<MerkleHash> = HashSet::new();
    for commit in new_commits {
        get_node_hashes_for_commit(
            repository,
            commit,
            maybe_subtrees,
            maybe_depth,
            is_download,
            &mut starting_node_hashes,
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
    starting_node_hashes: &mut HashSet<MerkleHash>,
    new_node_hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    log::debug!("get_node_hashes_for_commit: {}", commit.id);
    if let Some(subtrees) = maybe_subtrees {
        // Traverse up the tree to get all the parent directories
        let mut all_parent_paths: Vec<PathBuf> = Vec::new();
        log::debug!("subtrees: {subtrees:?}");
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
        log::debug!("all_parent_paths: {all_parent_paths:?}");

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

        log::debug!("new_node_hashes: {}", new_node_hashes.len());
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
    log::debug!("new_node_hashes: {}", new_node_hashes.len());
    // Add the commit hash itself
    new_node_hashes.insert(commit.hash()?);
    Ok(())
}

fn get_node_hashes_for_subtree(
    repository: &LocalRepository,
    commit: &Commit,
    subtree_path: &Option<PathBuf>,
    depth: &Option<i32>,
    base_hashes: &mut HashSet<MerkleHash>,
    new_node_hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    let mut unique_hashes = HashSet::new();

    let Ok(Some(_)) = CommitMerkleTreeLatest::read_depth_from_path_and_collect_hashes(
        repository,
        commit,
        subtree_path.clone().unwrap_or(PathBuf::from(".")),
        Some(base_hashes),
        Some(&mut unique_hashes),
        None,
        depth.unwrap_or(-1),
    ) else {
        // If the subtree is not found, then we don't need to add any nodes to the unique node hashes
        return Ok(());
    };

    base_hashes.extend(unique_hashes.clone());
    new_node_hashes.extend(unique_hashes);

    Ok(())
}

pub fn populate_starting_hashes(
    repository: &LocalRepository,
    commit: &Commit,
    subtree_path: &Option<PathBuf>,
    depth: &Option<i32>,
    unique_hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    let Ok(Some(_)) = CommitMerkleTreeLatest::read_depth_from_path_and_collect_hashes(
        repository,
        commit,
        subtree_path.clone().unwrap_or(PathBuf::from(".")),
        None,
        Some(unique_hashes),
        None,
        depth.unwrap_or(-1),
    ) else {
        // If the subtree is not found, then we don't need to add any nodes to the unique node hashes
        return Ok(());
    };

    Ok(())
}

// Collect the hashes of every ancestor of each path in subtree_paths
pub fn get_ancestor_nodes(
    repo: &LocalRepository,
    commit: &Commit,
    subtree_paths: &Vec<PathBuf>,
    unique_hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    for subtree_path in subtree_paths {
        let parent = subtree_path.parent();
        match parent {
            Some(parent) => {
                let ancestors = parent
                    .ancestors()
                    .map(|p| p.to_path_buf())
                    .collect::<Vec<PathBuf>>();
                for ancestor in ancestors.iter().rev() {
                    let Some(node) =
                        repositories::tree::get_node_by_path_with_children(repo, commit, ancestor)?
                    else {
                        return Err(OxenError::basic_str(format!(
                            "Ancestor {ancestor:?} for subtree path {subtree_path:?} not found in merkle tree"
                        )));
                    };

                    // Extend unique_hashes with the dir node and its vnode hashes
                    let ancestor_node_hashes = node.list_dir_and_vnode_hashes()?;
                    log::debug!(
                        "get_ancestor_nodes found {} hashes for subtree path {:?}",
                        ancestor_node_hashes.len(),
                        subtree_path
                    );
                    unique_hashes.extend(ancestor_node_hashes);
                }
            }
            None => {
                break; // subtree is root. no need to check other paths
            }
        }
    }

    Ok(())
}

// TODO: Deduplicate these
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
    let tree = get_subtree_by_depth(repo, commit, subtree, depth)?.unwrap();
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

/// The merkle node hashes and version-blob hashes a commit's tree introduces relative to a base.
#[derive(Debug, Default)]
pub struct AddedObjects {
    /// Merkle tree node hashes (dirs and vnodes) reachable from the added subtrees.
    pub nodes: HashSet<MerkleHash>,
    /// Content hashes of the version blobs the added files reference.
    pub versions: HashSet<String>,
}

/// The merkle nodes and version blobs that `head`'s tree introduces relative to `base`. Returns
/// `None` when `head`'s tree root isn't present at all.
///
/// The walk descends only into subtrees whose hashes differ between `base` and `head`
/// (content-addressed nodes with equal hashes are identical and skipped), so cost scales with the
/// changed set rather than the whole tree. `base = None` treats every object in `head` as added.
pub fn added_objects(
    repo: &LocalRepository,
    base: Option<&Commit>,
    head: &Commit,
) -> Result<Option<AddedObjects>, OxenError> {
    let Some(head_root) = root_dir_hash(repo, head)? else {
        return Ok(None);
    };
    let base_root = match base {
        Some(base) => root_dir_hash(repo, base)?,
        None => None,
    };

    let mut added = AddedObjects::default();
    collect_added_from_dir(
        repo,
        base_root,
        head_root,
        &mut added.nodes,
        &mut added.versions,
    )?;
    Ok(Some(added))
}

/// The merkle nodes and version blobs that a commit references but the repository is missing,
/// scoped to the objects the commit *adds* relative to a base commit.
#[derive(Debug, Default, Clone)]
pub struct MissingReachableObjects {
    /// Hex hashes of merkle tree nodes referenced by the added subtrees but absent on disk.
    pub nodes: Vec<String>,
    /// Content hashes of version blobs referenced by added files but absent from the store.
    pub versions: Vec<String>,
}

impl MissingReachableObjects {
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty() && self.versions.is_empty()
    }
}

/// Find the merkle nodes and version blobs that `head`'s tree references but the repository is
/// missing, limited to the objects `head` introduces relative to `base`.
///
/// Used to gate a branch-ref advance: a non-empty result means the ref would point at a commit
/// whose newly-added objects aren't all present. `base = None` checks `head`'s entire tree.
pub async fn find_missing_added_objects(
    repo: &LocalRepository,
    base: Option<&Commit>,
    head: &Commit,
) -> Result<MissingReachableObjects, OxenError> {
    let Some(added) = added_objects(repo, base, head)? else {
        // No root dir node for head means the commit's tree wasn't uploaded at all.
        return Ok(MissingReachableObjects {
            nodes: vec![head.hash()?.to_string()],
            versions: vec![],
        });
    };

    let store = repo.merkle_node_store();
    let mut missing_nodes = Vec::new();
    for hash in &added.nodes {
        if !store.exists(hash)? {
            missing_nodes.push(hash.to_string());
        }
    }

    let added_versions: Vec<String> = added.versions.into_iter().collect();
    let missing_versions = repo
        .version_store()
        .find_missing_versions(&added_versions)
        .await?;

    Ok(MissingReachableObjects {
        nodes: missing_nodes,
        versions: missing_versions,
    })
}

/// The hash of a commit's root directory node, or `None` if the commit node isn't present.
fn root_dir_hash(repo: &LocalRepository, commit: &Commit) -> Result<Option<MerkleHash>, OxenError> {
    let commit_hash = commit.hash()?;
    let children = MerkleTreeNode::read_children_from_hash(repo, &commit_hash)?;
    Ok(children
        .into_iter()
        .find(|(_, node)| matches!(node.node, EMerkleTreeNode::Directory(_)))
        .map(|(hash, _)| hash))
}

/// Recursively collect the node hashes and version hashes that `head_dir` adds relative to
/// `base_dir`. Identical subtrees (equal node hash) are pruned, so only changed directories are
/// flattened and compared.
fn collect_added_from_dir(
    repo: &LocalRepository,
    base_dir: Option<MerkleHash>,
    head_dir: MerkleHash,
    added_nodes: &mut HashSet<MerkleHash>,
    added_versions: &mut HashSet<String>,
) -> Result<(), OxenError> {
    added_nodes.insert(head_dir);

    let (head_entries, head_vnodes) = dir_entries(repo, &head_dir)?;
    added_nodes.extend(head_vnodes);

    let base_entries = match base_dir {
        Some(base_dir) => dir_entries(repo, &base_dir)?.0,
        None => HashMap::new(),
    };

    for (name, entry) in head_entries {
        if base_entries
            .get(&name)
            .is_some_and(|base_entry| base_entry.hash == entry.hash)
        {
            // Identical entry (same content-addressed hash) — already present from the base.
            continue;
        }

        match entry.kind {
            EntryKind::File { version } => {
                // A file node lives inline in its parent vnode's DB (which we just read), not as a
                // standalone node, so its presence is already covered. Verify its content blob.
                added_versions.insert(version);
            }
            EntryKind::Dir => {
                let base_sub = base_entries
                    .get(&name)
                    .filter(|base_entry| matches!(base_entry.kind, EntryKind::Dir))
                    .map(|base_entry| base_entry.hash);
                collect_added_from_dir(repo, base_sub, entry.hash, added_nodes, added_versions)?;
            }
        }
    }

    Ok(())
}

enum EntryKind {
    /// A file entry, carrying its content (version-store) hash.
    File {
        version: String,
    },
    Dir,
}

struct DirEntry {
    hash: MerkleHash,
    kind: EntryKind,
}

/// The immediate file/dir entries of a directory node (flattening its vnode layer), keyed by name,
/// plus the hashes of the vnodes traversed. Returns empty when the directory node is absent.
fn dir_entries(
    repo: &LocalRepository,
    dir_hash: &MerkleHash,
) -> Result<(HashMap<String, DirEntry>, Vec<MerkleHash>), OxenError> {
    let mut entries = HashMap::new();
    let mut vnode_hashes = Vec::new();

    for (child_hash, child) in MerkleTreeNode::read_children_from_hash(repo, dir_hash)? {
        match &child.node {
            EMerkleTreeNode::VNode(_) => {
                vnode_hashes.push(child_hash);
                for (entry_hash, entry) in
                    MerkleTreeNode::read_children_from_hash(repo, &child_hash)?
                {
                    insert_entry(&mut entries, entry_hash, &entry);
                }
            }
            // Some trees may attach files/dirs directly under a directory node.
            _ => insert_entry(&mut entries, child_hash, &child),
        }
    }

    Ok((entries, vnode_hashes))
}

fn insert_entry(entries: &mut HashMap<String, DirEntry>, hash: MerkleHash, node: &MerkleTreeNode) {
    match &node.node {
        EMerkleTreeNode::File(file_node) => {
            entries.insert(
                file_node.name().to_string(),
                DirEntry {
                    hash,
                    kind: EntryKind::File {
                        version: file_node.hash().to_string(),
                    },
                },
            );
        }
        EMerkleTreeNode::Directory(dir_node) => {
            entries.insert(
                dir_node.name().to_string(),
                DirEntry {
                    hash,
                    kind: EntryKind::Dir,
                },
            );
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::model::Commit;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use bytesize::ByteSize;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    // The incremental check must flag a blob the head commit *adds* when it's missing,
    // and must NOT re-check blobs inherited unchanged from the base (that's the whole point of
    // scoping to the changed set rather than walking the full tree).
    #[tokio::test]
    async fn test_find_missing_added_objects_scopes_to_the_added_set() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Base commit references a.txt; head adds b.txt and leaves a.txt unchanged.
            let a_path = repo.path.join("a.txt");
            test::write_txt_file_to_path(&a_path, "alpha")?;
            repositories::add(&repo, &a_path).await?;
            let base = repositories::commit(&repo, "add a.txt")?;

            let b_path = repo.path.join("b.txt");
            test::write_txt_file_to_path(&b_path, "beta")?;
            repositories::add(&repo, &b_path).await?;
            let head = repositories::commit(&repo, "add b.txt")?;

            // Read the version-store keys the way the check collects them (the file node hash).
            let a_hash = get_node_by_path(&repo, &head, "a.txt")?
                .expect("a.txt in head")
                .file()?
                .hash()
                .to_string();
            let b_hash = get_node_by_path(&repo, &head, "b.txt")?
                .expect("b.txt in head")
                .file()?
                .hash()
                .to_string();

            // Intact repo: nothing missing.
            let missing = find_missing_added_objects(&repo, Some(&base), &head).await?;
            assert!(
                missing.is_empty(),
                "intact repo reported missing: {missing:?}"
            );

            let version_store = repo.version_store();

            // Drop the inherited blob (a.txt is unchanged from base). The check is scoped to the
            // added set, so it must not flag it.
            version_store.delete_version(&a_hash).await?;
            let missing = find_missing_added_objects(&repo, Some(&base), &head).await?;
            assert!(
                !missing.versions.contains(&a_hash),
                "inherited blob must not be flagged by the incremental check: {missing:?}"
            );

            // Drop the added blob (b.txt is new in head). This is in the changed set, so it must
            // be flagged.
            version_store.delete_version(&b_hash).await?;
            let missing = find_missing_added_objects(&repo, Some(&base), &head).await?;
            assert!(
                missing.versions.contains(&b_hash),
                "added-but-missing blob must be flagged: {missing:?}"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_tabular_files_in_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
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
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            // List files
            let files = repositories::tree::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(files.len(), 3);

            // Add another tabular file
            let filename = "dogs.tsv";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "1\t2\t3\nhello\tworld\tsup\n")?;

            // Add and commit all
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Adding additional file")?;

            let files = repositories::tree::list_tabular_files_in_repo(&repo, &commit)?;

            assert_eq!(files.len(), 4);

            // Remove the deeply nested dir
            util::fs::remove_dir_all(&dir_path)?;

            let opts = RmOpts {
                path: dir_path,
                staged: false,
                recursive: true,
            };

            repositories::rm(&repo, &opts).await?;
            let commit = repositories::commit(&repo, "Removing dir")?;

            let files = repositories::tree::list_tabular_files_in_repo(&repo, &commit)?;
            assert_eq!(files.len(), 2);

            Ok(())
        })
        .await
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

            repositories::add(&local_repo, &path_1).await?;
            repositories::add(&local_repo, &path_2).await?;

            let status = repositories::status(&local_repo).await?;

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

    #[tokio::test]
    async fn test_get_node_hashes_between_commits() -> Result<(), OxenError> {
        test::run_local_repo_training_data_committed_async(|repo| async move {
            // Get the initial commit from training data to use as baseline
            let starting_commit = repositories::commits::head_commit(&repo)?;
            println!("Starting commit: {}", starting_commit.id);

            // Add some new files and make first new commit
            let new_file1 = repo.path.join("new_file1.txt");
            let new_file2 = repo.path.join("new_dir1").join("new_file2.txt");
            util::fs::create_dir_all(new_file2.parent().unwrap())?;

            util::fs::write(&new_file1, "This is new file 1 content")?;
            util::fs::write(&new_file2, "This is new file 2 content")?;

            repositories::add(&repo, &new_file1).await?;
            repositories::add(&repo, &new_file2).await?;
            let commit1 = repositories::commit(&repo, "Add first batch of new files")?;

            // Add more files and make second new commit
            let new_file3 = repo.path.join("new_dir2").join("new_file3.csv");
            util::fs::create_dir_all(new_file3.parent().unwrap())?;
            util::fs::write(&new_file3, "col1,col2,col3\nval1,val2,val3\n")?;

            repositories::add(&repo, &new_file3).await?;
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

            // TODO: This was an edge case I hacked out of the first time
            // Look elsewhere before fixing it
            assert!(
                depth_limited_hashes.len() == 6,
                "Depth filter should return 6 hashes, got {}",
                depth_limited_hashes.len()
            );

            Ok(())
        })
        .await
    }
    /// Gunzip + collect tar entries into a deterministic map for byte-compat comparison.
    /// Gzip-compressed bytes aren't stable (mtime field varies), but the tar entry set is.
    /// Results in a panic! if internal errors are encountered.
    fn list_tar_entries(buffer: &[u8]) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut out = BTreeMap::new();
        let decoder = GzDecoder::new(buffer);
        let mut archive = Archive::new(decoder);
        for entry in archive.entries().expect("tar entries failed") {
            let mut entry = entry.expect("tar entry failed");
            let path = entry.path().expect("tar path failed").into_owned();
            let mut bytes = Vec::new();
            std::io::Read::read_to_end(&mut entry, &mut bytes).expect("tar read failed");
            out.insert(path, bytes);
        }
        out
    }

    // --------------------------------------------------------------------------------------------
    //
    // [START] Reference Implementation
    //
    // Never called at runtime. Straightforward, non-streaming copies of the Merkle tree
    // read, write, and pack/unpack operations. The tests below pack and unpack through both
    // these copies and the production `pack_*`/`unpack` functions, asserting byte-identical
    // output so the wire format stays compatible across older and newer clients and servers.
    //
    // --------------------------------------------------------------------------------------------

    fn compress_tree(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);
        compress_full_tree(repository, &mut tar)?;
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);

        log::debug!("Compressed entire tree size is {}", ByteSize::b(total_size));

        Ok(buffer)
    }

    fn compress_full_tree(
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

        log::debug!("Compressing tree in dir {nodes_dir:?}");

        if nodes_dir.exists() {
            tar.append_dir_all(&tar_subdir, nodes_dir)?;
        }

        Ok(())
    }

    fn compress_nodes(
        repository: &LocalRepository,
        hashes: &HashSet<MerkleHash>,
    ) -> Result<Vec<u8>, OxenError> {
        // zip up the node directories for each commit tree
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);

        log::debug!("Compressing {} unique nodes...", hashes.len());
        for hash in hashes {
            // This will be the subdir within the tarball
            // so when we untar it, all the subdirs will be extracted to
            // tree/nodes/...
            let dir_prefix = hash.to_hex_hash().node_db_prefix();
            let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

            let node_dir = node_db_path(&repository.path, hash);
            // log::debug!("Compressing node from dir {:?}", node_dir);
            if node_dir.exists() {
                tar.append_dir_all(&tar_subdir, node_dir)?;
            }
        }
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        Ok(buffer)
    }

    fn compress_node(
        repository: &LocalRepository,
        hash: &MerkleHash,
    ) -> Result<Vec<u8>, OxenError> {
        // This will be the subdir within the tarball
        // so when we untar it, all the subdirs will be extracted to
        // tree/nodes/...
        let dir_prefix = hash.to_hex_hash().node_db_prefix();
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

        // zip up the node directory
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);
        let node_dir = node_db_path(&repository.path, hash);

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

    fn compress_commits(
        repository: &LocalRepository,
        commits: &Vec<Commit>,
    ) -> Result<Vec<u8>, OxenError> {
        // zip up the node directory
        let enc = GzEncoder::new(Vec::new(), Compression::fast());
        let mut tar = tar::Builder::new(enc);

        for commit in commits {
            let hash = commit.hash()?;
            // This will be the subdir within the tarball
            // so when we untar it, all the subdirs will be extracted to
            // tree/nodes/...
            let dir_prefix = hash.to_hex_hash().node_db_prefix();
            let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

            let node_dir = node_db_path(&repository.path, &hash);
            log::debug!("Compressing commit from dir {node_dir:?}");
            if node_dir.exists() {
                tar.append_dir_all(&tar_subdir, node_dir)?;
            }
        }
        tar.finish()?;

        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        Ok(buffer)
    }

    /// Synchronous mirror of `main`'s `api::client::tree::node_download_request` unpack
    /// step, sans HTTP/streaming. Allows for comparing bytes-on-disk in the old
    /// `unpack_async_tar_archive` install produced for a given input tarball.
    ///
    /// Wire identity with existing oxen behavior:
    ///   - decompress the gzip stream as-is;
    ///   - construct an `async_tar::Archive`;
    ///   - call `util::fs::unpack_async_tar_archive(archive, <oxen_hidden>)`.
    ///
    /// We bridge a sync `&[u8]` into the async iface via `futures::io::Cursor`. The
    /// install is identical to running the actual HTTP path against a server that hands
    /// back exactly `buffer` as the response body.
    async fn node_download_request_unpack_old(
        repository: &LocalRepository,
        buffer: &[u8],
    ) -> Result<(), OxenError> {
        let cursor = futures::io::Cursor::new(buffer.to_vec());
        let decoder = async_compression::futures::bufread::GzipDecoder::new(
            futures::io::BufReader::new(cursor),
        );
        let archive = async_tar::Archive::new(decoder);
        let dst = repository.path.join(OXEN_HIDDEN_DIR);
        util::fs::create_dir_all(&dst)?;
        util::fs::unpack_async_tar_archive(archive, &dst).await?;
        Ok(())
    }

    /// Copy of `api::client::tree::create_nodes`'s pack body, stripped of HTTP / progress
    /// concerns. Used as the byte-level source of truth so the tests can confirm that the
    /// `pack_*`/`unpack` functions keep older and newer clients & servers interoperable.
    fn create_nodes_pack_old(
        local_repo: &LocalRepository,
        nodes: &HashSet<MerkleHash>,
    ) -> Result<Vec<u8>, OxenError> {
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);
        let node_path = local_repo
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(TREE_DIR)
            .join(NODES_DIR);
        for node_hash in nodes {
            let dir_prefix = node_hash.to_hex_hash().node_db_prefix();
            let node_dir = node_path.join(&dir_prefix);
            tar.append_dir_all(dir_prefix, node_dir)?;
        }
        tar.finish()?;
        let buffer: Vec<u8> = tar.into_inner()?.finish()?;
        Ok(buffer)
    }

    fn unpack_nodes(
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
            if dst_path.exists() {
                log::debug!("Node already exists at {dst_path:?}");
                continue;
            }
            file.unpack(&dst_path)?;

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
                hashes.insert(id.parse()?);
            }
        }
        Ok(hashes)
    }

    // --------------------------------------------------------------------------------------------
    //
    // [END] Reference Implementation
    //
    // --------------------------------------------------------------------------------------------

    /// Pack every node in a repo, unpack into a fresh empty repo, and verify every
    /// installed hash is readable from the target store.
    #[tokio::test]
    async fn test_transport_round_trip() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let mut packed = Vec::new();
            pack_all(&repo, &mut packed).expect("pack_all failed");
            assert!(!packed.is_empty(), "pack_all produced empty buffer");

            let tmp = tempfile::TempDir::new()?;
            let clone = repositories::init(tmp.path())?;
            let installed =
                unpack(&clone, &mut &packed[..], UnpackOptions::Overwrite).expect("unpack failed");
            assert!(!installed.is_empty(), "unpack installed no nodes");

            for hash in &installed {
                assert!(
                    clone.merkle_node_store().exists(hash)?,
                    "expected installed hash {hash} to be readable"
                );
            }
            Ok(())
        })
        .await
    }

    /// The tar entry set produced by the `compress_nodes` helper must equal the one
    /// produced by `pack_nodes`. Gzip bytes differ on mtime, but the decompressed tar
    /// payload must be identical.
    #[tokio::test]
    async fn test_compress_nodes_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hashes = HashSet::from_iter([head.hash().expect("no commit for head")]);

            // prior code for packing Merkle nodes into a .tar.gz
            let old_pack_method =
                compress_nodes(&repo, &hashes).expect("could not compress Merkle tree nodes");

            let new_pack_method = {
                let mut via_pack = Vec::new();
                pack_nodes(&repo, &hashes, PackOptions::ServerCanonical, &mut via_pack)
                    .expect("pack_nodes failed");
                via_pack
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_nodes helper and pack_nodes"
            );
            Ok(())
        })
        .await
    }

    /// Same byte-compat check for the whole-tree path.
    #[tokio::test]
    async fn test_compress_tree_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            // prior code for packing an entire Merkle tree into a .tar.gz
            let old_pack_method = compress_tree(&repo)?;

            let new_pack_method = {
                let mut via_pack = Vec::new();
                pack_all(&repo, &mut via_pack).expect("pack_all failed");
                via_pack
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_tree helper and pack_all"
            );
            Ok(())
        })
        .await
    }

    /// Byte-compat for the single-hash pack path (`compress_node` vs
    /// `pack_nodes(&{hash})`).
    #[tokio::test]
    async fn test_compress_node_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hash = head.hash().expect("no commit for head");

            // prior code for packing a single Merkle node into a .tar.gz
            let old_pack_method =
                compress_node(&repo, &hash).expect("could not compress Merkle tree node");

            let new_pack_method = {
                let hashes = HashSet::from_iter([hash]);
                let mut via_pack = Vec::new();
                pack_nodes(&repo, &hashes, PackOptions::ServerCanonical, &mut via_pack)
                    .expect("pack_nodes failed");
                via_pack
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_node helper and pack_nodes"
            );
            Ok(())
        })
        .await
    }

    /// Byte-compat for the commit-set pack path (`compress_commits` vs
    /// `pack_nodes(&{commit hashes})`).
    #[tokio::test]
    async fn test_compress_commits_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let commits: Vec<Commit> = vec![head];

            // prior code for packing a set of commit hashes into a .tar.gz
            let old_pack_method =
                compress_commits(&repo, &commits).expect("could not compress Merkle tree commits");

            let new_pack_method = {
                let mut hashes = HashSet::with_capacity(commits.len());
                for c in &commits {
                    hashes.insert(c.hash().expect("no hash for commit"));
                }
                let mut via_pack = Vec::new();
                pack_nodes(&repo, &hashes, PackOptions::ServerCanonical, &mut via_pack)
                    .expect("pack_nodes failed");
                via_pack
            };

            assert_eq!(
                list_tar_entries(&old_pack_method),
                list_tar_entries(&new_pack_method),
                "tar entry set differs between compress_commits helper and pack_nodes"
            );
            Ok(())
        })
        .await
    }

    /// Build a tarball in the legacy client-push layout (entries begin at
    /// `{prefix}/{suffix}/...` with no `tree/nodes/` prefix) — this is the format
    /// the old `api::client::tree::create_nodes` emitted and the one
    /// `unpack_nodes` was designed to consume.
    fn compress_nodes_client_push_format(
        repo: &LocalRepository,
        hashes: &HashSet<MerkleHash>,
    ) -> Result<Vec<u8>, OxenError> {
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);
        let node_path = repo
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(TREE_DIR)
            .join(NODES_DIR);
        for hash in hashes {
            let dir_prefix = hash.to_hex_hash().node_db_prefix();
            let node_dir = node_path.join(&dir_prefix);
            if node_dir.exists() {
                tar.append_dir_all(dir_prefix, node_dir)?;
            }
        }
        tar.finish()?;
        Ok(tar.into_inner()?.finish()?)
    }

    /// Behavioral parity between `unpack_nodes` and `unpack` on a legacy
    /// client-push-format tarball: same reported hash set, same readability through the
    /// store in both target repos.
    #[tokio::test]
    async fn test_unpack_nodes_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hashes = HashSet::from_iter([head.hash().expect("no commit for head")]);

            // Produce a legacy client-push-format tarball (the one old `unpack_nodes`
            // was designed to consume).
            let bytes = compress_nodes_client_push_format(&repo, &hashes)
                .expect("client-push-format pack failed");

            // Unpack into two fresh repos: one via `unpack_nodes`, one via `unpack`.
            let tmp_old = tempfile::TempDir::new()?;
            let repo_old = repositories::init(tmp_old.path())?;
            let old_hashes = unpack_nodes(&repo_old, &bytes).expect("old unpack_nodes failed");

            let tmp_new = tempfile::TempDir::new()?;
            let repo_new = repositories::init(tmp_new.path())?;
            // Old `unpack_nodes` skipped existing files; mirror that with
            // `UnpackOptions::SkipExisting` so the parity check is semantically faithful.
            let new_hashes = unpack(&repo_new, &mut &bytes[..], UnpackOptions::SkipExisting)
                .expect("new unpack failed");

            assert_eq!(
                old_hashes, new_hashes,
                "reported hash sets differ between unpack_nodes helper and unpack"
            );
            assert!(
                !new_hashes.is_empty(),
                "no hashes were unpacked — test input was empty"
            );

            // Every installed hash must be readable through both stores.
            for h in &new_hashes {
                assert!(
                    repo_old.merkle_node_store().exists(h)?,
                    "hash {h} not readable in repo unpacked via legacy unpack_nodes"
                );
                assert!(
                    repo_new.merkle_node_store().exists(h)?,
                    "hash {h} not readable in repo unpacked via unpack"
                );
            }
            Ok(())
        })
        .await
    }

    /// Byte-for-byte compare two directory trees. Used to assert that the old and new
    /// unpack code paths produce identical on-disk state.
    fn collect_dir_contents(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        let mut out: BTreeMap<PathBuf, Vec<u8>> = BTreeMap::new();
        if !root.exists() {
            return out;
        }
        for entry in walkdir::WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_map(Result::ok)
        {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let rel = path.strip_prefix(root).unwrap_or(path).to_path_buf();
            let bytes = std::fs::read(path).expect("failed to read file under root");
            out.insert(rel, bytes);
        }
        out
    }

    /// Behavioural parity for the download-side unpack path. Pack a server-canonical
    /// tarball (the kind a server emits via `compress_tree` on `main` / `pack_all`
    /// today), feed the **same** bytes to:
    ///   - the old client unpack: `node_download_request_unpack_old` (the verbatim
    ///     `unpack_async_tar_archive` install from `main`'s `node_download_request`),
    ///   - the new client unpack: the free `unpack(...)` (overwrite-existing
    ///     default, matching `unpack_async_tar_archive`'s behaviour).
    /// The on-disk merkle-node tree under `<oxen_hidden>/tree/nodes/` must be identical
    /// in both target repos. The set of hashes `unpack` reports is also asserted to
    /// cover every installed hash directory.
    #[tokio::test]
    async fn test_node_download_request_unpack_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let mut packed = Vec::new();
            pack_all(&repo, &mut packed).expect("pack_all failed");
            assert!(!packed.is_empty(), "pack_all produced empty buffer");

            // Old client install path (mirror of node_download_request on main).
            let tmp_old = tempfile::TempDir::new()?;
            let repo_old = repositories::init(tmp_old.path())?;
            node_download_request_unpack_old(&repo_old, &packed)
                .await
                .expect("old unpack failed");

            // New client install path: `unpack`, with download-path overwrite semantics.
            let tmp_new = tempfile::TempDir::new()?;
            let repo_new = repositories::init(tmp_new.path())?;
            let installed = unpack(&repo_new, &mut &packed[..], UnpackOptions::Overwrite)
                .expect("new unpack failed");

            // 1. The on-disk node trees must be identical.
            let old_tree = collect_dir_contents(
                &repo_old
                    .path
                    .join(OXEN_HIDDEN_DIR)
                    .join(TREE_DIR)
                    .join(NODES_DIR),
            );
            let new_tree = collect_dir_contents(
                &repo_new
                    .path
                    .join(OXEN_HIDDEN_DIR)
                    .join(TREE_DIR)
                    .join(NODES_DIR),
            );
            assert!(
                !old_tree.is_empty(),
                "no files installed via old node_download_request unpack — test input was empty"
            );
            assert_eq!(
                old_tree, new_tree,
                "on-disk merkle node trees differ between old node_download_request \
                 unpack and new unpack"
            );

            // 2. Every installed hash must be readable through the new store.
            assert!(!installed.is_empty(), "unpack reported no hashes");
            for h in &installed {
                assert!(
                    repo_new.merkle_node_store().exists(h)?,
                    "hash {h} not readable in repo unpacked via unpack"
                );
            }
            Ok(())
        })
        .await
    }

    /// Path-traversal guard: an entry whose path resolves above its container must be
    /// rejected (matches `main`'s `unpack_async_tar_archive`).
    ///
    /// `tar::Header::set_path` validates and rejects `..` at write time, so we bypass
    /// that by writing directly into the old-style `name` field on the header — which
    /// is exactly the kind of malicious tarball a hostile server could construct.
    #[tokio::test]
    async fn test_unpack_rejects_path_traversal() -> Result<(), OxenError> {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_old();
            header.set_size(0);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            // Bypass `set_path`'s `..` rejection by writing the raw name bytes.
            let name_bytes = b"tree/nodes/../escape";
            let old = header.as_old_mut();
            old.name[..name_bytes.len()].copy_from_slice(name_bytes);
            header.set_cksum();
            tar.append(&header, std::io::Cursor::new(Vec::new()))?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = unpack(&repo, &mut &buf[..], UnpackOptions::Overwrite)
            .expect_err("path traversal must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Path traversal"),
            "unexpected error message: {msg}"
        );
        Ok(())
    }

    /// Entry-type guard: anything that isn't a regular file or directory must be
    /// rejected (matches `main`'s `unpack_async_tar_archive`).
    #[tokio::test]
    async fn test_unpack_rejects_unsupported_entry_type() -> Result<(), OxenError> {
        // Build a tarball whose single entry is a symlink (entry type "Symlink").
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o777);
            header.set_entry_type(tar::EntryType::Symlink);
            // tar requires the link target to be set on a Symlink entry header.
            header.set_link_name("/etc/passwd")?;
            header.set_cksum();
            tar.append_data(
                &mut header,
                "tree/nodes/some_link",
                std::io::Cursor::new(Vec::new()),
            )?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = unpack(&repo, &mut &buf[..], UnpackOptions::Overwrite)
            .expect_err("unsupported entry type must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Unsupported tar entry"),
            "unexpected error message: {msg}"
        );
        Ok(())
    }

    /// Regression: a hash whose hex form has stripped leading zeros (so its
    /// `node_db_prefix()` produces a `{prefix}/{suffix}` shorter than 32 chars
    /// total) must still round-trip through pack → unpack and appear in the
    /// returned hash set.
    ///
    /// Before the fix to `extract_hash_from_entry_path`, the unpack side had a
    /// silent `id.len() == 32` gate that dropped these entries.
    #[tokio::test]
    async fn test_unpack_recovers_hash_with_leading_zero_nibbles() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Pick a small `u128` whose hex form is much shorter than 32 chars.
            // `MerkleHash`'s `Display` is `{:x}` (no zero padding) so this is
            // exactly the shape that triggered the bug.
            let stripped_hash = MerkleHash::new(0x1234_u128);
            let hex = stripped_hash.to_string();
            assert!(
                hex.len() < 32,
                "expected hex form < 32 chars to exercise the regression, got {hex:?}"
            );

            // Manually plant a `{prefix}/{suffix}/node` and `.../children`
            // pair on disk so `pack_nodes` will tar them up.
            let prefix = stripped_hash.to_hex_hash().node_db_prefix();
            let nodes_root = repo
                .path
                .join(crate::constants::OXEN_HIDDEN_DIR)
                .join(crate::constants::TREE_DIR)
                .join(crate::constants::NODES_DIR);
            let node_dir = nodes_root.join(prefix);
            std::fs::create_dir_all(&node_dir)?;
            std::fs::write(node_dir.join("node"), b"node-bytes")?;
            std::fs::write(node_dir.join("children"), b"children-bytes")?;

            // Pack just this hash.
            let hashes = HashSet::from_iter([stripped_hash]);
            let mut buf = Vec::new();
            pack_nodes(&repo, &hashes, PackOptions::ServerCanonical, &mut buf)
                .expect("pack_nodes failed");

            // Unpack into a fresh repo and confirm the short hash made it out.
            let tmp = tempfile::TempDir::new()?;
            let target = repositories::init(tmp.path())?;
            let installed =
                unpack(&target, &mut &buf[..], UnpackOptions::Overwrite).expect("unpack failed");

            assert!(
                installed.contains(&stripped_hash),
                "unpack must report the short-hex hash; got {installed:?}"
            );
            Ok(())
        })
    }

    /// A `{prefix}/{suffix}` dir entry whose name isn't valid hex must produce
    /// the structured `InvalidNodeIdHex` error, not a silent skip and not a
    /// generic `ParseIntError`.
    #[tokio::test]
    async fn test_unpack_rejects_non_hex_node_id() -> Result<(), OxenError> {
        // Build a server-canonical-format tarball with a bogus dir name where
        // the suffix contains non-hex characters.
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            // Three levels of dir entries: tree/nodes/abc/zzzznothex.
            for path in &["tree/nodes", "tree/nodes/abc", "tree/nodes/abc/zzzznothex"] {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(tar::EntryType::Directory);
                header.set_cksum();
                tar.append_data(&mut header, path, std::io::Cursor::new(Vec::new()))?;
            }
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = unpack(&repo, &mut &buf[..], UnpackOptions::Overwrite)
            .expect_err("non-hex node id must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Invalid merkle node id") && msg.contains("abczzzznothex"),
            "expected InvalidNodeIdHex error mentioning the offending id; got {msg}"
        );
        Ok(())
    }

    /// A tar entry whose path goes deeper than `tree/nodes/{prefix}/{suffix}/{node|children}`
    /// must produce `InvalidTarStructure`.
    #[tokio::test]
    async fn test_unpack_rejects_path_too_deep() -> Result<(), OxenError> {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            // Five levels under tree/nodes — too deep.
            for path in &[
                "tree/nodes",
                "tree/nodes/abc",
                "tree/nodes/abc/def0123",
                "tree/nodes/abc/def0123/extra",
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(tar::EntryType::Directory);
                header.set_cksum();
                tar.append_data(&mut header, path, std::io::Cursor::new(Vec::new()))?;
            }
            // Append a file at the over-deep level.
            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(0);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.set_cksum();
            tar.append_data(
                &mut file_header,
                "tree/nodes/abc/def0123/extra/node",
                std::io::Cursor::new(Vec::new()),
            )?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = unpack(&repo, &mut &buf[..], UnpackOptions::Overwrite)
            .expect_err("over-deep entry must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Invalid merkle tar archive structure"),
            "expected InvalidTarStructure error; got {msg}"
        );
        Ok(())
    }

    /// A leaf file under `{prefix}/{suffix}/` whose name is neither `node`
    /// nor `children` must be rejected with `InvalidTarStructure`.
    #[tokio::test]
    async fn test_unpack_rejects_unknown_leaf_filename() -> Result<(), OxenError> {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            for path in &["tree/nodes", "tree/nodes/abc", "tree/nodes/abc/def0123"] {
                let mut header = tar::Header::new_gnu();
                header.set_size(0);
                header.set_mode(0o755);
                header.set_entry_type(tar::EntryType::Directory);
                header.set_cksum();
                tar.append_data(&mut header, path, std::io::Cursor::new(Vec::new()))?;
            }
            // Bogus leaf filename `unexpected.txt` instead of `node`/`children`.
            let mut file_header = tar::Header::new_gnu();
            file_header.set_size(0);
            file_header.set_mode(0o644);
            file_header.set_entry_type(tar::EntryType::Regular);
            file_header.set_cksum();
            tar.append_data(
                &mut file_header,
                "tree/nodes/abc/def0123/unexpected.txt",
                std::io::Cursor::new(Vec::new()),
            )?;
            tar.finish()?;
            tar.into_inner()?.finish()?;
        }

        let tmp = tempfile::TempDir::new()?;
        let repo = repositories::init(tmp.path())?;
        let err = unpack(&repo, &mut &buf[..], UnpackOptions::Overwrite)
            .expect_err("unknown leaf filename must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("Invalid merkle tar archive structure") && msg.contains("unexpected.txt"),
            "expected InvalidTarStructure error mentioning the offending filename; got {msg}"
        );
        Ok(())
    }

    /// Cross-platform sanity check for `extract_hash_from_entry_path`. We
    /// bypass tar entirely and feed the helper paths constructed with
    /// explicit forward-slash literals — the same shape that
    /// `tar::Entry::path()` produces on every platform (tar archives are
    /// canonically `/`-separated, and the `tar` crate's `bytes2path` impl
    /// just feeds those bytes into `Path::new`).
    ///
    /// On Windows, `Path::components()` and `Path::strip_prefix` both treat
    /// `/` and `\` as separators, so the helper's match on segment-name
    /// strings dispatches the same way regardless of host. This test pins
    /// that down without needing CI on every OS.
    #[test]
    fn test_extract_hash_from_entry_path_is_path_separator_agnostic() {
        // Concrete hash whose hex form is < 32 chars (exercising the
        // leading-zero-nibble fix at the same time).
        let hash = MerkleHash::new(0xfeed_face_u128);
        let hex = hash.to_string();
        let prefix: String = hex.chars().take(3).collect();
        let suffix: String = hex.chars().skip(3).collect();

        let oxen_hidden = PathBuf::from("repo").join(".oxen");

        // Helper to build a tar-style path under `tree/nodes/...`.
        let under_tree_nodes = |tail: &[&str]| -> PathBuf {
            let mut s = PathBuf::from("repo")
                .join(".oxen")
                .join("tree")
                .join("nodes");
            for t in tail {
                if !t.is_empty() {
                    s.push(t);
                }
            }
            s
        };

        // tree/nodes itself — intermediate, no hash.
        let p = under_tree_nodes(&[""]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // tree/nodes/{prefix} — intermediate prefix dir, no hash.
        let p = under_tree_nodes(&[prefix.as_str()]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // tree/nodes/{prefix}/{suffix} — hash-bearing dir.
        let p = under_tree_nodes(&[prefix.as_str(), suffix.as_str()]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            Some(hash),
            "{prefix}/{suffix} must classify as the hash dir on every platform"
        );

        // tree/nodes/{prefix}/{suffix}/node — leaf file, no hash from this entry.
        let p = under_tree_nodes(&[prefix.as_str(), suffix.as_str(), "node"]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // tree/nodes/{prefix}/{suffix}/children — leaf file, no hash from this entry.
        let p = under_tree_nodes(&[prefix.as_str(), suffix.as_str(), "children"]);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden)
                .expect("Could not extract hash from entry path"),
            None
        );

        // Mixed-separator construction: `oxen_hidden` via platform-native
        // `Path::join` (uses `\` on Windows, `/` on Unix), but the entry
        // path itself uses forward slashes from a tar archive. `strip_prefix`
        // is documented to be component-aware, so this must still match on
        // every platform.
        let p = oxen_hidden
            .join("tree")
            .join("nodes")
            .join(&prefix)
            .join(&suffix);
        assert_eq!(
            extract_hash_from_entry_path(&p, &oxen_hidden).unwrap(),
            Some(hash),
            "platform-native joins must classify the same as forward-slash literals"
        );
    }

    /// Byte-level parity for the upload-pack path: the `create_nodes` body copy vs
    /// `pack_nodes(&hashes, PackOptions::LegacyClientPush, ...)`. The two tar entry sets
    /// must match exactly — that's the proof of zero externally visible change for the
    /// upload wire format.
    #[tokio::test]
    async fn test_create_nodes_wire_format_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let hashes = HashSet::from_iter([head.hash().expect("no commit for head")]);

            let old_pack =
                create_nodes_pack_old(&repo, &hashes).expect("legacy create_nodes pack failed");

            let new_pack = {
                let mut buf = Vec::new();
                pack_nodes(&repo, &hashes, PackOptions::LegacyClientPush, &mut buf)
                    .expect("new pack failed");
                buf
            };

            assert_eq!(
                list_tar_entries(&old_pack),
                list_tar_entries(&new_pack),
                "tar entry set differs between create_nodes pack body and new upload pack"
            );
            Ok(())
        })
        .await
    }

    // -- Trait edge cases (Finding 5) ------------------------------------------------

    /// `unpack` on an empty tar-gz stream (the result of `pack_nodes` with an empty
    /// hash set) returns `Ok(empty hash set)`.
    #[tokio::test]
    async fn test_unpack_empty_tarball() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let mut buf = Vec::new();
            pack_nodes(
                &repo,
                &HashSet::new(),
                PackOptions::ServerCanonical,
                &mut buf,
            )
            .expect("pack_nodes(empty) must not error");

            let tmp = tempfile::TempDir::new()?;
            let target = repositories::init(tmp.path())?;
            let installed = unpack(&target, &mut &buf[..], UnpackOptions::Overwrite)
                .expect("unpack of empty tarball must not error");
            assert!(
                installed.is_empty(),
                "expected empty hash set from unpacking an empty tarball, got {} entries",
                installed.len()
            );
            Ok(())
        })
        .await
    }

    /// `pack_nodes` with a mix of valid and absent hashes silently skips the absent
    /// ones (matches `compress_nodes` semantics from `main`). The output tarball must
    /// contain entries only for the valid hash's prefix.
    #[tokio::test]
    async fn test_pack_nodes_skips_absent_hashes() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo).expect("no head commit");
            let head_hash = head.hash().expect("no commit for head");
            let absent = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);

            let mut hashes = HashSet::with_capacity(2);
            hashes.insert(head_hash);
            hashes.insert(absent);

            let mut buf = Vec::new();
            pack_nodes(&repo, &hashes, PackOptions::ServerCanonical, &mut buf)
                .expect("pack_nodes failed");

            // tar entry paths always use `/` separators, but `node_db_prefix()`
            // returns a `PathBuf` that renders with `\` on Windows. Normalize
            // both sides to forward slashes before substring-matching.
            let to_fwd = |p: &Path| p.to_string_lossy().replace('\\', "/");
            let head_prefix = to_fwd(&head_hash.to_hex_hash().node_db_prefix());
            let absent_prefix = to_fwd(&absent.to_hex_hash().node_db_prefix());
            let entries = list_tar_entries(&buf);
            let any_head = entries.keys().any(|p| to_fwd(p).contains(&head_prefix));
            let any_absent = entries.keys().any(|p| to_fwd(p).contains(&absent_prefix));
            assert!(any_head, "expected entries for the head hash prefix");
            assert!(
                !any_absent,
                "expected no entries for the absent hash prefix; got at least one matching entry"
            );
            Ok(())
        })
        .await
    }

    /// `pack_nodes_byte_estimate` should be a tight upper bound on the actual
    /// uncompressed tar payload size for a given hash set. Verify that:
    /// 1. for a present hash, the estimate is non-zero;
    /// 2. the estimate is >= the actual gzipped output (i.e., usable as a progress total
    ///    that the upload won't blow past).
    /// 3. for an absent hash, the estimate contributes 0 (matches `pack_nodes`'s skip).
    #[tokio::test]
    async fn test_pack_nodes_byte_estimate_is_upper_bound() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let head = repositories::commits::head_commit(&repo)?;
            let head_hash = head.hash().expect("no commit for head");
            let mut hashes = HashSet::new();
            hashes.insert(head_hash);

            let estimate = pack_nodes_byte_estimate(&repo, &hashes);
            assert!(estimate > 0, "estimate must be non-zero for a present hash");

            let mut buf = Vec::new();
            pack_nodes(&repo, &hashes, PackOptions::ServerCanonical, &mut buf)
                .expect("pack_nodes failed");
            assert!(
                estimate >= buf.len() as u64,
                "estimate ({estimate}) should be an upper bound on actual gzipped output \
                 ({} bytes)",
                buf.len()
            );

            // Absent hash: should contribute 0 to the estimate.
            let absent = MerkleHash::new(0xDEAD_BEEF_DEAD_BEEF_DEAD_BEEF_DEAD_BEEF_u128);
            let absent_only: HashSet<_> = HashSet::from_iter([absent]);
            assert_eq!(
                pack_nodes_byte_estimate(&repo, &absent_only),
                0,
                "absent hash should contribute 0 to the estimate"
            );

            // Mixed: head + absent should equal head-only.
            let mut mixed = HashSet::new();
            mixed.insert(head_hash);
            mixed.insert(absent);
            assert_eq!(
                pack_nodes_byte_estimate(&repo, &mixed),
                pack_nodes_byte_estimate(&repo, &hashes),
                "absent hash must not change the estimate when added to a present hash"
            );
            Ok(())
        })
        .await
    }

    /// VFS branch in `unpack`: when `is_vfs()` is true, unpack stages into a tempdir and
    /// copies through `copy_dir_all`. Pack the source repo, flip the target to vfs=true,
    /// unpack, and assert hashes are readable.
    #[tokio::test]
    async fn test_unpack_via_vfs_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let mut packed = Vec::new();
            pack_all(&repo, &mut packed).expect("pack_all failed");
            assert!(!packed.is_empty(), "pack_all produced empty buffer");

            let tmp = tempfile::TempDir::new()?;
            let mut clone = repositories::init(tmp.path())?;
            clone.set_vfs(Some(true));
            assert!(clone.is_vfs(), "vfs flag should be on for this test");

            let installed = unpack(&clone, &mut &packed[..], UnpackOptions::Overwrite)
                .expect("unpack via vfs branch failed");
            assert!(
                !installed.is_empty(),
                "vfs unpack reported no installed hashes"
            );
            for h in &installed {
                assert!(
                    clone.merkle_node_store().exists(h)?,
                    "hash {h} not readable in vfs-cloned repo"
                );
            }
            Ok(())
        })
        .await
    }
}
