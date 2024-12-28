use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::core::v0_19_0::index::merkle_node_db::node_db_path;
use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{
    DirNodeWithPath, EMerkleTreeNode, FileNode, FileNodeWithDir, MerkleTreeNode,
};
use crate::model::{Commit, EntryDataType, LocalRepository, MerkleHash};
use crate::{repositories, util};

pub fn get_by_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<CommitMerkleTree, OxenError> {
    CommitMerkleTree::from_commit(repo, commit)
}

pub fn get_node_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = false;
    CommitMerkleTree::read_node(repo, hash, load_recursive)
}

pub fn get_node_by_id_recursive(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = true;
    CommitMerkleTree::read_node(repo, hash, load_recursive)
}

pub fn get_node_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let load_recursive = false;
    let node = CommitMerkleTree::from_path(repo, commit, path, load_recursive)?;
    Ok(Some(node.root))
}

pub fn get_file_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    let load_recursive = false;
    let tree = CommitMerkleTree::from_path(repo, commit, path, load_recursive)?;
    match tree.root.node {
        EMerkleTreeNode::File(file_node) => Ok(Some(file_node.clone())),
        _ => Ok(None),
    }
}

pub fn get_dir_with_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    CommitMerkleTree::dir_with_children(repo, commit, path)
}

pub fn get_dir_without_children(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    CommitMerkleTree::dir_without_children(repo, commit, path)
}

pub fn get_dir_with_children_recursive(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    CommitMerkleTree::dir_with_children_recursive(repo, commit, path)
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
) -> Result<CommitMerkleTree, OxenError> {
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
        _ => {
            log::debug!("Getting full tree for commit {}", commit);
            get_by_commit(repo, commit)
        }
    }
}

pub fn get_subtree(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    depth: i32,
) -> Result<CommitMerkleTree, OxenError> {
    CommitMerkleTree::from_path_depth(repo, commit, path, depth)
}

pub fn get_entries(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Vec<FileNode>, OxenError> {
    if let Some(dir_node) = CommitMerkleTree::dir_with_children(repo, commit, &path)? {
        log::debug!("get_entries found dir node: {dir_node:?}");
        CommitMerkleTree::dir_entries(&dir_node)
    } else {
        Err(OxenError::basic_str(format!(
            "Error: path not found in tree: {:?}",
            path.as_ref()
        )))
    }
}

pub fn read_nodes_along_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: Vec<PathBuf>,
    hashes: &mut HashSet<MerkleHash>,
) -> Result<(), OxenError> {
    let root_path = path.first().unwrap();
    let tree = CommitMerkleTree::from_path(repo, commit, root_path, true)?;

    let (_root_node, nodes) = tree.root.get_nodes_along_path(path)?;
    for node in nodes {
        hashes.insert(node.hash);
    }
    Ok(())
}

pub fn get_node_data_by_id(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Option<MerkleTreeNode>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Ok(None);
    };
    Ok(Some(node))
}

pub fn list_missing_file_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let Some(node) = CommitMerkleTree::read_depth(repo, hash, 1)? else {
        return Err(OxenError::basic_str(format!("Node {} not found", hash)));
    };
    node.list_missing_file_hashes(repo)
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
                let tree =
                    repositories::tree::get_subtree_by_depth(repo, &commit, &Some(path), depth)?;
                tree.walk_tree(|node| {
                    if node.is_file() {
                        candidate_hashes.insert(node.hash);
                    }
                });
            }
        } else {
            let tree = CommitMerkleTree::from_commit(repo, &commit)?;
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

// Given a set of hashes, return the hashes that are missing from the tree
pub fn list_missing_node_hashes(
    repo: &LocalRepository,
    hashes: &HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let mut results = HashSet::new();
    for hash in hashes {
        let dir_prefix = node_db_path(repo, hash);
        if !(dir_prefix.join("node").exists() && dir_prefix.join("children").exists()) {
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
    for hash in hashes {
        let version_path = util::fs::version_path_from_hash(repo, hash.to_string());
        if !version_path.exists() {
            results.insert(*hash);
        }
    }
    Ok(results)
}

pub fn child_hashes(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<Vec<MerkleHash>, OxenError> {
    let Some(node) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Err(OxenError::basic_str(format!("Node {} not found", hash)));
    };
    let mut children = vec![];
    for child in node.children {
        children.push(child.hash);
    }
    Ok(children)
}

pub fn list_all_files(tree: &CommitMerkleTree) -> Result<HashSet<FileNodeWithDir>, OxenError> {
    let mut file_nodes = HashSet::new();
    r_list_all_files(&tree.root, PathBuf::from(""), &mut file_nodes)?;
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
                let new_path = traversed_path.join(&dir_node.name);
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
pub fn list_all_dirs(tree: &CommitMerkleTree) -> Result<HashSet<DirNodeWithPath>, OxenError> {
    let mut dir_nodes = HashSet::new();
    r_list_all_dirs(&tree.root, PathBuf::from(""), &mut dir_nodes)?;
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
                let new_path = traversed_path.join(&dir_node.name);
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
    tree: &CommitMerkleTree,
) -> Result<(HashSet<FileNodeWithDir>, HashSet<DirNodeWithPath>), OxenError> {
    let mut file_nodes = HashSet::new();
    let mut dir_nodes = HashSet::new();
    r_list_files_and_dirs(&tree.root, PathBuf::new(), &mut file_nodes, &mut dir_nodes)?;
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
                let new_path = traversed_path.join(&dir_node.name);
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
    let tree = CommitMerkleTree::from_commit(repo, commit)?;
    r_list_files_by_type(&tree.root, data_type, &mut file_nodes, PathBuf::new())?;
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
                if file_node.data_type == *data_type {
                    let mut file_node = file_node.to_owned();
                    let full_path = traversed_path.join(&file_node.name);
                    file_node.name = full_path.to_string_lossy().to_string();
                    file_nodes.insert(file_node);
                }
            }
            EMerkleTreeNode::Directory(dir_node) => {
                let full_path = traversed_path.join(&dir_node.name);
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

            let tree = repositories::tree::get_by_commit(&local_repo, &commit)?;

            assert!(tree.has_path(PathBuf::from(p1))?);
            assert!(tree.has_path(PathBuf::from(p2))?);

            Ok(())
        })
        .await
    }
}
