use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};

use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::db::merkle_node::MerkleNodeDB;

use crate::model::merkle_tree::node::EMerkleTreeNode;

use crate::model::merkle_tree::node::FileNode;
use crate::model::merkle_tree::node::MerkleTreeNode;

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MerkleHash, MerkleTreeNodeType, PartialNode};

use crate::util::{self, hasher};

use std::str::FromStr;

pub struct CommitMerkleTree {
    pub root: MerkleTreeNode,
    pub dir_hashes: HashMap<PathBuf, MerkleHash>,
}

impl CommitMerkleTree {
    // Commit db is the directories per commit
    // This helps us skip to a directory in the tree
    // .oxen/history/{COMMIT_ID}/dir_hashes
    fn dir_hash_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(&commit.id)
            .join(DIR_HASHES_DIR)
    }

    pub fn dir_hash_db_path_from_commit_id(
        repo: &LocalRepository,
        commit_id: MerkleHash,
    ) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(Path::new(HISTORY_DIR))
            .join(commit_id.to_string())
            .join(DIR_HASHES_DIR)
    }

    // MODULARIZE: We don't need to use this directly, go through repositories::tree

    pub fn root_with_children(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        CommitMerkleTree::read_node(repo, &node_hash, true)
    }

    // MODULARIZE: We don't need to use this directly, go through repositories::tree

    pub fn root_without_children(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        // Read the root node at depth 1 to get the directory node as well
        CommitMerkleTree::read_depth(repo, &node_hash, 1)
    }

    // MODULARIZE: This needs an access method in repositories::tree

    // Used in the checkout logic to simultaneously load in the target tree and find all of its dir and vnode hashes
    // Saves an extra tree traversal needed to list these hashes
    pub fn root_with_children_and_hashes(
        repo: &LocalRepository,
        commit: &Commit,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        CommitMerkleTree::read_node_with_hashes(repo, &node_hash, hashes)
    }

    // MODULARIZE: This needs an access method in repositories::tree

    // Used in the checkout logic to simultaneosuly:
    // A: load only the children of the from tree which aren't present in the target tree
    // B: list the shared dir and vnode hashes between the trees
    // C: find all of its unique nodes, reduced to 'PartialNodes' to save memory

    // Saves 2 extra tree traversals needed to list the shared hashes and the unique nodes
    pub fn root_with_unique_children(
        repo: &LocalRepository,
        commit: &Commit,
        base_hashes: &mut HashSet<MerkleHash>,
        shared_hashes: &mut HashSet<MerkleHash>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        CommitMerkleTree::read_unique_nodes(
            repo,
            &node_hash,
            base_hashes,
            shared_hashes,
            partial_nodes,
        )
    }

    pub fn get_unique_children_for_commit(
        repo: &LocalRepository,
        commit: &Commit,
        shared_hashes: &mut HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = MerkleHash::from_str(&commit.id)?;
        CommitMerkleTree::read_unique_children_for_commit(
            repo,
            &node_hash,
            shared_hashes,
            unique_hashes,
        )
    }

    pub fn from_commit(repo: &LocalRepository, commit: &Commit) -> Result<Self, OxenError> {
        // This debug log is to help make sure we don't load the tree too many times
        // if you see it in the logs being called too much, it could be why the code is slow.
        log::debug!("Load tree from commit: {} in repo: {:?}", commit, repo.path);
        let node_hash = MerkleHash::from_str(&commit.id)?;
        let root =
            CommitMerkleTree::read_node(repo, &node_hash, true)?.ok_or(OxenError::basic_str(
                format!("Merkle tree hash not found for commit: '{}'", commit.id),
            ))?;
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        Ok(Self { root, dir_hashes })
    }

    pub fn from_path_recursive(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Self, OxenError> {
        let load_recursive = true;
        CommitMerkleTree::from_path(repo, commit, path, load_recursive)
    }

    pub fn from_path_depth(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let mut node_path = path.as_ref().to_path_buf();
        if node_path == PathBuf::from(".") {
            node_path = PathBuf::from("");
        }
        log::debug!(
            "Read path {:?} in commit {:?} depth: {}",
            node_path,
            commit,
            depth
        );
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let Some(node_hash) = dir_hashes.get(&node_path).cloned() else {
            log::debug!(
                "dir_hashes {:?} does not contain path: {:?}",
                dir_hashes,
                node_path
            );
            return Err(OxenError::basic_str(format!(
                "Can only load a subtree with an existing directory path: '{}'",
                node_path.to_str().unwrap()
            )));
        };

        let Some(root) = CommitMerkleTree::read_depth(repo, &node_hash, depth)? else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for: '{}' hash: {:?}",
                node_path.to_str().unwrap(),
                node_hash
            )));
        };
        Ok(Some(root))
    }

    pub fn from_path_depth_children_and_hashes(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        depth: i32,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let mut node_path = path.as_ref().to_path_buf();
        if node_path == PathBuf::from(".") {
            node_path = PathBuf::from("");
        }
        log::debug!(
            "Read path {:?} in commit {:?} depth: {}",
            node_path,
            commit,
            depth
        );
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let Some(node_hash) = dir_hashes.get(&node_path).cloned() else {
            log::debug!(
                "dir_hashes {:?} does not contain path: {:?}",
                dir_hashes,
                node_path
            );
            return Err(OxenError::basic_str(format!(
                "Can only load a subtree with an existing directory path: '{}'",
                node_path.to_str().unwrap()
            )));
        };

        let Some(root) =
            CommitMerkleTree::read_depth_children_and_hashes(repo, &node_hash, depth, hashes)?
        else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for: '{}' hash: {:?}",
                node_path.to_str().unwrap(),
                node_hash
            )));
        };
        Ok(Some(root))
    }

    pub fn from_path_depth_unique_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        depth: i32,
        shared_hashes: &mut HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let mut node_path = path.as_ref().to_path_buf();
        if node_path == PathBuf::from(".") {
            node_path = PathBuf::from("");
        }
        log::debug!(
            "Read path {:?} in commit {:?} depth: {}",
            node_path,
            commit,
            depth
        );

        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let Some(node_hash) = dir_hashes.get(&node_path).cloned() else {
            /*log::debug!(
                "dir_hashes {:?} does not contain path: {:?}",
                dir_hashes,
                node_path
            );*/
            return Err(OxenError::basic_str(format!(
                "Can only load a subtree with an existing directory path: '{}'",
                node_path.to_str().unwrap()
            )));
        };

        let Some(root) = CommitMerkleTree::read_depth_unique_children(
            repo,
            &node_hash,
            depth,
            shared_hashes,
            unique_hashes,
        )?
        else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree hash not found for: '{}' hash: {:?}",
                node_path.to_str().unwrap(),
                node_hash
            )));
        };
        Ok(Some(root))
    }

    pub fn from_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        load_recursive: bool,
    ) -> Result<Self, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();

        let root = if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, &node_hash, load_recursive)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for dir: '{}' in commit: '{}'",
                    node_path.to_str().unwrap(),
                    commit.id
                )),
            )?
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {:?}", node_path);
            CommitMerkleTree::read_file(repo, &dir_hashes, node_path)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for file: '{}' in commit: '{}'",
                    node_path.to_str().unwrap(),
                    commit.id
                )),
            )?
        };
        Ok(Self { root, dir_hashes })
    }

    pub fn dir_without_children_with_dirhash(
        repo: &LocalRepository,
        path: impl AsRef<Path>,
        dir_hashes: &HashMap<PathBuf, MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, &node_hash, false)
        } else {
            Ok(None)
        }
    }

    /// Read the dir metadata from the path, without reading the children
    pub fn dir_without_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            CommitMerkleTree::read_node(repo, &node_hash, false)
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_children_from_dirhash(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        dir_hashes: &HashMap<PathBuf, MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir {:?}", node_path);
            // Read the node at depth 1 to get VNodes and Sub-Files/Dirs
            // We don't count VNodes in the depth
            CommitMerkleTree::read_depth(repo, &node_hash, 1)
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir {:?}", node_path);
            // Read the node at depth 1 to get VNodes and Sub-Files/Dirs
            // We don't count VNodes in the depth
            CommitMerkleTree::read_depth(repo, &node_hash, 1)
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_children_recursive(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {:?} in commit {:?}", node_path, commit);
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {:?}", node_path);
            // Read the node at depth 2 to get VNodes and Sub-Files/Dirs
            CommitMerkleTree::read_node(repo, &node_hash, true)
        } else {
            Ok(None)
        }
    }

    pub fn read_node(
        repo: &LocalRepository,
        hash: &MerkleHash,
        recurse: bool,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        CommitMerkleTree::read_children_from_node(repo, &mut node, recurse)?;
        // log::debug!("read_node done: {:?} recurse: {}", node.hash, recurse);
        Ok(Some(node))
    }

    fn read_node_with_hashes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        CommitMerkleTree::load_children_with_hashes(repo, &mut node, hashes)?;
        Ok(Some(node))
    }

    fn read_unique_nodes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: &mut HashSet<MerkleHash>,
        shared_hashes: &mut HashSet<MerkleHash>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let start_path = PathBuf::new();
        CommitMerkleTree::load_unique_children(
            repo,
            &mut node,
            &start_path,
            base_hashes,
            shared_hashes,
            partial_nodes,
        )?;
        Ok(Some(node))
    }

    fn read_unique_children_for_commit(
        repo: &LocalRepository,
        hash: &MerkleHash,
        shared_hashes: &mut HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let start_path = PathBuf::new();

        CommitMerkleTree::load_unique_children_for_commit(
            repo,
            &mut node,
            &start_path,
            shared_hashes,
            unique_hashes,
        )?;
        Ok(Some(node))
    }

    pub fn read_depth(
        repo: &LocalRepository,
        hash: &MerkleHash,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read depth {} node hash [{}]", depth, hash);
        if !MerkleNodeDB::exists(repo, hash) {
            log::debug!(
                "read_depth merkle node db does not exist for hash: {}",
                hash
            );
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        CommitMerkleTree::read_children_until_depth(repo, &mut node, depth, 0)?;
        // log::debug!("Read depth {} node done: {:?}", depth, node.hash);
        Ok(Some(node))
    }

    pub fn read_depth_children_and_hashes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        depth: i32,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read depth {} node hash [{}]", depth, hash);
        if !MerkleNodeDB::exists(repo, hash) {
            log::debug!(
                "read_depth merkle node db does not exist for hash: {}",
                hash
            );
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;

        CommitMerkleTree::load_children_until_depth_children_and_hashes(
            repo, &mut node, depth, 0, hashes,
        )?;
        // log::debug!("Read depth {} node done: {:?}", depth, node.hash);
        Ok(Some(node))
    }

    pub fn read_depth_unique_children(
        repo: &LocalRepository,
        hash: &MerkleHash,
        depth: i32,
        shared_hashes: &mut HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read depth {} node hash [{}]", depth, hash);
        if !MerkleNodeDB::exists(repo, hash) {
            log::debug!(
                "read_depth merkle node db does not exist for hash: {}",
                hash
            );
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let start_path = PathBuf::new();

        CommitMerkleTree::load_children_until_depth_unique_children(
            repo,
            &mut node,
            &start_path,
            depth,
            0,
            shared_hashes,
            unique_hashes,
        )?;
        // log::debug!("Read depth {} node done: {:?}", depth, node.hash);
        Ok(Some(node))
    }

    /// The dir hashes allow you to skip to a directory in the tree
    pub fn dir_hashes(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<HashMap<PathBuf, MerkleHash>, OxenError> {
        let node_db_dir = CommitMerkleTree::dir_hash_db_path(repo, commit);
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
        // log::debug!(
        //     "read {} dir_hashes from commit: {}",
        //     dir_hashes.len(),
        //     commit
        // );
        Ok(dir_hashes)
    }

    pub fn read_nodes(
        repo: &LocalRepository,
        commit: &Commit,
        paths: &[PathBuf],
    ) -> Result<HashMap<PathBuf, MerkleTreeNode>, OxenError> {
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        // log::debug!(
        //     "read_nodes dir_hashes from commit: {} count: {}",
        //     commit,
        //     dir_hashes.len()
        // );
        // for (path, hash) in &dir_hashes {
        //     log::debug!("read_nodes dir_hashes path: {:?} hash: {:?}", path, hash);
        // }

        let mut nodes = HashMap::new();
        for path in paths.iter() {
            // Skip to the nodes
            let Some(hash) = dir_hashes.get(path) else {
                continue;
            };

            // log::debug!("Loading node for path: {:?} hash: {}", path, hash);
            let Some(node) = CommitMerkleTree::read_depth(repo, hash, 1)? else {
                log::warn!(
                    "Merkle tree hash not found for parent: {:?} hash: {:?}",
                    path,
                    hash
                );
                continue;
            };
            nodes.insert(path.clone(), node);
        }
        Ok(nodes)
    }

    pub fn has_dir(&self, path: impl AsRef<Path>) -> bool {
        // log::debug!("has_dir path: {:?}", path.as_ref());
        // log::debug!("has_dir dir_hashes: {:?}", self.dir_hashes);
        let path = path.as_ref();
        self.dir_hashes.contains_key(path)
    }

    pub fn has_path(&self, path: impl AsRef<Path>) -> Result<bool, OxenError> {
        let path = path.as_ref();
        let node = self.root.get_by_path(path)?;
        Ok(node.is_some())
    }

    pub fn get_by_path(&self, path: impl AsRef<Path>) -> Result<Option<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let node = self.root.get_by_path(path)?;
        Ok(node)
    }

    pub fn get_vnodes_for_dir(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let nodes = self.root.get_vnodes_for_dir(path)?;
        Ok(nodes)
    }

    pub fn list_dir_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        self.root.list_dir_paths()
    }

    pub fn files_and_folders(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<HashSet<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let node = self
            .root
            .get_by_path(path)?
            .ok_or(OxenError::basic_str(format!(
                "Merkle tree hash not found for parent: {:?}",
                path
            )))?;
        let mut children = HashSet::new();
        for child in &node.children {
            children.extend(child.children.iter().cloned());
        }
        Ok(children)
    }

    // MODULARIZE: List_files_and_folders in repositories::tree
    pub fn node_files_and_folders(node: &MerkleTreeNode) -> Result<Vec<MerkleTreeNode>, OxenError> {
        if MerkleTreeNodeType::Dir != node.node.node_type() {
            return Err(OxenError::basic_str(format!(
                "node_files_and_folders Merkle tree node is not a directory: '{:?}'",
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

    pub fn total_vnodes(&self) -> usize {
        self.root.total_vnodes()
    }

    // MODULARIZE: dir_entries_with_paths

    pub fn dir_entries(node: &MerkleTreeNode) -> Result<Vec<FileNode>, OxenError> {
        let mut file_entries = Vec::new();

        match &node.node {
            EMerkleTreeNode::Directory(_) | EMerkleTreeNode::VNode(_) => {
                for child in &node.children {
                    match &child.node {
                        EMerkleTreeNode::File(file_node) => {
                            file_entries.push(file_node.clone());
                        }
                        EMerkleTreeNode::Directory(_) | EMerkleTreeNode::VNode(_) => {
                            file_entries.extend(Self::dir_entries(child)?);
                        }
                        _ => {}
                    }
                }
                Ok(file_entries)
            }
            EMerkleTreeNode::File(file_node) => Ok(vec![file_node.clone()]),
            _ => Err(OxenError::basic_str(format!(
                "Unexpected node type: {:?}",
                node.node.node_type()
            ))),
        }
    }

    /// This uses the dir_hashes db to skip right to a file in the tree
    pub fn read_file(
        repo: &LocalRepository,
        dir_hashes: &HashMap<PathBuf, MerkleHash>,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // Get the directory from the path
        let path = path.as_ref();
        let Some(parent_path) = path.parent() else {
            return Ok(None);
        };
        let file_name = path.file_name().unwrap().to_str().unwrap();

        log::debug!(
            "read_file path {:?} parent_path {:?} file_name {:?}",
            path,
            parent_path,
            file_name
        );

        // Look up the directory hash
        let node_hash: Option<MerkleHash> = dir_hashes.get(parent_path).cloned();
        let Some(node_hash) = node_hash else {
            log::debug!("read_file could not find parent dir: {:?}", parent_path);
            return Ok(None);
        };

        log::debug!("read_file parent node_hash: {:?}", node_hash);

        // Read the directory node at depth 0 to get all the vnodes
        let dir_merkle_node = CommitMerkleTree::read_depth(repo, &node_hash, 0)?;
        let Some(dir_merkle_node) = dir_merkle_node else {
            return Ok(None);
        };

        let EMerkleTreeNode::Directory(dir_node) = &dir_merkle_node.node else {
            return Err(OxenError::basic_str(format!(
                "Expected directory node, got {:?}",
                dir_merkle_node.node.node_type()
            )));
        };

        // log::debug!("read_file merkle_node: {:?}", dir_merkle_node);

        let vnodes = dir_merkle_node.children;

        // log::debug!("read_file vnodes: {}", vnodes.len());

        // Calculate the total number of children in the vnodes
        // And use this to skip to the correct vnode
        // log::debug!("read_file dir_node {:?}", dir_node);
        let total_children = dir_node.num_entries();
        let vnode_size = repo.vnode_size();
        let num_vnodes = (total_children as f32 / vnode_size as f32).ceil() as u128;

        log::debug!("read_file total_children: {}", total_children);
        log::debug!("read_file vnode_size: {}", vnode_size);
        log::debug!("read_file num_vnodes: {}", num_vnodes);

        if num_vnodes == 0 {
            log::debug!("read_file num_vnodes is 0, returning None");
            return Ok(None);
        }

        // Calculate the bucket to skip to based on the path and the number of vnodes
        let path_hash = hasher::hash_buffer_128bit(path.to_str().unwrap().as_bytes());
        let bucket = path_hash % num_vnodes;

        log::debug!("read_file bucket: {}", bucket);

        // We did not load recursively, so we need to load the children for the specific vnode
        let vnode_without_children = &vnodes[bucket as usize];

        // Load the children for the vnode
        let vnode_with_children =
            CommitMerkleTree::read_depth(repo, &vnode_without_children.hash, 0)?;
        // log::debug!("read_file vnode_with_children: {:?}", vnode_with_children);
        let Some(vnode_with_children) = vnode_with_children else {
            return Ok(None);
        };

        // Get the file node from the vnode, which does a binary search under the hood
        vnode_with_children.get_by_path(file_name)
    }

    fn read_children_until_depth(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        requested_depth: i32,
        traversed_depth: i32,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        // log::debug!(
        //     "read_children_until_depth requested_depth {} traversed_depth {} node {}",
        //     requested_depth,
        //     traversed_depth,
        //     node
        // );

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        log::debug!(
            "read_children_until_depth requested_depth node {} traversed_depth {} Got {} children",
            node,
            traversed_depth,
            children.len()
        );

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!(
            //     "read_children_until_depth {} child: {} -> {}",
            //     depth,
            //     key,
            //     child
            // );
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if requested_depth > traversed_depth || requested_depth == -1 {
                        // Depth that is passed in is the number of dirs to traverse
                        // VNodes should not increase the depth
                        let traversed_depth = if child.node.node_type() == MerkleTreeNodeType::VNode
                        {
                            traversed_depth
                        } else {
                            traversed_depth + 1
                        };
                        CommitMerkleTree::read_children_until_depth(
                            repo,
                            &mut child,
                            requested_depth,
                            traversed_depth,
                        )?;
                    }
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        Ok(())
    }

    fn load_children_until_depth_children_and_hashes(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        requested_depth: i32,
        traversed_depth: i32,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        hashes.insert(node.hash);
        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;

        for (_key, child) in children {
            let mut child = child.to_owned();

            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if requested_depth > traversed_depth || requested_depth == -1 {
                        // Depth that is passed in is the number of dirs to traverse
                        // VNodes should not increase the depth
                        let traversed_depth = if child.node.node_type() == MerkleTreeNodeType::VNode
                        {
                            traversed_depth
                        } else {
                            traversed_depth + 1
                        };

                        // Here we have to not panic on error, because if we clone a subtree we might not have all of the children nodes of a particular dir
                        // given that we are only loading the nodes that are needed.
                        CommitMerkleTree::load_children_until_depth_children_and_hashes(
                            repo,
                            &mut child,
                            requested_depth,
                            traversed_depth,
                            hashes,
                        )?;
                    }
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        Ok(())
    }

    fn load_children_until_depth_unique_children(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        current_path: &PathBuf,
        requested_depth: i32,
        traversed_depth: i32,
        shared_hashes: &mut HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        log::debug!(
             "load_children_until_depth_unique_children requested_depth {} traversed_depth {} node {} ",
             requested_depth,
             traversed_depth,
             node,
         );

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        if shared_hashes.contains(&node.hash) {
            return Ok(());
        }

        unique_hashes.insert(node.hash);
        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;

        for (_key, child) in children {
            let mut child = child.to_owned();
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    // TODO: '(requested_depth == 0 && child.node.node_type() == MerkleTreeNodeType::VNode)' is a hack
                    // Figure out why repositories::tree::get_node_hashes_between_commits needs this
                    if requested_depth > traversed_depth
                        || requested_depth == -1
                        || (requested_depth == 0
                            && child.node.node_type() == MerkleTreeNodeType::VNode)
                    {
                        // Depth that is passed in is the number of dirs to traverse
                        // VNodes should not increase the depth
                        let traversed_depth = if child.node.node_type() != MerkleTreeNodeType::Dir {
                            traversed_depth
                        } else {
                            traversed_depth + 1
                        };

                        // Update current_path so that the partial nodes will have the correct path
                        let new_path = if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                            let name = PathBuf::from(dir_node.name());
                            &current_path.join(name)
                        } else {
                            current_path
                        };

                        // Here we have to not panic on error, because if we clone a subtree we might not have all of the children nodes of a particular dir
                        // given that we are only loading the nodes that are needed.
                        CommitMerkleTree::load_children_until_depth_unique_children(
                            repo,
                            &mut child,
                            new_path,
                            requested_depth,
                            traversed_depth,
                            shared_hashes,
                            unique_hashes,
                        )?;
                    }
                    node.children.push(child);
                }
                // FileChunks and Schemas are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        Ok(())
    }

    pub fn walk_tree(&self, f: impl FnMut(&MerkleTreeNode)) {
        self.root.walk_tree(f);
    }

    pub fn walk_tree_without_leaves(&self, f: impl FnMut(&MerkleTreeNode)) {
        self.root.walk_tree_without_leaves(f);
    }

    fn read_children_from_node(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        recurse: bool,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
            || !recurse
        {
            return Ok(());
        }

        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        // log::debug!("read_children_from_node Got {} children", children.len());

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("read_children_from_node child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if recurse {
                        // log::debug!("read_children_from_node recurse: {:?}", child.hash);
                        // log::debug!("read_children_from_node opened node_db: {:?}", child.hash);
                        CommitMerkleTree::read_children_from_node(repo, &mut child, recurse)?;
                    }
                    node.children.push(child);
                }
                // FileChunks and Files are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        // log::debug!("read_children_from_node done: {:?}", node.hash);

        Ok(())
    }

    fn load_children_with_hashes(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        hashes: &mut HashSet<MerkleHash>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        hashes.insert(node.hash);
        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        // log::debug!("load_children_with_hashes Got {} children", children.len());

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("load_children_with_hashes child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    // log::debug!("load_children_with_hashes recurse: {:?}", child.hash);
                    CommitMerkleTree::load_children_with_hashes(repo, &mut child, hashes)?;
                    node.children.push(child);
                }
                // FileChunks and Files are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        Ok(())
    }

    fn load_unique_children(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        current_path: &PathBuf,
        base_hashes: &mut HashSet<MerkleHash>,
        shared_hashes: &mut HashSet<MerkleHash>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        if base_hashes.contains(&node.hash) {
            shared_hashes.insert(node.hash);
            return Ok(());
        }

        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        // log::debug!("load_unique_children Got {} children", children.len());
        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("load_unique_children child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    // log::debug!("load_unique_children  recurse: {:?}", child.hash);
                    // Update current_path so that the partial nodes will have the correct path
                    let new_path = if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                        let name = PathBuf::from(dir_node.name());
                        &current_path.join(name)
                    } else {
                        current_path
                    };

                    // log::debug!("load_unique_children  opened node_db: {:?}", child.hash);
                    CommitMerkleTree::load_unique_children(
                        repo,
                        &mut child,
                        new_path,
                        base_hashes,
                        shared_hashes,
                        partial_nodes,
                    )?;
                    node.children.push(child);
                }
                // FileChunks and Files are leaf nodes
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    if let EMerkleTreeNode::File(file_node) = &child.node {
                        let file_path = current_path.join(PathBuf::from(file_node.name()));
                        let partial_node = PartialNode::from(
                            *file_node.hash(),
                            file_node.last_modified_seconds(),
                            file_node.last_modified_nanoseconds(),
                        );
                        partial_nodes.insert(file_path, partial_node);
                    }

                    node.children.push(child);
                }
            }
        }

        Ok(())
    }

    fn load_unique_children_for_commit(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        current_path: &PathBuf,
        shared_hashes: &mut HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        // Don't continue loading if encountering a shared hash (Right now, the common hashes are the 'base' hashes. These are all the hashes that have been seen previously)
        if shared_hashes.contains(&node.hash) {
            return Ok(());
        }

        // If found a unique hash, insert it into unique_hashes so we won't step on it in future runs
        unique_hashes.insert(node.hash);

        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        // log::debug!("load_unique_children Got {} children", children.len());
        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("load_unique_children child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Directories, VNodes, and Files have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    // log::debug!("load_unique_children  recurse: {:?}", child.hash);
                    let new_path = if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                        let name = PathBuf::from(dir_node.name());
                        &current_path.join(name)
                    } else {
                        current_path
                    };

                    // log::debug!("load_unique_children  opened node_db: {:?}", child.hash);
                    CommitMerkleTree::load_unique_children_for_commit(
                        repo,
                        &mut child,
                        new_path,
                        shared_hashes,
                        unique_hashes,
                    )?;
                    node.children.push(child);
                }
                // TODO: Error handling for unknown MerkleTreeNode type?
                MerkleTreeNodeType::FileChunk | MerkleTreeNodeType::File => {
                    node.children.push(child);
                }
            }
        }

        // log::debug!("load_unique_children " done: {:?}", node.hash);

        Ok(())
    }

    pub fn print(&self) {
        CommitMerkleTree::print_node(&self.root);
    }

    pub fn print_depth(&self, depth: i32) {
        CommitMerkleTree::print_node_depth(&self.root, depth);
    }

    pub fn print_node_depth(node: &MerkleTreeNode, depth: i32) {
        CommitMerkleTree::r_print(node, 0, depth);
    }

    pub fn print_node(node: &MerkleTreeNode) {
        // print all the way down
        CommitMerkleTree::r_print(node, 0, -1);
    }

    fn r_print(node: &MerkleTreeNode, indent: i32, depth: i32) {
        // log::debug!("r_print depth {:?} indent {:?}", depth, indent);
        // log::debug!(
        //     "r_print node dtype {:?} hash {} data.len() {} children.len() {}",
        //     node.dtype,
        //     node.hash,
        //     node.data.len(),
        //     node.children.len()
        // );
        if depth != -1 && depth > 0 && indent >= depth {
            return;
        }

        println!("{}{}", "  ".repeat(indent as usize), node);

        for child in &node.children {
            CommitMerkleTree::r_print(child, indent + 1, depth);
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use crate::core::v_latest::index::CommitMerkleTree;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::model::MerkleTreeNodeType;
    use crate::repositories;
    use crate::test;
    use crate::test::add_n_files_m_dirs;

    #[tokio::test]
    async fn test_load_dir_nodes() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::LATEST)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3).await?;
            let status = repositories::status(&repo)?;
            status.print();

            // Commit the data
            let commit = repositories::commits::commit(&repo, "First commit")?;

            let tree = CommitMerkleTree::from_commit(&repo, &commit)?;
            tree.print();

            /*
            The tree will look something like this

            [Commit] d9fc5c49451ad18335f9f8c1e1c8ac0b -> First commit parent_ids ""
                [Dir]  -> 172861146a4a0f5f0250f117ce93ef1e 60 B (1 nodes) (10 files)
                    [VNode] 3a5d6d3bdc8bf1f3fddcabaa3afcd821 (3 children)
                    [File] README.md -> beb36f69f0b6efd87dbe3bb3dcea661c 18 B
                    [Dir] files -> aefe7cf4ad104b759e46c13cb304ba16 60 B (1 nodes) (10 files)
                        [VNode] affcd15c283c42524ee3f2dc300b90fe (3 children)
                        [Dir] dir_0 -> ee97a66ee8498caa67605c50e9b24275 0 B (1 nodes) (0 files)
                            [VNode] 1756daa4caa26d51431b925250529838 (4 children)
                            [File] file0.txt -> 82d44cc82d2c1c957aeecb14293fb5ec 6 B
                            [File] file3.txt -> 9c8fe1177e78b0fe5ec104db52b5e449 6 B
                            [File] file6.txt -> 3cba14134797f8c246ee520c808817b4 6 B
                            [File] file9.txt -> ab8e4cdc8e9df49fb8d7bc1940df811f 6 B
                        [Dir] dir_1 -> 24467f616e4fba7beacb18b71b87652d 0 B (1 nodes) (0 files)
                            [VNode] 382eb89abe00193ed680c6a541f4b0c4 (3 children)
                            [File] file1.txt -> aab67365636cc292a767ad9e48ca6e1f 6 B
                            [File] file4.txt -> f8d4169182a41cc63bb7ed8fc36de960 6 B
                            [File] file7.txt -> b0335dcbf55c6c08471d8ebefbbf5de9 6 B
                        [Dir] dir_2 -> 7e2fbcd5b9e62847e1aaffd7e9d1aa8 0 B (1 nodes) (0 files)
                            [VNode] b87cfea40ada7cc374833ab2eca4636d (3 children)
                            [File] file2.txt -> 2101009797546bf98de2b0bbcbd59f0 6 B
                            [File] file5.txt -> 253badb52f99edddf74d1261b8c5f03a 6 B
                            [File] file8.txt -> 13fa116ba84c615eda1759b5e6ae5d6e 6 B
                    [File] files.csv -> 152b60b41558d5bfe80b7e451de7b276 151 B
            */

            // Make sure we have written the dir_hashes db
            let dir_hashes = CommitMerkleTree::dir_hashes(&repo, &commit)?;

            println!("Got {} dir_hashes", dir_hashes.len());
            for (key, value) in &dir_hashes {
                println!("dir: {:?} hash: {}", key, value);
            }

            // Should have ["", "files", "files/dir_0", "files/dir_1", "files/dir_2"]
            assert_eq!(dir_hashes.len(), 5);
            assert!(dir_hashes.contains_key(&PathBuf::from("")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files/dir_0")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files/dir_1")));
            assert!(dir_hashes.contains_key(&PathBuf::from("files/dir_2")));

            // Only load the root and files/dir_1
            let paths_to_load: Vec<PathBuf> =
                vec![PathBuf::from(""), PathBuf::from("files").join("dir_1")];
            let loaded_nodes = CommitMerkleTree::read_nodes(&repo, &commit, &paths_to_load)?;

            println!("loaded {} nodes", loaded_nodes.len());
            for (_, node) in loaded_nodes {
                println!("node: {}", node);
                CommitMerkleTree::print_node_depth(&node, 1);
                assert!(node.node.node_type() == MerkleTreeNodeType::Dir);
                assert!(node.parent_id.is_some());
                assert!(!node.children.is_empty());
                let dir = node.dir().unwrap();
                assert!(dir.num_files() > 0);
                assert!(dir.num_entries() > 0);
            }

            Ok(())
        })
        .await
    }
}
