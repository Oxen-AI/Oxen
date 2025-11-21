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

    /// The dir hashes allow you to skip to a directory in the tree
    pub fn dir_hashes(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<HashMap<PathBuf, MerkleHash>, OxenError> {
        let node_db_dir = CommitMerkleTree::dir_hash_db_path(repo, commit);
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
                    let hash = value.parse()?;
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

    pub fn from_commit(repo: &LocalRepository, commit: &Commit) -> Result<Self, OxenError> {
        // This debug log is to help make sure we don't load the tree too many times
        // if you see it in the logs being called too much, it could be why the code is slow.
        log::debug!("Load tree from commit: {} in repo: {:?}", commit, repo.path);
        let root =
            CommitMerkleTree::root_with_children(repo, commit)?.ok_or(OxenError::basic_str(
                format!("Merkle tree hash not found for commit: '{}'", commit.id),
            ))?;

        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        Ok(Self { root, dir_hashes })
    }

    pub fn from_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        load_recursive: bool,
    ) -> Result<Self, OxenError> {
        // This debug log is to help make sure we don't load the tree too many times
        // if you see it in the logs being called too much, it could be why the code is slow.
        log::debug!("Load tree from commit: {} in repo: {:?}", commit, repo.path);
        let node = CommitMerkleTree::read_from_path(repo, commit, path, load_recursive)?.ok_or(
            OxenError::basic_str(format!(
                "Merkle tree hash not found for commit: '{}'",
                commit.id
            )),
        )?;

        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;

        Ok(Self {
            root: node,
            dir_hashes,
        })
    }

    pub fn root_with_children(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = commit.id.parse()?;
        CommitMerkleTree::read_node(repo, &node_hash, true)
    }

    pub fn root_without_children(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = commit.id.parse()?;
        // Read the root node at depth 1 to get the directory node as well
        CommitMerkleTree::read_node(repo, &node_hash, false)
    }

    pub fn root_with_children_and_node_hashes(
        repo: &LocalRepository,
        commit: &Commit,
        base_hashes: Option<&HashSet<MerkleHash>>,
        unique_hashes: Option<&mut HashSet<MerkleHash>>,
        shared_hashes: Option<&mut HashSet<MerkleHash>>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = commit.id.parse()?;

        CommitMerkleTree::read_node_and_collect_hashes(
            repo,
            &node_hash,
            base_hashes,
            unique_hashes,
            shared_hashes,
        )
    }

    pub fn root_with_children_and_partial_nodes(
        repo: &LocalRepository,
        commit: &Commit,
        base_hashes: Option<&HashSet<MerkleHash>>,
        unique_hashes: Option<&mut HashSet<MerkleHash>>,
        shared_hashes: Option<&mut HashSet<MerkleHash>>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_hash = commit.id.parse()?;

        CommitMerkleTree::read_node_and_collect_partial_nodes(
            repo,
            &node_hash,
            base_hashes,
            unique_hashes,
            shared_hashes,
            partial_nodes,
        )
    }

    pub fn read_node(
        repo: &LocalRepository,
        hash: &MerkleHash,
        recurse: bool,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let depth = if recurse { -1 } else { 1 };

        CommitMerkleTree::read_depth(repo, hash, depth)
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
                log::warn!("Merkle tree hash not found for parent: {path:?} hash: {hash:?}");
                continue;
            };
            nodes.insert(path.clone(), node);
        }

        Ok(nodes)
    }

    pub fn read_node_and_collect_hashes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: Option<&HashSet<MerkleHash>>,
        unique_hashes: Option<&mut HashSet<MerkleHash>>,
        shared_hashes: Option<&mut HashSet<MerkleHash>>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let depth = -1;

        CommitMerkleTree::read_depth_and_collect_hashes(
            repo,
            hash,
            base_hashes,
            unique_hashes,
            shared_hashes,
            depth,
        )
    }

    pub fn read_node_and_collect_paths(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: Option<&HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        shared_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        unique_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let depth = -1;

        CommitMerkleTree::read_depth_and_collect_paths(
            repo,
            hash,
            base_hashes,
            shared_hashes,
            unique_hashes,
            depth,
        )
    }

    pub fn read_node_and_collect_partial_nodes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: Option<&HashSet<MerkleHash>>,
        unique_hashes: Option<&mut HashSet<MerkleHash>>,
        shared_hashes: Option<&mut HashSet<MerkleHash>>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let depth = -1;

        CommitMerkleTree::read_depth_and_collect_partial_nodes(
            repo,
            hash,
            base_hashes,
            unique_hashes,
            shared_hashes,
            partial_nodes,
            depth,
        )
    }

    pub fn read_depth(
        repo: &LocalRepository,
        hash: &MerkleHash,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read depth {} node hash [{}]", depth, hash);
        if !MerkleNodeDB::exists(repo, hash) {
            log::debug!("read_depth merkle node db does not exist for hash: {hash}");
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;

        let base_hashes = None;
        let unique_hashes = None;
        let shared_hashes = None;

        CommitMerkleTree::load_children(
            repo,
            &mut node,
            base_hashes,
            unique_hashes,
            shared_hashes,
            depth,
            0,
        )?;

        Ok(Some(node))
    }

    pub fn read_depth_and_collect_hashes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: Option<&HashSet<MerkleHash>>,
        unique_hashes: Option<&mut HashSet<MerkleHash>>,
        shared_hashes: Option<&mut HashSet<MerkleHash>>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;

        CommitMerkleTree::load_children(
            repo,
            &mut node,
            base_hashes,
            unique_hashes,
            shared_hashes,
            depth,
            0,
        )?;

        Ok(Some(node))
    }

    pub fn read_depth_and_collect_paths(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: Option<&HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        shared_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        unique_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let start_path = PathBuf::new();

        CommitMerkleTree::load_children_and_collect_paths(
            repo,
            &mut node,
            &start_path,
            base_hashes,
            shared_hashes,
            unique_hashes,
            depth,
            0,
        )?;

        Ok(Some(node))
    }

    pub fn read_depth_and_collect_partial_nodes(
        repo: &LocalRepository,
        hash: &MerkleHash,
        base_hashes: Option<&HashSet<MerkleHash>>,
        unique_hashes: Option<&mut HashSet<MerkleHash>>,
        shared_hashes: Option<&mut HashSet<MerkleHash>>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!("Read node hash [{}]", hash);
        if !MerkleNodeDB::exists(repo, hash) {
            // log::debug!("read_node merkle node db does not exist for hash: {}", hash);
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let start_path = PathBuf::new();

        CommitMerkleTree::load_children_and_collect_partial_nodes(
            repo,
            &mut node,
            &start_path,
            base_hashes,
            unique_hashes,
            shared_hashes,
            partial_nodes,
            depth,
            0,
        )?;

        Ok(Some(node))
    }

    pub fn read_from_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        load_recursive: bool,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let depth = if load_recursive { -1 } else { 1 };

        CommitMerkleTree::read_depth_from_path(repo, commit, path, depth)
    }

    pub fn read_from_path_maybe(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        load_recursive: bool,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let maybe_tree_node = CommitMerkleTree::read_from_path(repo, commit, path, load_recursive);

        match maybe_tree_node {
            Ok(node) => Ok(node),
            Err(_) => Ok(None),
        }
    }

    pub fn read_depth_from_path(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let mut node_path = path.as_ref().to_path_buf();
        if node_path == PathBuf::from(".") {
            node_path = PathBuf::from("");
        }

        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let root = if let Some(node_hash) = dir_hashes.get(&node_path).cloned() {
            // We are reading a node with children
            let Some(root) = CommitMerkleTree::read_depth(repo, &node_hash, depth)? else {
                return Err(OxenError::basic_str(format!(
                    "Merkle tree hash not found for dir: '{}' in commit: {:?}",
                    node_path.to_str().unwrap(),
                    commit.id
                )));
            };

            root
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {node_path:?}");
            CommitMerkleTree::read_file(repo, &dir_hashes, &node_path)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for file: '{}' in commit: '{}'",
                    node_path.to_str().unwrap(),
                    commit.id
                )),
            )?
        };

        Ok(Some(root))
    }

    pub fn read_depth_from_path_and_collect_hashes(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        base_hashes: Option<&HashSet<MerkleHash>>,
        unique_hashes: Option<&mut HashSet<MerkleHash>>,
        shared_hashes: Option<&mut HashSet<MerkleHash>>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let mut node_path = path.as_ref().to_path_buf();
        if node_path == PathBuf::from(".") {
            node_path = PathBuf::from("");
        }

        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let root = if let Some(node_hash) = dir_hashes.get(&node_path).cloned() {
            // We are reading a node with children
            let Some(root) = CommitMerkleTree::read_depth_and_collect_hashes(
                repo,
                &node_hash,
                base_hashes,
                unique_hashes,
                shared_hashes,
                depth,
            )?
            else {
                return Err(OxenError::basic_str(format!(
                    "Merkle tree hash not found for dir: '{}' in commit: {:?}",
                    node_path.to_str().unwrap(),
                    commit.id
                )));
            };

            root
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {node_path:?}");
            CommitMerkleTree::read_file(repo, &dir_hashes, &node_path)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for file: '{}' in commit: '{}'",
                    node_path.to_str().unwrap(),
                    commit.id
                )),
            )?
        };

        Ok(Some(root))
    }

    pub fn read_depth_from_path_and_collect_paths(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        base_hashes: Option<&HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        shared_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        unique_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let mut node_path = path.as_ref().to_path_buf();
        if node_path == PathBuf::from(".") {
            node_path = PathBuf::from("");
        }

        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        let root = if let Some(node_hash) = dir_hashes.get(&node_path).cloned() {
            // We are reading a node with children
            let Some(root) = CommitMerkleTree::read_depth_and_collect_paths(
                repo,
                &node_hash,
                base_hashes,
                unique_hashes,
                shared_hashes,
                depth,
            )?
            else {
                return Err(OxenError::basic_str(format!(
                    "Merkle tree hash not found for dir: '{}' in commit: {:?}",
                    node_path.to_str().unwrap(),
                    commit.id
                )));
            };

            root
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {node_path:?}");
            CommitMerkleTree::read_file(repo, &dir_hashes, &node_path)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for file: '{}' in commit: '{}'",
                    node_path.to_str().unwrap(),
                    commit.id
                )),
            )?
        };

        Ok(Some(root))
    }

    pub fn dir_without_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        let dir_hashes = if let Some(dir_hashes) = dir_hashes {
            dir_hashes
        } else {
            &CommitMerkleTree::dir_hashes(repo, commit)?
        };

        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // We are reading a node with children
            log::debug!("Look up dir 🗂️ {node_path:?}");

            // Read at depth 0 to skip loading file nodes
            CommitMerkleTree::read_depth(repo, &node_hash, 0)
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        let dir_hashes = if let Some(dir_hashes) = dir_hashes {
            dir_hashes
        } else {
            &CommitMerkleTree::dir_hashes(repo, commit)?
        };

        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            // Read the node at depth 1 to get VNodes and Sub-Files/Dirs
            // We don't count VNodes in the depth
            CommitMerkleTree::read_depth(repo, &node_hash, 1)
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_unique_children(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        base_hashes: &HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
        dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        let dir_hashes = if let Some(dir_hashes) = dir_hashes {
            dir_hashes
        } else {
            &CommitMerkleTree::dir_hashes(repo, commit)?
        };

        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            log::debug!("Look up dir {node_path:?}");
            // Read the node at depth 1 to get VNodes and Sub-Files/Dirs
            // We don't count VNodes in the depth
            CommitMerkleTree::read_node_and_collect_hashes(
                repo,
                &node_hash,
                Some(base_hashes),
                Some(unique_hashes),
                None,
            )
        } else {
            Ok(None)
        }
    }

    pub fn dir_with_children_recursive(
        repo: &LocalRepository,
        commit: &Commit,
        path: impl AsRef<Path>,
        dir_hashes: Option<&HashMap<PathBuf, MerkleHash>>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        let node_path = path.as_ref();
        log::debug!("Read path {node_path:?} in commit {commit:?}");
        let dir_hashes = if let Some(dir_hashes) = dir_hashes {
            dir_hashes
        } else {
            &CommitMerkleTree::dir_hashes(repo, commit)?
        };

        let node_hash: Option<MerkleHash> = dir_hashes.get(node_path).cloned();
        if let Some(node_hash) = node_hash {
            log::debug!("Look up dir 🗂️ {node_path:?}");
            CommitMerkleTree::read_depth(repo, &node_hash, -1)
        } else {
            Ok(None)
        }
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

    // call node_files_and_filders
    pub fn files_and_folders(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<HashSet<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let node = self
            .root
            .get_by_path(path)?
            .ok_or(OxenError::basic_str(format!(
                "Merkle tree hash not found for parent: {path:?}"
            )))?;
        let mut children = HashSet::new();
        for child in &node.children {
            children.extend(child.children.iter().cloned());
        }
        Ok(children)
    }

    pub fn total_vnodes(&self) -> usize {
        self.root.total_vnodes()
    }

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

        log::debug!("read_file path {path:?} parent_path {parent_path:?} file_name {file_name:?}");

        // Look up the directory hash
        let node_hash: Option<MerkleHash> = dir_hashes.get(parent_path).cloned();
        let Some(node_hash) = node_hash else {
            log::debug!("read_file could not find parent dir: {parent_path:?}");
            return Ok(None);
        };

        log::debug!("read_file parent node_hash: {node_hash:?}");

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

        log::debug!("read_file total_children: {total_children}");
        log::debug!("read_file vnode_size: {vnode_size}");
        log::debug!("read_file num_vnodes: {num_vnodes}");

        if num_vnodes == 0 {
            log::debug!("read_file num_vnodes is 0, returning None");
            return Ok(None);
        }

        // Calculate the bucket to skip to based on the path and the number of vnodes
        let path_hash = hasher::hash_buffer_128bit(path.to_str().unwrap().as_bytes());
        let bucket = path_hash % num_vnodes;

        log::debug!("read_file bucket: {bucket}");

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

    // TODO: We might want to simplify to one tree-loading method
    // The advantage of multiple is that it saves us tree traversals, when we want to collect something as we load in the tree
    // However, I'm not sure that's worth the cost of extending the code base. We could probably cut this file in half if we're willing to do extra tree traversals

    // TODO: Marking `unique_hashes` and `shared_hashes` as mut and then taking and dereferencing them
    // Is done to avoid ownership issues, but feels like an inelegant solution
    fn load_children(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        base_hashes: Option<&HashSet<MerkleHash>>,
        mut unique_hashes: Option<&mut HashSet<MerkleHash>>,
        mut shared_hashes: Option<&mut HashSet<MerkleHash>>,
        requested_depth: i32,
        traversed_depth: i32,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        // log::debug!(
        //     "load_children requested_depth {} traversed_depth {} node {}",
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

        if let Some(base_hashes) = base_hashes {
            if base_hashes.contains(&node.hash) {
                if let Some(ref mut shared_hashes) = shared_hashes {
                    shared_hashes.insert(node.hash);
                }

                return Ok(());
            }
        }

        if let Some(ref mut unique_hashes) = unique_hashes {
            unique_hashes.insert(node.hash);
        }

        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        // log::debug!(
        //     "load_children requested_depth node {} traversed_depth {} Got {} children",
        //     node,
        //     traversed_depth,
        //     children.len()
        // );

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!(
            //     "load_children {} child: {} -> {}",
            //     depth,
            //     key,
            //     child
            // );
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if requested_depth > traversed_depth
                        || requested_depth == -1
                        || (requested_depth == 0
                            && child.node.node_type() == MerkleTreeNodeType::VNode)
                    {
                        // Depth that is passed in is the number of dirs to traverse
                        // VNodes should not increase the depth
                        let traversed_depth = if child.node.node_type() == MerkleTreeNodeType::VNode
                        {
                            traversed_depth
                        } else {
                            traversed_depth + 1
                        };

                        CommitMerkleTree::load_children(
                            repo,
                            &mut child,
                            base_hashes,
                            unique_hashes.as_deref_mut(),
                            shared_hashes.as_deref_mut(),
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

    // TODO: The (MerkkleHash, MerkleTreeNodeType) keys are a bit strange
    // Consider refactoring to eliminate the need for them
    #[allow(clippy::too_many_arguments)]
    fn load_children_and_collect_paths(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        current_path: &PathBuf,
        base_hashes: Option<&HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        mut unique_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        mut shared_hashes: Option<&mut HashMap<(MerkleHash, MerkleTreeNodeType), PathBuf>>,
        requested_depth: i32,
        traversed_depth: i32,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        // log::debug!(
        //     "load_children_and_collect_hashes requested_depth {} traversed_depth {} node {}",
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

        if let Some(base_hashes) = base_hashes {
            if base_hashes.contains_key(&(node.hash, dtype)) {
                if let Some(ref mut shared_hashes) = shared_hashes {
                    shared_hashes.insert((node.hash, dtype), current_path.to_path_buf());
                }

                return Ok(());
            }
        }

        if let Some(ref mut unique_hashes) = unique_hashes {
            unique_hashes.insert((node.hash, dtype), current_path.to_path_buf());
        }

        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        // log::debug!(
        //     "load_children_and_collect_hashes requested_depth node {} traversed_depth {} Got {} children",
        //     node,
        //     traversed_depth,
        //     children.len()
        // );

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!(
            //     "load_children_and_collect_hashes {} child: {} -> {}",
            //     depth,
            //     key,
            //     child
            // );
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if requested_depth > traversed_depth
                        || requested_depth == -1
                        || (requested_depth == 0
                            && child.node.node_type() == MerkleTreeNodeType::VNode)
                    {
                        // Depth that is passed in is the number of dirs to traverse
                        // VNodes should not increase the depth
                        let traversed_depth = if child.node.node_type() == MerkleTreeNodeType::VNode
                        {
                            traversed_depth
                        } else {
                            traversed_depth + 1
                        };

                        // Update current_path with the dir name
                        let new_path = if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                            let name = PathBuf::from(dir_node.name());
                            &current_path.join(name)
                        } else {
                            current_path
                        };

                        CommitMerkleTree::load_children_and_collect_paths(
                            repo,
                            &mut child,
                            new_path,
                            base_hashes,
                            unique_hashes.as_deref_mut(),
                            shared_hashes.as_deref_mut(),
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

    #[allow(clippy::too_many_arguments)]
    fn load_children_and_collect_partial_nodes(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        current_path: &PathBuf,
        base_hashes: Option<&HashSet<MerkleHash>>,
        mut unique_hashes: Option<&mut HashSet<MerkleHash>>,
        mut shared_hashes: Option<&mut HashSet<MerkleHash>>,
        partial_nodes: &mut HashMap<PathBuf, PartialNode>,
        requested_depth: i32,
        traversed_depth: i32,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
        {
            return Ok(());
        }

        if let Some(base_hashes) = base_hashes {
            if base_hashes.contains(&node.hash) {
                if let Some(ref mut shared_hashes) = shared_hashes {
                    shared_hashes.insert(node.hash);
                }

                return Ok(());
            }
        }

        if let Some(ref mut unique_hashes) = unique_hashes {
            unique_hashes.insert(node.hash);
        }

        let children = MerkleTreeNode::read_children_from_hash(repo, &node.hash)?;
        // log::debug!("load_children_and_collect_partial_nodes Got {} children", children.len());
        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("load_children_and_collect_partial_nodes child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Commits, Directories, and VNodes have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if requested_depth > traversed_depth
                        || requested_depth == -1
                        || (requested_depth == 0
                            && child.node.node_type() == MerkleTreeNodeType::VNode)
                    {
                        // log::debug!("load_children_and_collect_partial_nodes  recurse: {:?}", child.hash);
                        // Update current_path so that the partial nodes will have the correct path

                        let traversed_depth = if child.node.node_type() == MerkleTreeNodeType::VNode
                        {
                            traversed_depth
                        } else {
                            traversed_depth + 1
                        };

                        let new_path = if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                            let name = PathBuf::from(dir_node.name());
                            &current_path.join(name)
                        } else {
                            current_path
                        };

                        // log::debug!("load_children_and_collect_partial_nodes opened node_db: {:?}", child.hash);
                        CommitMerkleTree::load_children_and_collect_partial_nodes(
                            repo,
                            &mut child,
                            new_path,
                            base_hashes,
                            unique_hashes.as_deref_mut(),
                            shared_hashes.as_deref_mut(),
                            partial_nodes,
                            requested_depth,
                            traversed_depth,
                        )?;
                    }

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
                            file_node.num_bytes(),
                        );
                        partial_nodes.insert(file_path, partial_node);
                    }

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

    pub fn load_unique_children_list(
        repo: &LocalRepository,
        node: &mut MerkleTreeNode,
        current_path: &PathBuf,
        shared_hashes: &mut HashSet<MerkleHash>,
        unique_hashes: &mut HashSet<MerkleHash>,
        list_hashes: &mut HashSet<(MerkleHash, MerkleTreeNode)>,
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
        list_hashes.insert((node.hash, node.clone()));
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
                    CommitMerkleTree::load_unique_children_list(
                        repo,
                        &mut child,
                        new_path,
                        shared_hashes,
                        unique_hashes,
                        list_hashes,
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
                println!("dir: {key:?} hash: {value}");
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
                println!("node: {node}");
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
