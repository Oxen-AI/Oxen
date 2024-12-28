use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded};

use crate::constants::{DIR_HASHES_DIR, HISTORY_DIR};
use crate::core::db;

use crate::core::v0_19_0::index::MerkleNodeDB;

use crate::model::merkle_tree::node::EMerkleTreeNode;

use crate::model::merkle_tree::node::{FileNode, MerkleTreeNode};

use crate::error::OxenError;
use crate::model::Commit;
use crate::model::{LocalRepository, MerkleHash, MerkleTreeNodeType};

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

    pub fn from_commit_or_subtree(
        repo: &LocalRepository,
        commit: &Commit,
    ) -> Result<Self, OxenError> {
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
                CommitMerkleTree::from_path_depth(repo, commit, &subtree_paths[0], depth)?.root
            }
            _ => {
                CommitMerkleTree::read_node(repo, &node_hash, true)?.ok_or(OxenError::basic_str(
                    format!("Merkle tree hash not found for commit: '{}'", commit.id),
                ))?
            }
        };

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
    ) -> Result<Self, OxenError> {
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
        Ok(Self { root, dir_hashes })
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
                    "Merkle tree hash not found for parent: '{}'",
                    node_path.to_str().unwrap()
                )),
            )?
        } else {
            // We are skipping to a file in the tree using the dir_hashes db
            log::debug!("Look up file 📄 {:?}", node_path);
            CommitMerkleTree::read_file(repo, &dir_hashes, node_path)?.ok_or(
                OxenError::basic_str(format!(
                    "Merkle tree hash not found for parent: '{}'",
                    node_path.to_str().unwrap()
                )),
            )?
        };
        Ok(Self { root, dir_hashes })
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

    pub fn dir_with_children(
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
        let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;
        CommitMerkleTree::read_children_from_node(repo, &mut node_db, &mut node, recurse)?;
        // log::debug!("read_node done: {:?} recurse: {}", node.hash, recurse);
        Ok(Some(node))
    }

    pub fn read_depth(
        repo: &LocalRepository,
        hash: &MerkleHash,
        depth: i32,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        log::debug!("Read depth {} node hash [{}]", depth, hash);
        if !MerkleNodeDB::exists(repo, hash) {
            log::debug!(
                "read_depth merkle node db does not exist for hash: {}",
                hash
            );
            return Ok(None);
        }

        let mut node = MerkleTreeNode::from_hash(repo, hash)?;
        let mut node_db = MerkleNodeDB::open_read_only(repo, hash)?;

        CommitMerkleTree::read_children_until_depth(repo, &mut node_db, &mut node, depth, 0)?;
        log::debug!("Read depth {} node done: {:?}", depth, node.hash);
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
        log::debug!(
            "read {} dir_hashes from commit: {}",
            dir_hashes.len(),
            commit
        );
        Ok(dir_hashes)
    }

    pub fn load_nodes(
        repo: &LocalRepository,
        commit: &Commit,
        paths: &[PathBuf],
    ) -> Result<HashMap<PathBuf, MerkleTreeNode>, OxenError> {
        let dir_hashes = CommitMerkleTree::dir_hashes(repo, commit)?;
        // log::debug!(
        //     "load_nodes dir_hashes from commit: {} count: {}",
        //     commit,
        //     dir_hashes.len()
        // );
        // for (path, hash) in &dir_hashes {
        //     log::debug!("load_nodes dir_hashes path: {:?} hash: {:?}", path, hash);
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
        // println!("Path for has_dir: {path:?}");
        // println!("Dir hashes: {:?}", self.dir_hashes);
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

    pub fn dir_files_and_folders(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let node = self
            .root
            .get_by_path(path)?
            .ok_or(OxenError::basic_str(format!(
                "Merkle tree hash when looking up dir children: '{:?}'",
                path
            )))?;

        CommitMerkleTree::node_files_and_folders(&node)
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

    pub fn node_files_and_folders(node: &MerkleTreeNode) -> Result<Vec<MerkleTreeNode>, OxenError> {
        if MerkleTreeNodeType::Dir != node.node.node_type() {
            return Err(OxenError::basic_str(format!(
                "Merkle tree node is not a directory: '{:?}'",
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

    /// Get the root directory node given a commit node
    pub fn get_root_dir_from_commit(node: &MerkleTreeNode) -> Result<&MerkleTreeNode, OxenError> {
        if node.node.node_type() != MerkleTreeNodeType::Commit {
            return Err(OxenError::basic_str(format!(
                "Expected a commit node, but got: '{:?}'",
                node.node.node_type()
            )));
        }

        // A commit node should have exactly one child, which is the root directory
        if node.children.len() != 1 {
            return Err(OxenError::basic_str(
                "Commit node should have exactly one child (root directory)",
            ));
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

    pub fn dir_entries_with_paths(
        node: &MerkleTreeNode,
        base_path: &PathBuf,
    ) -> Result<HashSet<(FileNode, PathBuf)>, OxenError> {
        let mut entries = HashSet::new();

        match &node.node {
            EMerkleTreeNode::Directory(_)
            | EMerkleTreeNode::VNode(_)
            | EMerkleTreeNode::Commit(_) => {
                for child in &node.children {
                    match &child.node {
                        EMerkleTreeNode::File(file_node) => {
                            let file_path = base_path.join(&file_node.name);
                            entries.insert((file_node.clone(), file_path));
                        }
                        EMerkleTreeNode::Directory(dir_node) => {
                            let new_base_path = base_path.join(&dir_node.name);
                            entries.extend(Self::dir_entries_with_paths(child, &new_base_path)?);
                        }
                        EMerkleTreeNode::VNode(_vnode) => {
                            entries.extend(Self::dir_entries_with_paths(child, base_path)?);
                        }
                        _ => {}
                    }
                }
            }
            EMerkleTreeNode::File(file_node) => {
                let file_path = base_path.join(&file_node.name);
                entries.insert((file_node.clone(), file_path));
            }
            _ => {
                return Err(OxenError::basic_str(format!(
                    "Unexpected node type: {:?}",
                    node.node.node_type()
                )))
            }
        }

        Ok(entries)
    }

    /// This uses the dir_hashes db to skip right to a file in the tree
    pub fn read_file(
        repo: &LocalRepository,
        dir_hashes: &HashMap<PathBuf, MerkleHash>,
        path: impl AsRef<Path>,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // Get the directory from the path
        let path = path.as_ref();
        let parent_path = path.parent().unwrap();
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
            return Ok(None);
        };

        // log::debug!("read_file parent node_hash: {:?}", node_hash);

        // Read the directory node at depth 0 to get all the vnodes
        let merkle_node = CommitMerkleTree::read_depth(repo, &node_hash, 0)?;
        let Some(merkle_node) = merkle_node else {
            return Ok(None);
        };

        // log::debug!("read_file merkle_node: {:?}", merkle_node);

        let vnodes = merkle_node.children;

        // log::debug!("read_file vnodes: {}", vnodes.len());

        // Calculate the total number of children in the vnodes
        // And use this to skip to the correct vnode
        let total_children = vnodes
            .iter()
            .map(|vnode| vnode.vnode().unwrap().num_entries.unwrap())
            .sum::<u64>();
        let vnode_size = repo.vnode_size();
        let num_vnodes = (total_children as f32 / vnode_size as f32).ceil() as u128;

        // log::debug!("read_file total_children: {}", total_children);
        // log::debug!("read_file vnode_size: {}", vnode_size);
        // log::debug!("read_file num_vnodes: {}", num_vnodes);

        // Calculate the bucket to skip to based on the path and the number of vnodes
        let bucket = hasher::hash_buffer_128bit(path.to_str().unwrap().as_bytes()) % num_vnodes;

        // log::debug!("read_file bucket: {}", bucket);

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
        node_db: &mut MerkleNodeDB,
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

        let children: Vec<(MerkleHash, MerkleTreeNode)> = node_db.map()?;
        log::debug!(
            "read_children_until_depth requested_depth {} traversed_depth {} Got {} children",
            requested_depth,
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
                // Directories, VNodes, and Files have children
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
                        if let Ok(mut node_db) = MerkleNodeDB::open_read_only(repo, &child.hash) {
                            CommitMerkleTree::read_children_until_depth(
                                repo,
                                &mut node_db,
                                &mut child,
                                requested_depth,
                                traversed_depth,
                            )?;
                        }
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
        node_db: &mut MerkleNodeDB,
        node: &mut MerkleTreeNode,
        recurse: bool,
    ) -> Result<(), OxenError> {
        let dtype = node.node.node_type();
        log::debug!(
            "read_children_from_node tree_db_dir: {:?} dtype {:?} recurse {}",
            node_db.path(),
            dtype,
            recurse
        );

        if dtype != MerkleTreeNodeType::Commit
            && dtype != MerkleTreeNodeType::Dir
            && dtype != MerkleTreeNodeType::VNode
            || !recurse
        {
            return Ok(());
        }

        let children: Vec<(MerkleHash, MerkleTreeNode)> = node_db.map()?;
        log::debug!("read_children_from_node Got {} children", children.len());

        for (_key, child) in children {
            let mut child = child.to_owned();
            // log::debug!("read_children_from_node child: {} -> {}", key, child);
            match &child.node.node_type() {
                // Directories, VNodes, and Files have children
                MerkleTreeNodeType::Commit
                | MerkleTreeNodeType::Dir
                | MerkleTreeNodeType::VNode => {
                    if recurse {
                        // log::debug!("read_children_from_node recurse: {:?}", child.hash);
                        let Ok(mut node_db) = MerkleNodeDB::open_read_only(repo, &child.hash)
                        else {
                            log::warn!("no child node db: {:?}", child.hash);
                            return Ok(());
                        };
                        // log::debug!("read_children_from_node opened node_db: {:?}", child.hash);
                        CommitMerkleTree::read_children_from_node(
                            repo,
                            &mut node_db,
                            &mut child,
                            recurse,
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

        // log::debug!("read_children_from_node done: {:?}", node.hash);

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

    use crate::core::v0_19_0::index::CommitMerkleTree;
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::model::MerkleTreeNodeType;
    use crate::repositories;
    use crate::test;
    use crate::test::add_n_files_m_dirs;

    #[test]
    fn test_load_dir_nodes() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate the correct version of the repo
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Write data to the repo
            add_n_files_m_dirs(&repo, 10, 3)?;
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
            let loaded_nodes = CommitMerkleTree::load_nodes(&repo, &commit, &paths_to_load)?;

            println!("loaded {} nodes", loaded_nodes.len());
            for (_, node) in loaded_nodes {
                println!("node: {}", node);
                CommitMerkleTree::print_node_depth(&node, 1);
                assert!(node.node.node_type() == MerkleTreeNodeType::Dir);
                assert!(node.parent_id.is_some());
                assert!(!node.children.is_empty());
                let dir = node.dir().unwrap();
                assert!(dir.num_files() > 0);
            }

            Ok(())
        })
    }
}
