use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::Path;

use std::path::PathBuf;

use super::*;
use crate::core::v0_19_0::index::MerkleNodeDB;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash, MerkleTreeNodeType};
use crate::util;

use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, Deserialize, Serialize)]
pub struct MerkleTreeNode {
    pub hash: MerkleHash,
    pub node: EMerkleTreeNode,
    pub parent_id: Option<MerkleHash>,
    pub children: Vec<MerkleTreeNode>,
}

impl MerkleTreeNode {
    /// Create an empty root node with a hash
    pub fn from_hash(repo: &LocalRepository, hash: &MerkleHash) -> Result<Self, OxenError> {
        let node_db = MerkleNodeDB::open_read_only(repo, hash)?;
        let parent_id = node_db.parent_id;
        Ok(MerkleTreeNode {
            hash: *hash,
            node: node_db.node()?,
            parent_id,
            children: Vec::new(),
        })
    }

    /// Check if the node is a leaf node (i.e. it has no children)
    pub fn is_leaf(&self) -> bool {
        self.children.is_empty()
    }

    /// Check if the node has children
    pub fn has_children(&self) -> bool {
        !self.children.is_empty()
    }

    /// Check if it is a file
    pub fn is_file(&self) -> bool {
        matches!(self.node, EMerkleTreeNode::File(_))
    }

    /// Check if it is a directory
    pub fn is_dir(&self) -> bool {
        matches!(self.node, EMerkleTreeNode::Directory(_))
    }

    /// Recursively count the total number of vnodes in the tree
    pub fn total_vnodes(&self) -> usize {
        let mut count = 0;
        for child in &self.children {
            if let EMerkleTreeNode::VNode(_) = child.node {
                count += 1
            }
            count += child.total_vnodes();
        }
        count
    }

    /// Count the number of vnodes a dir has (not recursive)
    pub fn num_vnodes(&self) -> u128 {
        let mut count = 0;
        for child in &self.children {
            if let EMerkleTreeNode::VNode(_) = child.node {
                count += 1
            }
        }
        count
    }

    /// Create a default DirNode with none of the metadata fields set
    pub fn default_dir() -> MerkleTreeNode {
        MerkleTreeNode {
            hash: MerkleHash::new(0),
            node: EMerkleTreeNode::Directory(DirNode::default()),
            parent_id: None,
            children: Vec::new(),
        }
    }

    /// Create a default DirNode with the given path
    pub fn default_dir_from_path(path: impl AsRef<Path>) -> MerkleTreeNode {
        let mut dir_node = DirNode::default();
        let dir_str = path.as_ref().to_str().unwrap().to_string();
        // let dir_hash = util::hasher::hash_buffer_128bit(&dir_str.as_bytes());
        dir_node.name = dir_str;
        MerkleTreeNode {
            hash: MerkleHash::new(0),
            node: EMerkleTreeNode::Directory(dir_node),
            parent_id: None,
            children: Vec::new(),
        }
    }

    /// Create a default FileNode with none of the metadata fields set
    pub fn default_file() -> MerkleTreeNode {
        MerkleTreeNode {
            hash: MerkleHash::new(0),
            node: EMerkleTreeNode::File(FileNode::default()),
            parent_id: None,
            children: Vec::new(),
        }
    }

    /// Create a MerkleTreeNode from a FileNode
    pub fn from_file(file_node: FileNode) -> MerkleTreeNode {
        MerkleTreeNode {
            hash: file_node.hash,
            node: EMerkleTreeNode::File(file_node),
            parent_id: None,
            children: Vec::new(),
        }
    }

    /// Create a MerkleTreeNode from a FileNode with the path relative to the repo
    pub fn from_file_relative_to_repo(file_node: FileNode) -> MerkleTreeNode {
        MerkleTreeNode {
            hash: file_node.hash,
            node: EMerkleTreeNode::File(file_node),
            parent_id: None,
            children: Vec::new(),
        }
    }

    /// Create a MerkleTreeNode from a DirNode
    pub fn from_dir(dir_node: DirNode) -> MerkleTreeNode {
        MerkleTreeNode {
            hash: dir_node.hash,
            node: EMerkleTreeNode::Directory(dir_node),
            parent_id: None,
            children: Vec::new(),
        }
    }

    pub fn maybe_path(&self) -> Result<PathBuf, OxenError> {
        if let EMerkleTreeNode::Directory(dir_node) = &self.node {
            return Ok(PathBuf::from(dir_node.name.clone()));
        }
        // From DEF of file_node, file_name.name == file_path to this file
        // e.g., the file 'happy' in the folder 'sad' is called 'sad//happy'
        if let EMerkleTreeNode::File(file_node) = &self.node {
            return Ok(PathBuf::from(file_node.name.clone()));
        }
        Err(OxenError::basic_str(format!(
            "MerkleTreeNode::maybe_path called on non-file or non-dir node: {:?}",
            self
        )))
    }

    /// Walk the tree
    pub fn walk_tree(&self, f: impl Fn(&MerkleTreeNode)) {
        f(self);
        for child in &self.children {
            child.walk_tree(&f);
        }
    }

    /// List all the directories in the tree
    pub fn list_dir_paths(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut dirs = Vec::new();
        let current_path = Path::new("");
        self.list_dir_paths_helper(current_path, &mut dirs)?;
        Ok(dirs)
    }

    fn list_dir_paths_helper(
        &self,
        current_path: &Path,
        dirs: &mut Vec<PathBuf>,
    ) -> Result<(), OxenError> {
        if let EMerkleTreeNode::Directory(_) = &self.node {
            dirs.push(current_path.to_path_buf());
        }
        for child in &self.children {
            if let EMerkleTreeNode::Directory(dir) = &child.node {
                let new_path = current_path.join(&dir.name);
                child.list_dir_paths_helper(&new_path, dirs)?;
            } else {
                child.list_dir_paths_helper(current_path, dirs)?;
            }
        }
        Ok(())
    }

    /// List missing file hashes
    pub fn list_missing_file_hashes(
        &self,
        repo: &LocalRepository,
    ) -> Result<HashSet<MerkleHash>, OxenError> {
        let mut missing_hashes = HashSet::new();
        for child in &self.children {
            if let EMerkleTreeNode::File(_) = &child.node {
                // Check if the file exists in the versions directory
                let file_path = util::fs::version_path_from_hash(repo, child.hash.to_string());
                if !file_path.exists() {
                    missing_hashes.insert(child.hash);
                }
            }
        }
        Ok(missing_hashes)
    }

    /// Get all files and dirs in a directory
    pub fn get_all_children(&self) -> Result<Vec<MerkleTreeNode>, OxenError> {
        let mut children = Vec::new();
        for child in &self.children {
            children.push(child.clone());
            if let EMerkleTreeNode::Directory(_) = &child.node {
                children.extend(child.get_all_children()?);
            }
        }
        Ok(children)
    }

    /// Get all the vnodes for a given directory
    pub fn get_vnodes_for_dir(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let Some(node) = self.get_by_path(path)? else {
            return Err(OxenError::basic_str(format!(
                "Merkle tree directory not found: '{:?}'",
                path
            )));
        };

        if MerkleTreeNodeType::Dir != node.node.dtype() {
            return Err(OxenError::basic_str(format!(
                "Merkle tree node is not a directory: '{:?}'",
                path
            )));
        }

        let mut vnodes = Vec::new();
        for child in &node.children {
            if let EMerkleTreeNode::VNode(_) = &child.node {
                vnodes.push(child.clone());
            }
        }
        Ok(vnodes)
    }

    /// Search for a file node by path
    pub fn get_by_path(&self, path: impl AsRef<Path>) -> Result<Option<MerkleTreeNode>, OxenError> {
        let path = path.as_ref();
        let traversed_path = Path::new("");
        self.get_by_path_helper(traversed_path, path)
    }

    fn get_by_path_helper(
        &self,
        traversed_path: &Path,
        path: &Path,
    ) -> Result<Option<MerkleTreeNode>, OxenError> {
        // log::debug!(
        //     "get_by_path_helper {} traversed_path: {:?} path: {:?}",
        //     self,
        //     traversed_path,
        //     path
        // );
        if traversed_path.components().count() > path.components().count() {
            // log::debug!(
            //     "get_by_path_helper {} returning None traversed_path {:?} is longer than path {:?}",
            //     self,
            //     traversed_path,
            //     path
            // );
            return Ok(None);
        }

        if let EMerkleTreeNode::File(_) = &self.node {
            let file_node = self.file()?;
            let file_path = traversed_path.join(file_node.name);
            // log::debug!(
            //     "get_by_path_helper {} is file! [{:?}] {:?} {:?}",
            //     self,
            //     self.node.dtype(),
            //     file_path,
            //     path
            // );
            if file_path == path {
                return Ok(Some(self.clone()));
            }
        }

        if let EMerkleTreeNode::Directory(_) = &self.node {
            // log::debug!(
            //     "get_by_path_helper {} is dir! {:?} {:?}",
            //     self,
            //     traversed_path,
            //     path
            // );
            if traversed_path == path {
                return Ok(Some(self.clone()));
            }
        }

        if let EMerkleTreeNode::VNode(_) = &self.node {
            // Binary search implementation
            let target_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            match self.children.binary_search_by(|child| {
                // log::debug!(
                //     "get_by_path_helper {} binary_search_by child: {}",
                //     self,
                //     child
                // );
                let child_name = match &child.node {
                    EMerkleTreeNode::Directory(dir) => Some(dir.name.to_owned()),
                    EMerkleTreeNode::File(file) => Some(file.name.to_owned()),
                    _ => None,
                };
                // log::debug!(
                //     "get_by_path_helper {} binary_search_by child_name: {:?} target_name: {:?}",
                //     self,
                //     child_name,
                //     target_name
                // );
                child_name.unwrap_or("".to_string()).cmp(&target_name)
            }) {
                Ok(index) => {
                    let child = &self.children[index];
                    // log::debug!(
                    //     "get_by_path_helper {} found index: {} child: {}",
                    //     self,
                    //     index,
                    //     child
                    // );
                    if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                        // log::debug!(
                        //     "get_by_path_helper {} traversing dir child: {}",
                        //     self,
                        //     dir_node
                        // );
                        if let Some(node) =
                            child.get_by_path_helper(&traversed_path.join(&dir_node.name), path)?
                        {
                            return Ok(Some(node));
                        }
                    } else {
                        // log::debug!(
                        //     "get_by_path_helper {} traversing other child: {}",
                        //     self,
                        //     child
                        // );
                        if let Some(node) = child.get_by_path_helper(traversed_path, path)? {
                            return Ok(Some(node));
                        }
                    }
                }
                Err(err) => {
                    // If the value is not found then Result::Err is returned,
                    // containing the index where a matching element could be inserted while maintaining sorted order.
                    log::debug!(
                        "get_by_path_helper {} could not find path: {:?} possible insert point: {:?}",
                        self,
                        target_name,
                        err
                    );
                }
            }
        }

        // log::debug!(
        //     "get_by_path_helper {} traversing children {}",
        //     self,
        //     self.children.len()
        // );
        for child in &self.children {
            // log::debug!("get_by_path_helper {} traversing child: {}", self, child);
            if let EMerkleTreeNode::Directory(dir_node) = &child.node {
                if let Some(node) =
                    child.get_by_path_helper(&traversed_path.join(&dir_node.name), path)?
                {
                    return Ok(Some(node));
                }
            } else if let Some(node) = child.get_by_path_helper(traversed_path, path)? {
                return Ok(Some(node));
            }
        }
        // log::debug!(
        //     "get_by_path_helper {} returning None for path: {:?}",
        //     self,
        //     path
        // );
        Ok(None)
    }

    pub fn to_node(&self) -> EMerkleTreeNode {
        self.node.clone()
    }

    pub fn deserialize_id(data: &[u8], dtype: MerkleTreeNodeType) -> Result<MerkleHash, OxenError> {
        match dtype {
            MerkleTreeNodeType::Commit => CommitNode::deserialize(data).map(|commit| commit.hash),
            MerkleTreeNodeType::VNode => VNode::deserialize(data).map(|vnode| vnode.hash),
            MerkleTreeNodeType::Dir => DirNode::deserialize(data).map(|dir| dir.hash),
            MerkleTreeNodeType::File => FileNode::deserialize(data).map(|file| file.hash),
            MerkleTreeNodeType::FileChunk => {
                FileChunkNode::deserialize(data).map(|file_chunk| file_chunk.hash)
            }
            MerkleTreeNodeType::Schema => SchemaNode::deserialize(data).map(|schema| schema.hash),
        }
    }

    pub fn commit(&self) -> Result<CommitNode, OxenError> {
        if let EMerkleTreeNode::Commit(commit_node) = &self.node {
            Ok(commit_node.clone())
        } else {
            Err(OxenError::basic_str(
                "MerkleTreeNode::commit called on non-commit node",
            ))
        }
    }

    pub fn vnode(&self) -> Result<VNode, OxenError> {
        if let EMerkleTreeNode::VNode(vnode) = &self.node {
            Ok(vnode.clone())
        } else {
            Err(OxenError::basic_str(
                "MerkleTreeNode::vnode called on non-vnode node",
            ))
        }
    }

    pub fn dir(&self) -> Result<DirNode, OxenError> {
        if let EMerkleTreeNode::Directory(dir_node) = &self.node {
            Ok(dir_node.clone())
        } else {
            Err(OxenError::basic_str(
                "MerkleTreeNode::dir called on non-dir node",
            ))
        }
    }

    pub fn file(&self) -> Result<FileNode, OxenError> {
        if let EMerkleTreeNode::File(file_node) = &self.node {
            Ok(file_node.clone())
        } else {
            Err(OxenError::basic_str(
                "MerkleTreeNode::file called on non-file node",
            ))
        }
    }

    pub fn file_chunk(&self) -> Result<FileChunkNode, OxenError> {
        if let EMerkleTreeNode::FileChunk(file_chunk_node) = &self.node {
            Ok(file_chunk_node.clone())
        } else {
            Err(OxenError::basic_str(
                "MerkleTreeNode::file_chunk called on non-file_chunk node",
            ))
        }
    }

    pub fn schema(&self) -> Result<SchemaNode, OxenError> {
        if let EMerkleTreeNode::Schema(schema_node) = &self.node {
            Ok(schema_node.clone())
        } else {
            Err(OxenError::basic_str(
                "MerkleTreeNode::schema called on non-schema node",
            ))
        }
    }
}

/// Debug is used for verbose multi-line output with println!("{:?}", node)
impl fmt::Debug for MerkleTreeNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "[{:?}]", self.node.dtype())?;
        writeln!(f, "hash: {}", self.hash)?;
        writeln!(f, "node: {:?}", self.node)?;
        writeln!(
            f,
            "parent_id: {}",
            self.parent_id
                .map_or("None".to_string(), |id| id.to_string())
        )?;
        writeln!(f, "children.len(): {}", self.children.len())?;
        writeln!(f, "=============")?;
        writeln!(f, "{}", self)?;
        writeln!(f, "=============")?;

        for child in &self.children {
            writeln!(f, "{}", child)?;
        }
        Ok(())
    }
}

impl Default for MerkleTreeNode {
    fn default() -> Self {
        Self::default_dir()
    }
}

/// Display is used for single line output with println!("{}", node)
impl fmt::Display for MerkleTreeNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.node {
            EMerkleTreeNode::Commit(commit) => {
                write!(f, "[{:?}] {} {}", self.node.dtype(), self.hash, commit)
            }
            EMerkleTreeNode::VNode(vnode) => {
                write!(
                    f,
                    "[{:?}] {} {} ({} children)",
                    self.node.dtype(),
                    self.hash,
                    vnode,
                    self.children.len()
                )
            }
            EMerkleTreeNode::Directory(dir) => {
                write!(
                    f,
                    "[{:?}] {} {} ({} children)",
                    self.node.dtype(),
                    self.hash,
                    dir,
                    self.children.len()
                )
            }
            EMerkleTreeNode::File(file) => {
                write!(f, "[{:?}] {} {}", self.node.dtype(), self.hash, file)
            }
            EMerkleTreeNode::FileChunk(file_chunk) => {
                write!(f, "[{:?}] {} {}", self.node.dtype(), self.hash, file_chunk)
            }
            EMerkleTreeNode::Schema(schema) => {
                write!(f, "[{:?}] {} {}", self.node.dtype(), self.hash, schema)
            }
        }
    }
}

impl PartialEq for MerkleTreeNode {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Hash for MerkleTreeNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.to_u128().hash(state);
        if let Ok(path) = self.maybe_path() {
            path.hash(state);
        }
    }
}
