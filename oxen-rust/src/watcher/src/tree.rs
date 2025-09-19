use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSystemTree {
    pub root: TreeNode,
    pub last_updated: SystemTime,
    pub scan_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeNode {
    pub name: String,
    pub path: PathBuf,
    pub node_type: NodeType,
    pub children: BTreeMap<String, TreeNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeType {
    Directory,
    File(FileMetadata),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileMetadata {
    pub size: u64,
    pub mtime: SystemTime,
    pub is_symlink: bool,
}

impl FileSystemTree {
    pub fn new() -> Self {
        Self {
            root: TreeNode::new_directory(".", PathBuf::from(".")),
            last_updated: SystemTime::now(),
            scan_complete: false,
        }
    }

    /// Get a node at a specific path
    pub fn get_node(&self, path: &Path) -> Option<&TreeNode> {
        if path == Path::new(".") || path == Path::new("") {
            return Some(&self.root);
        }

        let mut current = &self.root;
        for component in path.components() {
            match component {
                std::path::Component::Normal(name) => {
                    let name_str = name.to_string_lossy();
                    current = current.children.get(name_str.as_ref())?;
                }
                std::path::Component::CurDir => {}
                _ => return None,
            }
        }
        Some(current)
    }

    /// Get a mutable node at a specific path
    pub fn get_node_mut(&mut self, path: &Path) -> Option<&mut TreeNode> {
        if path == Path::new(".") || path == Path::new("") {
            return Some(&mut self.root);
        }

        let mut current = &mut self.root;
        for component in path.components() {
            match component {
                std::path::Component::Normal(name) => {
                    let name_str = name.to_string_lossy().into_owned();
                    current = current.children.get_mut(&name_str)?;
                }
                std::path::Component::CurDir => {}
                _ => return None,
            }
        }
        Some(current)
    }

    /// Update or insert a file/directory at the given path
    pub fn upsert(&mut self, path: &Path, metadata: Option<FileMetadata>) -> bool {
        // Can't insert root
        if path == Path::new(".") || path == Path::new("") {
            return false;
        }

        // Get parent path and file name
        let parent_path = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = match path.file_name() {
            Some(name) => name.to_string_lossy().into_owned(),
            None => return false,
        };

        // Ensure parent path exists (create directories as needed)
        self.ensure_path(parent_path);

        // Get the parent node
        let parent = match self.get_node_mut(parent_path) {
            Some(node) => node,
            None => return false,
        };

        // Insert or update the child
        let node_type = match metadata {
            Some(m) => NodeType::File(m),
            None => NodeType::Directory,
        };

        parent.children.insert(
            file_name.clone(),
            TreeNode {
                name: file_name,
                path: path.to_path_buf(),
                node_type,
                children: BTreeMap::new(),
            },
        );

        self.last_updated = SystemTime::now();
        true
    }

    /// Ensure a path exists, creating directories as needed
    fn ensure_path(&mut self, path: &Path) {
        if path == Path::new(".") || path == Path::new("") {
            return;
        }

        let mut current_path = PathBuf::new();
        for component in path.components() {
            match component {
                std::path::Component::Normal(name) => {
                    current_path.push(name);
                    let name_str = name.to_string_lossy().into_owned();

                    // Navigate to parent
                    let parent = if current_path.parent().is_some()
                        && current_path.parent() != Some(Path::new(""))
                    {
                        self.get_node_mut(current_path.parent().unwrap())
                    } else {
                        Some(&mut self.root)
                    };

                    if let Some(parent) = parent {
                        // Create directory if it doesn't exist
                        parent.children.entry(name_str.clone()).or_insert_with(|| {
                            TreeNode::new_directory(name_str, current_path.clone())
                        });
                    }
                }
                std::path::Component::CurDir => {}
                _ => {}
            }
        }
    }

    /// Remove a file/directory from the tree
    pub fn remove(&mut self, path: &Path) -> bool {
        if path == Path::new(".") || path == Path::new("") {
            return false;
        }

        let parent_path = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = match path.file_name() {
            Some(name) => name.to_string_lossy().into_owned(),
            None => return false,
        };

        let removed = if let Some(parent) = self.get_node_mut(parent_path) {
            parent.children.remove(&file_name).is_some()
        } else {
            false
        };
        
        if removed {
            self.last_updated = SystemTime::now();
        }
        
        removed
    }

    /// Iterate all files (depth-first)
    pub fn iter_files(&self) -> FileIterator {
        FileIterator::new(&self.root)
    }

    /// Count total nodes in tree
    pub fn count_nodes(&self) -> (usize, usize) {
        self.root.count_nodes()
    }
}

impl TreeNode {
    pub fn new_directory(name: impl Into<String>, path: PathBuf) -> Self {
        Self {
            name: name.into(),
            path,
            node_type: NodeType::Directory,
            children: BTreeMap::new(),
        }
    }

    pub fn new_file(name: impl Into<String>, path: PathBuf, metadata: FileMetadata) -> Self {
        Self {
            name: name.into(),
            path,
            node_type: NodeType::File(metadata),
            children: BTreeMap::new(),
        }
    }

    pub fn is_directory(&self) -> bool {
        matches!(self.node_type, NodeType::Directory)
    }

    pub fn is_file(&self) -> bool {
        matches!(self.node_type, NodeType::File(_))
    }

    pub fn metadata(&self) -> Option<&FileMetadata> {
        match &self.node_type {
            NodeType::File(m) => Some(m),
            NodeType::Directory => None,
        }
    }

    fn count_nodes(&self) -> (usize, usize) {
        let mut dirs = 0;
        let mut files = 0;

        match self.node_type {
            NodeType::Directory => dirs = 1,
            NodeType::File(_) => files = 1,
        }

        for child in self.children.values() {
            let (child_dirs, child_files) = child.count_nodes();
            dirs += child_dirs;
            files += child_files;
        }

        (dirs, files)
    }
}

/// Iterator over all files in the tree
pub struct FileIterator<'a> {
    stack: Vec<&'a TreeNode>,
}

impl<'a> FileIterator<'a> {
    fn new(root: &'a TreeNode) -> Self {
        let mut stack = Vec::new();
        // Add children in reverse order so we iterate in correct order
        for child in root.children.values().rev() {
            stack.push(child);
        }
        Self { stack }
    }
}

impl<'a> Iterator for FileIterator<'a> {
    type Item = (&'a PathBuf, &'a FileMetadata);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.stack.pop() {
            // Add children to stack (in reverse for correct order)
            for child in node.children.values().rev() {
                self.stack.push(child);
            }

            // Return if this is a file
            if let NodeType::File(metadata) = &node.node_type {
                return Some((&node.path, metadata));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tree() {
        let tree = FileSystemTree::new();
        assert_eq!(tree.root.name, ".");
        assert!(tree.root.is_directory());
        assert!(tree.root.children.is_empty());
        assert!(!tree.scan_complete);
    }

    #[test]
    fn test_upsert_file() {
        let mut tree = FileSystemTree::new();
        let metadata = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };

        assert!(tree.upsert(Path::new("file.txt"), Some(metadata.clone())));

        let node = tree.get_node(Path::new("file.txt")).unwrap();
        assert_eq!(node.name, "file.txt");
        assert!(node.is_file());
        assert_eq!(node.metadata(), Some(&metadata));
    }

    #[test]
    fn test_upsert_nested_file() {
        let mut tree = FileSystemTree::new();
        let metadata = FileMetadata {
            size: 200,
            mtime: SystemTime::now(),
            is_symlink: false,
        };

        assert!(tree.upsert(Path::new("dir1/dir2/file.txt"), Some(metadata.clone())));

        // Check directory structure was created
        assert!(tree.get_node(Path::new("dir1")).unwrap().is_directory());
        assert!(tree.get_node(Path::new("dir1/dir2")).unwrap().is_directory());

        // Check file exists
        let file = tree.get_node(Path::new("dir1/dir2/file.txt")).unwrap();
        assert!(file.is_file());
        assert_eq!(file.metadata(), Some(&metadata));
    }

    #[test]
    fn test_remove_file() {
        let mut tree = FileSystemTree::new();
        let metadata = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };

        tree.upsert(Path::new("file.txt"), Some(metadata));
        assert!(tree.get_node(Path::new("file.txt")).is_some());

        assert!(tree.remove(Path::new("file.txt")));
        assert!(tree.get_node(Path::new("file.txt")).is_none());
    }

    #[test]
    fn test_iter_files() {
        let mut tree = FileSystemTree::new();
        let metadata1 = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        let metadata2 = FileMetadata {
            size: 200,
            mtime: SystemTime::now(),
            is_symlink: false,
        };

        tree.upsert(Path::new("a.txt"), Some(metadata1.clone()));
        tree.upsert(Path::new("dir/b.txt"), Some(metadata2.clone()));
        tree.upsert(Path::new("dir/subdir/c.txt"), Some(metadata1.clone()));

        let files: Vec<_> = tree.iter_files().collect();
        assert_eq!(files.len(), 3);

        // Check files are returned in sorted order
        assert_eq!(files[0].0, &PathBuf::from("a.txt"));
        assert_eq!(files[1].0, &PathBuf::from("dir/b.txt"));
        assert_eq!(files[2].0, &PathBuf::from("dir/subdir/c.txt"));
    }

    #[test]
    fn test_count_nodes() {
        let mut tree = FileSystemTree::new();
        let metadata = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };

        tree.upsert(Path::new("file1.txt"), Some(metadata.clone()));
        tree.upsert(Path::new("dir1/file2.txt"), Some(metadata.clone()));
        tree.upsert(Path::new("dir1/dir2/file3.txt"), Some(metadata));

        let (dirs, files) = tree.count_nodes();
        assert_eq!(dirs, 3); // root, dir1, dir2
        assert_eq!(files, 3); // file1.txt, file2.txt, file3.txt
    }

    #[test]
    fn test_update_existing_file() {
        let mut tree = FileSystemTree::new();
        let metadata1 = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        let metadata2 = FileMetadata {
            size: 200,
            mtime: SystemTime::now(),
            is_symlink: false,
        };

        tree.upsert(Path::new("file.txt"), Some(metadata1));
        tree.upsert(Path::new("file.txt"), Some(metadata2.clone()));

        let node = tree.get_node(Path::new("file.txt")).unwrap();
        assert_eq!(node.metadata(), Some(&metadata2));
    }
}