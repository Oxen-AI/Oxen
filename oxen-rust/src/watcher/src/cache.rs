use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use liboxen::model::LocalRepository;
use log::{debug, info};
use tokio::sync::RwLock;

use crate::error::WatcherError;
use crate::tree::{FileMetadata, FileSystemTree, NodeType};


/// Filesystem metadata cache using tree structure
pub struct StatusCache {
    /// Tree structure containing all filesystem metadata
    tree: Arc<RwLock<FileSystemTree>>,
}

impl StatusCache {
    /// Create a new status cache for a repository
    pub fn new(repo_path: &Path) -> Result<Self, WatcherError> {
        // Verify it's a valid repository
        let _repo = LocalRepository::from_dir(repo_path)?;

        Ok(Self {
            tree: Arc::new(RwLock::new(FileSystemTree::new())),
        })
    }

    /// Get the entire tree or a subtree at a specific path
    pub async fn get_tree(&self, path: Option<&Path>) -> Option<FileSystemTree> {
        let tree = self.tree.read().await;
        
        if let Some(path) = path {
            // Return a subtree rooted at the specified path
            if let Some(node) = tree.get_node(path) {
                // Create a new tree with this node as root
                let mut subtree = FileSystemTree::new();
                subtree.root = node.clone();
                subtree.last_updated = tree.last_updated;
                subtree.scan_complete = tree.scan_complete;
                Some(subtree)
            } else {
                None
            }
        } else {
            Some(tree.clone())
        }
    }

    /// Update a single file's metadata
    pub async fn update_file(&self, path: PathBuf, metadata: FileMetadata) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        tree.upsert(&path, Some(metadata));
        debug!("Updated file metadata for: {:?}", path);
        Ok(())
    }

    /// Update a directory (no metadata)
    pub async fn update_directory(&self, path: PathBuf) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        tree.upsert(&path, None);
        debug!("Updated directory: {:?}", path);
        Ok(())
    }

    /// Remove a file or directory from the tree
    pub async fn remove_path(&self, path: PathBuf) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        if tree.remove(&path) {
            debug!("Removed path from cache: {:?}", path);
        }
        Ok(())
    }


    /// Mark the initial scan as complete
    pub async fn mark_scan_complete(&self) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        tree.scan_complete = true;
        info!("Initial scan marked as complete");
        Ok(())
    }

    /// Check if initial scan is complete
    pub async fn is_scan_complete(&self) -> bool {
        let tree = self.tree.read().await;
        tree.scan_complete
    }

    /// Get metadata for specific paths
    pub async fn get_metadata(&self, paths: &[PathBuf]) -> Vec<(PathBuf, Option<FileMetadata>)> {
        let tree = self.tree.read().await;
        let mut result = Vec::new();
        
        for path in paths {
            if let Some(node) = tree.get_node(path) {
                match &node.node_type {
                    NodeType::File(metadata) => {
                        result.push((path.clone(), Some(metadata.clone())));
                    }
                    NodeType::Directory => {
                        result.push((path.clone(), None));
                    }
                }
            }
        }
        
        result
    }

    /// Get statistics about the cache
    pub async fn get_stats(&self) -> (usize, usize, SystemTime, bool) {
        let tree = self.tree.read().await;
        let (dirs, files) = tree.count_nodes();
        (dirs, files, tree.last_updated, tree.scan_complete)
    }

    /// Clear the entire cache
    pub async fn clear(&self) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        *tree = FileSystemTree::new();
        info!("Cache cleared");
        Ok(())
    }

    // Compatibility method for existing code - will be removed in Phase 4
    pub async fn batch_update(&self, updates: Vec<crate::protocol::FileStatus>) -> Result<(), WatcherError> {
        let converted: Vec<(PathBuf, Option<FileMetadata>)> = updates
            .into_iter()
            .map(|status| {
                let metadata = FileMetadata {
                    size: status.size,
                    mtime: status.mtime,
                    is_symlink: false, // We don't track this in the old format
                };
                (status.path, Some(metadata))
            })
            .collect();
        
        self.batch_update_new(converted).await
    }
    
    // New batch update method that will replace the above
    pub async fn batch_update_new(&self, updates: Vec<(PathBuf, Option<FileMetadata>)>) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        let update_count = updates.len();
        
        for (path, metadata) in updates {
            tree.upsert(&path, metadata);
        }
        
        info!("Batch updated {} paths in cache", update_count);
        Ok(())
    }
    
    // Compatibility: get_status for old protocol
    pub async fn get_status(&self, paths: Option<Vec<PathBuf>>) -> crate::protocol::StatusResult {
        use crate::protocol::{FileStatus, FileStatusType, StatusResult};
        
        let tree = self.tree.read().await;
        let mut created = Vec::new();
        let modified = Vec::new();
        let removed = Vec::new();
        
        // For now, treat all files as "created" since we don't track state changes
        // This will be removed when we update the protocol
        for (path, metadata) in tree.iter_files() {
            let status = FileStatus {
                path: path.clone(),
                mtime: metadata.mtime,
                size: metadata.size,
                hash: None,
                status: FileStatusType::Created,
            };
            
            if let Some(ref filter_paths) = paths {
                if filter_paths.contains(path) {
                    created.push(status);
                }
            } else {
                created.push(status);
            }
        }
        
        StatusResult {
            created,
            modified,
            removed,
            scan_complete: tree.scan_complete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    async fn setup_test_cache() -> (StatusCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();
        
        // Initialize a test repository
        liboxen::repositories::init::init(repo_path).unwrap();
        
        let cache = StatusCache::new(repo_path).unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_cache_creation() {
        let (cache, _temp_dir) = setup_test_cache().await;
        assert!(!cache.is_scan_complete().await);
        
        let (dirs, files, _, complete) = cache.get_stats().await;
        assert_eq!(dirs, 1); // Just root
        assert_eq!(files, 0);
        assert!(!complete);
    }

    #[tokio::test]
    async fn test_update_file() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let metadata = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        
        cache.update_file(PathBuf::from("test.txt"), metadata.clone()).await.unwrap();
        
        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(Path::new("test.txt")).unwrap();
        assert_eq!(node.metadata(), Some(&metadata));
    }

    #[tokio::test]
    async fn test_batch_update() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let updates = vec![
            (PathBuf::from("file1.txt"), Some(FileMetadata {
                size: 100,
                mtime: SystemTime::now(),
                is_symlink: false,
            })),
            (PathBuf::from("dir1"), None),
            (PathBuf::from("dir1/file2.txt"), Some(FileMetadata {
                size: 200,
                mtime: SystemTime::now(),
                is_symlink: false,
            })),
        ];
        
        cache.batch_update_new(updates).await.unwrap();
        
        let (dirs, files, _, _) = cache.get_stats().await;
        assert_eq!(dirs, 2); // root + dir1
        assert_eq!(files, 2); // file1.txt + file2.txt
    }

    #[tokio::test]
    async fn test_remove_path() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let metadata = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        
        cache.update_file(PathBuf::from("test.txt"), metadata).await.unwrap();
        assert!(cache.get_tree(None).await.unwrap().get_node(Path::new("test.txt")).is_some());
        
        cache.remove_path(PathBuf::from("test.txt")).await.unwrap();
        assert!(cache.get_tree(None).await.unwrap().get_node(Path::new("test.txt")).is_none());
    }

    #[tokio::test]
    async fn test_get_subtree() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let updates = vec![
            (PathBuf::from("dir1/file1.txt"), Some(FileMetadata {
                size: 100,
                mtime: SystemTime::now(),
                is_symlink: false,
            })),
            (PathBuf::from("dir1/subdir/file2.txt"), Some(FileMetadata {
                size: 200,
                mtime: SystemTime::now(),
                is_symlink: false,
            })),
            (PathBuf::from("dir2/file3.txt"), Some(FileMetadata {
                size: 300,
                mtime: SystemTime::now(),
                is_symlink: false,
            })),
        ];
        
        cache.batch_update_new(updates).await.unwrap();
        
        // Get subtree for dir1
        let subtree = cache.get_tree(Some(Path::new("dir1"))).await.unwrap();
        
        // Subtree should contain only dir1's contents
        let files: Vec<_> = subtree.iter_files().collect();
        assert_eq!(files.len(), 2); // file1.txt and file2.txt
    }

    #[tokio::test]
    async fn test_get_metadata() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let metadata1 = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        let metadata2 = FileMetadata {
            size: 200,
            mtime: SystemTime::now(),
            is_symlink: true,
        };
        
        cache.update_file(PathBuf::from("file1.txt"), metadata1.clone()).await.unwrap();
        cache.update_file(PathBuf::from("file2.txt"), metadata2.clone()).await.unwrap();
        cache.update_directory(PathBuf::from("dir1")).await.unwrap();
        
        let paths = vec![
            PathBuf::from("file1.txt"),
            PathBuf::from("file2.txt"),
            PathBuf::from("dir1"),
            PathBuf::from("nonexistent.txt"),
        ];
        
        let results = cache.get_metadata(&paths).await;
        
        assert_eq!(results.len(), 3); // nonexistent.txt not included
        assert_eq!(results[0].0, PathBuf::from("file1.txt"));
        assert_eq!(results[0].1, Some(metadata1));
        assert_eq!(results[1].0, PathBuf::from("file2.txt"));
        assert_eq!(results[1].1, Some(metadata2));
        assert_eq!(results[2].0, PathBuf::from("dir1"));
        assert_eq!(results[2].1, None); // Directory has no metadata
    }

    #[tokio::test]
    async fn test_mark_scan_complete() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        assert!(!cache.is_scan_complete().await);
        cache.mark_scan_complete().await.unwrap();
        assert!(cache.is_scan_complete().await);
    }
}