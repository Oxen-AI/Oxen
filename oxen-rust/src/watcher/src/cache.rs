use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use log::{debug, info};
use tokio::sync::RwLock;

use crate::error::WatcherError;
use crate::tree::{FileMetadata, FileSystemTree, NodeType};
use crate::util;

#[cfg(test)]
#[path = "cache_test.rs"]
mod cache_test;

/// Filesystem metadata cache using tree structure
pub struct StatusCache {
    /// Tree structure containing all filesystem metadata
    tree: Arc<RwLock<FileSystemTree>>,
}

impl StatusCache {
    /// Create a new status cache for a repository
    pub fn new(repo_path: &Path) -> Result<Self, WatcherError> {
        // Verify it's a valid repository
        if !util::is_repository(repo_path) {
            return Err(WatcherError::RepositoryNotFound(
                repo_path.display().to_string(),
            ));
        }

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
    pub async fn update_file(
        &self,
        path: PathBuf,
        metadata: FileMetadata,
    ) -> Result<(), WatcherError> {
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub async fn get_stats(&self) -> (usize, usize, SystemTime, bool) {
        let tree = self.tree.read().await;
        let (dirs, files) = tree.count_nodes();
        (dirs, files, tree.last_updated, tree.scan_complete)
    }

    /// Clear the entire cache
    #[allow(dead_code)]
    pub async fn clear(&self) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        *tree = FileSystemTree::new();
        info!("Cache cleared");
        Ok(())
    }

    /// Batch update multiple paths in the cache
    pub async fn batch_update(
        &self,
        updates: Vec<(PathBuf, Option<FileMetadata>)>,
    ) -> Result<(), WatcherError> {
        let mut tree = self.tree.write().await;
        let update_count = updates.len();

        for (path, metadata) in updates {
            tree.upsert(&path, metadata);
        }

        info!("Batch updated {} paths in cache", update_count);
        Ok(())
    }
}
