use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use liboxen::model::LocalRepository;
use log::info;
use tokio::sync::RwLock;

use crate::error::WatcherError;
use crate::protocol::{FileStatus, FileStatusType, StatusResult};

#[path = "cache_test.rs"]
mod cache_test;

/// Memory-only status cache for fast access
pub struct StatusCache {
    /// In-memory cache
    cache: Arc<RwLock<MemoryCache>>,
}

/// In-memory cache data structure
struct MemoryCache {
    /// Files created since watcher started
    created: HashMap<PathBuf, FileStatus>,
    /// Files modified since watcher started
    modified: HashMap<PathBuf, FileStatus>,
    /// Files removed since watcher started
    removed: HashMap<PathBuf, FileStatus>,
    /// Whether initial scan is complete
    scan_complete: bool,
    /// Last update time
    last_update: SystemTime,
}

impl MemoryCache {
    /// Helper function to update cache for a single file status
    fn update_single_status(&mut self, status: FileStatus) {
        match status.status {
            FileStatusType::Created => {
                self.created.insert(status.path.clone(), status.clone());
                // If a file is created, it's no longer modified or removed
                self.modified.remove(&status.path);
                self.removed.remove(&status.path);
            }
            FileStatusType::Modified => {
                self.modified.insert(status.path.clone(), status.clone());
                // A modified file might have been previously created, keep that status
                // But it's definitely not removed
                self.removed.remove(&status.path);
            }
            FileStatusType::Removed => {
                self.removed.insert(status.path.clone(), status.clone());
                // If removed, clear from created and modified
                self.created.remove(&status.path);
                self.modified.remove(&status.path);
            }
        }
    }
}

impl StatusCache {
    /// Create a new status cache for a repository
    pub fn new(repo_path: &Path) -> Result<Self, WatcherError> {
        // Verify it's a valid repository
        let _repo = LocalRepository::from_dir(repo_path)?;

        // Initialize memory cache
        let cache = Arc::new(RwLock::new(MemoryCache {
            created: HashMap::new(),
            modified: HashMap::new(),
            removed: HashMap::new(),
            scan_complete: false,
            last_update: SystemTime::now(),
        }));

        Ok(Self { cache })
    }

    /// Get the current status, optionally filtered by paths
    pub async fn get_status(&self, paths: Option<Vec<PathBuf>>) -> StatusResult {
        let cache = self.cache.read().await;

        // Filter by paths if requested
        let (created, modified, removed) = if let Some(paths) = paths {
            let path_set: std::collections::HashSet<_> = paths.iter().collect();

            (
                cache
                    .created
                    .values()
                    .filter(|f| path_set.contains(&f.path))
                    .cloned()
                    .collect(),
                cache
                    .modified
                    .values()
                    .filter(|f| path_set.contains(&f.path))
                    .cloned()
                    .collect(),
                cache
                    .removed
                    .keys()
                    .filter(|p| path_set.contains(p))
                    .cloned()
                    .collect(),
            )
        } else {
            (
                cache.created.values().cloned().collect(),
                cache.modified.values().cloned().collect(),
                cache.removed.keys().cloned().collect(),
            )
        };

        if !cache.scan_complete {
            info!("Scan not complete");
        }

        StatusResult {
            created,
            modified,
            removed,
            scan_complete: cache.scan_complete,
        }
    }

    /// Update a file's status in the cache
    #[allow(dead_code)] // Used in tests
    pub async fn update_file_status(&self, status: FileStatus) -> Result<(), WatcherError> {
        let mut cache = self.cache.write().await;
        cache.update_single_status(status);
        cache.last_update = SystemTime::now();
        Ok(())
    }

    /// Batch update multiple file statuses
    pub async fn batch_update(&self, statuses: Vec<FileStatus>) -> Result<(), WatcherError> {
        let mut cache = self.cache.write().await;
        
        for status in statuses {
            cache.update_single_status(status);
        }
        
        cache.last_update = SystemTime::now();
        Ok(())
    }

    /// Mark the initial scan as complete
    pub async fn mark_scan_complete(&self) -> Result<(), WatcherError> {
        let mut cache = self.cache.write().await;
        cache.scan_complete = true;
        Ok(())
    }

    /// Clear the entire cache
    #[allow(dead_code)] // Used in tests
    pub async fn clear(&self) -> Result<(), WatcherError> {
        let mut cache = self.cache.write().await;
        cache.created.clear();
        cache.modified.clear();
        cache.removed.clear();
        cache.scan_complete = false;
        cache.last_update = SystemTime::now();
        Ok(())
    }
}
