use crate::error::WatcherError;
use crate::protocol::{FileStatus, FileStatusType, StatusResult};
use liboxen::model::LocalRepository;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

/// Memory-only status cache for fast access
pub struct StatusCache {
    repo: LocalRepository,
    /// In-memory cache
    cache: Arc<RwLock<MemoryCache>>,
}

/// In-memory cache data structure
struct MemoryCache {
    modified: HashMap<PathBuf, FileStatus>,
    added: HashMap<PathBuf, FileStatus>,
    removed: HashMap<PathBuf, FileStatus>,
    untracked: HashMap<PathBuf, FileStatus>,
    scan_complete: bool,
    last_update: SystemTime,
}

impl StatusCache {
    /// Create a new status cache for a repository
    pub fn new(repo_path: &Path) -> Result<Self, WatcherError> {
        let repo = LocalRepository::from_dir(repo_path)?;
        
        // Initialize memory cache
        let cache = Arc::new(RwLock::new(MemoryCache {
            modified: HashMap::new(),
            added: HashMap::new(),
            removed: HashMap::new(),
            untracked: HashMap::new(),
            scan_complete: false,
            last_update: SystemTime::now(),
        }));
        
        Ok(Self { repo, cache })
    }
    
    /// Get the current status, optionally filtered by paths
    pub async fn get_status(&self, paths: Option<Vec<PathBuf>>) -> StatusResult {
        let cache = self.cache.read().await;
        
        // Filter by paths if requested
        let (modified, added, removed, untracked) = if let Some(paths) = paths {
            let path_set: std::collections::HashSet<_> = paths.iter().collect();
            
            (
                cache.modified.values()
                    .filter(|f| path_set.contains(&f.path))
                    .cloned()
                    .collect(),
                cache.added.values()
                    .filter(|f| path_set.contains(&f.path))
                    .cloned()
                    .collect(),
                cache.removed.keys()
                    .filter(|p| path_set.contains(p))
                    .cloned()
                    .collect(),
                cache.untracked.keys()
                    .filter(|p| path_set.contains(p))
                    .cloned()
                    .collect(),
            )
        } else {
            (
                cache.modified.values().cloned().collect(),
                cache.added.values().cloned().collect(),
                cache.removed.keys().cloned().collect(),
                cache.untracked.keys().cloned().collect(),
            )
        };
        
        StatusResult {
            modified,
            added,
            removed,
            untracked,
            scan_complete: cache.scan_complete,
        }
    }
    
    /// Update a file's status in the cache
    pub async fn update_file_status(&self, status: FileStatus) -> Result<(), WatcherError> {
        let mut cache = self.cache.write().await;
        
        // Update memory cache
        match status.status {
            FileStatusType::Modified => {
                cache.modified.insert(status.path.clone(), status.clone());
                cache.added.remove(&status.path);
                cache.untracked.remove(&status.path);
            }
            FileStatusType::Added => {
                cache.added.insert(status.path.clone(), status.clone());
                cache.modified.remove(&status.path);
                cache.untracked.remove(&status.path);
            }
            FileStatusType::Removed => {
                cache.removed.insert(status.path.clone(), status.clone());
                cache.modified.remove(&status.path);
                cache.added.remove(&status.path);
                cache.untracked.remove(&status.path);
            }
            FileStatusType::Untracked => {
                cache.untracked.insert(status.path.clone(), status.clone());
                cache.modified.remove(&status.path);
                cache.added.remove(&status.path);
            }
        }
        
        cache.last_update = SystemTime::now();
        
        Ok(())
    }
    
    /// Batch update multiple file statuses
    pub async fn batch_update(&self, statuses: Vec<FileStatus>) -> Result<(), WatcherError> {
        let mut cache = self.cache.write().await;
        
        for status in statuses {
            // Update memory cache
            match status.status {
                FileStatusType::Modified => {
                    cache.modified.insert(status.path.clone(), status.clone());
                    cache.added.remove(&status.path);
                    cache.untracked.remove(&status.path);
                }
                FileStatusType::Added => {
                    cache.added.insert(status.path.clone(), status.clone());
                    cache.modified.remove(&status.path);
                    cache.untracked.remove(&status.path);
                }
                FileStatusType::Removed => {
                    cache.removed.insert(status.path.clone(), status.clone());
                    cache.modified.remove(&status.path);
                    cache.added.remove(&status.path);
                    cache.untracked.remove(&status.path);
                }
                FileStatusType::Untracked => {
                    cache.untracked.insert(status.path.clone(), status.clone());
                    cache.modified.remove(&status.path);
                    cache.added.remove(&status.path);
                }
            }
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
    pub async fn clear(&self) -> Result<(), WatcherError> {
        let mut cache = self.cache.write().await;
        cache.modified.clear();
        cache.added.clear();
        cache.removed.clear();
        cache.untracked.clear();
        cache.scan_complete = false;
        cache.last_update = SystemTime::now();
        Ok(())
    }
}