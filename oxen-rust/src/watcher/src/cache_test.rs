#[cfg(test)]
mod tests {
    use crate::cache::StatusCache;
    use crate::protocol::{FileStatus, FileStatusType};
    use std::path::PathBuf;
    use std::time::SystemTime;
    use tempfile::TempDir;

    async fn setup_test_cache() -> (StatusCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();
        
        // Create a fake .oxen directory
        std::fs::create_dir_all(repo_path.join(".oxen")).unwrap();
        
        // Initialize an empty oxen repo (minimal setup)
        liboxen::repositories::init::init(repo_path).unwrap();
        
        let cache = StatusCache::new(repo_path).unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_cache_new() {
        let (_cache, _temp_dir) = setup_test_cache().await;
        // Test passes if cache is created successfully
    }

    #[tokio::test]
    async fn test_empty_cache_status() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let status = cache.get_status(None).await;
        assert!(status.created.is_empty());
        assert!(status.modified.is_empty());
        assert!(status.removed.is_empty());
        assert!(!status.scan_complete);
    }

    #[tokio::test]
    async fn test_update_file_status() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let file_status = FileStatus {
            path: PathBuf::from("test.txt"),
            mtime: SystemTime::now(),
            size: 100,
            hash: Some("abc123".to_string()),
            status: FileStatusType::Modified,
        };
        
        cache.update_file_status(file_status.clone()).await.unwrap();
        
        let status = cache.get_status(None).await;
        assert_eq!(status.modified.len(), 1);
        assert_eq!(status.modified[0].path, PathBuf::from("test.txt"));
        assert!(status.created.is_empty());
        assert!(status.removed.is_empty());
    }

    #[tokio::test]
    async fn test_batch_update() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let statuses = vec![
            FileStatus {
                path: PathBuf::from("file1.txt"),
                mtime: SystemTime::now(),
                size: 100,
                hash: None,
                status: FileStatusType::Created,
            },
            FileStatus {
                path: PathBuf::from("file2.txt"),
                mtime: SystemTime::now(),
                size: 200,
                hash: None,
                status: FileStatusType::Modified,
            },
            FileStatus {
                path: PathBuf::from("file3.txt"),
                mtime: SystemTime::now(),
                size: 0,
                hash: None,
                status: FileStatusType::Removed,
            },
        ];
        
        cache.batch_update(statuses).await.unwrap();
        
        let status = cache.get_status(None).await;
        assert_eq!(status.created.len(), 1);
        assert_eq!(status.modified.len(), 1);
        assert_eq!(status.removed.len(), 1);
    }

    #[tokio::test]
    async fn test_status_transitions() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let path = PathBuf::from("test.txt");
        
        // Start as created
        cache.update_file_status(FileStatus {
            path: path.clone(),
            mtime: SystemTime::now(),
            size: 100,
            hash: None,
            status: FileStatusType::Created,
        }).await.unwrap();
        
        let status = cache.get_status(None).await;
        assert_eq!(status.created.len(), 1);
        
        // Transition to modified
        cache.update_file_status(FileStatus {
            path: path.clone(),
            mtime: SystemTime::now(),
            size: 150,
            hash: Some("newhash".to_string()),
            status: FileStatusType::Modified,
        }).await.unwrap();
        
        let status = cache.get_status(None).await;
        assert_eq!(status.modified.len(), 1);
        assert_eq!(status.created.len(), 1); // Created status is preserved
        
        // Transition to removed
        cache.update_file_status(FileStatus {
            path: path.clone(),
            mtime: SystemTime::now(),
            size: 0,
            hash: None,
            status: FileStatusType::Removed,
        }).await.unwrap();
        
        let status = cache.get_status(None).await;
        // File was created and removed in same session, so should not appear anywhere
        assert_eq!(status.removed.len(), 0); // Not in removed (net effect is nothing)
        assert_eq!(status.created.len(), 0); // Removed clears created
        assert_eq!(status.modified.len(), 0); // Removed clears modified
    }

    #[tokio::test]
    async fn test_remove_existing_file() {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();
        
        // Initialize a proper oxen repo
        liboxen::repositories::init::init(repo_path).unwrap();
        
        let cache = StatusCache::new(repo_path).unwrap();
        
        let path = PathBuf::from("existing.txt");
        
        // File starts as Modified (existed before watcher, was modified)
        cache.update_file_status(FileStatus {
            path: path.clone(),
            mtime: SystemTime::now(),
            size: 100,
            hash: None,
            status: FileStatusType::Modified,
        }).await.unwrap();
        
        let status = cache.get_status(None).await;
        assert_eq!(status.modified.len(), 1);
        assert_eq!(status.created.len(), 0);
        
        // Now remove it
        cache.update_file_status(FileStatus {
            path: path.clone(),
            mtime: SystemTime::now(),
            size: 0,
            hash: None,
            status: FileStatusType::Removed,
        }).await.unwrap();
        
        let status = cache.get_status(None).await;
        // File existed before session and was removed, should show in removed list
        assert_eq!(status.removed.len(), 1);
        assert_eq!(status.created.len(), 0);
        assert_eq!(status.modified.len(), 0);
    }

    #[tokio::test]
    async fn test_path_filtering() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let statuses = vec![
            FileStatus {
                path: PathBuf::from("dir1/file1.txt"),
                mtime: SystemTime::now(),
                size: 100,
                hash: None,
                status: FileStatusType::Modified,
            },
            FileStatus {
                path: PathBuf::from("dir2/file2.txt"),
                mtime: SystemTime::now(),
                size: 200,
                hash: None,
                status: FileStatusType::Modified,
            },
        ];
        
        cache.batch_update(statuses).await.unwrap();
        
        // Get all files
        let status = cache.get_status(None).await;
        assert_eq!(status.modified.len(), 2);
        
        // Filter by specific path
        let filtered = cache.get_status(Some(vec![PathBuf::from("dir1/file1.txt")])).await;
        assert_eq!(filtered.modified.len(), 1);
        assert_eq!(filtered.modified[0].path, PathBuf::from("dir1/file1.txt"));
    }

    #[tokio::test]
    async fn test_scan_complete() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        let status = cache.get_status(None).await;
        assert!(!status.scan_complete);
        
        cache.mark_scan_complete().await.unwrap();
        
        let status = cache.get_status(None).await;
        assert!(status.scan_complete);
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let (cache, _temp_dir) = setup_test_cache().await;
        
        // Add some data
        let statuses = vec![
            FileStatus {
                path: PathBuf::from("file1.txt"),
                mtime: SystemTime::now(),
                size: 100,
                hash: None,
                status: FileStatusType::Created,
            },
            FileStatus {
                path: PathBuf::from("file2.txt"),
                mtime: SystemTime::now(),
                size: 200,
                hash: None,
                status: FileStatusType::Modified,
            },
        ];
        
        cache.batch_update(statuses).await.unwrap();
        cache.mark_scan_complete().await.unwrap();
        
        // Verify data exists
        let status = cache.get_status(None).await;
        assert_eq!(status.created.len(), 1);
        assert_eq!(status.modified.len(), 1);
        assert!(status.scan_complete);
        
        // Clear cache
        cache.clear().await.unwrap();
        
        // Verify cache is empty
        let status = cache.get_status(None).await;
        assert!(status.created.is_empty());
        assert!(status.modified.is_empty());
        assert!(!status.scan_complete);
    }
}