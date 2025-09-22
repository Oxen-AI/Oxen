#[cfg(test)]
mod tests {
    use crate::cache::StatusCache;
    use crate::tree::FileMetadata;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use tempfile::TempDir;

    async fn setup_test_cache() -> (StatusCache, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();

        // Create a fake .oxen directory
        std::fs::create_dir_all(repo_path.join(".oxen")).unwrap();

        let cache = StatusCache::new(repo_path).unwrap();
        (cache, temp_dir)
    }

    #[tokio::test]
    async fn test_cache_new() {
        let (_cache, _temp_dir) = setup_test_cache().await;
        // Test passes if cache is created successfully
    }

    #[tokio::test]
    async fn test_empty_cache_tree() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let tree = cache.get_tree(None).await.unwrap();
        assert_eq!(tree.root.name, ".");
        assert!(tree.root.is_directory());
        assert!(tree.root.children.is_empty());
        assert!(!tree.scan_complete);
    }

    #[tokio::test]
    async fn test_update_file() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let metadata = FileMetadata {
            mtime: SystemTime::now(),
            size: 100,
            is_symlink: false,
        };

        cache
            .update_file(PathBuf::from("test.txt"), metadata.clone())
            .await
            .unwrap();

        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test.txt"));
        assert!(node.is_some());

        let node = node.unwrap();
        assert!(node.is_file());
        assert_eq!(node.metadata(), Some(&metadata));
    }

    #[tokio::test]
    async fn test_update_nested_file() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let metadata = FileMetadata {
            mtime: SystemTime::now(),
            size: 200,
            is_symlink: false,
        };

        cache
            .update_file(PathBuf::from("dir1/dir2/test.txt"), metadata.clone())
            .await
            .unwrap();

        let tree = cache.get_tree(None).await.unwrap();

        // Check directory structure was created
        assert!(tree.get_node(&PathBuf::from("dir1")).is_some());
        assert!(tree
            .get_node(&PathBuf::from("dir1"))
            .unwrap()
            .is_directory());
        assert!(tree.get_node(&PathBuf::from("dir1/dir2")).is_some());
        assert!(tree
            .get_node(&PathBuf::from("dir1/dir2"))
            .unwrap()
            .is_directory());

        // Check file exists
        let file = tree.get_node(&PathBuf::from("dir1/dir2/test.txt"));
        assert!(file.is_some());
        let file = file.unwrap();
        assert!(file.is_file());
        assert_eq!(file.metadata(), Some(&metadata));
    }

    #[tokio::test]
    async fn test_update_directory() {
        let (cache, _temp_dir) = setup_test_cache().await;

        cache
            .update_directory(PathBuf::from("test_dir"))
            .await
            .unwrap();

        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test_dir"));
        assert!(node.is_some());

        let node = node.unwrap();
        assert!(node.is_directory());
        assert!(node.children.is_empty());
    }

    #[tokio::test]
    async fn test_batch_update() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let updates = vec![
            (
                PathBuf::from("file1.txt"),
                Some(FileMetadata {
                    mtime: SystemTime::now(),
                    size: 100,
                    is_symlink: false,
                }),
            ),
            (PathBuf::from("dir1"), None), // Directory
            (
                PathBuf::from("dir1/file2.txt"),
                Some(FileMetadata {
                    mtime: SystemTime::now(),
                    size: 200,
                    is_symlink: false,
                }),
            ),
        ];

        cache.batch_update(updates).await.unwrap();

        let tree = cache.get_tree(None).await.unwrap();

        // Check all nodes exist
        assert!(tree.get_node(&PathBuf::from("file1.txt")).is_some());
        assert!(tree
            .get_node(&PathBuf::from("file1.txt"))
            .unwrap()
            .is_file());

        assert!(tree.get_node(&PathBuf::from("dir1")).is_some());
        assert!(tree
            .get_node(&PathBuf::from("dir1"))
            .unwrap()
            .is_directory());

        assert!(tree.get_node(&PathBuf::from("dir1/file2.txt")).is_some());
        assert!(tree
            .get_node(&PathBuf::from("dir1/file2.txt"))
            .unwrap()
            .is_file());
    }

    #[tokio::test]
    async fn test_remove_file() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let metadata = FileMetadata {
            mtime: SystemTime::now(),
            size: 100,
            is_symlink: false,
        };

        // Add a file
        cache
            .update_file(PathBuf::from("test.txt"), metadata)
            .await
            .unwrap();

        // Verify it exists
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("test.txt")).is_some());

        // Remove it
        cache.remove_path(PathBuf::from("test.txt")).await.unwrap();

        // Verify it's gone
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("test.txt")).is_none());
    }

    #[tokio::test]
    async fn test_remove_directory_with_children() {
        let (cache, _temp_dir) = setup_test_cache().await;

        // Add files in nested directories
        let metadata = FileMetadata {
            mtime: SystemTime::now(),
            size: 100,
            is_symlink: false,
        };

        cache
            .update_file(PathBuf::from("dir1/file1.txt"), metadata.clone())
            .await
            .unwrap();
        cache
            .update_file(PathBuf::from("dir1/dir2/file2.txt"), metadata)
            .await
            .unwrap();

        // Remove the parent directory
        cache.remove_path(PathBuf::from("dir1")).await.unwrap();

        // Verify entire subtree is gone
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("dir1")).is_none());
        assert!(tree.get_node(&PathBuf::from("dir1/file1.txt")).is_none());
        assert!(tree.get_node(&PathBuf::from("dir1/dir2")).is_none());
        assert!(tree
            .get_node(&PathBuf::from("dir1/dir2/file2.txt"))
            .is_none());
    }

    #[tokio::test]
    async fn test_get_subtree() {
        let (cache, _temp_dir) = setup_test_cache().await;

        // Build a tree structure
        let metadata = FileMetadata {
            mtime: SystemTime::now(),
            size: 100,
            is_symlink: false,
        };

        cache
            .update_file(PathBuf::from("root.txt"), metadata.clone())
            .await
            .unwrap();
        cache
            .update_file(PathBuf::from("dir1/file1.txt"), metadata.clone())
            .await
            .unwrap();
        cache
            .update_file(PathBuf::from("dir1/subdir/file2.txt"), metadata.clone())
            .await
            .unwrap();
        cache
            .update_file(PathBuf::from("dir2/file3.txt"), metadata)
            .await
            .unwrap();

        // Get subtree for dir1
        let subtree = cache.get_tree(Some(&PathBuf::from("dir1"))).await.unwrap();

        // Subtree should have dir1 as root
        assert_eq!(subtree.root.name, "dir1");
        assert!(subtree.root.is_directory());

        // Check subtree contains only dir1's children
        assert!(subtree.root.children.contains_key("file1.txt"));
        assert!(subtree.root.children.contains_key("subdir"));

        // Should not contain root.txt or dir2
        assert!(!subtree.root.children.contains_key("root.txt"));
        assert!(!subtree.root.children.contains_key("dir2"));
    }

    #[tokio::test]
    async fn test_scan_complete() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let tree = cache.get_tree(None).await.unwrap();
        assert!(!tree.scan_complete);

        cache.mark_scan_complete().await.unwrap();

        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.scan_complete);
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let (cache, _temp_dir) = setup_test_cache().await;

        // Add some data
        let metadata = FileMetadata {
            mtime: SystemTime::now(),
            size: 100,
            is_symlink: false,
        };

        cache
            .update_file(PathBuf::from("file1.txt"), metadata.clone())
            .await
            .unwrap();
        cache
            .update_file(PathBuf::from("dir1/file2.txt"), metadata)
            .await
            .unwrap();
        cache.mark_scan_complete().await.unwrap();

        // Verify data exists
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("file1.txt")).is_some());
        assert!(tree.get_node(&PathBuf::from("dir1/file2.txt")).is_some());
        assert!(tree.scan_complete);

        // Clear cache
        cache.clear().await.unwrap();

        // Verify cache is empty
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.root.children.is_empty());
        assert!(!tree.scan_complete);
    }

    #[tokio::test]
    async fn test_node_count() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let metadata = FileMetadata {
            mtime: SystemTime::now(),
            size: 100,
            is_symlink: false,
        };

        // Add files and directories
        cache
            .update_file(PathBuf::from("file1.txt"), metadata.clone())
            .await
            .unwrap();
        cache
            .update_file(PathBuf::from("dir1/file2.txt"), metadata.clone())
            .await
            .unwrap();
        cache
            .update_file(PathBuf::from("dir1/dir2/file3.txt"), metadata)
            .await
            .unwrap();

        let tree = cache.get_tree(None).await.unwrap();
        let (dirs, files) = tree.count_nodes();

        assert_eq!(dirs, 3); // root, dir1, dir2
        assert_eq!(files, 3); // file1.txt, file2.txt, file3.txt
    }

    #[tokio::test]
    async fn test_update_existing_file() {
        let (cache, _temp_dir) = setup_test_cache().await;

        let metadata1 = FileMetadata {
            mtime: SystemTime::now(),
            size: 100,
            is_symlink: false,
        };

        let metadata2 = FileMetadata {
            mtime: SystemTime::now(),
            size: 200,
            is_symlink: false,
        };

        // Add initial file
        cache
            .update_file(PathBuf::from("test.txt"), metadata1)
            .await
            .unwrap();

        // Update with new metadata
        cache
            .update_file(PathBuf::from("test.txt"), metadata2.clone())
            .await
            .unwrap();

        // Verify metadata was updated
        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test.txt")).unwrap();
        assert_eq!(node.metadata(), Some(&metadata2));
        assert_eq!(node.metadata().unwrap().size, 200);
    }
}
