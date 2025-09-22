#[cfg(test)]
mod tests {
    use crate::cache::StatusCache;
    use crate::event_processor::EventProcessor;
    use crate::tree::FileMetadata;
    use notify::Event;
    use notify::EventKind;
    use notify_debouncer_full::{DebounceEventResult, DebouncedEvent};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::time;

    async fn setup_test_processor() -> (Arc<StatusCache>, mpsc::Sender<DebounceEventResult>, TempDir)
    {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path().canonicalize().unwrap();

        // Create a fake .oxen directory
        std::fs::create_dir_all(repo_path.join(".oxen")).unwrap();

        let cache = Arc::new(StatusCache::new(&repo_path).unwrap());
        let (event_tx, event_rx) = mpsc::channel::<DebounceEventResult>(100);

        let processor = EventProcessor::new(cache.clone(), repo_path.clone());

        // Start processor in background
        tokio::spawn(async move {
            processor.run(event_rx).await;
        });

        (cache, event_tx, temp_dir)
    }

    fn create_debounced_event(paths: Vec<PathBuf>, kind: EventKind) -> DebouncedEvent {
        DebouncedEvent {
            event: Event {
                kind,
                paths,
                attrs: Default::default(),
            },
            time: Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_file_create_event() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        // Create a test file
        let file_path = temp_dir.path().canonicalize().unwrap().join("test.txt");
        std::fs::write(&file_path, "test content").unwrap();

        // Send create event
        let event = create_debounced_event(
            vec![file_path.clone()],
            EventKind::Create(notify::event::CreateKind::File),
        );
        event_tx.send(Ok(vec![event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(500)).await;

        // Get the tree and check for the file
        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test.txt"));
        assert!(node.is_some(), "File should be in tree");

        let node = node.unwrap();
        assert!(node.is_file(), "Node should be a file");
        assert_eq!(node.metadata().unwrap().size, 12); // "test content" is 12 bytes
    }

    #[tokio::test]
    async fn test_file_modify_event() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        // Create and add a file first
        let file_path = temp_dir.path().canonicalize().unwrap().join("test.txt");
        std::fs::write(&file_path, "initial").unwrap();

        // Manually add to cache
        let metadata = FileMetadata {
            size: 7,
            mtime: std::time::SystemTime::now(),
            is_symlink: false,
        };
        cache
            .update_file(PathBuf::from("test.txt"), metadata)
            .await
            .unwrap();

        // Now modify it
        std::fs::write(&file_path, "modified content").unwrap();

        // Send modify event
        let event = create_debounced_event(
            vec![file_path.clone()],
            EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Content,
            )),
        );
        event_tx.send(Ok(vec![event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(500)).await;

        // Check that the file metadata was updated
        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test.txt")).unwrap();
        assert_eq!(node.metadata().unwrap().size, 16); // "modified content" is 16 bytes
    }

    #[tokio::test]
    async fn test_file_remove_event() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        // Add a file to cache first
        let metadata = FileMetadata {
            size: 100,
            mtime: std::time::SystemTime::now(),
            is_symlink: false,
        };
        cache
            .update_file(PathBuf::from("test.txt"), metadata)
            .await
            .unwrap();

        // Send remove event
        let file_path = temp_dir.path().canonicalize().unwrap().join("test.txt");
        let event = create_debounced_event(
            vec![file_path],
            EventKind::Remove(notify::event::RemoveKind::File),
        );
        event_tx.send(Ok(vec![event])).await.unwrap();

        // Wait for processing - increase delay to ensure event is processed
        time::sleep(Duration::from_millis(500)).await;

        // Check that the file was removed from tree
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("test.txt")).is_none());
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let mut events = Vec::new();

        // Create multiple files and events
        for i in 0..5 {
            let file_path = temp_dir
                .path()
                .canonicalize()
                .unwrap()
                .join(format!("file{}.txt", i));
            std::fs::write(&file_path, format!("content{}", i)).unwrap();

            events.push(create_debounced_event(
                vec![file_path],
                EventKind::Create(notify::event::CreateKind::File),
            ));
        }

        // Send all events as a batch
        event_tx.send(Ok(events)).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(500)).await;

        // Check that all files are in the tree
        let tree = cache.get_tree(None).await.unwrap();
        for i in 0..5 {
            let path = PathBuf::from(format!("file{}.txt", i));
            assert!(
                tree.get_node(&path).is_some(),
                "File {} should be in tree",
                i
            );
        }
    }

    #[tokio::test]
    async fn test_different_event_types() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        // Test Create event
        let create_file = temp_dir.path().canonicalize().unwrap().join("created.txt");
        std::fs::write(&create_file, "content").unwrap();

        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![create_file.clone()],
                EventKind::Create(notify::event::CreateKind::File),
            )]))
            .await
            .unwrap();

        // Test Modify event
        let modify_file = temp_dir.path().canonicalize().unwrap().join("modified.txt");
        std::fs::write(&modify_file, "content").unwrap();

        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![modify_file.clone()],
                EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Any,
                )),
            )]))
            .await
            .unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(500)).await;

        // Check that files are in the tree
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("created.txt")).is_some());
        assert!(tree.get_node(&PathBuf::from("modified.txt")).is_some());
    }

    #[tokio::test]
    async fn test_skip_access_events() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let file = temp_dir.path().canonicalize().unwrap().join("accessed.txt");
        std::fs::write(&file, "content").unwrap();

        // Send Access event (should be skipped)
        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![file.clone()],
                EventKind::Access(notify::event::AccessKind::Read),
            )]))
            .await
            .unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(500)).await;

        // File should not be in tree (Access events are skipped)
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("accessed.txt")).is_none());
    }

    #[tokio::test]
    async fn test_error_handling() {
        let (cache, event_tx, _temp_dir) = setup_test_processor().await;

        // Send an error result (simulating debouncer errors)
        let errors = vec![
            notify::Error::generic("Test error 1"),
            notify::Error::generic("Test error 2"),
        ];

        event_tx.send(Err(errors)).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(50)).await;

        // Should still be running and tree should be empty
        let tree = cache.get_tree(None).await.unwrap();
        assert_eq!(
            tree.iter_files().count(),
            0,
            "Tree should be empty after errors"
        );
    }

    #[tokio::test]
    async fn test_file_create_then_delete() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let test_file = temp_dir
            .path()
            .canonicalize()
            .unwrap()
            .join("test_create_delete.txt");

        // First create the file and send a create event
        std::fs::write(&test_file, "content").unwrap();

        let create_event = create_debounced_event(
            vec![test_file.clone()],
            EventKind::Create(notify::event::CreateKind::File),
        );
        event_tx.send(Ok(vec![create_event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(500)).await;

        // Verify file is in tree
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree
            .get_node(&PathBuf::from("test_create_delete.txt"))
            .is_some());

        // Now delete the file and send a remove event
        std::fs::remove_file(&test_file).unwrap();

        let remove_event = create_debounced_event(
            vec![test_file.clone()],
            EventKind::Remove(notify::event::RemoveKind::File),
        );
        event_tx.send(Ok(vec![remove_event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(500)).await;

        // File should be gone from tree
        let tree = cache.get_tree(None).await.unwrap();
        assert!(
            tree.get_node(&PathBuf::from("test_create_delete.txt"))
                .is_none(),
            "File should not be in tree after deletion"
        );
    }
}
