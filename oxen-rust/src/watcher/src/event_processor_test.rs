#[cfg(test)]
mod tests {
    use crate::cache::StatusCache;
    use crate::event_processor::EventProcessor;
    use notify::EventKind;
    use notify_debouncer_full::{DebounceEventResult, DebouncedEvent};
    use notify::Event;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::time;

    async fn setup_test_processor() -> (Arc<StatusCache>, mpsc::Sender<DebounceEventResult>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();

        // Create a fake .oxen directory
        std::fs::create_dir_all(repo_path.join(".oxen")).unwrap();

        // Initialize an empty oxen repo
        liboxen::repositories::init::init(repo_path).unwrap();

        let cache = Arc::new(StatusCache::new(repo_path).unwrap());
        let (event_tx, event_rx) = mpsc::channel::<DebounceEventResult>(100);

        let processor = EventProcessor::new(cache.clone(), repo_path.to_path_buf());

        // Start processor in background
        tokio::spawn(async move {
            processor.run(event_rx).await;
        });

        // Give processor time to start
        time::sleep(Duration::from_millis(10)).await;

        (cache, event_tx, temp_dir)
    }

    fn create_debounced_event(paths: Vec<std::path::PathBuf>, kind: EventKind) -> DebouncedEvent {
        let mut event = Event::new(kind);
        event.paths = paths;
        DebouncedEvent {
            event,
            time: std::time::Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_debounced_events() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let test_file = temp_dir.path().join("test.txt");
        std::fs::write(&test_file, "content").unwrap();

        // Send a debounced create event
        let event = create_debounced_event(
            vec![test_file.clone()],
            EventKind::Create(notify::event::CreateKind::File),
        );
        event_tx.send(Ok(vec![event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Check cache was updated
        let status = cache.get_status(None).await;
        assert_eq!(status.created.len(), 1);
    }

    #[tokio::test]
    async fn test_ignore_oxen_directory() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let oxen_file = temp_dir.path().join(".oxen").join("some_file.db");

        let event = create_debounced_event(
            vec![oxen_file],
            EventKind::Create(notify::event::CreateKind::File),
        );

        event_tx.send(Ok(vec![event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Should have no entries
        let status = cache.get_status(None).await;
        assert!(status.created.is_empty());
        assert!(status.modified.is_empty());
        assert!(status.removed.is_empty());
    }

    #[tokio::test]
    async fn test_ignore_directories() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let dir_path = temp_dir.path().join("some_directory");
        std::fs::create_dir_all(&dir_path).unwrap();

        let event = create_debounced_event(
            vec![dir_path],
            EventKind::Create(notify::event::CreateKind::Folder),
        );

        event_tx.send(Ok(vec![event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Should have no entries (directories are skipped)
        let status = cache.get_status(None).await;
        assert!(status.created.is_empty());
        assert!(status.modified.is_empty());
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let mut events = Vec::new();
        
        // Create multiple files and events
        for i in 0..5 {
            let file_path = temp_dir.path().join(format!("file{}.txt", i));
            std::fs::write(&file_path, format!("content{}", i)).unwrap();
            
            events.push(create_debounced_event(
                vec![file_path],
                EventKind::Create(notify::event::CreateKind::File),
            ));
        }

        // Send all events as a batch (this is what the debouncer does)
        event_tx.send(Ok(events)).await.unwrap();

        // Wait for batch processing
        time::sleep(Duration::from_millis(200)).await;

        // Should have all files
        let status = cache.get_status(None).await;
        assert_eq!(status.created.len(), 5);
    }

    #[tokio::test]
    async fn test_event_kinds_mapping() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        // Test Create event
        let create_file = temp_dir.path().join("created.txt");
        std::fs::write(&create_file, "content").unwrap();

        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![create_file.clone()],
                EventKind::Create(notify::event::CreateKind::File),
            )]))
            .await
            .unwrap();

        // Test Modify event
        let modify_file = temp_dir.path().join("modified.txt");
        std::fs::write(&modify_file, "content").unwrap();

        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![modify_file.clone()],
                EventKind::Modify(notify::event::ModifyKind::Data(notify::event::DataChange::Any)),
            )]))
            .await
            .unwrap();

        // Test Remove event
        let remove_file = temp_dir.path().join("removed.txt");

        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![remove_file.clone()],
                EventKind::Remove(notify::event::RemoveKind::File),
            )]))
            .await
            .unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(200)).await;

        let status = cache.get_status(None).await;

        // Should have entries in different categories
        let total = status.created.len()
            + status.modified.len()
            + status.removed.len();
        assert!(total > 0, "Should have processed events");
    }

    #[tokio::test]
    async fn test_skip_access_events() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let file = temp_dir.path().join("accessed.txt");
        std::fs::write(&file, "content").unwrap();

        // Send Access event (should be ignored)
        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![file.clone()],
                EventKind::Access(notify::event::AccessKind::Read),
            )]))
            .await
            .unwrap();

        // Send Other event (should be ignored)
        event_tx
            .send(Ok(vec![create_debounced_event(
                vec![file],
                EventKind::Other,
            )]))
            .await
            .unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Should have no entries
        let status = cache.get_status(None).await;
        assert!(status.created.is_empty());
        assert!(status.modified.is_empty());
        assert!(status.removed.is_empty());
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

        // Should still be running and cache should be empty
        let status = cache.get_status(None).await;
        assert!(status.created.is_empty());
        assert!(status.modified.is_empty());
        assert!(status.removed.is_empty());
    }

    #[tokio::test]
    async fn test_file_create_then_delete() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let test_file = temp_dir.path().join("test_create_delete.txt");
        
        // First create the file and send a create event
        std::fs::write(&test_file, "content").unwrap();
        
        let create_event = create_debounced_event(
            vec![test_file.clone()],
            EventKind::Create(notify::event::CreateKind::File),
        );
        event_tx.send(Ok(vec![create_event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Verify file is in created list
        let status = cache.get_status(None).await;
        assert_eq!(status.created.len(), 1, "File should be in created list");
        assert!(status.modified.is_empty());
        assert!(status.removed.is_empty());

        // Now delete the file and send a remove event
        std::fs::remove_file(&test_file).unwrap();
        
        let remove_event = create_debounced_event(
            vec![test_file.clone()],
            EventKind::Remove(notify::event::RemoveKind::File),
        );
        event_tx.send(Ok(vec![remove_event])).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // After deletion, file should be removed from created list
        // and should either be in removed list or completely gone
        let status = cache.get_status(None).await;
        
        assert!(
            status.created.is_empty(), 
            "File should not be in created list after deletion"
        );
        assert!(status.modified.is_empty());
        // The file was created and deleted within the watcher session,
        // so it should not appear in any list (net effect is no change)
        assert!(
            status.removed.is_empty(),
            "File created and deleted in same session should not appear in removed list"
        );
    }
}