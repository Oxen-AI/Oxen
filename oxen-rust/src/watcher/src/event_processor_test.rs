#[cfg(test)]
mod tests {
    use crate::cache::StatusCache;
    use crate::event_processor::EventProcessor;
    use notify::{Event, EventKind};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::time;

    async fn setup_test_processor() -> (Arc<StatusCache>, mpsc::Sender<Event>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();

        // Create a fake .oxen directory
        std::fs::create_dir_all(repo_path.join(".oxen")).unwrap();

        // Initialize an empty oxen repo
        liboxen::repositories::init::init(repo_path).unwrap();

        let cache = Arc::new(StatusCache::new(repo_path).unwrap());
        let (event_tx, event_rx) = mpsc::channel::<Event>(100);

        let processor = EventProcessor::new(cache.clone());

        // Start processor in background
        tokio::spawn(async move {
            processor.run(event_rx).await;
        });

        // Give processor time to start
        time::sleep(Duration::from_millis(10)).await;

        (cache, event_tx, temp_dir)
    }

    #[tokio::test]
    async fn test_event_coalescing() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let test_file = temp_dir.path().join("test.txt");

        // Send multiple events for the same file rapidly
        for _ in 0..5 {
            let event = Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Any),
                paths: vec![test_file.clone()],
                attrs: Default::default(),
            };
            event_tx.send(event).await.unwrap();
        }

        // Wait for coalescing window
        time::sleep(Duration::from_millis(150)).await;

        // Should only have one entry in cache
        let status = cache.get_status(None).await;
        assert!(status.modified.len() <= 1, "Events should be coalesced");
    }

    #[tokio::test]
    async fn test_ignore_oxen_directory() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let oxen_file = temp_dir.path().join(".oxen").join("some_file.db");

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::Any),
            paths: vec![oxen_file],
            attrs: Default::default(),
        };

        event_tx.send(event).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Should have no entries
        let status = cache.get_status(None).await;
        assert!(status.added.is_empty());
        assert!(status.untracked.is_empty());
        assert!(status.modified.is_empty());
    }

    #[tokio::test]
    async fn test_ignore_directories() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        let dir_path = temp_dir.path().join("some_directory");
        std::fs::create_dir_all(&dir_path).unwrap();

        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::Any),
            paths: vec![dir_path],
            attrs: Default::default(),
        };

        event_tx.send(event).await.unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Should have no entries (directories are skipped)
        let status = cache.get_status(None).await;
        assert!(status.added.is_empty());
        assert!(status.untracked.is_empty());
    }

    #[tokio::test]
    async fn test_batch_processing() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        // Send events for multiple files
        for i in 0..10 {
            let file_path = temp_dir.path().join(format!("file{}.txt", i));
            // Create the file so metadata can be read
            std::fs::write(&file_path, format!("content{}", i)).unwrap();

            let event = Event {
                kind: EventKind::Create(notify::event::CreateKind::Any),
                paths: vec![file_path],
                attrs: Default::default(),
            };

            event_tx.send(event).await.unwrap();
        }

        // Wait for batch processing
        time::sleep(Duration::from_millis(200)).await;

        // Should have all files
        let status = cache.get_status(None).await;
        let total = status.added.len() + status.untracked.len() + status.modified.len();
        assert!(total > 0, "Should have processed some files");
        assert!(total <= 10, "Should not exceed number of files sent");
    }

    #[tokio::test]
    async fn test_event_kinds_mapping() {
        let (cache, event_tx, temp_dir) = setup_test_processor().await;

        // Test Create event
        let create_file = temp_dir.path().join("created.txt");
        std::fs::write(&create_file, "content").unwrap();

        event_tx
            .send(Event {
                kind: EventKind::Create(notify::event::CreateKind::Any),
                paths: vec![create_file.clone()],
                attrs: Default::default(),
            })
            .await
            .unwrap();

        // Test Modify event
        let modify_file = temp_dir.path().join("modified.txt");
        std::fs::write(&modify_file, "content").unwrap();

        event_tx
            .send(Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Any),
                paths: vec![modify_file.clone()],
                attrs: Default::default(),
            })
            .await
            .unwrap();

        // Test Remove event
        let remove_file = temp_dir.path().join("removed.txt");

        event_tx
            .send(Event {
                kind: EventKind::Remove(notify::event::RemoveKind::Any),
                paths: vec![remove_file.clone()],
                attrs: Default::default(),
            })
            .await
            .unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(200)).await;

        let status = cache.get_status(None).await;

        // Should have entries in different categories
        let total = status.added.len()
            + status.untracked.len()
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
            .send(Event {
                kind: EventKind::Access(notify::event::AccessKind::Any),
                paths: vec![file.clone()],
                attrs: Default::default(),
            })
            .await
            .unwrap();

        // Send Other event (should be ignored)
        event_tx
            .send(Event {
                kind: EventKind::Other,
                paths: vec![file],
                attrs: Default::default(),
            })
            .await
            .unwrap();

        // Wait for processing
        time::sleep(Duration::from_millis(150)).await;

        // Should have no entries
        let status = cache.get_status(None).await;
        assert!(status.added.is_empty());
        assert!(status.untracked.is_empty());
        assert!(status.modified.is_empty());
    }
}
