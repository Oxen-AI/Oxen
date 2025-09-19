use crate::cache::StatusCache;
use crate::tree::FileMetadata;
use liboxen::util;
use log::{debug, error, trace, warn};
use notify::EventKind;
use notify_debouncer_full::{DebounceEventResult, DebouncedEvent};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::mpsc;

#[path = "event_processor_test.rs"]
mod event_processor_test;

/// Processes filesystem events and updates the cache
pub struct EventProcessor {
    cache: Arc<StatusCache>,
    repo_path: PathBuf,
}

impl EventProcessor {
    pub fn new(cache: Arc<StatusCache>, repo_path: PathBuf) -> Self {
        Self { cache, repo_path }
    }

    /// Run the event processing loop
    pub async fn run(self, mut event_rx: mpsc::Receiver<DebounceEventResult>) {
        loop {
            // Wait for debounced events
            match event_rx.recv().await {
                Some(Ok(events)) => {
                    // Process the batch of debounced events
                    self.handle_debounced_events(events).await;
                }
                Some(Err(errors)) => {
                    // Log errors from the debouncer
                    for error in errors {
                        error!("Debouncer error: {:?}", error);
                    }
                }
                None => {
                    // Channel closed, exit
                    debug!("Event channel closed, exiting processor");
                    break;
                }
            }
        }
    }

    /// Handle a batch of debounced events
    async fn handle_debounced_events(&self, events: Vec<DebouncedEvent>) {
        for debounced_event in events {
            trace!("Processing debounced event: {:?}", debounced_event);

            let event = &debounced_event.event;

            // Process each path in the event
            // Note: .oxen paths are already filtered in the monitor
            for path in &event.paths {
                // Convert absolute path to relative path using liboxen
                let relative_path = match util::fs::path_relative_to_dir(path, &self.repo_path) {
                    Ok(rel) => rel,
                    Err(e) => {
                        trace!(
                            "Path not within repo, skipping: {:?} (repo: {:?}, error: {})",
                            path,
                            self.repo_path,
                            e
                        );
                        continue;
                    }
                };

                // Handle different event types
                match event.kind {
                    EventKind::Create(_) | EventKind::Modify(_) => {
                        // Get fresh metadata for the file/directory
                        match std::fs::metadata(path) {
                            Ok(metadata) => {
                                if metadata.is_file() {
                                    // Update file metadata in tree
                                    let file_metadata = FileMetadata {
                                        size: metadata.len(),
                                        mtime: metadata
                                            .modified()
                                            .unwrap_or_else(|_| SystemTime::now()),
                                        is_symlink: metadata.is_symlink(),
                                    };
                                    
                                    debug!(
                                        "Updating file metadata for: {:?} (size: {}, mtime: {:?})",
                                        relative_path, file_metadata.size, file_metadata.mtime
                                    );
                                    
                                    if let Err(e) = self.cache.update_file(relative_path, file_metadata).await {
                                        error!("Failed to update file in cache: {}", e);
                                    }
                                } else if metadata.is_dir() {
                                    // Update directory in tree (no metadata)
                                    debug!("Updating directory: {:?}", relative_path);
                                    
                                    if let Err(e) = self.cache.update_directory(relative_path).await {
                                        error!("Failed to update directory in cache: {}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                // File might have been deleted between event and processing
                                warn!("Could not get metadata for {:?}: {}", path, e);
                            }
                        }
                    }
                    EventKind::Remove(_) => {
                        // Remove from tree
                        debug!("Removing from cache: {:?}", relative_path);
                        
                        if let Err(e) = self.cache.remove_path(relative_path).await {
                            error!("Failed to remove path from cache: {}", e);
                        }
                    }
                    EventKind::Any | EventKind::Access(_) | EventKind::Other => {
                        // Skip these events
                        trace!("Skipping event kind: {:?}", event.kind);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tree::FileSystemTree;
    use notify::{Event, EventKind};
    use notify_debouncer_full::DebouncedEvent;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    async fn setup_test_processor() -> (EventProcessor, Arc<StatusCache>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let repo_path = temp_dir.path();
        
        // Initialize a test repository
        liboxen::repositories::init::init(repo_path).unwrap();
        
        let cache = Arc::new(StatusCache::new(repo_path).unwrap());
        let processor = EventProcessor::new(cache.clone(), repo_path.to_path_buf());
        
        (processor, cache, temp_dir)
    }

    #[tokio::test]
    async fn test_file_create_event() {
        let (processor, cache, temp_dir) = setup_test_processor().await;
        let repo_path = temp_dir.path();
        
        // Create a test file
        let file_path = repo_path.join("test.txt");
        std::fs::write(&file_path, "test content").unwrap();
        
        // Create a create event
        let event = DebouncedEvent {
            event: Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![file_path.clone()],
                attrs: Default::default(),
            },
            time: Instant::now(),
        };
        
        // Process the event
        processor.handle_debounced_events(vec![event]).await;
        
        // Check that the file was added to the cache
        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test.txt"));
        assert!(node.is_some());
        
        let node = node.unwrap();
        assert!(node.is_file());
        assert_eq!(node.metadata().unwrap().size, 12); // "test content" is 12 bytes
    }

    #[tokio::test]
    async fn test_file_modify_event() {
        let (processor, cache, temp_dir) = setup_test_processor().await;
        let repo_path = temp_dir.path();
        
        // Create and add a file first
        let file_path = repo_path.join("test.txt");
        std::fs::write(&file_path, "initial").unwrap();
        
        let metadata = FileMetadata {
            size: 7,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        cache.update_file(PathBuf::from("test.txt"), metadata).await.unwrap();
        
        // Now modify it
        std::fs::write(&file_path, "modified content").unwrap();
        
        // Create a modify event
        let event = DebouncedEvent {
            event: Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Content,
                )),
                paths: vec![file_path.clone()],
                attrs: Default::default(),
            },
            time: Instant::now(),
        };
        
        // Process the event
        processor.handle_debounced_events(vec![event]).await;
        
        // Check that the file metadata was updated
        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test.txt")).unwrap();
        assert_eq!(node.metadata().unwrap().size, 16); // "modified content" is 16 bytes
    }

    #[tokio::test]
    async fn test_file_remove_event() {
        let (processor, cache, temp_dir) = setup_test_processor().await;
        let repo_path = temp_dir.path();
        
        // Add a file to cache first
        let metadata = FileMetadata {
            size: 100,
            mtime: SystemTime::now(),
            is_symlink: false,
        };
        cache.update_file(PathBuf::from("test.txt"), metadata).await.unwrap();
        
        // Create a remove event
        let file_path = repo_path.join("test.txt");
        let event = DebouncedEvent {
            event: Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![file_path],
                attrs: Default::default(),
            },
            time: Instant::now(),
        };
        
        // Process the event
        processor.handle_debounced_events(vec![event]).await;
        
        // Check that the file was removed from cache
        let tree = cache.get_tree(None).await.unwrap();
        assert!(tree.get_node(&PathBuf::from("test.txt")).is_none());
    }

    #[tokio::test]
    async fn test_directory_create_event() {
        let (processor, cache, temp_dir) = setup_test_processor().await;
        let repo_path = temp_dir.path();
        
        // Create a test directory
        let dir_path = repo_path.join("test_dir");
        std::fs::create_dir(&dir_path).unwrap();
        
        // Create a create event for directory
        let event = DebouncedEvent {
            event: Event {
                kind: EventKind::Create(notify::event::CreateKind::Folder),
                paths: vec![dir_path.clone()],
                attrs: Default::default(),
            },
            time: Instant::now(),
        };
        
        // Process the event
        processor.handle_debounced_events(vec![event]).await;
        
        // Check that the directory was added to the cache
        let tree = cache.get_tree(None).await.unwrap();
        let node = tree.get_node(&PathBuf::from("test_dir"));
        assert!(node.is_some());
        
        let node = node.unwrap();
        assert!(node.is_directory());
    }

    #[tokio::test]
    async fn test_path_outside_repo() {
        let (processor, cache, _temp_dir) = setup_test_processor().await;
        
        // Create an event for a path outside the repo
        let event = DebouncedEvent {
            event: Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![PathBuf::from("/tmp/outside_file.txt")],
                attrs: Default::default(),
            },
            time: Instant::now(),
        };
        
        // Process the event - should be skipped
        processor.handle_debounced_events(vec![event]).await;
        
        // Check that nothing was added to the cache
        let tree = cache.get_tree(None).await.unwrap();
        let files: Vec<_> = tree.iter_files().collect();
        assert!(files.is_empty());
    }
}