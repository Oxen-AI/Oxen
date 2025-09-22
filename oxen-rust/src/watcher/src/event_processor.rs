use crate::cache::StatusCache;
use crate::tree::FileMetadata;
use crate::util;
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
                // Convert absolute path to relative path
                let relative_path = match util::path_relative_to_dir(path, &self.repo_path) {
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

                                    if let Err(e) =
                                        self.cache.update_file(relative_path, file_metadata).await
                                    {
                                        error!("Failed to update file in cache: {}", e);
                                    }
                                } else if metadata.is_dir() {
                                    // Update directory in tree (no metadata)
                                    debug!("Updating directory: {:?}", relative_path);

                                    if let Err(e) = self.cache.update_directory(relative_path).await
                                    {
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
