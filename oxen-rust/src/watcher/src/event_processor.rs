use crate::cache::StatusCache;
use crate::protocol::{FileStatus, FileStatusType};
use liboxen::util;
use log::{debug, error, trace, warn};
use notify::EventKind;
use notify_debouncer_full::{DebounceEventResult, DebouncedEvent};
use std::path::PathBuf;
use std::sync::Arc;
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
        // Canonicalize the repo path once to handle symlinks properly
        let repo_path = repo_path.canonicalize().unwrap_or(repo_path);
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
        let mut updates = Vec::new();

        for debounced_event in events {
            trace!("Processing debounced event: {:?}", debounced_event);

            let event = &debounced_event.event;

            // Process each path in the event
            // Note: .oxen paths are already filtered in the monitor
            for path in &event.paths {
                // Skip directories for now
                if path.is_dir() {
                    continue;
                }

                // Determine the status type based on event kind
                let status_type = match event.kind {
                    EventKind::Create(_) => FileStatusType::Created,
                    EventKind::Modify(_) => FileStatusType::Modified,
                    EventKind::Remove(_) => FileStatusType::Removed,
                    EventKind::Any | EventKind::Access(_) | EventKind::Other => {
                        // Skip these events
                        continue;
                    }
                };

                // Get file metadata if it exists
                let (mtime, size) = if let Ok(metadata) = std::fs::metadata(path) {
                    (
                        metadata.modified().unwrap_or(std::time::SystemTime::now()),
                        metadata.len(),
                    )
                } else if status_type == FileStatusType::Removed {
                    // File was removed, use current time and zero size
                    (std::time::SystemTime::now(), 0)
                } else {
                    // Skip if we can't get metadata for non-removed files
                    warn!("Could not get metadata for file: {:?}", path);
                    continue;
                };

                // Convert absolute path to relative path
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

                debug!(
                    "Processing event for {:?}: {:?}",
                    relative_path, status_type
                );

                updates.push(FileStatus {
                    path: relative_path,
                    mtime,
                    size,
                    hash: None, // Will be computed later if needed
                    status: status_type,
                });
            }
        }

        // Batch update the cache
        if !updates.is_empty() {
            debug!("Updating cache with {} file status changes", updates.len());
            if let Err(e) = self.cache.batch_update(updates).await {
                error!("Failed to update cache: {}", e);
            }
        }
    }
}
