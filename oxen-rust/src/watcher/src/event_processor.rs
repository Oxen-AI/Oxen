use crate::cache::StatusCache;
use crate::protocol::{FileStatus, FileStatusType};
use log::{debug, error, trace};
use notify::{Event, EventKind};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time;

#[path = "event_processor_test.rs"]
mod event_processor_test;

/// Processes filesystem events and updates the cache
pub struct EventProcessor {
    cache: Arc<StatusCache>,
}

impl EventProcessor {
    pub fn new(cache: Arc<StatusCache>) -> Self {
        Self { cache }
    }
    
    /// Run the event processing loop
    pub async fn run(self, mut event_rx: mpsc::Receiver<Event>) {
        // Buffer for coalescing events
        let mut event_buffer: HashMap<PathBuf, (EventKind, Instant)> = HashMap::new();
        let coalesce_window = Duration::from_millis(100);
        let batch_size = 1000;
        
        let mut interval = time::interval(coalesce_window);
        
        loop {
            tokio::select! {
                // Process incoming events
                Some(event) = event_rx.recv() => {
                    self.handle_event(event, &mut event_buffer);
                    
                    // Flush if buffer is getting large
                    if event_buffer.len() >= batch_size {
                        self.flush_events(&mut event_buffer).await;
                    }
                }
                
                // Periodic flush of coalesced events
                _ = interval.tick() => {
                    if !event_buffer.is_empty() {
                        self.flush_events(&mut event_buffer).await;
                    }
                }
            }
        }
    }
    
    /// Handle a single filesystem event
    fn handle_event(&self, event: Event, buffer: &mut HashMap<PathBuf, (EventKind, Instant)>) {
        trace!("Received event: {:?}", event);
        
        for path in event.paths {
            // Skip .oxen directory
            if path.components().any(|c| c.as_os_str() == ".oxen") {
                continue;
            }
            
            // Skip non-file events for now
            if path.is_dir() {
                continue;
            }
            
            // Coalesce events for the same path
            buffer.insert(path, (event.kind, Instant::now()));
        }
    }
    
    /// Flush buffered events to the cache
    async fn flush_events(&self, buffer: &mut HashMap<PathBuf, (EventKind, Instant)>) {
        if buffer.is_empty() {
            return;
        }
        
        debug!("Flushing {} events to cache", buffer.len());
        
        let mut updates = Vec::new();
        let now = Instant::now();
        let stale_threshold = Duration::from_millis(200);
        
        // Process each buffered event
        for (path, (kind, timestamp)) in buffer.drain() {
            // Skip stale events
            if now.duration_since(timestamp) > stale_threshold {
                continue;
            }
            
            // Determine the status type based on event kind and file existence
            let status_type = match kind {
                EventKind::Create(_) => {
                    // New file created
                    FileStatusType::Untracked
                }
                EventKind::Modify(_) => {
                    // File modified - need to check if it's tracked
                    // For now, assume modified if it exists
                    FileStatusType::Modified
                }
                EventKind::Remove(_) => {
                    // File removed
                    FileStatusType::Removed
                }
                EventKind::Any | EventKind::Access(_) | EventKind::Other => {
                    // Skip these events
                    continue;
                }
            };
            
            // Get file metadata if it exists
            let (mtime, size) = if let Ok(metadata) = std::fs::metadata(&path) {
                (
                    metadata.modified().unwrap_or(std::time::SystemTime::now()),
                    metadata.len(),
                )
            } else if status_type == FileStatusType::Removed {
                // File was removed, use current time and zero size
                (std::time::SystemTime::now(), 0)
            } else {
                // Skip if we can't get metadata for non-removed files
                continue;
            };
            
            updates.push(FileStatus {
                path: path.clone(),
                mtime,
                size,
                hash: None, // Will be computed later if needed
                status: status_type,
            });
        }
        
        // Batch update the cache
        if !updates.is_empty() {
            if let Err(e) = self.cache.batch_update(updates).await {
                error!("Failed to update cache: {}", e);
            }
        }
    }
}