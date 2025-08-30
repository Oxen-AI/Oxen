use log::{error, info, warn};
use notify::RecursiveMode;
use notify_debouncer_full::{new_debouncer, DebounceEventResult};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use liboxen::core;
use liboxen::model::LocalRepository;

use crate::cache::StatusCache;
use crate::error::WatcherError;
use crate::event_processor::EventProcessor;
use crate::ipc::IpcServer;

/// Main filesystem watcher that coordinates all components
pub struct FileSystemWatcher {
    repo_path: PathBuf,
    cache: Arc<StatusCache>,
}

impl FileSystemWatcher {
    /// Create a new filesystem watcher for a repository
    pub fn new(repo_path: PathBuf) -> Result<Self, WatcherError> {
        // Verify repository exists
        if !repo_path.join(".oxen").exists() {
            return Err(WatcherError::RepositoryNotFound(
                repo_path.display().to_string(),
            ));
        }

        let cache = Arc::new(StatusCache::new(&repo_path)?);

        Ok(Self { repo_path, cache })
    }

    /// Run the watcher daemon
    pub async fn run(self) -> Result<(), WatcherError> {
        info!(
            "Starting filesystem watcher for {}",
            self.repo_path.display()
        );

        // Write PID file
        let pid_file = self.repo_path.join(".oxen/watcher.pid");
        std::fs::write(&pid_file, std::process::id().to_string())?;

        // Create channel for debounced events
        let (event_tx, event_rx) = mpsc::channel::<DebounceEventResult>(1000);

        // Create the debounced watcher with a 100ms timeout
        let mut debouncer = new_debouncer(
            Duration::from_millis(100),
            None, // No cache override
            move |result: DebounceEventResult| {
                // Filter out .oxen directory events before sending
                let filtered_result = match result {
                    Ok(events) => {
                        let filtered: Vec<_> = events
                            .into_iter()
                            .filter(|event| {
                                // Skip events for paths containing .oxen
                                !event
                                    .event
                                    .paths
                                    .iter()
                                    .any(|p| p.components().any(|c| c.as_os_str() == ".oxen"))
                            })
                            .collect();

                        if filtered.is_empty() {
                            return; // Don't send empty events
                        }
                        Ok(filtered)
                    }
                    Err(e) => Err(e),
                };

                // Try to send filtered event, block if channel is full
                // TODO: How should we handle this?
                let _ = event_tx.blocking_send(filtered_result);
            },
        )?;

        // Watch the repository directory
        debouncer.watch(&self.repo_path, RecursiveMode::Recursive)?;

        info!("Watching directory: {}", self.repo_path.display());

        // Start the event processor
        let processor = EventProcessor::new(self.cache.clone(), self.repo_path.clone());
        let processor_handle = tokio::spawn(async move { processor.run(event_rx).await });

        // Start the IPC server
        let ipc_server = IpcServer::new(self.repo_path.clone(), self.cache.clone());
        let ipc_handle = tokio::spawn(async move {
            if let Err(e) = ipc_server.run().await {
                error!("IPC server error: {}", e);
            }
        });

        // Start initial scan
        let cache_clone = self.cache.clone();
        let repo_path_clone = self.repo_path.clone();
        let _scan_handle = tokio::spawn(async move {
            if let Err(e) = initial_scan(repo_path_clone, cache_clone).await {
                error!("Initial scan error: {}", e);
            }
        });

        // Wait for shutdown signal or handle termination
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal");
            }
            _ = processor_handle => {
                warn!("Event processor terminated");
            }
            _ = ipc_handle => {
                warn!("IPC server terminated");
            }
        }

        // Cleanup
        info!("Shutting down filesystem watcher");
        drop(debouncer);

        // Remove PID file
        let _ = std::fs::remove_file(&pid_file);

        // Remove socket file
        let socket_path = self.repo_path.join(".oxen/watcher.sock");
        let _ = std::fs::remove_file(&socket_path);

        Ok(())
    }
}

/// Perform initial scan of the repository
async fn initial_scan(repo_path: PathBuf, cache: Arc<StatusCache>) -> Result<(), WatcherError> {
    info!("Starting initial repository scan");

    // Load the repository
    let repo = LocalRepository::from_dir(&repo_path)?;

    // Use liboxen status implementation to establish a baseline.
    // Then the watcher tracks changes relative to this baseline.
    match liboxen::repositories::status::status(&repo) {
        Ok(status) => {
            let mut file_statuses = Vec::new();

            // Convert untracked files to "Created" events
            for path in &status.untracked_files {
                if let Ok(metadata) = std::fs::metadata(repo_path.join(path)) {
                    file_statuses.push(crate::protocol::FileStatus {
                        path: path.clone(),
                        mtime: metadata.modified().unwrap_or(std::time::SystemTime::now()),
                        size: metadata.len(),
                        hash: None,
                        status: crate::protocol::FileStatusType::Created,
                    });
                }
            }

            // Convert modified files to "Modified" events
            for path in &status.modified_files {
                if let Ok(metadata) = std::fs::metadata(repo_path.join(path)) {
                    file_statuses.push(crate::protocol::FileStatus {
                        path: path.clone(),
                        mtime: metadata.modified().unwrap_or(std::time::SystemTime::now()),
                        size: metadata.len(),
                        hash: None,
                        status: crate::protocol::FileStatusType::Modified,
                    });
                }
            }

            // Convert removed files to "Removed" events
            for path in &status.removed_files {
                // For removed files, we can't get metadata since they don't exist
                file_statuses.push(crate::protocol::FileStatus {
                    path: path.clone(),
                    mtime: std::time::SystemTime::now(),
                    size: 0,
                    hash: None,
                    status: crate::protocol::FileStatusType::Removed,
                });
            }

            // Batch update the cache with initial state
            let total_files = status.untracked_files.len()
                + status.modified_files.len()
                + status.removed_files.len();
            if !file_statuses.is_empty() {
                cache.batch_update(file_statuses).await?;
                info!("Populated cache with {} initial file states", total_files);
            }

            cache.mark_scan_complete().await?;
            info!("Initial scan complete - established baseline, now tracking filesystem changes");
        }
        Err(e) => {
            error!("Failed to get initial status: {}", e);
            // Mark scan as complete anyway to avoid blocking
            cache.mark_scan_complete().await?;
        }
    }

    // Remove cached ref DB connection so it doesn't block other connections
    // TODO: update the ref_manager with the option to NOT cache the connection,
    // similar to how we configure the merkle tree node cache
    core::refs::remove_from_cache(repo.path)?;
    Ok(())
}
