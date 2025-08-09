use crate::cache::StatusCache;
use crate::error::WatcherError;
use crate::event_processor::EventProcessor;
use crate::ipc::IpcServer;
use liboxen::model::LocalRepository;
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;

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

        // Create channel for filesystem events
        let (event_tx, event_rx) = mpsc::channel::<Event>(1000);

        // Create the notify watcher
        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            match res {
                Ok(event) => {
                    // Try to send event, drop if channel is full
                    let _ = event_tx.blocking_send(event);
                }
                Err(e) => error!("Filesystem watch error: {}", e),
            }
        })?;

        // Watch the repository directory (excluding .oxen)
        watcher.watch(&self.repo_path, RecursiveMode::Recursive)?;
        info!("Watching directory: {}", self.repo_path.display());

        // Start the event processor
        let processor = EventProcessor::new(self.cache.clone());
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
        drop(watcher);

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

    // Use Oxen's existing status implementation for initial state
    match liboxen::repositories::status::status(&repo) {
        Ok(status) => {
            let mut file_statuses = Vec::new();

            // Convert Oxen status to our format
            for path in status.modified_files {
                if let Ok(metadata) = std::fs::metadata(repo_path.join(path.clone())) {
                    file_statuses.push(crate::protocol::FileStatus {
                        path: path.clone(),
                        mtime: metadata.modified().unwrap_or(std::time::SystemTime::now()),
                        size: metadata.len(),
                        hash: None,
                        status: crate::protocol::FileStatusType::Modified,
                    });
                }
            }

            for (path, entry) in status.staged_files {
                let file_status_type = match entry.status {
                    liboxen::model::StagedEntryStatus::Added => {
                        crate::protocol::FileStatusType::Added
                    }
                    liboxen::model::StagedEntryStatus::Modified => {
                        crate::protocol::FileStatusType::Modified
                    }
                    liboxen::model::StagedEntryStatus::Removed => {
                        crate::protocol::FileStatusType::Removed
                    }
                    liboxen::model::StagedEntryStatus::Unmodified => {
                        continue; // Skip unmodified files
                    }
                };

                let (mtime, size) = if let Ok(metadata) = std::fs::metadata(repo_path.join(path.clone()))
                {
                    (
                        metadata.modified().unwrap_or(std::time::SystemTime::now()),
                        metadata.len(),
                    )
                } else {
                    // File might not exist if it was removed
                    (std::time::SystemTime::now(), 0)
                };

                file_statuses.push(crate::protocol::FileStatus {
                    path: path.clone(),
                    mtime,
                    size,
                    hash: Some(entry.hash),
                    status: file_status_type,
                });
            }

            for path in status.untracked_files {
                if let Ok(metadata) = std::fs::metadata(repo_path.join(path.clone())) {
                    file_statuses.push(crate::protocol::FileStatus {
                        path: path.clone(),
                        mtime: metadata.modified().unwrap_or(std::time::SystemTime::now()),
                        size: metadata.len(),
                        hash: None,
                        status: crate::protocol::FileStatusType::Untracked,
                    });
                }
            }

            // Batch update the cache
            cache.batch_update(file_statuses).await?;
            cache.mark_scan_complete().await?;

            info!("Initial scan complete");
        }
        Err(e) => {
            error!("Failed to get initial status: {}", e);
            // Mark scan as complete anyway to avoid blocking
            cache.mark_scan_complete().await?;
        }
    }

    Ok(())
}
