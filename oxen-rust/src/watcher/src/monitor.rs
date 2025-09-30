use ignore::WalkBuilder;
use log::{error, info, warn};
use notify::RecursiveMode;
use notify_debouncer_full::{new_debouncer, DebounceEventResult};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;

use crate::cache::StatusCache;
use crate::constants;
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

        // Canonicalize the repo path to handle symlinks
        let repo_path = repo_path.canonicalize()?;

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
        // TODO: start processor _after_ the initial scan
        let processor_handle = tokio::spawn(async move { processor.run(event_rx).await });

        info!("Event processor started");

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

/// Perform initial scan of the repository using parallel walk
async fn initial_scan(repo_path: PathBuf, cache: Arc<StatusCache>) -> Result<(), WatcherError> {
    use crate::tree::FileMetadata;
    use std::sync::Mutex;

    info!("Starting initial repository scan with parallel walk");
    let start = Instant::now();

    // Perform parallel walk to enumerate all files and get metadata
    let walker = WalkBuilder::new(&repo_path)
        .threads(num_cpus::get())
        .hidden(false) // Include hidden files
        .add_custom_ignore_filename(constants::OXEN_IGNORE_FILE)
        .filter_entry(|entry| {
            // Skip .oxen directory
            entry
                .file_name()
                .to_str()
                .map(|name| name != ".oxen")
                .unwrap_or(true)
        })
        .build_parallel();

    // Collect file paths and metadata in parallel
    let (tx, rx) = std::sync::mpsc::channel();
    let tx = Arc::new(Mutex::new(tx));

    walker.run(|| {
        let tx = tx.clone();
        Box::new(move |result| {
            if let Ok(entry) = result {
                // Get file type and metadata
                if let Some(ft) = entry.file_type() {
                    if ft.is_file() {
                        // Get metadata for files
                        if let Ok(metadata) = entry.metadata() {
                            let file_metadata = FileMetadata {
                                size: metadata.len(),
                                mtime: metadata.modified().unwrap_or_else(|_| SystemTime::now()),
                                is_symlink: ft.is_symlink(),
                            };

                            if let Ok(tx) = tx.lock() {
                                let _ = tx.send((entry.into_path(), Some(file_metadata)));
                            }
                        }
                    } else if ft.is_dir() {
                        // Directories don't have metadata in our tree
                        if let Ok(tx) = tx.lock() {
                            let _ = tx.send((entry.into_path(), None));
                        }
                    }
                }
            }
            ignore::WalkState::Continue
        })
    });

    // Drop the original sender to signal completion
    drop(tx);

    // Collect all paths with metadata
    let mut updates = Vec::new();
    while let Ok((path, metadata)) = rx.recv() {
        // Convert absolute path to relative
        if let Ok(relative_path) = path.strip_prefix(&repo_path) {
            // Skip the root directory itself
            if relative_path == Path::new("") {
                continue;
            }
            updates.push((relative_path.to_path_buf(), metadata));
        }
    }

    let scan_duration = start.elapsed();
    let file_count = updates.iter().filter(|(_, m)| m.is_some()).count();
    let dir_count = updates.iter().filter(|(_, m)| m.is_none()).count();

    info!(
        "Initial scan found {} files and {} directories in {:.2}s",
        file_count,
        dir_count,
        scan_duration.as_secs_f64()
    );

    // Batch update the cache with initial state
    if !updates.is_empty() {
        cache.batch_update(updates).await?;
        info!(
            "Populated cache with {} total entries",
            file_count + dir_count
        );
    }

    cache.mark_scan_complete().await?;

    let total_duration = start.elapsed();
    info!(
        "Initial scan complete in {:.2}s",
        total_duration.as_secs_f64()
    );

    Ok(())
}
