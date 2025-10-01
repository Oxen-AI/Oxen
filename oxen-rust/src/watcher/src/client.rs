use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::error::WatcherError;
use crate::protocol::{WatcherRequest, WatcherResponse};
use crate::tree::FileSystemTree;

/// Client for communicating with the filesystem watcher daemon
pub struct WatcherClient {
    socket_path: PathBuf,
}

/// Status data received from the watcher
#[derive(Debug, Clone)]
pub struct WatcherStatus {
    /// Files created since watcher started (includes untracked files from initial scan)
    pub created: HashSet<PathBuf>,
    /// Files modified since watcher started
    pub modified: HashSet<PathBuf>,
    /// Files removed since watcher started
    pub removed: HashSet<PathBuf>,
    /// Whether the initial scan is complete
    pub scan_complete: bool,
    /// Last update time
    pub last_updated: SystemTime,
}

impl WatcherClient {
    /// Create a new client for a repository path
    pub fn new(repo_path: impl AsRef<Path>) -> Self {
        let socket_path = repo_path.as_ref().join(".oxen/watcher.sock");
        Self { socket_path }
    }

    /// Try to connect to the watcher daemon for a repository
    pub async fn connect(repo_path: impl AsRef<Path>) -> Option<Self> {
        let socket_path = repo_path.as_ref().join(".oxen/watcher.sock");

        // Check if socket exists
        if !socket_path.exists() {
            log::debug!("Watcher socket does not exist at {:?}", socket_path);
            return None;
        }

        // Return client with socket path - actual connection happens in get_status/ping
        log::debug!("Watcher socket found at {:?}", socket_path);
        Some(Self { socket_path })
    }

    /// Get the current filesystem tree from the watcher
    pub async fn get_tree(&self, path: Option<PathBuf>) -> Result<FileSystemTree, WatcherError> {
        let start = Instant::now();
        // Connect to the socket
        let mut stream = UnixStream::connect(&self.socket_path).await.map_err(|e| {
            WatcherError::Communication(format!("Failed to connect to watcher: {}", e))
        })?;

        // Create request
        let request = WatcherRequest::GetTree { path };
        let request_bytes = request.to_bytes()?;

        // Send request (length-prefixed)
        let len = request_bytes.len() as u32;
        stream.write_all(&len.to_le_bytes()).await.map_err(|e| {
            WatcherError::Communication(format!("Failed to write request length: {}", e))
        })?;
        stream
            .write_all(&request_bytes)
            .await
            .map_err(|e| WatcherError::Communication(format!("Failed to write request: {}", e)))?;
        stream
            .flush()
            .await
            .map_err(|e| WatcherError::Communication(format!("Failed to flush stream: {}", e)))?;

        // Read response length
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(|e| {
            WatcherError::Communication(format!("Failed to read response length: {}", e))
        })?;
        let response_len = u32::from_le_bytes(len_buf) as usize;

        // Sanity check response size
        if response_len > 100 * 1024 * 1024 {
            // 100MB max
            return Err(WatcherError::Communication(format!(
                "Response too large: {} bytes",
                response_len
            )));
        }

        // Read response body
        let mut response_buf = vec![0u8; response_len];
        stream
            .read_exact(&mut response_buf)
            .await
            .map_err(|e| WatcherError::Communication(format!("Failed to read response: {}", e)))?;

        let read_time = start.elapsed();
        log::info!(
            "Received response of {} bytes in {}ms",
            response_len,
            read_time.as_millis()
        );

        // Deserialize response
        let response = WatcherResponse::from_bytes(&response_buf)?;

        let deserialize_time = start.elapsed() - read_time;
        log::info!(
            "Deserialized response in {}ms",
            deserialize_time.as_millis()
        );

        // Gracefully shutdown the connection
        let _ = stream.shutdown().await;

        // Convert response to tree
        match response {
            WatcherResponse::Tree(tree) => Ok(tree),
            WatcherResponse::Error(msg) => Err(WatcherError::Communication(format!(
                "Watcher error: {}",
                msg
            ))),
            _ => Err(WatcherError::Communication(
                "Unexpected response from watcher".to_string(),
            )),
        }
    }

    // TODO: this is just a stub for compatibility
    pub async fn get_status(&self) -> Result<WatcherStatus, WatcherError> {
        // For now, just return an empty status
        Ok(WatcherStatus {
            created: HashSet::new(),
            modified: HashSet::new(),
            removed: HashSet::new(),
            scan_complete: true,
            last_updated: SystemTime::now(),
        })
    }

    /// Check if the watcher is responsive
    pub async fn ping(&self) -> bool {
        // Try to connect to the socket
        let Ok(mut stream) = UnixStream::connect(&self.socket_path).await else {
            return false;
        };

        // Serialize ping request
        let request = WatcherRequest::Ping;
        let Ok(request_bytes) = request.to_bytes() else {
            return false;
        };

        // Send request length
        let len = request_bytes.len() as u32;
        let Ok(_) = stream.write_all(&len.to_le_bytes()).await else {
            return false;
        };

        // Send request
        let Ok(_) = stream.write_all(&request_bytes).await else {
            return false;
        };

        // Flush the stream
        let Ok(_) = stream.flush().await else {
            return false;
        };

        // Read response length
        let mut len_buf = [0u8; 4];
        let Ok(_) = stream.read_exact(&mut len_buf).await else {
            return false;
        };

        let response_len = u32::from_le_bytes(len_buf) as usize;

        // Sanity check: ping response should be small
        if response_len >= 1000 {
            return false;
        }

        // Read response
        let mut response_buf = vec![0u8; response_len];
        let Ok(_) = stream.read_exact(&mut response_buf).await else {
            return false;
        };

        // Gracefully shutdown the connection
        let _ = stream.shutdown().await;

        // Check if we got an Ok response
        let Ok(response) = WatcherResponse::from_bytes(&response_buf) else {
            return false;
        };

        matches!(response, WatcherResponse::Ok)
    }
}
