use crate::error::OxenError;
use crate::model::LocalRepository;
use std::collections::HashSet;
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

/// Client for communicating with the filesystem watcher daemon
pub struct WatcherClient {
    socket_path: PathBuf,
}

/// Status data received from the watcher
#[derive(Debug, Clone)]
pub struct WatcherStatus {
    pub untracked: HashSet<PathBuf>,
    pub modified: HashSet<PathBuf>,
    pub removed: HashSet<PathBuf>,
    pub scan_complete: bool,
    pub last_updated: SystemTime,
}

impl WatcherClient {
    /// Try to connect to the watcher daemon for a repository
    pub async fn connect(repo: &LocalRepository) -> Option<Self> {
        let socket_path = repo.path.join(".oxen/watcher.sock");

        // Check if socket exists
        if !socket_path.exists() {
            log::debug!("Watcher socket does not exist at {:?}", socket_path);
            return None;
        }

        // Return client with socket path - actual connection happens in get_status/ping
        log::debug!("Watcher socket found at {:?}", socket_path);
        Some(Self { socket_path })
    }

    /// Get the current status from the watcher
    pub async fn get_status(&self) -> Result<WatcherStatus, OxenError> {
        // Connect to the socket
        let mut stream = UnixStream::connect(&self.socket_path)
            .await
            .map_err(|e| OxenError::basic_str(&format!("Failed to connect to watcher: {}", e)))?;

        // Create request using the watcher protocol
        // We need to import the protocol types from the watcher crate
        let request = WatcherRequest::GetStatus { paths: None };
        let request_bytes = rmp_serde::to_vec(&request)
            .map_err(|e| OxenError::basic_str(&format!("Failed to serialize request: {}", e)))?;

        // Send request (length-prefixed)
        let len = request_bytes.len() as u32;
        stream
            .write_all(&len.to_le_bytes())
            .await
            .map_err(|e| OxenError::basic_str(&format!("Failed to write request length: {}", e)))?;
        stream
            .write_all(&request_bytes)
            .await
            .map_err(|e| OxenError::basic_str(&format!("Failed to write request: {}", e)))?;
        stream
            .flush()
            .await
            .map_err(|e| OxenError::basic_str(&format!("Failed to flush stream: {}", e)))?;

        // Read response length
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| OxenError::basic_str(&format!("Failed to read response length: {}", e)))?;
        let response_len = u32::from_le_bytes(len_buf) as usize;

        // Sanity check response size
        if response_len > 100 * 1024 * 1024 {
            // 100MB max
            return Err(OxenError::basic_str(&format!(
                "Response too large: {} bytes",
                response_len
            )));
        }

        // Read response body
        let mut response_buf = vec![0u8; response_len];
        stream
            .read_exact(&mut response_buf)
            .await
            .map_err(|e| OxenError::basic_str(&format!("Failed to read response: {}", e)))?;

        // Deserialize response
        let response: WatcherResponse = rmp_serde::from_slice(&response_buf)
            .map_err(|e| OxenError::basic_str(&format!("Failed to deserialize response: {}", e)))?;
        
        // Gracefully shutdown the connection
        let _ = stream.shutdown().await;

        // Convert response to WatcherStatus
        match response {
            WatcherResponse::Status(status_result) => Ok(WatcherStatus {
                untracked: status_result.untracked.into_iter().collect(),
                modified: status_result.modified.into_iter().map(|f| f.path).collect(),
                removed: status_result.removed.into_iter().collect(),
                scan_complete: status_result.scan_complete,
                last_updated: SystemTime::now(),
            }),
            WatcherResponse::Error(msg) => {
                Err(OxenError::basic_str(&format!("Watcher error: {}", msg)))
            }
            _ => Err(OxenError::basic_str("Unexpected response from watcher")),
        }
    }

    /// Check if the watcher is responsive
    pub async fn ping(&self) -> bool {
        match UnixStream::connect(&self.socket_path).await {
            Ok(mut stream) => {
                // Send ping request
                let request = WatcherRequest::Ping;
                if let Ok(request_bytes) = rmp_serde::to_vec(&request) {
                    let len = request_bytes.len() as u32;
                    if stream.write_all(&len.to_le_bytes()).await.is_ok()
                        && stream.write_all(&request_bytes).await.is_ok()
                        && stream.flush().await.is_ok()
                    {
                        // Try to read response
                        let mut len_buf = [0u8; 4];
                        if stream.read_exact(&mut len_buf).await.is_ok() {
                            let response_len = u32::from_le_bytes(len_buf) as usize;
                            if response_len < 1000 {
                                // Ping response should be small
                                let mut response_buf = vec![0u8; response_len];
                                if stream.read_exact(&mut response_buf).await.is_ok() {
                                    // Gracefully shutdown the connection before checking response
                                    let _ = stream.shutdown().await;
                                    if let Ok(response) =
                                        rmp_serde::from_slice::<WatcherResponse>(&response_buf)
                                    {
                                        matches!(response, WatcherResponse::Ok)
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    }
}

// We need to define the protocol types here temporarily
// In a real implementation, these would be imported from the watcher crate
// or defined in a shared protocol module

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum WatcherRequest {
    GetStatus { paths: Option<Vec<PathBuf>> },
    GetSummary,
    Refresh { paths: Vec<PathBuf> },
    Shutdown,
    Ping,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum WatcherResponse {
    Status(StatusResult),
    Summary {
        modified: usize,
        added: usize,
        removed: usize,
        untracked: usize,
        last_updated: SystemTime,
    },
    Ok,
    Error(String),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StatusResult {
    pub modified: Vec<FileStatus>,
    pub added: Vec<FileStatus>,
    pub removed: Vec<PathBuf>,
    pub untracked: Vec<PathBuf>,
    pub scan_complete: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FileStatus {
    pub path: PathBuf,
    pub mtime: SystemTime,
    pub size: u64,
    pub hash: Option<String>,
    pub status: FileStatusType,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum FileStatusType {
    Modified,
    Added,
    Removed,
    Untracked,
}
