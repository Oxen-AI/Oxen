use crate::cache::StatusCache;
use crate::error::WatcherError;
use crate::protocol::{WatcherRequest, WatcherResponse};
use log::{debug, error, info};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

/// IPC server that handles client requests
pub struct IpcServer {
    repo_path: PathBuf,
    cache: Arc<StatusCache>,
}

impl IpcServer {
    pub fn new(repo_path: PathBuf, cache: Arc<StatusCache>) -> Self {
        Self { repo_path, cache }
    }

    /// Run the IPC server
    pub async fn run(self) -> Result<(), WatcherError> {
        let socket_path = self.repo_path.join(".oxen/watcher.sock");

        // Remove old socket if it exists
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        // Create the Unix socket listener
        let listener = UnixListener::bind(&socket_path)?;
        info!("IPC server listening on {}", socket_path.display());

        // Track last request time for idle timeout
        let idle_timeout = Duration::from_secs(600); // 10 minutes
        let mut last_request = Instant::now();

        loop {
            // Accept connections with timeout check
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            last_request = Instant::now();

                            // Handle client in a separate task
                            let cache = self.cache.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_client(stream, cache).await {
                                    error!("Error handling client: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }

                // Check for idle timeout
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    if last_request.elapsed() > idle_timeout {
                        info!("Idle timeout reached, shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }
}

/// Handle a single client connection
async fn handle_client(
    mut stream: UnixStream,
    cache: Arc<StatusCache>,
) -> Result<(), WatcherError> {
    info!("Handling incoming client connection");
    // Read message length (4 bytes, little-endian)
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    // Sanity check message size (max 10MB)
    if len > 10 * 1024 * 1024 {
        error!("Message too large: {} bytes", len);
        return Err(WatcherError::Communication("Message too large".to_string()));
    }

    // Read message body
    let mut msg_buf = vec![0u8; len];
    stream.read_exact(&mut msg_buf).await?;

    // Deserialize request
    let request = WatcherRequest::from_bytes(&msg_buf)?;
    info!("Received request: {:?}", request);

    // Process request
    let response = match request {
        WatcherRequest::GetStatus { paths } => {
            let status = cache.get_status(paths).await;
            WatcherResponse::Status(status)
        }

        WatcherRequest::GetSummary => {
            let status = cache.get_status(None).await;
            WatcherResponse::Summary {
                modified: status.modified.len(),
                added: status.added.len(),
                removed: status.removed.len(),
                untracked: status.untracked.len(),
                last_updated: std::time::SystemTime::now(),
            }
        }

        WatcherRequest::Refresh { paths } => {
            // TODO: Implement forced refresh
            debug!("Refresh requested for {:?}", paths);
            WatcherResponse::Ok
        }

        WatcherRequest::Shutdown => {
            info!("Shutdown requested via IPC");
            // Send response before shutting down
            let response = WatcherResponse::Ok;
            send_response(&mut stream, &response).await?;

            // Exit the process
            std::process::exit(0);
        }

        WatcherRequest::Ping => WatcherResponse::Ok,
    };

    // Send response
    send_response(&mut stream, &response).await?;
    info!("Sent response");

    Ok(())
}

/// Send a response to the client
async fn send_response(
    stream: &mut UnixStream,
    response: &WatcherResponse,
) -> Result<(), WatcherError> {
    // Serialize response
    let msg = response.to_bytes()?;

    // Write length prefix
    let len = msg.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;

    // Write message
    stream.write_all(&msg).await?;
    stream.flush().await?;

    Ok(())
}

/// Send a request to the watcher (used by CLI)
pub async fn send_request(
    socket_path: &PathBuf,
    request: WatcherRequest,
) -> Result<WatcherResponse, WatcherError> {
    // Connect to the socket
    let mut stream = UnixStream::connect(socket_path)
        .await
        .map_err(|e| WatcherError::Communication(format!("Failed to connect: {}", e)))?;

    // Serialize request
    let msg = request.to_bytes()?;

    // Send length prefix
    let len = msg.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;

    // Send message
    stream.write_all(&msg).await?;
    stream.flush().await?;

    // Read response length
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;

    // Read response body
    let mut msg_buf = vec![0u8; len];
    stream.read_exact(&mut msg_buf).await?;

    // Deserialize response
    let response = WatcherResponse::from_bytes(&msg_buf)?;

    Ok(response)
}
