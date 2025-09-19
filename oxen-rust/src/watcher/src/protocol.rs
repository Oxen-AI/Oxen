// Protocol types for watcher communication
// These will eventually replace the types in liboxen::core::v_latest::watcher_client

use crate::tree::{FileMetadata, FileSystemTree};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// Re-export old types for compatibility - will be removed after migration
pub use liboxen::core::v_latest::watcher_client::{
    FileStatus, FileStatusType, StatusResult, WatcherRequest as OldWatcherRequest,
    WatcherResponse as OldWatcherResponse,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatcherRequest {
    /// Get the full tree or a subtree at a specific path
    GetTree { path: Option<PathBuf> },
    /// Get metadata for specific files
    GetMetadata { paths: Vec<PathBuf> },
    /// Check if watcher is responsive
    Ping,
    /// Shut down the watcher daemon
    Shutdown,
    /// Old protocol support (will be removed)
    GetStatus { paths: Option<Vec<PathBuf>> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatcherResponse {
    /// Full or partial filesystem tree
    Tree(FileSystemTree),
    /// Metadata for requested files
    Metadata(Vec<(PathBuf, Option<FileMetadata>)>),
    /// Simple acknowledgment
    Ok,
    /// Error message
    Error(String),
    /// Old protocol support (will be removed)
    Status(StatusResult),
}

impl WatcherRequest {
    pub fn to_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

impl WatcherResponse {
    pub fn to_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

// Helper to convert old request to new format
impl From<OldWatcherRequest> for WatcherRequest {
    fn from(old: OldWatcherRequest) -> Self {
        match old {
            OldWatcherRequest::GetStatus { paths } => WatcherRequest::GetStatus { paths },
            OldWatcherRequest::Ping => WatcherRequest::Ping,
            OldWatcherRequest::Shutdown => WatcherRequest::Shutdown,
            _ => WatcherRequest::Ping, // Default fallback
        }
    }
}

// Helper to convert new response to old format
impl WatcherResponse {
    pub fn to_old(&self) -> OldWatcherResponse {
        match self {
            WatcherResponse::Status(s) => OldWatcherResponse::Status(s.clone()),
            WatcherResponse::Ok => OldWatcherResponse::Ok,
            WatcherResponse::Error(e) => OldWatcherResponse::Error(e.clone()),
            WatcherResponse::Tree(_) | WatcherResponse::Metadata(_) => {
                OldWatcherResponse::Error("Unsupported response type".to_string())
            }
        }
    }
}

#[path = "protocol_test.rs"]
mod protocol_test;