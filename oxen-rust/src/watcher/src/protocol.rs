use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;

/// Request messages sent from CLI to Watcher
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatcherRequest {
    /// Get the current status of the repository
    GetStatus {
        /// Optional paths to filter status for
        paths: Option<Vec<PathBuf>>,
    },
    /// Get a summary of changes (just counts)
    GetSummary,
    /// Force a refresh/rescan of specific paths
    Refresh {
        paths: Vec<PathBuf>,
    },
    /// Shutdown the watcher daemon
    Shutdown,
    /// Health check ping
    Ping,
}

/// Response messages sent from Watcher to CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatcherResponse {
    /// Full status result
    Status(StatusResult),
    /// Summary of changes
    Summary {
        modified: usize,
        added: usize,
        removed: usize,
        untracked: usize,
        last_updated: SystemTime,
    },
    /// Simple acknowledgment
    Ok,
    /// Error response
    Error(String),
}

/// Detailed status result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResult {
    pub modified: Vec<FileStatus>,
    pub added: Vec<FileStatus>,
    pub removed: Vec<PathBuf>,
    pub untracked: Vec<PathBuf>,
    /// False if still doing initial scan
    pub scan_complete: bool,
}

/// Status of a single file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatus {
    pub path: PathBuf,
    pub mtime: SystemTime,
    pub size: u64,
    pub hash: Option<String>,
    pub status: FileStatusType,
}

/// Type of file status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FileStatusType {
    Modified,
    Added,
    Removed,
    Untracked,
}

impl WatcherRequest {
    /// Serialize request to MessagePack bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }
    
    /// Deserialize request from MessagePack bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}

impl WatcherResponse {
    /// Serialize response to MessagePack bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
        rmp_serde::to_vec(self)
    }
    
    /// Deserialize response from MessagePack bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, rmp_serde::decode::Error> {
        rmp_serde::from_slice(bytes)
    }
}