// Protocol types for watcher communication

use crate::tree::FileSystemTree;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatcherRequest {
    /// Get the full tree or a subtree at a specific path
    GetTree { path: Option<PathBuf> },
    /// Get metadata for specific paths
    GetMetadata { paths: Vec<PathBuf> },
    /// Check if watcher is responsive
    Ping,
    /// Shut down the watcher daemon
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WatcherResponse {
    /// Full or partial filesystem tree
    Tree(FileSystemTree),
    /// Metadata for requested paths
    Metadata(Vec<(PathBuf, Option<crate::tree::FileMetadata>)>),
    /// Simple acknowledgment
    Ok,
    /// Error message
    Error(String),
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

#[cfg(test)]
#[path = "protocol_test.rs"]
mod protocol_test;
