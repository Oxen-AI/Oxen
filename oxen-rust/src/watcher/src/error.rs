use thiserror::Error;

#[derive(Error, Debug)]
pub enum WatcherError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Notify error: {0}")]
    Notify(#[from] notify::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(#[from] rmp_serde::encode::Error),
    
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] rmp_serde::decode::Error),
    
    #[error("Oxen error: {0}")]
    Oxen(#[from] liboxen::error::OxenError),
    
    #[error("Repository not found at: {0}")]
    RepositoryNotFound(String),
    
    #[error("Watcher already running")]
    #[allow(dead_code)]  // Will be used when we implement multiple watcher prevention
    AlreadyRunning,
    
    #[error("Failed to communicate with watcher: {0}")]
    Communication(String),
}