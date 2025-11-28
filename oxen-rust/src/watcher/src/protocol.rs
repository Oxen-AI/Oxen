// Re-export protocol types from liboxen in case we move them in the future
pub use liboxen::core::v_latest::watcher_client::{
    FileStatus, FileStatusType, StatusResult, WatcherRequest, WatcherResponse,
};

#[path = "protocol_test.rs"]
mod protocol_test;
