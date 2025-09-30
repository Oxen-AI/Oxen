pub mod cache;
pub mod cli;
pub mod client;
pub mod constants;
pub mod error;
pub mod event_processor;
pub mod ipc;
pub mod monitor;
pub mod protocol;
pub mod tree;
pub mod util;

pub use error::WatcherError;
pub use monitor::FileSystemWatcher;
// pub use tree::{FileMetadata, FileSystemTree, NodeType, TreeNode};
// pub use cli::{Args, Commands};
// pub use client::{WatcherClient, WatcherStatus};
// pub use ipc::send_request;
// pub use protocol::{WatcherRequest, WatcherResponse};
