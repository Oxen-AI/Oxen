pub mod cache;
pub mod cli;
pub mod error;
pub mod event_processor;
pub mod ipc;
pub mod monitor;
pub mod protocol;

pub use error::WatcherError;
pub use protocol::{WatcherRequest, WatcherResponse};