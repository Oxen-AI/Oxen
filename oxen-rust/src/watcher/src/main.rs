mod cache;
mod cli;
mod error;
mod event_processor;
mod ipc;
mod monitor;
mod protocol;

use clap::Parser;
use log::info;
use std::path::{Path, PathBuf};

use crate::cli::Args;
use crate::error::WatcherError;

#[tokio::main]
async fn main() -> Result<(), WatcherError> {
    env_logger::init();
    
    let args = Args::parse();
    
    match args.command {
        cli::Commands::Start { repo } => {
            info!("Starting watcher for repository: {}", repo.display());
            start_watcher(repo).await
        }
        cli::Commands::Stop { repo } => {
            info!("Stopping watcher for repository: {}", repo.display());
            stop_watcher(repo).await
        }
        cli::Commands::Status { repo } => {
            info!("Checking watcher status for repository: {}", repo.display());
            check_status(repo).await
        }
    }
}

async fn start_watcher(repo_path: PathBuf) -> Result<(), WatcherError> {
    // Check if watcher is already running
    if is_watcher_running(&repo_path).await? {
        info!("Watcher is already running for this repository");
        return Ok(());
    }
    
    // Initialize and run the watcher
    let watcher = monitor::FileSystemWatcher::new(repo_path)?;
    watcher.run().await
}

async fn stop_watcher(repo_path: PathBuf) -> Result<(), WatcherError> {
    let socket_path = repo_path.join(".oxen/watcher.sock");
    
    // Send shutdown request
    match ipc::send_request(&socket_path, protocol::WatcherRequest::Shutdown).await {
        Ok(_) => {
            info!("Watcher stopped successfully");
            Ok(())
        }
        Err(e) => {
            log::warn!("Failed to stop watcher: {}", e);
            // Clean up pid file if present
            let pid_file = repo_path.join(".oxen/watcher.pid");
            if pid_file.exists() {
                std::fs::remove_file(pid_file)?;
            }
            Ok(())
        }
    }
}

async fn check_status(repo_path: PathBuf) -> Result<(), WatcherError> {
    if is_watcher_running(&repo_path).await? {
        println!("Watcher is running");
    } else {
        println!("Watcher is not running");
    }
    Ok(())
}

async fn is_watcher_running(repo_path: &Path) -> Result<bool, WatcherError> {
    let socket_path = repo_path.join(".oxen/watcher.sock");
    
    // Try to ping the watcher
    match ipc::send_request(&socket_path, protocol::WatcherRequest::Ping).await {
        Ok(protocol::WatcherResponse::Ok) => Ok(true),
        _ => Ok(false),
    }
}