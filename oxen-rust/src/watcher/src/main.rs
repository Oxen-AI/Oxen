mod cache;
mod cli;
mod error;
mod event_processor;
mod ipc;
mod monitor;
mod protocol;
mod tree;
mod util;

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
        cli::Commands::Tree {
            repo,
            path,
            metadata,
            depth,
            stats,
        } => query_and_display_tree(repo, path, metadata, depth, stats).await,
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

async fn query_and_display_tree(
    repo_path: PathBuf,
    path: Option<PathBuf>,
    show_metadata: bool,
    max_depth: usize,
    show_stats: bool,
) -> Result<(), WatcherError> {
    let socket_path = repo_path.join(".oxen/watcher.sock");

    // Send request to get tree
    let request = protocol::WatcherRequest::GetTree { path: path.clone() };
    let response = ipc::send_request(&socket_path, request).await?;

    match response {
        protocol::WatcherResponse::Tree(tree) => {
            // Pretty print the tree
            if let Some(subtree_path) = &path {
                println!("📁 Tree at: {}", subtree_path.display());
            } else {
                println!("📁 Repository tree:");
            }
            println!();

            // When querying a subtree, print its contents directly
            // When querying the full tree, skip the root node
            let is_subtree_query = path.is_some();
            if is_subtree_query {
                // For subtree queries, print the children of the root directly
                let child_count = tree.root.children.len();
                let mut child_idx = 0;

                for (_name, child) in &tree.root.children {
                    child_idx += 1;
                    let is_last_child = child_idx == child_count;
                    // For subtree, we're showing the contents at depth 1
                    print_tree_node(child, "", is_last_child, show_metadata, max_depth, 1);
                }
            } else {
                // For full tree queries, process normally (skip root)
                print_tree_node(&tree.root, "", true, show_metadata, max_depth, 0);
            }

            // Show statistics if requested
            if show_stats {
                println!();
                println!("─────────────────────────────────────────");
                print_tree_stats(&tree);
            }

            Ok(())
        }
        protocol::WatcherResponse::Error(msg) => {
            eprintln!("Error querying tree: {}", msg);
            Err(WatcherError::Communication(msg))
        }
        _ => {
            eprintln!("Unexpected response from watcher");
            Err(WatcherError::Communication(
                "Unexpected response type".to_string(),
            ))
        }
    }
}

fn print_tree_node(
    node: &tree::TreeNode,
    prefix: &str,
    is_last: bool,
    show_metadata: bool,
    max_depth: usize,
    current_depth: usize,
) {
    // Check depth limit
    if max_depth > 0 && current_depth > max_depth {
        return;
    }

    // Skip printing the root node itself on first call
    if current_depth > 0 {
        // Print the tree branch characters
        print!("{}", prefix);
        if is_last {
            print!("└── ");
        } else {
            print!("├── ");
        }

        // Print the node name with appropriate icon
        match node.node_type {
            tree::NodeType::Directory => {
                print!("📁 {}", node.name);
                if !node.children.is_empty() {
                    print!(" ({} items)", node.children.len());
                }
            }
            tree::NodeType::File(ref metadata) => {
                print!("📄 {}", node.name);
                if show_metadata {
                    print!(
                        " ({}, {})",
                        format_size(metadata.size),
                        format_time(&metadata.mtime)
                    );
                }
            }
        }
        println!();
    }

    // Process children
    let child_count = node.children.len();
    let mut child_idx = 0;

    for (_name, child) in &node.children {
        child_idx += 1;
        let is_last_child = child_idx == child_count;

        let new_prefix = if current_depth == 0 {
            String::new()
        } else {
            format!("{}{}    ", prefix, if is_last { " " } else { "│" })
        };

        print_tree_node(
            child,
            &new_prefix,
            is_last_child,
            show_metadata,
            max_depth,
            current_depth + 1,
        );
    }
}

fn format_size(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", size as u64, UNITS[unit_idx])
    } else {
        format!("{:.1} {}", size, UNITS[unit_idx])
    }
}

fn format_time(time: &std::time::SystemTime) -> String {
    use chrono::{DateTime, Local};

    let datetime: DateTime<Local> = (*time).into();
    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn print_tree_stats(tree: &tree::FileSystemTree) {
    let mut file_count = 0;
    let mut dir_count = 0;
    let mut total_size = 0u64;

    count_nodes(&tree.root, &mut file_count, &mut dir_count, &mut total_size);

    println!("📊 Statistics:");
    println!("  Files:       {}", file_count);
    println!("  Directories: {}", dir_count);
    println!("  Total size:  {}", format_size(total_size));
    println!("  Last update: {}", format_time(&tree.last_updated));
    println!(
        "  Scan complete: {}",
        if tree.scan_complete {
            "✅ Yes"
        } else {
            "⏳ No"
        }
    );
}

fn count_nodes(
    node: &tree::TreeNode,
    file_count: &mut usize,
    dir_count: &mut usize,
    total_size: &mut u64,
) {
    match &node.node_type {
        tree::NodeType::Directory => {
            *dir_count += 1;
            for (_name, child) in &node.children {
                count_nodes(child, file_count, dir_count, total_size);
            }
        }
        tree::NodeType::File(metadata) => {
            *file_count += 1;
            *total_size += metadata.size;
        }
    }
}
