use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "oxen-watcher")]
#[command(about = "Filesystem watcher daemon for Oxen repositories")]
#[command(version)]
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the filesystem watcher for a repository
    Start {
        /// Path to the repository
        #[arg(short, long)]
        repo: PathBuf,
        /// Idle timeout in seconds (default: 600 = 10 minutes, minimum: 60 - enforced in monitor)
        #[arg(short = 't', long, default_value = "600")]
        idle_timeout: u64,
    },
    /// Stop the filesystem watcher for a repository
    Stop {
        /// Path to the repository
        #[arg(short, long)]
        repo: PathBuf,
    },
    /// Check if the watcher is running for a repository
    Status {
        /// Path to the repository
        #[arg(short, long)]
        repo: PathBuf,
    },
    /// Query and display the current filesystem tree from the watcher
    Tree {
        /// Path to the repository
        #[arg(short, long)]
        repo: PathBuf,
        /// Optional path to query a subtree
        #[arg(short, long)]
        path: Option<PathBuf>,
        /// Show file metadata (size, mtime)
        #[arg(short = 'm', long)]
        metadata: bool,
        /// Maximum depth to display (0 = unlimited)
        #[arg(short, long, default_value = "0")]
        depth: usize,
        /// Show statistics summary at the end
        #[arg(short, long)]
        stats: bool,
    },
}
