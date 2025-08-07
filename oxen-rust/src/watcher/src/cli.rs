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
}