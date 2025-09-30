use clap::{Parser, Subcommand};
use std::{fs, string};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use framework::{FrameworkError, FrameworkResult};

pub mod framework;
pub mod migrate;

struct TestMetrics {
    pack_time: Duration,
    unpack_time: Duration,
    _pack_cpu_usage: f32,
    _pack_memory_usage_bytes: u64,
    _unpack_cpu_usage: f32,
    _unpack_memory_usage_bytes: u64,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Migrate,
}

fn main() -> FrameworkResult<()> {
    let args = Args::parse();

    match args.command {
        Commands::Migrate => {
            migrate::migrate()
        }
    }
}
