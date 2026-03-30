use clap::{Parser, Subcommand};
use std::time::Duration;

use framework::FrameworkResult;

pub mod framework;
pub mod lmdb;
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
    Test,
}

fn main() -> FrameworkResult<()> {
    let args = Args::parse();

    match args.command {
        Commands::Migrate => migrate::migrate(),
        Commands::Test => migrate::test(),
    }
}
