use clap::Subcommand;
use std::time::Duration;

use super::framework::FrameworkResult;
use super::migrate;

#[allow(dead_code)]
struct TestMetrics {
    pack_time: Duration,
    unpack_time: Duration,
    _pack_cpu_usage: f32,
    _pack_memory_usage_bytes: u64,
    _unpack_cpu_usage: f32,
    _unpack_memory_usage_bytes: u64,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Migrate,
    Test,
}

pub fn run(command: Commands) -> FrameworkResult<()> {
    match command {
        Commands::Migrate => migrate::migrate(),
        Commands::Test => migrate::test(),
    }
}
