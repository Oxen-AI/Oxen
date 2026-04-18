pub mod common;
pub mod mixed;
pub mod read;
pub mod write;

use clap::Subcommand;

#[derive(Subcommand, Debug)]
pub enum BenchCommands {
    /// Measure `commit_tree` throughput on a randomly generated Merkle tree.
    Write(write::WriteArgs),
    /// Commit a tree, then measure `.node()`, `.commit()`, and `.path()` throughput.
    Read(read::ReadArgs),
    /// Read-heavy, write-infrequent mixed workload with a pre-seed phase.
    Mixed(mixed::MixedArgs),
}

pub async fn run(command: BenchCommands) {
    match command {
        BenchCommands::Write(args) => write::run(args).await,
        BenchCommands::Read(args) => read::run(args).await,
        BenchCommands::Mixed(args) => mixed::run(args).await,
    }
}
