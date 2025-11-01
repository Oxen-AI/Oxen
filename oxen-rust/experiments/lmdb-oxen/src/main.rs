use clap::{Parser, Subcommand};
use std::{fs, string};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use framework::{FrameworkError, FrameworkResult};

pub mod framework;
pub mod migrate;
pub mod lmdb;
pub mod nodes;

// Include the generated Cap'n Proto code at the crate root
pub mod merkle_tree_capnp {
    include!(concat!(env!("OUT_DIR"), "/merkle_tree_capnp.rs"));
}

pub mod capnp_serde;
pub mod capnp_wrapper;
pub mod benchmark_capnp;
pub mod benchmark_db;
pub mod lmdb_zerocopy;

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
    /// Benchmark Cap'n Proto vs MessagePack deserialization
    Benchmark {
        /// Number of iterations for the benchmark
        #[arg(short, long, default_value = "1000")]
        iterations: usize,
    },
    /// Benchmark database operations with Cap'n Proto vs MessagePack
    BenchmarkDb,
    // Test,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    match args.command {
        Commands::Migrate => {
            migrate::migrate()
        }
        Commands::Benchmark { iterations } => {
            run_benchmark(iterations)
        }
        Commands::BenchmarkDb => {
            run_db_benchmark()
        }
        // Commands::Test => {
        //     migrate::test()
        // }
    }
}

fn run_benchmark(iterations: usize) -> Result<(), Box<dyn std::error::Error>> {
    use liboxen::model::merkle_tree::node::MerkleTreeNode;
    use liboxen::model::{LocalRepository, MerkleHash};
    use liboxen::repositories;
    
    // Load repository and get a sample node
    let repo = LocalRepository::from_current_dir()?;
    let head_commit = repositories::commits::head_commit_maybe(&repo)?.ok_or("No head commit found")?;
    let commit_hash: MerkleHash = head_commit.id.parse()?;
    
    println!("Loading MerkleTreeNode from commit: {}", commit_hash);
    let node = MerkleTreeNode::from_hash(&repo, &commit_hash)?;
    
    // Run benchmark
    benchmark_capnp::run_benchmark(&node, iterations);
    
    Ok(())
}

fn run_db_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    use std::collections::HashSet;
    use std::path::PathBuf;
    use liboxen::model::merkle_tree::node::MerkleTreeNode;
    use liboxen::model::{LocalRepository, MerkleHash};
    use liboxen::repositories;
    use liboxen::core::v_latest::index::CommitMerkleTree;
    
    // Load repository and collect some nodes
    let repo = LocalRepository::from_current_dir()?;
    let head_commit = repositories::commits::head_commit_maybe(&repo)?.ok_or("No head commit found")?;
    
    println!("Loading nodes from commit: {}", head_commit.id);
    let commit_tree = CommitMerkleTree::from_commit(&repo, &head_commit)?;
    
    // Collect nodes (limit to avoid too large dataset)
    let mut nodes = HashSet::new();
    let mut count = 0;
    let max_nodes = 100; // Adjust as needed
    
    commit_tree.root.walk_tree(|node| {
        if count < max_nodes {
            nodes.insert((node.hash, node.clone()));
            count += 1;
        }
    });
    
    println!("Collected {} nodes for benchmarking", nodes.len());
    println!();
    
    // Create temporary database paths
    let db_path_capnp = PathBuf::from("./benchmark_db_capnp");
    let db_path_msgpack = PathBuf::from("./benchmark_db_msgpack");
    
    // Clean up old databases if they exist
    let _ = std::fs::remove_dir_all(&db_path_capnp);
    let _ = std::fs::remove_dir_all(&db_path_msgpack);
    
    // Run benchmark
    benchmark_db::benchmark_db_operations(nodes, &db_path_capnp, &db_path_msgpack)?;
    
    println!();
    println!("Benchmark databases created at:");
    println!("  Cap'n Proto: {:?}", db_path_capnp);
    println!("  MessagePack: {:?}", db_path_msgpack);
    
    Ok(())
}
