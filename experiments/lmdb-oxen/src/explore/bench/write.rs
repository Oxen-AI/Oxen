use std::time::Instant;

use clap::Args;
use rand::SeedableRng;
use rand::rngs::StdRng;

use crate::explore::bench::common::{self, LmdbSetup, TreeGenArgs};
use crate::explore::hash::{HasHash, HexHash};
use crate::explore::lazy_merkle::UncomittedRoot;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;

#[derive(Args, Debug)]
pub struct WriteArgs {
    #[command(flatten)]
    tree: TreeGenArgs,

    /// PRNG seed for reproducible runs.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

pub async fn run(args: WriteArgs) {
    let tree_args = common::validate(args.tree);
    println!(
        "bench write config: node_count={}, max_depth={}, max_file_bytes={}, max_children_per_dir={}, seed={}",
        tree_args.node_count,
        tree_args.max_depth,
        tree_args.max_file_bytes,
        tree_args.max_children_per_dir,
        args.seed,
    );

    let LmdbSetup { store, _cleanup } = common::setup();

    let mut rng = StdRng::seed_from_u64(args.seed);

    let t_gen = Instant::now();
    let (root_children, stats) = common::generate(&mut rng, &tree_args);
    let gen_elapsed = t_gen.elapsed();

    let total_nodes = stats.files + stats.dirs;
    println!(
        "generated {} nodes ({} files, {} dirs), max depth {}, total file bytes {}",
        total_nodes, stats.files, stats.dirs, stats.max_depth, stats.total_file_bytes,
    );
    println!("generation: {gen_elapsed:?}");

    let uncomitted = UncomittedRoot {
        parent: None,
        repository: store.repository().clone(),
        children: root_children,
    };

    let t_commit = Instant::now();
    let commit = store
        .commit_tree(uncomitted)
        .expect("failed to commit tree");
    let commit_elapsed = t_commit.elapsed();

    let throughput = total_nodes as f64 / commit_elapsed.as_secs_f64();
    println!("commit hash: {}", HexHash::new(commit.hash()));
    println!("commit_tree: {commit_elapsed:?}");
    println!("throughput:  {throughput:.0} nodes/sec");
}
