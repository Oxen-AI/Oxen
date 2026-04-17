use std::time::{Duration, Instant};

use clap::Args;
use rand::SeedableRng;
use rand::rngs::StdRng;

use crate::explore::bench::common::{
    self, DurStats, LmdbSetup, ReadOp, TreeGenArgs, print_path_by_depth, run_read_op,
};
use crate::explore::hash::{HasHash, HexHash};
use crate::explore::lazy_merkle::UncomittedRoot;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;

#[derive(Args, Debug)]
pub struct ReadArgs {
    #[command(flatten)]
    pub tree: TreeGenArgs,

    /// Number of timed operations per read type.
    #[arg(long, default_value_t = 10_000)]
    pub read_count: usize,

    /// Number of untimed warmup operations before timing begins.
    #[arg(long, default_value_t = 1_000)]
    pub warmup_count: usize,

    /// PRNG seed for reproducible runs.
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
}

pub async fn run(args: ReadArgs) {
    let tree_args = common::validate(args.tree);
    println!(
        "bench read config: node_count={}, max_depth={}, max_file_bytes={}, max_children_per_dir={}, read_count={}, warmup_count={}, seed={}",
        tree_args.node_count,
        tree_args.max_depth,
        tree_args.max_file_bytes,
        tree_args.max_children_per_dir,
        args.read_count,
        args.warmup_count,
        args.seed,
    );

    let LmdbSetup { store, _cleanup } = common::setup();

    let mut rng = StdRng::seed_from_u64(args.seed);

    let t_gen = Instant::now();
    let (root_children, stats) = common::generate(&mut rng, &tree_args);
    let gen_elapsed = t_gen.elapsed();
    let total_nodes = stats.files + stats.dirs;
    println!(
        "generated {} nodes ({} files, {} dirs), max depth {}",
        total_nodes, stats.files, stats.dirs, stats.max_depth,
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
    println!(
        "commit: {} (in {:?})",
        HexHash::new(commit.hash()),
        commit_elapsed
    );

    let catalog = stats.node_catalog;
    let commits = vec![commit.hash()];

    // Warmup: interleave all three ops, discard timings.
    if args.warmup_count > 0 {
        const WARMUP_OPS: [ReadOp; 3] = [ReadOp::Node, ReadOp::Commit, ReadOp::Path];
        for i in 0..args.warmup_count {
            let op = WARMUP_OPS[i % WARMUP_OPS.len()];
            let _ = run_read_op(&store, &catalog, &commits, op, &mut rng);
        }
        println!("warmup: {} ops ignored", args.warmup_count);
    }

    // --- Timed .node() phase ---
    let mut node_durs = Vec::with_capacity(args.read_count);
    for _ in 0..args.read_count {
        let (d, _) = run_read_op(&store, &catalog, &commits, ReadOp::Node, &mut rng);
        node_durs.push(d);
    }
    DurStats::from_durations(&mut node_durs).print("node()");

    // --- Timed .commit() phase ---
    let mut commit_durs = Vec::with_capacity(args.read_count);
    for _ in 0..args.read_count {
        let (d, _) = run_read_op(&store, &catalog, &commits, ReadOp::Commit, &mut rng);
        commit_durs.push(d);
    }
    DurStats::from_durations(&mut commit_durs).print("commit()");
    println!("          (single commit hash reused across all calls; hot-path measurement)");

    // --- Timed .path() phase ---
    let mut path_samples: Vec<(usize, Duration)> = Vec::with_capacity(args.read_count);
    let mut path_only_durs: Vec<Duration> = Vec::with_capacity(args.read_count);
    for _ in 0..args.read_count {
        let (d, depth) = run_read_op(&store, &catalog, &commits, ReadOp::Path, &mut rng);
        let depth = depth.expect("Path op must return depth");
        path_samples.push((depth, d));
        path_only_durs.push(d);
    }
    DurStats::from_durations(&mut path_only_durs).print("path()");
    println!();
    print_path_by_depth(&path_samples);
}
