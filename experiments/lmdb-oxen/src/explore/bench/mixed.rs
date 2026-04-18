use std::time::{Duration, Instant};

use clap::Args;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::weighted::WeightedIndex;
use rand::rngs::StdRng;

use crate::explore::bench::common::{
    self, DurStats, LmdbSetup, ReadOp, TreeGenArgs, fit_linear_ols, print_path_by_depth,
    print_path_depth_log_fit, run_read_op, sample_target_count,
};
use crate::explore::hash::{ContentHash, HasContentHash, HexHash, LocationHash};
use crate::explore::lazy_merkle::UncomittedRoot;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::merkle_store::MerkleStore;

#[derive(Args, Debug)]
pub struct MixedArgs {
    #[command(flatten)]
    pub tree: TreeGenArgs,

    /// Additional commits appended before timing begins (untimed). Primes the
    /// `.commit()` hash pool so the first timed read phase has many choices.
    #[arg(long, default_value_t = 20)]
    pub seed_commits: usize,

    /// Number of timed write commits interleaved between read phases.
    #[arg(long, default_value_t = 10)]
    pub write_phases: usize,

    /// Number of timed read operations per read phase.
    #[arg(long, default_value_t = 5_000)]
    pub reads_per_phase: usize,

    /// Read op weight percentages as `path,node,commit` (must sum to 100).
    #[arg(long, default_value = "35,60,5")]
    pub read_op_weights: String,

    /// Standard deviation of the normal distribution used to sample each
    /// write-phase tree's node count. Mean is `--avg-size`. If omitted,
    /// defaults to `avg_size / 4` at runtime. Samples are clamped to
    /// `[NODE_COUNT_MIN, --max-node-count]`.
    #[arg(long)]
    pub size_stddev: Option<f64>,

    /// Whether each new commit parents on the previous commit (default: true).
    /// Use `--chain-commits false` to disable.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub chain_commits: bool,

    /// Number of untimed warmup reads before timing begins.
    #[arg(long, default_value_t = 1_000)]
    pub warmup_count: usize,

    /// PRNG seed for reproducible runs.
    #[arg(long, default_value_t = 42)]
    pub seed: u64,
}

struct OpWeights {
    path: u32,
    node: u32,
    commit: u32,
}

fn parse_weights(s: &str) -> OpWeights {
    let parts: Vec<&str> = s.split(',').collect();
    assert_eq!(
        parts.len(),
        3,
        "read-op-weights must be three comma-separated integers (path,node,commit), got {s:?}"
    );
    let parsed: Vec<u32> = parts
        .iter()
        .map(|p| {
            p.trim()
                .parse::<u32>()
                .unwrap_or_else(|_| panic!("invalid weight value: {p:?}"))
        })
        .collect();
    assert_eq!(
        parsed.iter().sum::<u32>(),
        100,
        "read-op-weights must sum to 100, got {parsed:?}"
    );
    assert!(
        parsed.iter().any(|&w| w > 0),
        "at least one read-op weight must be > 0"
    );
    OpWeights {
        path: parsed[0],
        node: parsed[1],
        commit: parsed[2],
    }
}

pub async fn run(args: MixedArgs) {
    let tree_args = common::validate(args.tree);
    let weights = parse_weights(&args.read_op_weights);
    let size_stddev = args
        .size_stddev
        .unwrap_or(tree_args.avg_size as f64 / 4.0)
        .max(0.0);

    println!(
        "bench mixed config: max_node_count={}, avg_size={}, size_stddev={:.1}, max_depth={}, max_file_bytes={}, max_children_per_dir={}, seed_commits={}, write_phases={}, reads_per_phase={}, weights=(path={},node={},commit={}), chain_commits={}, warmup_count={}, seed={}",
        tree_args.max_node_count,
        tree_args.avg_size,
        size_stddev,
        tree_args.max_depth,
        tree_args.max_file_bytes,
        tree_args.max_children_per_dir,
        args.seed_commits,
        args.write_phases,
        args.reads_per_phase,
        weights.path,
        weights.node,
        weights.commit,
        args.chain_commits,
        args.warmup_count,
        args.seed,
    );

    let LmdbSetup { store, _cleanup } = common::setup();
    let mut rng = StdRng::seed_from_u64(args.seed);

    // --- Initial tree + commit ---
    let (root_children, stats) = common::generate(&mut rng, &tree_args, tree_args.avg_size);
    let initial_nodes = stats.files + stats.dirs;
    let mut catalog: Vec<(LocationHash, usize)> = stats.node_catalog;
    let t_init = Instant::now();
    let initial_commit = store
        .commit_tree(UncomittedRoot {
            parent: None,
            repository: store.repository().clone(),
            children: root_children,
        })
        .expect("failed to commit initial tree");
    let init_elapsed = t_init.elapsed();
    let mut commits: Vec<ContentHash> = vec![initial_commit.content_hash()];
    println!(
        "initial tree: {} nodes ({} files, {} dirs), commit {} in {:?}",
        initial_nodes,
        stats.files,
        stats.dirs,
        HexHash::from(initial_commit.content_hash()),
        init_elapsed,
    );

    // --- Pre-seed phase (untimed) ---
    // Pre-seed trees use the fixed avg_size; only timed write phases sample the
    // normal distribution for their node count.
    if args.seed_commits > 0 {
        for _ in 0..args.seed_commits {
            let (children, st) = common::generate(&mut rng, &tree_args, tree_args.avg_size);
            catalog.extend(st.node_catalog);
            let parent = if args.chain_commits {
                commits.last().copied()
            } else {
                None
            };
            let c = store
                .commit_tree(UncomittedRoot {
                    parent,
                    repository: store.repository().clone(),
                    children,
                })
                .expect("failed to commit pre-seed tree");
            commits.push(c.content_hash());
        }
        println!(
            "pre-seed: committed {} additional trees (untimed). catalog={} entries, commits={}",
            args.seed_commits,
            catalog.len(),
            commits.len(),
        );
    }

    // --- Prepare weighted sampling ---
    // Index 0 = Path, 1 = Node, 2 = Commit — matches the CLI `path,node,commit` order.
    let dist = WeightedIndex::new([weights.path, weights.node, weights.commit])
        .expect("failed to create WeightedIndex (at least one weight must be > 0)");
    const OP_BY_IDX: [ReadOp; 3] = [ReadOp::Path, ReadOp::Node, ReadOp::Commit];

    // --- Warmup ---
    if args.warmup_count > 0 {
        for _ in 0..args.warmup_count {
            let op = OP_BY_IDX[dist.sample(&mut rng)];
            let _ = run_read_op(&store, &catalog, &commits, op, &mut rng);
        }
        println!("warmup: {} ops ignored", args.warmup_count);
    }

    // --- Main loop ---
    let total_iters = args.write_phases + 1; // one extra read phase after last write
    let mut all_node_durs: Vec<Duration> = Vec::new();
    let mut all_path_samples: Vec<(usize, Duration)> = Vec::new();
    let mut all_commit_durs: Vec<Duration> = Vec::new();
    // (nodes_written, elapsed) per timed write commit. Keeps node count
    // per commit so we can report size vs. time and fit elapsed = f(nodes).
    let mut write_records: Vec<(usize, Duration)> = Vec::new();

    println!();
    println!("iter |         reads |          write |  nodes | nodes/sec");
    for i in 0..total_iters {
        // Read phase (timed)
        let t_read = Instant::now();
        for _ in 0..args.reads_per_phase {
            let op = OP_BY_IDX[dist.sample(&mut rng)];
            let (d, depth) = run_read_op(&store, &catalog, &commits, op, &mut rng);
            match op {
                ReadOp::Node => all_node_durs.push(d),
                ReadOp::Commit => all_commit_durs.push(d),
                ReadOp::Path => {
                    all_path_samples.push((depth.expect("Path op must return depth"), d))
                }
            }
        }
        let read_elapsed = t_read.elapsed();

        // Write phase (timed), skipped on the last iteration.
        // Each write samples a target node count from N(avg_size, size_stddev).
        let (write_col, nodes_col, throughput_col) = if i < args.write_phases {
            let target = sample_target_count(
                &mut rng,
                tree_args.avg_size as f64,
                size_stddev,
                tree_args.max_node_count,
            );
            let (children, st) = common::generate(&mut rng, &tree_args, target);
            let written_nodes = st.files + st.dirs;
            let parent = if args.chain_commits {
                commits.last().copied()
            } else {
                None
            };
            let t_write = Instant::now();
            let c = store
                .commit_tree(UncomittedRoot {
                    parent,
                    repository: store.repository().clone(),
                    children,
                })
                .expect("failed to commit mid-bench tree");
            let elapsed = t_write.elapsed();
            catalog.extend(st.node_catalog);
            commits.push(c.content_hash());
            write_records.push((written_nodes, elapsed));
            let nps = written_nodes as f64 / elapsed.as_secs_f64();
            (
                format!("{elapsed:?}"),
                format!("{written_nodes}"),
                format!("{nps:.0}"),
            )
        } else {
            ("-".to_string(), "-".to_string(), "-".to_string())
        };

        println!(
            "{:>4} | {:>13?} | {:>14} | {:>6} | {:>9}",
            i + 1,
            read_elapsed,
            write_col,
            nodes_col,
            throughput_col,
        );
    }

    // --- Aggregate reporting ---
    let total_reads = all_node_durs.len() + all_path_samples.len() + all_commit_durs.len();
    println!();
    println!("aggregate over {} read ops:", total_reads);
    if !all_node_durs.is_empty() {
        DurStats::from_durations(&mut all_node_durs).print("node()");
    }
    if !all_path_samples.is_empty() {
        let mut path_durs: Vec<Duration> = all_path_samples.iter().map(|(_, d)| *d).collect();
        DurStats::from_durations(&mut path_durs).print("path()");
    }
    if !all_commit_durs.is_empty() {
        DurStats::from_durations(&mut all_commit_durs).print("commit()");
    }

    if !all_path_samples.is_empty() {
        println!();
        print_path_by_depth(&all_path_samples);
        print_path_depth_log_fit(&all_path_samples);
    }

    if !write_records.is_empty() {
        print_write_aggregate(&write_records);
    }
}

/// Expanded write-side reporting: totals, per-commit latency / size / per-commit
/// throughput distributions, and a linear fit of `elapsed = alpha + beta * nodes`
/// so you can separate fixed commit overhead from marginal per-node cost.
fn print_write_aggregate(records: &[(usize, Duration)]) {
    let n = records.len();
    let total_nodes: usize = records.iter().map(|(k, _)| *k).sum();
    let total_elapsed: Duration = records.iter().map(|(_, d)| *d).sum();
    let agg_throughput = if total_elapsed.as_secs_f64() > 0.0 {
        total_nodes as f64 / total_elapsed.as_secs_f64()
    } else {
        f64::NAN
    };

    println!();
    println!("write aggregate ({} timed commits):", n);
    println!("  total nodes written:   {total_nodes}");
    println!("  total write time:      {total_elapsed:?}");
    println!("  aggregate throughput:  {agg_throughput:.0} nodes/sec");

    // Per-commit latency distribution.
    let mut elapsed_only: Vec<Duration> = records.iter().map(|(_, d)| *d).collect();
    DurStats::from_durations(&mut elapsed_only).print("commit_tree");

    // Per-commit size distribution (min / mean / p50 / max).
    let mut sizes: Vec<usize> = records.iter().map(|(k, _)| *k).collect();
    sizes.sort_unstable();
    let size_min = *sizes.first().expect("records non-empty");
    let size_max = *sizes.last().expect("records non-empty");
    let size_mean = total_nodes as f64 / n as f64;
    let size_p50 = sizes[n / 2];
    println!(
        "  commit size (nodes):   min={size_min:>6}  mean={size_mean:>7.0}  p50={size_p50:>6}  max={size_max:>6}"
    );

    // Per-commit throughput distribution. Note: this is distinct from the
    // aggregate throughput above — small commits have worse amortized
    // throughput (fixed overhead dominates), so `mean(per-commit)` ≠
    // `total / total_time`.
    let mut per_commit_thru: Vec<f64> = records
        .iter()
        .map(|(k, d)| *k as f64 / d.as_secs_f64())
        .collect();
    per_commit_thru.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let thru_min = per_commit_thru.first().copied().unwrap_or(0.0);
    let thru_max = per_commit_thru.last().copied().unwrap_or(0.0);
    let thru_mean = per_commit_thru.iter().sum::<f64>() / n as f64;
    let thru_p50 = per_commit_thru[n / 2];
    println!(
        "  per-commit nodes/sec:  min={thru_min:>7.0}  mean={thru_mean:>7.0}  p50={thru_p50:>7.0}  max={thru_max:>7.0}"
    );

    // Linear fit of elapsed(ns) vs nodes: alpha = fixed overhead per commit,
    // beta = marginal cost per node. Needs at least 2 commits with distinct
    // sizes — skip otherwise. With small stddev or few commits the fit can
    // be noisy; R² tells you whether to trust the decomposition.
    let points: Vec<(f64, f64)> = records
        .iter()
        .map(|(k, d)| (*k as f64, d.as_nanos() as f64))
        .collect();
    match fit_linear_ols(&points) {
        None => {
            println!("  linear fit:            skipped (need ≥2 commits with distinct sizes)");
        }
        Some((beta, alpha, r_squared)) => {
            let alpha_dur = if alpha.is_finite() && alpha >= 0.0 {
                format!("{:?}", Duration::from_nanos(alpha.round() as u64))
            } else {
                format!("{alpha:.0}ns")
            };
            let beta_dur = if beta.is_finite() && beta >= 0.0 {
                format!("{:?}/node", Duration::from_nanos(beta.round() as u64))
            } else {
                format!("{beta:.0}ns/node")
            };
            let beta_nps = if beta > 0.0 { 1e9 / beta } else { f64::NAN };
            println!();
            println!("write linear fit: elapsed = alpha + beta * nodes");
            println!("  alpha (fixed per-commit overhead): {alpha:>12.0} ns  ({alpha_dur})");
            println!(
                "  beta  (per-node marginal cost):    {beta:>12.0} ns  ({beta_dur}; ~{beta_nps:.0} nodes/sec asymptotic)"
            );
            println!("  R²:                                {r_squared:>12.4}");
        }
    }
}
