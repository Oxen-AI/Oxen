use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::Args;
use heed::EnvOpenOptions;
use rand::rngs::StdRng;
use rand::{Rng, RngExt};

use crate::explore::hash::Hash;
use crate::explore::lazy_merkle::MerkleTreeB;
use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::merkle_reader::MerkleReader;
use crate::explore::paths::{AbsolutePath, Name};

pub const NODE_COUNT_MIN: usize = 1_000;

// Floor on the depth-tapered `dir_prob`; without it, deep levels can
// collapse to file-only and subtree expansion dies too fast.
const DIR_PROB_FLOOR: f64 = 0.05;

// LMDB map size. heed's default (~10 MiB) overflows for 100k nodes.
const LMDB_MAP_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;

const MIN_SAMPLES_PER_BUCKET: usize = 10;

// Fewer than this many distinct depth buckets and a log-fit is degenerate
// (two points will trivially have R² = 1 regardless of shape).
const MIN_BUCKETS_FOR_LOG_FIT: usize = 3;

#[derive(Args, Debug, Clone)]
pub struct TreeGenArgs {
    /// Hard upper bound on the size of any generated tree.
    #[arg(long, default_value_t = 100_000)]
    pub max_node_count: usize,

    /// Baseline tree size. Used as the exact size for `bench write` / `bench read`,
    /// the initial and pre-seed commits in `bench mixed`, and as the mean of the
    /// normal distribution that mixed's write phases sample from.
    #[arg(long, default_value_t = 10_000)]
    pub avg_size: usize,

    /// Maximum directory nesting depth. Root's direct children live at depth 1.
    /// Default picked to roughly match the natural depth at `--avg-size 10000`
    /// with the default branching; raise it for depth-diverse workloads.
    #[arg(long, default_value_t = 10)]
    pub max_depth: usize,

    /// Maximum random file content size in bytes. Files draw uniformly from 1..=this.
    #[arg(long, default_value_t = 1024)]
    pub max_file_bytes: usize,

    /// Maximum children per directory. Actual count is uniform in 1..=this.
    #[arg(long, default_value_t = 32)]
    pub max_children_per_dir: usize,

    /// Base probability a new child slot becomes a directory. The effective
    /// probability tapers linearly from this value at depth 1 down to
    /// `DIR_PROB_FLOOR` at `--target-depth`.
    #[arg(long, default_value_t = 0.35)]
    pub dir_prob: f64,

    /// Depth at which `dir_prob` reaches its floor. If unset, defaults to
    /// `--max-depth`. Set lower to make dirs concentrate at shallow levels.
    #[arg(long)]
    pub target_depth: Option<usize>,
}

pub fn validate(mut args: TreeGenArgs) -> TreeGenArgs {
    args.max_node_count = args.max_node_count.max(NODE_COUNT_MIN);
    args.avg_size = args.avg_size.clamp(NODE_COUNT_MIN, args.max_node_count);
    args.max_depth = args.max_depth.max(1);
    args.max_children_per_dir = args.max_children_per_dir.max(1);
    args.max_file_bytes = args.max_file_bytes.max(1);
    args.dir_prob = args.dir_prob.clamp(0.0, 1.0);
    if let Some(td) = args.target_depth {
        args.target_depth = Some(td.max(1));
    }
    args
}

/// Linearly tapered dir-probability: at `depth = 1` returns ~`base`, at
/// `target_depth` drops to `DIR_PROB_FLOOR`, and never falls below the floor.
fn effective_dir_prob(depth: usize, base: f64, target_depth: usize) -> f64 {
    let t = target_depth.max(1) as f64;
    let d = depth as f64;
    let factor = (1.0 - d / t).max(0.0);
    (base * factor).clamp(DIR_PROB_FLOOR, 1.0)
}

pub struct GenStats {
    pub files: usize,
    pub dirs: usize,
    pub total_file_bytes: usize,
    pub max_depth: usize,
    /// (hash, depth) for every generated node, in generation order.
    /// Depth 1 = root's direct child.
    pub node_catalog: Vec<(Hash, usize)>,
}

struct GenState {
    next_id: u64,
    stats: GenStats,
}

/// Generate a random Merkle tree of approximately `target_count` nodes.
/// `target_count` is clamped to `[NODE_COUNT_MIN, args.max_node_count]` before
/// generation. The actual tree may be *smaller* than `target_count`: the
/// per-child budget split (option 3) discards surplus budget when a slot
/// becomes a file, trading exact size for more balanced depth coverage.
/// Caller owns the RNG so benches that need multiple trees can reuse state.
pub fn generate(
    rng: &mut StdRng,
    args: &TreeGenArgs,
    target_count: usize,
) -> (Vec<MerkleTreeB>, GenStats) {
    let target = target_count.clamp(NODE_COUNT_MIN, args.max_node_count);
    let mut state = GenState {
        next_id: 0,
        stats: GenStats {
            files: 0,
            dirs: 0,
            total_file_bytes: 0,
            max_depth: 0,
            node_catalog: Vec::with_capacity(target),
        },
    };
    let target_depth = args.target_depth.unwrap_or(args.max_depth);

    // Per-child budget split naturally underfills (files drop surplus,
    // recursive dirs underfill their inner budget). Retry at the root
    // level, appending more root-level subtrees, until we've reached the
    // target or hit a pass cap. Each retry goes through the same
    // balanced generator, so depth spread is preserved.
    const MAX_OUTER_PASSES: usize = 64;
    let mut root_children = Vec::new();
    let mut remaining = target;
    let mut pass = 0;
    while remaining > 0 && pass < MAX_OUTER_PASSES {
        let before = state.stats.files + state.stats.dirs;
        let batch = build_dir_contents(rng, 1, &mut state, args, target_depth, remaining);
        if batch.is_empty() {
            break;
        }
        let produced = (state.stats.files + state.stats.dirs) - before;
        if produced == 0 {
            break;
        }
        remaining = remaining.saturating_sub(produced);
        root_children.extend(batch);
        pass += 1;
    }
    (root_children, state.stats)
}

/// Build the contents of a directory that is allowed to produce up to
/// `subtree_budget` nodes (files + subdirs, including self's children).
///
/// Splits the budget across the chosen number of children so no single child
/// can monopolize it — this replaces the prior DFS-greedy allocation and is
/// what spreads nodes across depths. Surplus budget from a file-child (or a
/// dir-child whose recursion underfilled) rolls over to the next sibling so
/// the total node count stays close to `subtree_budget` despite the split.
fn build_dir_contents(
    rng: &mut StdRng,
    depth: usize,
    state: &mut GenState,
    args: &TreeGenArgs,
    target_depth: usize,
    subtree_budget: usize,
) -> Vec<MerkleTreeB> {
    if subtree_budget == 0 {
        return Vec::new();
    }
    let cap = args.max_children_per_dir.min(subtree_budget);
    let n = rng.random_range(1..=cap);
    let per_child = subtree_budget / n;
    let remainder = subtree_budget % n;

    let dp = effective_dir_prob(depth, args.dir_prob, target_depth);

    let mut out = Vec::with_capacity(n);
    let mut rollover: usize = 0;
    for i in 0..n {
        let base_budget = per_child + if i < remainder { 1 } else { 0 };
        let my_budget = base_budget + rollover;
        rollover = 0;
        if my_budget == 0 {
            continue;
        }

        let id = state.next_id;
        state.next_id += 1;
        state.stats.max_depth = state.stats.max_depth.max(depth);

        // Count what this child actually ends up producing so we can roll
        // unused budget to the next sibling (files consume 1; dirs may
        // underfill their inner budget).
        let produced_before = state.stats.files + state.stats.dirs;

        // A directory uses 1 slot for itself plus at least 1 for a child.
        let children_budget = my_budget - 1;
        let can_be_dir = depth < args.max_depth && children_budget > 0;
        let is_dir = can_be_dir && rng.random_bool(dp);

        if is_dir {
            let kids =
                build_dir_contents(rng, depth + 1, state, args, target_depth, children_budget);
            let name: Name = Path::new(&format!("d_{id}"))
                .try_into()
                .expect("generated dir name is a valid Name");
            let hash = Hash::hash_of_hashes(kids.iter());
            state.stats.dirs += 1;
            state.stats.node_catalog.push((hash, depth));
            out.push(MerkleTreeB::Dir {
                hash,
                name,
                children: kids,
            });
        } else {
            let size = rng.random_range(1..=args.max_file_bytes);
            // Prepend 8 id bytes so each file's content (and therefore its hash)
            // is unique within this tree.
            let mut data = Vec::with_capacity(size + 8);
            data.extend_from_slice(&id.to_le_bytes());
            let tail_start = data.len();
            data.resize(tail_start + size, 0);
            rng.fill_bytes(&mut data[tail_start..]);
            let hash = Hash::new(&data);

            let name: Name = Path::new(&format!("f_{id}.bin"))
                .try_into()
                .expect("generated file name is a valid Name");
            state.stats.files += 1;
            state.stats.total_file_bytes += data.len();
            state.stats.node_catalog.push((hash, depth));
            out.push(MerkleTreeB::File { hash, name });
        }

        let produced = (state.stats.files + state.stats.dirs) - produced_before;
        if my_budget > produced {
            rollover = my_budget - produced;
        }
    }
    out
}

pub struct LmdbSetup {
    pub store: LmdbMerkleDB,
    // Declared after `store` so the LMDB env drops (closing its file handles)
    // before the directory is removed.
    pub _cleanup: DeleteOnDrop,
}

pub fn setup() -> LmdbSetup {
    let repo_root = {
        let tmp_path: PathBuf =
            std::env::temp_dir().join(format!("lmdb_oxen_bench_{}", rand::random::<u16>()));
        std::fs::create_dir_all(&tmp_path).expect("failed to create temp dir");
        AbsolutePath::new(tmp_path).expect("tmp_path is not absolute")
    };
    let db_location = repo_root.join(&Path::new("lmdb_data").try_into().unwrap());
    std::fs::create_dir_all(db_location.as_path()).expect("failed to create db directory");

    let options = {
        let mut o = EnvOpenOptions::new();
        o.map_size(LMDB_MAP_SIZE_BYTES);
        o
    };
    let store = LmdbMerkleDB::new(&repo_root, &db_location, &options)
        .expect("failed to create LmdbMerkleDB");
    LmdbSetup {
        store,
        _cleanup: DeleteOnDrop(repo_root),
    }
}

pub struct DeleteOnDrop(AbsolutePath);

impl Drop for DeleteOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.as_path());
    }
}

#[derive(Copy, Clone, Debug)]
pub struct DurStats {
    pub count: usize,
    pub total: Duration,
    pub min: Duration,
    pub mean: Duration,
    pub p50: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub max: Duration,
}

impl DurStats {
    pub fn from_durations(ds: &mut [Duration]) -> Self {
        assert!(!ds.is_empty(), "cannot summarize empty durations");
        ds.sort_unstable();
        let count = ds.len();
        let total: Duration = ds.iter().copied().sum();
        let mean = total / (count as u32);
        let min = ds[0];
        let max = ds[count - 1];
        let p = |q: f64| -> Duration {
            let idx = ((count as f64 - 1.0) * q).round() as usize;
            ds[idx]
        };
        Self {
            count,
            total,
            min,
            mean,
            p50: p(0.50),
            p95: p(0.95),
            p99: p(0.99),
            max,
        }
    }

    pub fn throughput_ops_per_sec(&self) -> f64 {
        self.count as f64 / self.total.as_secs_f64()
    }

    pub fn print(&self, label: &str) {
        println!(
            "{label:<9} count={:>7}  total={:>10?}  min={:>10?}  mean={:>10?}  p50={:>10?}  p95={:>10?}  p99={:>10?}  max={:>10?}  throughput={:>10.0} ops/s",
            self.count,
            self.total,
            self.min,
            self.mean,
            self.p50,
            self.p95,
            self.p99,
            self.max,
            self.throughput_ops_per_sec(),
        );
    }
}

pub fn print_path_by_depth(samples: &[(usize, Duration)]) {
    let mut by_depth: BTreeMap<usize, Vec<Duration>> = BTreeMap::new();
    for &(d, dur) in samples {
        by_depth.entry(d).or_default().push(dur);
    }

    println!("path() by depth (buckets with >= {MIN_SAMPLES_PER_BUCKET} samples):");
    println!("  depth | count |        mean |         p50 |         p95");
    for (depth, mut ds) in by_depth {
        if ds.len() < MIN_SAMPLES_PER_BUCKET {
            continue;
        }
        let s = DurStats::from_durations(&mut ds);
        println!(
            "  {:>5} | {:>5} | {:>11?} | {:>11?} | {:>11?}",
            depth, s.count, s.mean, s.p50, s.p95
        );
    }
}

/// Fit `mean_time_ns = a * ln(depth) + b` via ordinary least squares on
/// per-depth mean durations. Reports coefficients, R² (coefficient of
/// determination), and the model prediction alongside observed means so the
/// reader can judge residuals by eye.
///
/// Only buckets with `>= MIN_SAMPLES_PER_BUCKET` samples are used — matches
/// what `print_path_by_depth` prints, so the fit is over the same visible data.
pub fn print_path_depth_log_fit(samples: &[(usize, Duration)]) {
    let mut by_depth: BTreeMap<usize, Vec<Duration>> = BTreeMap::new();
    for &(d, dur) in samples {
        by_depth.entry(d).or_default().push(dur);
    }

    // (ln_depth, mean_time_ns, depth)
    let mut points: Vec<(f64, f64, usize)> = Vec::new();
    for (&depth, ds) in &by_depth {
        if ds.len() < MIN_SAMPLES_PER_BUCKET || depth == 0 {
            continue;
        }
        let mean_ns: f64 = ds.iter().map(|d| d.as_nanos() as f64).sum::<f64>() / ds.len() as f64;
        points.push(((depth as f64).ln(), mean_ns, depth));
    }

    if points.len() < MIN_BUCKETS_FOR_LOG_FIT {
        println!(
            "path() log-fit: skipped — only {} depth bucket(s) with >= {} samples (need >= {})",
            points.len(),
            MIN_SAMPLES_PER_BUCKET,
            MIN_BUCKETS_FOR_LOG_FIT,
        );
        return;
    }

    // Simple OLS on (x = ln(depth), y = mean_ns): y = a*x + b
    let n = points.len() as f64;
    let sum_x: f64 = points.iter().map(|p| p.0).sum();
    let sum_y: f64 = points.iter().map(|p| p.1).sum();
    let sum_xy: f64 = points.iter().map(|p| p.0 * p.1).sum();
    let sum_xx: f64 = points.iter().map(|p| p.0 * p.0).sum();

    let denom = n * sum_xx - sum_x * sum_x;
    if denom.abs() < f64::EPSILON {
        println!("path() log-fit: skipped — degenerate x values (all ln(depth) identical)");
        return;
    }
    let slope = (n * sum_xy - sum_x * sum_y) / denom;
    let intercept = (sum_y - slope * sum_x) / n;

    let mean_y = sum_y / n;
    let ss_tot: f64 = points.iter().map(|p| (p.1 - mean_y).powi(2)).sum();
    let ss_res: f64 = points
        .iter()
        .map(|p| {
            let pred = slope * p.0 + intercept;
            (p.1 - pred).powi(2)
        })
        .sum();
    let r_squared = if ss_tot > f64::EPSILON {
        1.0 - ss_res / ss_tot
    } else {
        f64::NAN
    };

    println!();
    println!("path() log-fit: mean_time_ns = a * ln(depth) + b");
    println!("  a (slope):     {slope:>14.2}");
    println!("  b (intercept): {intercept:>14.2}");
    println!(
        "  R²:            {:>14.4}  ({})",
        r_squared,
        goodness_label(r_squared),
    );
    println!(
        "  buckets used:  {} (depths {}..={}, >= {} samples each)",
        points.len(),
        points.first().map(|p| p.2).unwrap_or(0),
        points.last().map(|p| p.2).unwrap_or(0),
        MIN_SAMPLES_PER_BUCKET,
    );
    println!();
    println!("  depth |    observed mean |   model prediction |      residual");
    for (ln_d, obs_ns, depth) in &points {
        let pred_ns = slope * ln_d + intercept;
        let obs = Duration::from_nanos(obs_ns.round().max(0.0) as u64);
        let pred_str = if pred_ns.is_finite() && pred_ns >= 0.0 {
            format!("{:?}", Duration::from_nanos(pred_ns.round() as u64))
        } else {
            format!("{pred_ns:.0} ns (invalid)")
        };
        let residual_ns = obs_ns - pred_ns;
        println!(
            "  {:>5} | {:>16?} | {:>18} | {:>+10.0} ns",
            depth, obs, pred_str, residual_ns,
        );
    }
}

fn goodness_label(r_squared: f64) -> &'static str {
    if !r_squared.is_finite() {
        "undefined"
    } else if r_squared > 0.95 {
        "excellent"
    } else if r_squared > 0.80 {
        "good"
    } else if r_squared > 0.50 {
        "moderate"
    } else if r_squared > 0.0 {
        "poor"
    } else {
        "worse than mean-only baseline"
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ReadOp {
    Node,
    Commit,
    Path,
}

/// Execute one timed read op. Returns (elapsed, depth) where `depth` is
/// `Some` only for `Path` ops (used for per-depth bucketing).
pub fn run_read_op(
    store: &LmdbMerkleDB,
    catalog: &[(Hash, usize)],
    commits: &[Hash],
    op: ReadOp,
    rng: &mut StdRng,
) -> (Duration, Option<usize>) {
    match op {
        ReadOp::Node => {
            let (hash, _depth) = pick(catalog, rng);
            let t = Instant::now();
            let result = store.node(hash).expect("node() failed");
            let elapsed = t.elapsed();
            assert!(result.is_some(), "sampled node hash should resolve");
            (elapsed, None)
        }
        ReadOp::Commit => {
            let hash = pick(commits, rng);
            let t = Instant::now();
            let result = store.commit(hash).expect("commit() failed");
            let elapsed = t.elapsed();
            assert!(result.is_some(), "sampled commit hash should resolve");
            (elapsed, None)
        }
        ReadOp::Path => {
            let (hash, depth) = pick(catalog, rng);
            let t = Instant::now();
            let result = store.path(hash).expect("path() failed");
            let elapsed = t.elapsed();
            let rel = result.expect("sampled hash should resolve to a path");
            let got_depth = rel.components().count();
            assert_eq!(
                got_depth, depth,
                "path() depth mismatch: expected {depth}, got {got_depth}"
            );
            (elapsed, Some(depth))
        }
    }
}

fn pick<T: Copy>(slice: &[T], rng: &mut StdRng) -> T {
    let i = rng.random_range(0..slice.len());
    slice[i]
}

/// Sample from `N(mean, std_dev)` using the Box-Muller transform.
/// Kept local so the experiment crate avoids pulling in `rand_distr`.
pub fn sample_normal(rng: &mut StdRng, mean: f64, std_dev: f64) -> f64 {
    let mut u1: f64 = rng.random();
    // `u1 == 0` makes `ln` diverge; rerolling is cheap and correct.
    while u1 <= 0.0 {
        u1 = rng.random();
    }
    let u2: f64 = rng.random();
    let z0 = (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos();
    mean + std_dev * z0
}

/// Draw one `N(mean, std_dev)` sample and clamp/round to a valid node count
/// in `[NODE_COUNT_MIN, max_node_count]`.
pub fn sample_target_count(
    rng: &mut StdRng,
    mean: f64,
    std_dev: f64,
    max_node_count: usize,
) -> usize {
    let raw = sample_normal(rng, mean, std_dev).round();
    if !raw.is_finite() {
        return mean
            .round()
            .clamp(NODE_COUNT_MIN as f64, max_node_count as f64) as usize;
    }
    let rounded = raw as i64;
    rounded.clamp(NODE_COUNT_MIN as i64, max_node_count as i64) as usize
}
