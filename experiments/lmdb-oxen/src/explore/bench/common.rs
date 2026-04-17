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
pub const NODE_COUNT_MAX: usize = 100_000;

// Probability a new node is a directory rather than a file.
// Tuned to keep trees neither degenerate-linear nor shallow-and-wide.
const DIR_PROB: f64 = 0.35;

// LMDB map size. heed's default (~10 MiB) overflows for 100k nodes.
const LMDB_MAP_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;

const MIN_SAMPLES_PER_BUCKET: usize = 10;

#[derive(Args, Debug, Clone)]
pub struct TreeGenArgs {
    /// Target total node count (files + directories). Clamped to [1_000, 100_000].
    #[arg(long, default_value_t = 10_000)]
    pub node_count: usize,

    /// Maximum directory nesting depth. Root's direct children live at depth 1.
    #[arg(long, default_value_t = 64)]
    pub max_depth: usize,

    /// Maximum random file content size in bytes. Files draw uniformly from 1..=this.
    #[arg(long, default_value_t = 1024)]
    pub max_file_bytes: usize,

    /// Maximum children per directory. Actual count is uniform in 1..=this.
    #[arg(long, default_value_t = 32)]
    pub max_children_per_dir: usize,
}

pub fn validate(mut args: TreeGenArgs) -> TreeGenArgs {
    args.node_count = args.node_count.clamp(NODE_COUNT_MIN, NODE_COUNT_MAX);
    args.max_depth = args.max_depth.max(1);
    args.max_children_per_dir = args.max_children_per_dir.max(1);
    args.max_file_bytes = args.max_file_bytes.max(1);
    args
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
    remaining: usize,
    next_id: u64,
    stats: GenStats,
}

/// Generate a random Merkle tree of approximately `args.node_count` nodes.
/// Caller owns the RNG so benches that need multiple trees can reuse state.
pub fn generate(rng: &mut StdRng, args: &TreeGenArgs) -> (Vec<MerkleTreeB>, GenStats) {
    let mut state = GenState {
        remaining: args.node_count,
        next_id: 0,
        stats: GenStats {
            files: 0,
            dirs: 0,
            total_file_bytes: 0,
            max_depth: 0,
            node_catalog: Vec::with_capacity(args.node_count),
        },
    };
    let mut root_children = Vec::new();
    while state.remaining > 0 {
        let batch = build_dir_contents(rng, 1, &mut state, args);
        if batch.is_empty() {
            break;
        }
        root_children.extend(batch);
    }
    (root_children, state.stats)
}

fn build_dir_contents(
    rng: &mut StdRng,
    depth: usize,
    state: &mut GenState,
    args: &TreeGenArgs,
) -> Vec<MerkleTreeB> {
    if state.remaining == 0 {
        return Vec::new();
    }
    let cap = args.max_children_per_dir.min(state.remaining);
    let n = rng.random_range(1..=cap);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if state.remaining == 0 {
            break;
        }
        state.remaining -= 1;
        let id = state.next_id;
        state.next_id += 1;

        state.stats.max_depth = state.stats.max_depth.max(depth);

        // A directory needs budget for at least one child of its own.
        let can_be_dir = depth < args.max_depth && state.remaining > 0;
        let is_dir = can_be_dir && rng.random_bool(DIR_PROB);

        if is_dir {
            let kids = build_dir_contents(rng, depth + 1, state, args);
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
