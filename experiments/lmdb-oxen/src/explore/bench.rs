use std::path::{Path, PathBuf};
use std::time::Instant;

use clap::Args;
use heed::EnvOpenOptions;
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};

use crate::explore::hash::{HasHash, Hash, HexHash};
use crate::explore::lazy_merkle::{MerkleTreeB, UncomittedRoot};
use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::merkle_store::MerkleStore;
use crate::explore::paths::{AbsolutePath, Name};

#[derive(Args, Debug)]
pub struct BenchArgs {
    /// Target total node count (files + directories). Clamped to [1000, 100_000].
    #[arg(long, default_value_t = 10_000)]
    node_count: usize,

    /// Maximum directory nesting depth. Root's direct children live at depth 1.
    #[arg(long, default_value_t = 64)]
    max_depth: usize,

    /// Maximum random file content size in bytes. Files draw uniformly from 1..=this.
    #[arg(long, default_value_t = 1024)]
    max_file_bytes: usize,

    /// Maximum children per directory. Actual count is uniform in 1..=this.
    #[arg(long, default_value_t = 32)]
    max_children_per_dir: usize,

    /// PRNG seed for reproducible runs.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

const NODE_COUNT_MIN: usize = 1_000;
const NODE_COUNT_MAX: usize = 100_000;

// Probability a new node is a directory rather than a file.
// Tuned empirically to keep trees neither degenerate-linear nor shallow-and-wide.
const DIR_PROB: f64 = 0.35;

// LMDB map size. heed's default (~10 MiB) overflows for 100k nodes.
const LMDB_MAP_SIZE_BYTES: usize = 2 * 1024 * 1024 * 1024;

pub async fn run(args: BenchArgs) {
    let args = validate(args);
    println!("bench config: {args:?}");

    let repo_root = {
        let tmp_path: PathBuf =
            std::env::temp_dir().join(format!("lmdb_oxen_bench_{}", rand::random::<u16>()));
        std::fs::create_dir_all(&tmp_path).expect("failed to create temp dir");
        AbsolutePath::new(tmp_path).expect("tmp_path is not absolute")
    };
    let _cleanup = DeleteOnDrop(&repo_root);

    let db_location = repo_root.join(&Path::new("lmdb_data").try_into().unwrap());
    std::fs::create_dir_all(db_location.as_path()).expect("failed to create db directory");

    let options = {
        let mut o = EnvOpenOptions::new();
        o.map_size(LMDB_MAP_SIZE_BYTES);
        o
    };
    let store = LmdbMerkleDB::new(&repo_root, &db_location, &options)
        .expect("failed to create LmdbMerkleDB");

    let t_gen = Instant::now();
    let (root_children, stats) = generate(&args);
    let gen_elapsed = t_gen.elapsed();

    let total_nodes = stats.files + stats.dirs;
    println!(
        "generated {} nodes ({} files, {} dirs), max depth {}, total file bytes {}",
        total_nodes, stats.files, stats.dirs, stats.max_depth, stats.total_file_bytes,
    );
    println!("generation: {gen_elapsed:?}");

    let uncomitted = UncomittedRoot {
        parent: None,
        repository: repo_root.clone(),
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

fn validate(mut args: BenchArgs) -> BenchArgs {
    args.node_count = args.node_count.clamp(NODE_COUNT_MIN, NODE_COUNT_MAX);
    args.max_depth = args.max_depth.max(1);
    args.max_children_per_dir = args.max_children_per_dir.max(1);
    args.max_file_bytes = args.max_file_bytes.max(1);
    args
}

struct GenStats {
    files: usize,
    dirs: usize,
    total_file_bytes: usize,
    max_depth: usize,
}

struct GenState {
    remaining: usize,
    next_id: u64,
    stats: GenStats,
}

fn generate(args: &BenchArgs) -> (Vec<MerkleTreeB>, GenStats) {
    let mut rng = StdRng::seed_from_u64(args.seed);
    let mut state = GenState {
        remaining: args.node_count,
        next_id: 0,
        stats: GenStats {
            files: 0,
            dirs: 0,
            total_file_bytes: 0,
            max_depth: 0,
        },
    };
    let mut root_children = Vec::new();
    while state.remaining > 0 {
        let batch = build_dir_contents(&mut rng, 1, &mut state, args);
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
    args: &BenchArgs,
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
            out.push(MerkleTreeB::Dir {
                hash,
                name,
                children: kids,
            });
        } else {
            let size = rng.random_range(1..=args.max_file_bytes);
            // Prepend 8 id bytes so each file's content (and therefore its hash) is unique.
            // Without this, random collisions would cause LMDB to dedupe nodes and skew the
            // benchmark toward "writes that were short-circuited".
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
            out.push(MerkleTreeB::File { hash, name });
        }
    }
    out
}

struct DeleteOnDrop<'a>(&'a AbsolutePath);

impl<'a> Drop for DeleteOnDrop<'a> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.as_path());
    }
}
