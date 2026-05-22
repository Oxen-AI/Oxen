//! Micro-benchmarks for the [`FileBackend`] and [`LmdbBackend`] Merkle tree stores.
//!
//! Three benchmark groups:
//! - `merkle_store_writes`  — populate N commit→dir→vnode subtrees from scratch
//! - `merkle_store_reads`   — `exists` / `get_node` / `get_children` on a pre-populated store
//! - `merkle_store_tree_walk` — top-down walk of all subtrees in a pre-populated store
//!
//! # Running
//! ```bash
//! # Quick smoke run (both backends):
//! BENCHMARK_ITERS=3 BENCHMARK_BACKENDS=both \
//!   cargo bench --features liboxen/test-utils --bench merkle_store -- --quick
//!
//! # Full run (results under target/criterion/):
//! cargo bench --features liboxen/test-utils --bench merkle_store
//!
//! # Single backend:
//! BENCHMARK_BACKENDS=lmdb cargo bench --features liboxen/test-utils --bench merkle_store
//! ```
//!
//! # Cold-cache reads
//! To measure cold-cache performance (OS page-cache bypassed), run these commands
//! manually between criterion samples:
//! - Linux: `sync && echo 3 | sudo tee /proc/sys/vm/drop_caches`
//! - macOS: `sync && sudo purge`

use std::collections::HashMap;

use criterion::{BatchSize, BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use tempfile::TempDir;
use time::OffsetDateTime;

#[path = "bench_support.rs"]
mod bench_support;
use bench_support::backend_kinds_from_env;

use liboxen::config::repository_config::MerkleStoreKind;
use liboxen::constants::MIN_OXEN_VERSION;
use liboxen::model::merkle_tree::MerkleStore;
use liboxen::model::merkle_tree::node::{
    CommitNode, DirNode, FileNode, VNode, commit_node::CommitNodeOpts, dir_node::DirNodeOpts,
    file_node::FileNodeOpts, vnode::VNodeOpts,
};
use liboxen::model::{EntryDataType, LocalRepository, MerkleHash};
use liboxen::repositories;

// ─── repo setup ──────────────────────────────────────────────────────────────

fn build_bench_repo(kind: MerkleStoreKind) -> (TempDir, LocalRepository) {
    let tmp = TempDir::new().expect("create temp dir");
    let repo =
        repositories::init::init_with_version_and_merkle_store(tmp.path(), MIN_OXEN_VERSION, kind)
            .expect("init repo");
    (tmp, repo)
}

// ─── deterministic hash helpers ──────────────────────────────────────────────
//
// Each "type" occupies a distinct region of the 128-bit hash space so hashes
// can never collide across node kinds or subtree indices.

fn commit_hash(idx: u64) -> MerkleHash {
    MerkleHash::new((1u128 << 96) | idx as u128)
}
fn dir_hash(idx: u64) -> MerkleHash {
    MerkleHash::new((2u128 << 96) | idx as u128)
}
fn vnode_hash(idx: u64) -> MerkleHash {
    MerkleHash::new((3u128 << 96) | idx as u128)
}
fn file_hash(subtree: u64, file_idx: u64) -> MerkleHash {
    // Up to 2^16 files per subtree and 2^48 subtrees before collision.
    MerkleHash::new((4u128 << 96) | ((subtree as u128) << 16) | file_idx as u128)
}

// ─── node fixture helpers ─────────────────────────────────────────────────────

fn make_commit(repo: &LocalRepository, idx: u64) -> CommitNode {
    CommitNode::new(
        repo,
        CommitNodeOpts {
            hash: commit_hash(idx),
            parent_ids: vec![],
            email: String::new(),
            author: String::new(),
            message: String::new(),
            timestamp: OffsetDateTime::UNIX_EPOCH,
        },
    )
    .expect("CommitNode::new")
}

fn make_dir(repo: &LocalRepository, idx: u64) -> DirNode {
    DirNode::new(
        repo,
        DirNodeOpts {
            name: String::new(),
            hash: dir_hash(idx),
            num_entries: 0,
            num_bytes: 0,
            last_commit_id: MerkleHash::new(0),
            last_modified_seconds: 0,
            last_modified_nanoseconds: 0,
            data_type_counts: HashMap::new(),
            data_type_sizes: HashMap::new(),
        },
    )
    .expect("DirNode::new")
}

fn make_vnode(repo: &LocalRepository, idx: u64) -> VNode {
    VNode::new(
        repo,
        VNodeOpts {
            hash: vnode_hash(idx),
            num_entries: 0,
        },
    )
    .expect("VNode::new")
}

fn make_files(repo: &LocalRepository, subtree_idx: u64, fan_out: usize) -> Vec<FileNode> {
    (0..fan_out as u64)
        .map(|j| {
            FileNode::new(
                repo,
                FileNodeOpts {
                    name: format!("f{j}"),
                    hash: file_hash(subtree_idx, j),
                    combined_hash: MerkleHash::new(0),
                    metadata_hash: None,
                    num_bytes: 0,
                    last_modified_seconds: 0,
                    last_modified_nanoseconds: 0,
                    data_type: EntryDataType::Binary,
                    metadata: None,
                    mime_type: String::new(),
                    extension: String::new(),
                },
            )
            .expect("FileNode::new")
        })
        .collect()
}

// ─── populate helper ─────────────────────────────────────────────────────────

/// Write `n` commit→dir→vnode subtrees into `repo`'s Merkle store.
/// Each vnode has `fan_out` file children.
fn populate(repo: &LocalRepository, n: usize, fan_out: usize) {
    let store = repo.merkle_store().expect("merkle_store");
    let session = store.begin().expect("begin write session");
    for i in 0..n as u64 {
        let commit = make_commit(repo, i);
        let dir = make_dir(repo, i);
        let vnode = make_vnode(repo, i);
        let files = make_files(repo, i, fan_out);

        {
            let mut ns = session
                .create_node(&commit, None)
                .expect("create commit node");
            ns.add_child(&dir).expect("add dir child");
            ns.finish().expect("finish commit node");
        }
        {
            let mut ns = session
                .create_node(&dir, Some(commit_hash(i)))
                .expect("create dir node");
            ns.add_child(&vnode).expect("add vnode child");
            ns.finish().expect("finish dir node");
        }
        {
            let mut ns = session
                .create_node(&vnode, Some(dir_hash(i)))
                .expect("create vnode node");
            for f in &files {
                ns.add_child(f).expect("add file child");
            }
            ns.finish().expect("finish vnode node");
        }
    }
    session.finish().expect("finish write session");
}

/// Collect all commit / dir / vnode hashes for `n` subtrees.
fn collect_hashes(n: usize) -> Vec<MerkleHash> {
    let mut hashes = Vec::with_capacity(n * 3);
    for i in 0..n as u64 {
        hashes.push(commit_hash(i));
        hashes.push(dir_hash(i));
        hashes.push(vnode_hash(i));
    }
    hashes
}

// ─── walk helper ─────────────────────────────────────────────────────────────

fn walk_subtree(store: &dyn MerkleStore, hash: &MerkleHash) {
    let _ = store.get_node(hash).expect("get_node");
    for (child_hash, _) in store.get_children(hash).expect("get_children") {
        walk_subtree(store, &child_hash);
    }
}

// ─── benchmarks ──────────────────────────────────────────────────────────────

/// Measures the time to write N subtrees from scratch into an empty store.
///
/// `measurement_time` is sized per-N to keep `--quick` mode's loop above
/// criterion's early-exit branch. That branch (codspeed-criterion-compat-walltime
/// ≤ 2.10.1 `routine.rs:104`; upstream criterion.rs PR #685) tries to perturb
/// the second sample with `t_prev + 0.000001` to avoid identical-sample KDE
/// crashes, but the perturbation rounds away in f64 above ~1 s, so the samples
/// collapse to equality and downstream KDE produces NaN — panicking
/// `Sample::new`. Keeping a single iteration under `measurement_time` routes
/// us through the loop's normal-exit path instead, where OS jitter naturally
/// separates `t_prev` and `t_now`.
fn bench_writes(c: &mut Criterion) {
    let fan_out = 10;
    let mut group = c.benchmark_group("merkle_store_writes");
    let backends = backend_kinds_from_env();
    for &n in &[100usize, 1_000, 10_000] {
        // FileBackend is the slow path at ~30 ms/subtree (fan_out=10, 13 nodes
        // per subtree). Budget 4× the expected single-iteration cost so the
        // loop can take its second sample before the timeout, plus a 5 s floor.
        let budget_secs = ((n as u64 * 30 * 4) / 1_000).max(5);
        group.measurement_time(std::time::Duration::from_secs(budget_secs));
        for &(kind, name) in &backends {
            group.bench_with_input(
                BenchmarkId::new(format!("{name}_subtrees"), n),
                &n,
                |b, &n| {
                    b.iter_batched(
                        || build_bench_repo(kind),
                        |(_tmp, repo)| {
                            populate(&repo, black_box(n), fan_out);
                        },
                        BatchSize::PerIteration,
                    )
                },
            );
        }
    }
    group.finish();
}

/// Measures `exists`, `get_node`, and `get_children` on a pre-populated store.
/// Hashes are sampled in random order to model realistic read access patterns.
fn bench_reads(c: &mut Criterion) {
    let fan_out = 10;
    let sample_size = 1_000;
    let mut group = c.benchmark_group("merkle_store_reads");
    let backends = backend_kinds_from_env();
    for &n in &[1_000usize, 10_000] {
        for &(kind, name) in &backends {
            let (_tmp, repo) = build_bench_repo(kind);
            populate(&repo, n, fan_out);
            let all_hashes = collect_hashes(n);
            let mut rng = StdRng::seed_from_u64(0xC0FFEE);
            let mut shuffled = all_hashes.clone();
            shuffled.shuffle(&mut rng);
            let sampled: Vec<MerkleHash> = shuffled
                .into_iter()
                .take(sample_size.min(all_hashes.len()))
                .collect();

            let store = repo.merkle_store().expect("merkle_store");

            group.bench_function(BenchmarkId::new(format!("{name}_exists"), n), |b| {
                b.iter(|| {
                    for h in &sampled {
                        black_box(store.exists(h).expect("exists"));
                    }
                })
            });

            group.bench_function(BenchmarkId::new(format!("{name}_get_node"), n), |b| {
                b.iter(|| {
                    for h in &sampled {
                        black_box(store.get_node(h).expect("get_node"));
                    }
                })
            });

            group.bench_function(BenchmarkId::new(format!("{name}_get_children"), n), |b| {
                b.iter(|| {
                    for h in &sampled {
                        black_box(store.get_children(h).expect("get_children"));
                    }
                })
            });

            // `store` borrows from `repo`; both are dropped at end of loop body.
            // Rust's reverse-declaration order (store → repo → tmp) is the safe order.
        }
    }
    group.finish();
}

/// Measures a full top-down tree walk across all subtrees in the store.
/// Models the read pattern used during migration / verification passes.
fn bench_tree_walk(c: &mut Criterion) {
    let fan_out = 10;
    let mut group = c.benchmark_group("merkle_store_tree_walk");
    let backends = backend_kinds_from_env();
    for &n in &[100usize, 1_000] {
        for &(kind, name) in &backends {
            let (_tmp, repo) = build_bench_repo(kind);
            populate(&repo, n, fan_out);
            let commit_hashes: Vec<MerkleHash> = (0..n as u64).map(commit_hash).collect();
            let store = repo.merkle_store().expect("merkle_store");

            group.bench_function(BenchmarkId::new(name, n), |b| {
                b.iter(|| {
                    for h in &commit_hashes {
                        walk_subtree(store, h);
                    }
                })
            });
        }
    }
    group.finish();
}

criterion_group!(benches, bench_writes, bench_reads, bench_tree_walk);
criterion_main!(benches);

// ─── correctness check ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {

    /// Confirm that both backends produce identical round-trip results for a
    /// fixed N=10 subtree fixture. Catches future divergence between backends.
    #[test]
    fn both_backends_round_trip_identically() {
        let n = 10;
        let fan_out = 3;

        let mut results: Vec<Vec<(MerkleHash, String)>> = Vec::new();
        for kind in [MerkleStoreKind::File, MerkleStoreKind::Lmdb] {
            let (_tmp, repo) = build_bench_repo(kind);
            populate(&repo, n, fan_out);
            let store = repo.merkle_store().expect("merkle_store");

            let mut tuples: Vec<(MerkleHash, String)> = Vec::new();
            for i in 0..n as u64 {
                for h in [commit_hash(i), dir_hash(i), vnode_hash(i)] {
                    assert!(
                        store.exists(&h).expect("exists"),
                        "hash {h} missing for {kind:?}"
                    );
                    let entry = store.get_node(&h).expect("get_node");
                    let kind_name = entry
                        .map(|e| format!("{:?}", e.node.node_type()))
                        .unwrap_or_else(|| "None".to_string());
                    tuples.push((h, kind_name));
                }
            }
            tuples.sort_by_key(|(h, _)| h.to_u128());
            results.push(tuples);
            // store → repo → tmp drop in correct order (reverse declaration) at end of body.
        }

        assert_eq!(
            results[0], results[1],
            "File and LMDB backends returned different results for the same fixture"
        );
    }
}
