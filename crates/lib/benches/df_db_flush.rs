//! Benchmarks the cost of `flush_all_df_db_connections` so we can sanity-check
//! the assumption that the post-shutdown CHECKPOINT pass fits comfortably
//! inside the supervisor's stop budget.
//!
//! Each iteration:
//!   1. Re-populates the connection cache with `n_dbs` DuckDB databases, each
//!      holding `n_rows` rows of uncheckpointed work
//!      (`PRAGMA disable_checkpoint_on_shutdown` keeps the WAL on disk).
//!   2. Calls `flush_all_df_db_connections` and times only that call.
//!
//! Setup time is excluded from the measurement via `iter_custom`. The
//! per-iteration setup is heavy, so the sample size is small.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use liboxen::core::db::data_frames::df_db::{flush_all_df_db_connections, with_df_db_manager};
use liboxen::util;

const TABLE: &str = "df";

fn populate_one_db(db_file: &Path, n_rows: usize) {
    with_df_db_manager(db_file, |manager| {
        manager.with_conn(|conn| {
            // Keep the WAL on disk so the only thing that flushes it is our
            // explicit `flush_all_df_db_connections` call.
            conn.execute_batch("PRAGMA disable_checkpoint_on_shutdown")?;
            conn.execute(
                &format!("CREATE TABLE {TABLE} (id INTEGER, name VARCHAR, value DOUBLE)"),
                [],
            )?;
            // Single bulk insert via `range` — keeps setup fast, leaves all
            // the rows in WAL until our flush touches them.
            conn.execute(
                &format!(
                    "INSERT INTO {TABLE} \
                     SELECT i AS id, 'name_' || i AS name, CAST(i AS DOUBLE) * 1.5 AS value \
                     FROM range(0, {n_rows}) t(i)"
                ),
                [],
            )?;
            Ok(())
        })
    })
    .expect("populate_one_db: with_df_db_manager failed");
}

fn populate_cache(base_dir: &Path, n_dbs: usize, n_rows: usize) {
    if base_dir.exists() {
        util::fs::remove_dir_all(base_dir).expect("clear base_dir");
    }
    util::fs::create_dir_all(base_dir).expect("create base_dir");

    for i in 0..n_dbs {
        let db_dir = base_dir.join(format!("db_{i}"));
        util::fs::create_dir_all(&db_dir).expect("create db_dir");
        let db_file = db_dir.join("db");
        populate_one_db(&db_file, n_rows);
    }
}

fn df_db_flush_benchmark(c: &mut Criterion) {
    let base_dir = PathBuf::from("data/test/benches/df_db_flush");
    if base_dir.exists() {
        util::fs::remove_dir_all(&base_dir).expect("clear base_dir at start");
    }

    let mut group = c.benchmark_group("df_db_flush");
    // Per-iteration setup is heavy (many DuckDB opens + 10k-row inserts), so
    // keep the sample count small. Bump via env if you want tighter intervals.
    let sample_size = std::env::var("BENCHMARK_ITERS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);
    group.sample_size(sample_size);

    // Primary case is the user-asked shape: 100 dataframes × 10k rows.
    // The smaller/larger shapes give a sense of how the cost scales without
    // requiring separate bench invocations.
    let cases: &[(usize, usize)] = &[
        (10, 10_000),   // small — sanity baseline
        (100, 10_000),  // primary — matches the production cache cap
        (100, 100_000), // 10× the primary row count, to probe scaling
    ];

    for &(n_dbs, n_rows) in cases {
        let case_dir = base_dir.join(format!("{n_dbs}x{n_rows}"));
        group.bench_with_input(
            BenchmarkId::new("flush", format!("{n_dbs}_dbs_{n_rows}_rows")),
            &(n_dbs, n_rows),
            |b, &(n_dbs, n_rows)| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        // Setup: rebuild the cache. Excluded from the measurement.
                        populate_cache(&case_dir, n_dbs, n_rows);
                        // Measurement: just the flush call.
                        let start = Instant::now();
                        flush_all_df_db_connections();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }
    group.finish();

    if base_dir.exists() {
        let _ = util::fs::remove_dir_all(&base_dir);
    }
}

criterion_group!(benches, df_db_flush_benchmark);
criterion_main!(benches);
