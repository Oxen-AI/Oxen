use liboxen::config::repository_config::MerkleStoreKind;

pub const BACKEND_KINDS: &[(MerkleStoreKind, &str)] = &[
    (MerkleStoreKind::File, "file"),
    (MerkleStoreKind::Lmdb, "lmdb"),
];

/// Returns the backends to run based on the `BENCHMARK_BACKENDS` env var.
///
/// Set the variable to `"file"`, `"lmdb"`, or `"both"` (default when unset).
/// Example:
/// ```bash
/// BENCHMARK_BACKENDS=lmdb cargo bench --features liboxen/test-utils --bench add
/// ```
pub fn backend_kinds_from_env() -> Vec<(MerkleStoreKind, &'static str)> {
    let filter = std::env::var("BENCHMARK_BACKENDS").ok();
    BACKEND_KINDS
        .iter()
        .filter(|(_, name)| match filter.as_deref() {
            None | Some("both") => true,
            Some(want) => *name == want,
        })
        .copied()
        .collect()
}
