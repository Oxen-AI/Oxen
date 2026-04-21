//! Paths resolved from `CARGO_MANIFEST_DIR` at build time, used by the test harness
//! (`crate::test`) and by the dev-mode `TEST` env-var branches in the config getters.
//!
//! Kept in its own always-compiled module so the full `crate::test` helper module can stay
//! gated behind `#[cfg(any(test, feature = "test-utils"))]` without breaking the config getters.

use std::path::PathBuf;
use std::sync::LazyLock;

pub static REPO_ROOT: LazyLock<PathBuf> = LazyLock::new(|| {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .unwrap()
        .to_path_buf()
});

pub static TEST_DATA_DIR: LazyLock<PathBuf> = LazyLock::new(|| REPO_ROOT.join("data"));
