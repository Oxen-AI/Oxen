//! One process-wide, path-keyed cache of open `LmdbEnv` handles. It enforces ONE invariant: at
//! most one live env per canonical path per process (heed forbids opening one env dir twice in a
//! process, so overlapping callers must rendezvous on the same live `LmdbEnv`).
//!
//! WEAK RETENTION. The registry stores only a `Weak` reference to each env: the env lives exactly
//! as long as some external `Arc` holds it, and closes when the last one drops. The registry's job
//! is deduplication (rendezvous overlapping opens onto one live env) and liveness observation —
//! not keeping envs warm.
//!
//! Why weak, and not a keep-warm strong cache? Opening an LMDB env is cheap — an `mmap` plus a
//! meta-page read. LMDB's warm working set lives in the OS page cache, which survives the env
//! close, so a reopen re-faults hot pages from RAM in microseconds. There is therefore almost no
//! in-process warmth to lose, and holding handles strongly would instead pin each env's large,
//! sparse virtual-address reservation whether or not the repo is in use. If a consumer ever needs
//! an env kept open across non-overlapping operations, the right place for that strong reference is
//! the layer that owns the resource (e.g. a repository cache holding the `Arc`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Weak};
use std::thread::sleep;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};

use super::lmdb_env::{LmdbEnv, LmdbEnvConfig, open_lmdb_env};
use super::lmdb_error::LmdbLayerError;
use crate::util::fs::canonicalize;

/// How long `get_or_open` waits out a concurrent close before surfacing `EnvAlreadyOpened`.
const REOPEN_RETRIES: u32 = 100;
const REOPEN_RETRY_INTERVAL: Duration = Duration::from_millis(2);

/// Path-keyed registry of shared `LmdbEnv` handles. `parking_lot::RwLock` internally (cannot
/// poison), so there is no lock-poisoned error path. Holds no config — the per-store `map_size`
/// etc. is passed to `get_or_open`, so one registry (the process-global [`open_shared_env`]) serves
/// every store rather than needing one registry per store type.
#[derive(Default)]
pub struct LmdbEnvRegistry {
    slots: RwLock<HashMap<PathBuf, Weak<LmdbEnv>>>,
    /// Serializes opens so two first-opens of the same path cannot race.
    open_lock: Mutex<()>,
}

impl LmdbEnvRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Shared env for `path`, opening it sized by `config` on a miss (`config` is ignored on a
    /// hit; see [`open_shared_env`] for why one registry serves all stores).
    ///
    /// Open-on-miss order (avoids canonicalizing a brand-new path that does not exist yet): the
    /// registry does NOT canonicalize the requested path up front. On a miss it calls
    /// `open_lmdb_env(path, ..)` (which creates the dir and opens), then keys the cache on the
    /// opened env's `path()`. Opens are serialized so two first-opens of the same path cannot
    /// race.
    ///
    /// If `open_lmdb_env` reports the env already open — an open racing the final drop of a
    /// previous handle, where heed has not finished closing the env yet — the registry re-resolves
    /// the live handle if one is observable, otherwise waits out the close briefly and retries
    /// rather than surfacing the error.
    pub fn get_or_open(
        &self,
        path: &Path,
        config: &LmdbEnvConfig,
    ) -> Result<Arc<LmdbEnv>, LmdbLayerError> {
        if let Some(handle) = self.lookup(path) {
            return Ok(handle);
        }
        let _open_guard = self.open_lock.lock();
        if let Some(handle) = self.lookup(path) {
            return Ok(handle);
        }
        let mut attempts = 0;
        loop {
            match open_lmdb_env(path, config) {
                Ok(env) => {
                    let handle = Arc::new(env);
                    self.insert(&handle);
                    return Ok(handle);
                }
                Err(err) if err.is_env_already_opened() => {
                    if let Some(handle) = self.lookup(path) {
                        return Ok(handle);
                    }
                    // A live env exists but is not (or no longer) in the map — the last external
                    // `Arc` is mid-drop and heed has not finished closing it. Wait it out
                    // briefly, then surface the error.
                    attempts += 1;
                    if attempts >= REOPEN_RETRIES {
                        return Err(err);
                    }
                    sleep(REOPEN_RETRY_INTERVAL);
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Whether an env for `path` is currently live (held by some external `Arc`). Non-blocking.
    ///
    /// This is the defense-in-depth precondition check for callers that are about to mutate an
    /// env's directory on disk (delete/rename, or a fork's snapshot destination), where the
    /// caller's contract is already "this repo is not in use." Under weak retention an idle env
    /// is already closed, so a `false` here confirms no open mmap blocks the filesystem op; a
    /// `true` means the precondition is violated and the caller should refuse rather than corrupt
    /// or race.
    pub fn is_live(&self, path: &Path) -> bool {
        self.lookup(path).is_some()
    }

    fn lookup(&self, path: &Path) -> Option<Arc<LmdbEnv>> {
        let slots = self.slots.read();
        if let Some(handle) = lookup_key(&slots, path) {
            return Some(handle);
        }
        // Entries are keyed by the env's canonical path; retry under the canonical spelling.
        // Canonicalize fails for a path that does not exist yet — an ordinary miss.
        let canonical = canonicalize(path).ok()?;
        if canonical.as_path() == path {
            None
        } else {
            lookup_key(&slots, &canonical)
        }
    }

    fn insert(&self, handle: &Arc<LmdbEnv>) {
        // Key on the env's reported path, normalized through the same `canonicalize` lookups use
        // so the two always agree (notably on Windows, where heed may report a `\\?\`-prefixed
        // path that `canonicalize` writes without the prefix).
        let reported = handle.path();
        let key = canonicalize(reported).unwrap_or_else(|_| reported.to_path_buf());
        let mut slots = self.slots.write();
        slots.insert(key, Arc::downgrade(handle));
        // Prune dead weak entries so the map does not accumulate tombstones. The entry just
        // inserted is live (we hold `handle`), so it survives the sweep.
        slots.retain(|_, weak| weak.strong_count() > 0);
    }
}

/// The process-global registry behind [`open_shared_env`] / [`shared_env_is_live`]. Private so the
/// free functions are the only entry point: exactly one registry per process keeps the "one live
/// env per canonical path" invariant global.
static SHARED_REGISTRY: LazyLock<LmdbEnvRegistry> = LazyLock::new(LmdbEnvRegistry::new);

/// Open (or share) the LMDB env at `dir`, sized by `config`, through the process-global registry —
/// so a store does not stand up its own registry. Hold the returned `Arc<LmdbEnv>` to keep the env
/// live (weak retention; see the module docs). `config` is consulted only on an actual open (a
/// miss); on a shared hit it is ignored, and "one logical store per env" means every caller of a
/// given `dir` passes the same `config`.
pub fn open_shared_env(dir: &Path, config: &LmdbEnvConfig) -> Result<Arc<LmdbEnv>, LmdbLayerError> {
    SHARED_REGISTRY.get_or_open(dir, config)
}

/// Whether the env at `dir` is currently live in the process-global registry — the precondition
/// check before deleting/renaming an env dir (see [`LmdbEnvRegistry::is_live`]).
pub fn shared_env_is_live(dir: &Path) -> bool {
    SHARED_REGISTRY.is_live(dir)
}

fn lookup_key(slots: &HashMap<PathBuf, Weak<LmdbEnv>>, key: &Path) -> Option<Arc<LmdbEnv>> {
    slots.get(key)?.upgrade()
}

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;

    use super::*;

    fn test_config() -> LmdbEnvConfig {
        LmdbEnvConfig::new(1, ByteSize::mib(16))
    }

    fn test_registry() -> LmdbEnvRegistry {
        LmdbEnvRegistry::new()
    }

    /// Overlapping opens of one path must rendezvous on the same `Arc` — a second physical open
    /// would hit LMDB's one-env-per-process restriction.
    #[test]
    fn get_or_open_dedups_overlapping_opens() {
        let registry = test_registry();
        let dir = tempfile::tempdir().expect("create temp dir");
        let a = registry
            .get_or_open(dir.path(), &test_config())
            .expect("first open");
        let b = registry
            .get_or_open(dir.path(), &test_config())
            .expect("second open");
        assert!(Arc::ptr_eq(&a, &b));
    }

    /// The env closes exactly when the last external `Arc` drops (the registry holds only a
    /// `Weak`), and a later open is a fresh env.
    #[test]
    fn env_closes_when_last_external_arc_drops() {
        let registry = test_registry();
        let dir = tempfile::tempdir().expect("create temp dir");
        let handle = registry
            .get_or_open(dir.path(), &test_config())
            .expect("open");
        let observer = Arc::downgrade(&handle);
        drop(handle);
        assert!(
            observer.upgrade().is_none(),
            "registry must not keep the env alive after the last external Arc drops"
        );
        registry
            .get_or_open(dir.path(), &test_config())
            .expect("reopen after close");
    }

    /// A brand-new path is opened first (no pre-open canonicalize, which would fail on a path
    /// that does not exist yet), then keyed on the canonical path the opened env reports — so
    /// every later spelling of the path resolves to the same handle.
    #[test]
    fn open_on_miss_keys_on_canonical_path() {
        let registry = test_registry();
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = dir.path().join("store");
        let alias = dir.path().join(".").join("store");

        let via_alias = registry
            .get_or_open(&alias, &test_config())
            .expect("open brand-new path via non-canonical spelling");
        let via_plain = registry
            .get_or_open(&store, &test_config())
            .expect("open via plain path");
        let via_canonical = registry
            .get_or_open(via_alias.path(), &test_config())
            .expect("open via canonical path");
        assert!(Arc::ptr_eq(&via_alias, &via_plain));
        assert!(Arc::ptr_eq(&via_alias, &via_canonical));
    }

    /// `is_live` reflects whether an external `Arc` is still outstanding, and resolves both the
    /// exact and canonical spelling of the path.
    #[test]
    fn is_live_tracks_external_holders() {
        let registry = test_registry();
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = dir.path().join("store");
        let alias = dir.path().join(".").join("store");

        assert!(!registry.is_live(&store), "unopened path is not live");
        let handle = registry.get_or_open(&store, &test_config()).expect("open");
        assert!(registry.is_live(&store), "open env is live");
        assert!(
            registry.is_live(&alias),
            "liveness resolves a non-canonical spelling too"
        );
        drop(handle);
        assert!(!registry.is_live(&store), "closed env is not live");
    }

    /// `open_shared_env` rendezvouses on one env per path through the process-global registry, and
    /// `shared_env_is_live` reflects it.
    #[test]
    fn open_shared_env_dedups_through_the_global_registry() {
        let dir = tempfile::tempdir().expect("create temp dir");
        let store = dir.path().join("shared_store");

        assert!(!shared_env_is_live(&store));
        let a = open_shared_env(&store, &test_config()).expect("first open");
        let b = open_shared_env(&store, &test_config()).expect("second open");
        assert!(Arc::ptr_eq(&a, &b));
        assert!(shared_env_is_live(&store));
        drop(a);
        drop(b);
        assert!(!shared_env_is_live(&store), "closed shared env is not live");
    }
}
