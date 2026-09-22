//! Path-keyed registry of shared DuckDB connections: at most one live connection per file.
//!
//! Opening a DuckDB file this process already has open yields a second, independent database
//! rather than joining the first, and whichever instance folds its state into the file last
//! discards the other's writes. Every caller for a path therefore shares one connection, behind a
//! mutex that is also the lock on that data frame.
//!
//! A slot holds `None` while its connection is closed, which is how a caller holds a path against
//! a reopen while doing filesystem work on its files, and how a connection closed for one query
//! is left for the next caller to reopen.
//!
//! Retention is [`WeakDbCache`]'s keep-warm, which holds a connection open past its last caller
//! and never answers a lookup, so evicting one costs the next caller an open and can never hand
//! two callers different connections for one path.

use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::core::db::data_frames::DataFrameError;
use crate::core::db::weak_cache::WeakDbCache;

/// A shared DuckDB connection slot; `None` while no connection is open.
pub(crate) type CachedConn = Arc<Mutex<Option<duckdb::Connection>>>;

pub(crate) struct WeakDuckCache {
    inner: WeakDbCache<Mutex<Option<duckdb::Connection>>>,
}

impl WeakDuckCache {
    /// A registry that holds the `warm_capacity` most recently opened connections open after
    /// their last caller drops them.
    pub(crate) fn new(warm_capacity: NonZeroUsize) -> Self {
        Self {
            inner: WeakDbCache::new(warm_capacity),
        }
    }

    /// The shared slot for `path`, calling `open` to fill it when no live slot exists.
    ///
    /// `open` runs only while `path` has no live slot, and only one caller at a time runs it for
    /// a given path.
    pub(crate) fn get_or_open(
        &self,
        path: &Path,
        open: impl FnOnce() -> Result<duckdb::Connection, DataFrameError>,
    ) -> Result<CachedConn, DataFrameError> {
        self.inner
            .get_or_open(path, || Ok(Mutex::new(Some(open()?))))
    }

    /// The shared slot for `path`, holding no connection when no live slot exists.
    pub(crate) fn get_or_empty(&self, path: &Path) -> CachedConn {
        match self
            .inner
            .get_or_open(path, || Ok::<_, Infallible>(Mutex::new(None)))
        {
            Ok(slot) => slot,
            Err(never) => match never {},
        }
    }

    /// Drops the registry entries and warm connections under `prefix`. A connection a caller
    /// still holds stays open and closes on its last drop.
    pub(crate) fn forget_prefix(&self, prefix: &Path) {
        self.inner.forget_prefix(prefix);
    }

    /// Every connection keep-warm holds, with its path, taken out of keep-warm. One a caller
    /// still holds stays open for that caller.
    pub(crate) fn drain_warm(&self) -> Vec<(PathBuf, CachedConn)> {
        self.inner.drain_warm()
    }

    /// The live slot for `path`, or `None` when neither a caller nor keep-warm holds one.
    #[cfg(test)]
    pub(crate) fn live_slot(&self, path: &Path) -> Option<CachedConn> {
        self.inner.live_handle(path)
    }
}
