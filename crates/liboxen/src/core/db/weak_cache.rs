//! Path-keyed registry of shared database handles: at most one live handle per path per process.
//!
//! A handle lives exactly as long as some caller holds the `Arc` that [`WeakDbCache::get_or_open`]
//! returned, and closes when the last one drops. The registry itself holds only a `Weak`, so an
//! in-use handle can never be evicted and every concurrent caller for a path shares one handle,
//! which is what compound read-modify-write sequences rely on.
//!
//! Opening runs under a per-path lock rather than the map lock, so an open for one path does not
//! block callers of any other path. Two concurrent first-opens of the same path rendezvous on that
//! lock and only one of them opens.
//!
//! The per-path lock is not re-entrant: an `open` closure must not call back into the same cache
//! for the same path.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

/// One path's handle. The mutex both serializes opens for that path and holds the weak handle, so
/// whichever caller finds the handle dead is the one that reopens it.
struct Slot<V> {
    handle: Mutex<Weak<V>>,
}

impl<V> Default for Slot<V> {
    fn default() -> Self {
        Self {
            handle: Mutex::new(Weak::new()),
        }
    }
}

pub(crate) struct WeakDbCache<V> {
    slots: RwLock<HashMap<PathBuf, Arc<Slot<V>>>>,
}

impl<V> Default for WeakDbCache<V> {
    fn default() -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
        }
    }
}

impl<V> WeakDbCache<V> {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The shared handle for `path`, calling `open` to create one when no live handle exists.
    ///
    /// `open` runs only while `path` has no live handle, and only one caller at a time runs it for
    /// a given path.
    pub(crate) fn get_or_open<E>(
        &self,
        path: &Path,
        open: impl FnOnce() -> Result<V, E>,
    ) -> Result<Arc<V>, E> {
        let slot = self.slot(path);
        let mut handle = slot.handle.lock();
        if let Some(live) = handle.upgrade() {
            return Ok(live);
        }
        let opened = Arc::new(open()?);
        *handle = Arc::downgrade(&opened);
        Ok(opened)
    }

    /// Drops `path`'s registry entry. A handle a caller still holds stays open and closes on its
    /// last drop; the next opener for `path` opens a fresh handle.
    pub(crate) fn forget(&self, path: &Path) {
        self.slots.write().remove(path);
    }

    /// Drops the registry entries under `prefix`, with the same effect on live handles as
    /// [`Self::forget`].
    pub(crate) fn forget_prefix(&self, prefix: &Path) {
        self.slots
            .write()
            .retain(|path, _| !path.starts_with(prefix));
    }

    fn slot(&self, path: &Path) -> Arc<Slot<V>> {
        if let Some(slot) = self.slots.read().get(path) {
            return Arc::clone(slot);
        }
        let mut slots = self.slots.write();
        // Sweep entries no caller can be using. `try_lock` never waits, so a path being opened
        // right now is kept rather than stalling this write behind that open.
        slots.retain(|_, slot| {
            Arc::strong_count(slot) > 1
                || slot
                    .handle
                    .try_lock()
                    .is_none_or(|handle| handle.strong_count() > 0)
        });
        Arc::clone(slots.entry(path.to_path_buf()).or_default())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    fn counting_open(opens: &AtomicUsize) -> Result<String, ()> {
        opens.fetch_add(1, Ordering::SeqCst);
        Ok("handle".to_string())
    }

    /// Announce this opener, then wait for a peer to announce too, and report how many arrived.
    /// Bounded, so serialized opens report 1 rather than hanging the test.
    fn wait_for_a_peer_opener(inside: &AtomicUsize) -> usize {
        inside.fetch_add(1, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(2);
        while inside.load(Ordering::SeqCst) < 2 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        inside.load(Ordering::SeqCst)
    }

    #[test]
    fn test_shares_one_handle_per_path_and_reopens_after_the_last_drop() {
        let cache: WeakDbCache<String> = WeakDbCache::new();
        let opens = AtomicUsize::new(0);
        let path = Path::new("repo/refs");

        let first = cache.get_or_open(path, || counting_open(&opens)).unwrap();
        let second = cache.get_or_open(path, || counting_open(&opens)).unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "concurrent callers for one path must share a handle"
        );
        assert_eq!(
            opens.load(Ordering::SeqCst),
            1,
            "a live handle was reopened"
        );

        drop(first);
        drop(second);
        let reopened = cache.get_or_open(path, || counting_open(&opens)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            2,
            "the handle did not close when its last Arc dropped"
        );
        drop(reopened);

        assert!(
            cache
                .get_or_open(path, || Err::<String, &str>("no disk"))
                .is_err(),
            "a failed open must surface its error"
        );
        cache.get_or_open(path, || counting_open(&opens)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            3,
            "a failed open left the path unopenable"
        );
    }

    #[test]
    fn test_forget_drops_an_entry_and_forget_prefix_drops_a_subtree() {
        let cache: WeakDbCache<String> = WeakDbCache::new();
        let opens = AtomicUsize::new(0);
        let one = Path::new("repo/one");
        let two = Path::new("repo/two");

        let held_one = cache.get_or_open(one, || counting_open(&opens)).unwrap();
        let held_two = cache.get_or_open(two, || counting_open(&opens)).unwrap();

        cache.forget(one);
        let reopened_one = cache.get_or_open(one, || counting_open(&opens)).unwrap();
        assert!(
            !Arc::ptr_eq(&held_one, &reopened_one),
            "a forgotten path still deduplicated onto the old handle"
        );
        assert!(
            Arc::ptr_eq(
                &held_two,
                &cache.get_or_open(two, || counting_open(&opens)).unwrap()
            ),
            "forget removed a sibling path"
        );

        cache.forget_prefix(Path::new("repo"));
        let reopened_two = cache.get_or_open(two, || counting_open(&opens)).unwrap();
        assert!(
            !Arc::ptr_eq(&held_two, &reopened_two),
            "a forgotten subtree still deduplicated onto the old handle"
        );
    }

    /// The property the per-path lock exists for: one path's open must not exclude another's.
    #[test]
    fn test_an_open_does_not_block_an_open_for_a_different_path() {
        let cache: Arc<WeakDbCache<usize>> = Arc::new(WeakDbCache::new());
        let inside = Arc::new(AtomicUsize::new(0));

        let openers: Vec<_> = ["repo/one", "repo/two"]
            .into_iter()
            .map(|path| {
                let cache = Arc::clone(&cache);
                let inside = Arc::clone(&inside);
                thread::spawn(move || {
                    cache
                        .get_or_open(Path::new(path), || {
                            Ok::<_, ()>(wait_for_a_peer_opener(&inside))
                        })
                        .map(|peers| *peers)
                        .unwrap()
                })
            })
            .collect();

        for opener in openers {
            let peers = opener.join().expect("an opener thread panicked");
            assert_eq!(
                peers, 2,
                "an open for one path blocked an open for another path"
            );
        }
    }
}
