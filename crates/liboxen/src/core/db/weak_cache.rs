//! Path-keyed registry of shared database handles: at most one live handle per path per process.
//!
//! A handle lives as long as some caller holds the `Arc` that [`WeakDbCache::get_or_open`]
//! returned or the keep-warm cache holds it, and closes when the last of those drops. The slot map
//! holds only a `Weak`, so an in-use handle can never be evicted and every concurrent caller for a
//! path shares one handle, which is what compound read-modify-write sequences rely on.
//!
//! Keep-warm is retention alone and never answers a lookup, so evicting a warm handle costs the
//! next caller an open and can never hand two callers different handles for one path.
//!
//! [`WeakDbCache::forget`] and [`WeakDbCache::forget_prefix`] drop the warm handle along with the
//! slot, so a caller that forgets a path before moving or deleting its directory leaves nothing
//! holding the old files open.
//!
//! Opening runs under a per-path lock rather than the map lock, so an open for one path does not
//! block callers of any other path. Two concurrent first-opens of the same path rendezvous on that
//! lock and only one of them opens.
//!
//! The per-path lock is not re-entrant: an `open` closure must not call
//! [`WeakDbCache::get_or_open`] for the same path.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use lru::LruCache;
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
    warm: Mutex<LruCache<PathBuf, Arc<V>>>,
}

impl<V> WeakDbCache<V> {
    /// A registry that holds the `warm_capacity` most recently opened handles open after their
    /// last caller drops them.
    pub(crate) fn new(warm_capacity: NonZeroUsize) -> Self {
        Self {
            slots: RwLock::new(HashMap::new()),
            warm: Mutex::new(LruCache::new(warm_capacity)),
        }
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
        // Closing an evicted database runs with every lock released, so it never blocks a caller
        // for its path or for any other.
        drop(handle);
        // Keep-warm takes the handle only while the slot map still holds this open's slot, so a
        // path forgotten during the open stays forgotten.
        let evicted = {
            let slots = self.slots.read();
            if slots.get(path).is_some_and(|live| Arc::ptr_eq(live, &slot)) {
                self.warm
                    .lock()
                    .push(path.to_path_buf(), Arc::clone(&opened))
            } else {
                None
            }
        };
        drop(evicted);
        Ok(opened)
    }

    /// Drops `path`'s registry entry and its warm handle. A handle a caller still holds stays open
    /// and closes on its last drop; the next opener for `path` opens a fresh handle.
    pub(crate) fn forget(&self, path: &Path) {
        self.slots.write().remove(path);
        let forgotten = self.warm.lock().pop(path);
        drop(forgotten);
    }

    /// Drops the registry entries and warm handles under `prefix`, with the same effect on live
    /// handles as [`Self::forget`].
    pub(crate) fn forget_prefix(&self, prefix: &Path) {
        self.slots
            .write()
            .retain(|path, _| !path.starts_with(prefix));
        let forgotten: Vec<_> = {
            let mut warm = self.warm.lock();
            let under_prefix: Vec<PathBuf> = warm
                .iter()
                .map(|(path, _)| path)
                .filter(|path| path.starts_with(prefix))
                .cloned()
                .collect();
            under_prefix
                .iter()
                .filter_map(|path| warm.pop(path))
                .collect()
        };
        drop(forgotten);
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

    fn cache_holding<V>(warm: usize) -> WeakDbCache<V> {
        WeakDbCache::new(NonZeroUsize::new(warm).expect("a warm capacity must be non-zero"))
    }

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
    fn test_shares_one_handle_per_path_and_keeps_it_warm_until_evicted() {
        let cache: WeakDbCache<String> = cache_holding(1);
        let opens = AtomicUsize::new(0);
        let path = Path::new("repo/refs");
        let other = Path::new("repo/other");

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
        let warm = cache.get_or_open(path, || counting_open(&opens)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            1,
            "the handle was reopened rather than served warm after its last caller dropped it"
        );
        let watch = Arc::downgrade(&warm);
        drop(warm);

        // Capacity is one, so opening another path evicts this one.
        drop(cache.get_or_open(other, || counting_open(&opens)).unwrap());
        assert!(watch.upgrade().is_none(), "an evicted handle stayed open");
        cache.get_or_open(path, || counting_open(&opens)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            3,
            "an evicted handle was not reopened"
        );

        let fresh = Path::new("repo/fresh");
        assert!(
            cache
                .get_or_open(fresh, || Err::<String, &str>("no disk"))
                .is_err(),
            "a failed open must surface its error"
        );
        cache.get_or_open(fresh, || counting_open(&opens)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            4,
            "a failed open left the path unopenable"
        );
    }

    #[test]
    fn test_forget_drops_an_entry_and_forget_prefix_drops_a_subtree() {
        let cache: WeakDbCache<String> = cache_holding(4);
        let opens = AtomicUsize::new(0);
        let one = Path::new("repo/one");
        let two = Path::new("repo/two");

        let held_one = cache.get_or_open(one, || counting_open(&opens)).unwrap();
        let held_two = cache.get_or_open(two, || counting_open(&opens)).unwrap();
        let watch_one = Arc::downgrade(&held_one);
        let watch_two = Arc::downgrade(&held_two);
        drop(held_one);
        drop(held_two);

        cache.forget(one);
        assert!(
            watch_one.upgrade().is_none(),
            "forget left the handle open, so moving its directory would meet its own LOCK file"
        );
        assert!(
            watch_two.upgrade().is_some(),
            "forget closed a sibling path's handle"
        );
        cache.get_or_open(one, || counting_open(&opens)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            3,
            "a forgotten path was still served from the warm cache"
        );

        cache.forget_prefix(Path::new("repo"));
        assert!(
            watch_two.upgrade().is_none(),
            "forget_prefix left a handle in the subtree open"
        );
        cache.get_or_open(two, || counting_open(&opens)).unwrap();
        assert_eq!(
            opens.load(Ordering::SeqCst),
            4,
            "a forgotten subtree was still served from the warm cache"
        );

        let three = Path::new("repo/three");
        let opened_while_forgotten = cache
            .get_or_open(three, || {
                cache.forget(three);
                counting_open(&opens)
            })
            .unwrap();
        let watch_three = Arc::downgrade(&opened_while_forgotten);
        drop(opened_while_forgotten);
        assert!(
            watch_three.upgrade().is_none(),
            "a path forgotten during its own open kept a warm handle"
        );
    }

    /// The property the per-path lock exists for: one path's open must not exclude another's.
    #[test]
    fn test_an_open_does_not_block_an_open_for_a_different_path() {
        let cache: Arc<WeakDbCache<usize>> = Arc::new(cache_holding(2));
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
