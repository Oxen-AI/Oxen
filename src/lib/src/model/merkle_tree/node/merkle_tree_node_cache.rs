//! Caching layer for MerkleTreeNode operations to improve performance when loading large trees.
//!
//! This module provides LRU caching for merkle tree node disk reads, specifically for the
//! `from_hash` and `read_children_from_hash` operations. Each repository gets its own set
//! of caches to avoid cross-repository contamination.
//!
//! # Environment Variables
//!
//! - `OXEN_MERKLE_CACHE_SIZE`: Set the number of entries per cache (default: 1000)
//! - `OXEN_DISABLE_MERKLE_CACHE`: Set to any value to disable caching
//!
//! # Temporarily Disabling Cache
//!
//! ## Using a closure:
//! ```ignore
//! use oxen::model::merkle_tree::with_cache_disabled;
//!
//! let result = with_cache_disabled(|| {
//!     migrate_merkle_nodes(&repo)
//! })?;
//! ```
//!
//! # Example
//!
//! ```ignore
//! use oxen::model::merkle_tree::remove_merkle_tree_node_from_cache;
//!
//! // The caching is automatic when using MerkleTreeNode::from_hash
//! let node = MerkleTreeNode::from_hash(&repo, &hash)?;
//!
//! // To clear a repository's cache (e.g., after major operations)
//! remove_merkle_tree_node_from_cache(&repo.path)?;
//! ```

use std::cell::Cell;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::Mutex;

use super::MerkleTreeNode;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash};

// Thread-local flag for temporarily disabling cache
thread_local! {
    static THREAD_CACHE_DISABLED: Cell<bool> = const { Cell::new(false) };
}

/// Guard that re-enables caching when dropped
pub struct CacheDisableGuard;

impl Drop for CacheDisableGuard {
    fn drop(&mut self) {
        THREAD_CACHE_DISABLED.with(|c| c.set(false));
    }
}

/// Temporarily disable merkle tree node caching for the current thread.
/// Returns a guard that will re-enable caching when dropped.
///
/// # Example
///
/// ```ignore
/// use oxen::model::merkle_tree::disable_merkle_cache_for_scope;
///
/// {
///     let _guard = disable_merkle_cache_for_scope();
///     // All merkle tree operations in this scope will bypass the cache
///     // ...
/// } // Cache automatically re-enabled here
/// ```
fn disable_merkle_cache_for_scope() -> CacheDisableGuard {
    THREAD_CACHE_DISABLED.with(|c| c.set(true));
    CacheDisableGuard
}

/// Execute a function with merkle tree node caching temporarily disabled.
/// The cache is automatically re-enabled after the function completes.
///
/// # Example
///
/// ```ignore
/// use oxen::model::merkle_tree::with_cache_disabled;
///
/// let result = with_cache_disabled(|| {
///     // All merkle tree operations in this closure will bypass the cache
///     migrate_merkle_nodes(&repo)
/// })?;
/// ```
pub fn with_cache_disabled<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    let _guard = disable_merkle_cache_for_scope();
    f()
}

// Default cache size if not specified via environment variable
const DEFAULT_CACHE_SIZE: usize = 1000;

// Type aliases for readability
type NodeCache = Arc<Mutex<LruCache<MerkleHash, Arc<MerkleTreeNode>>>>;
type ChildrenCache = Arc<Mutex<LruCache<MerkleHash, Arc<Vec<(MerkleHash, MerkleTreeNode)>>>>>;
type NodeCacheMap = HashMap<PathBuf, NodeCache>;
type ChildrenCacheMap = HashMap<PathBuf, ChildrenCache>;

// Cache for individual nodes (from_hash results)
static NODE_CACHES: LazyLock<Mutex<NodeCacheMap>> = LazyLock::new(|| Mutex::new(HashMap::new()));

// Cache for children reads (read_children_from_hash results)
static CHILDREN_CACHES: LazyLock<Mutex<ChildrenCacheMap>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// Cached configuration values - computed once at startup
static CACHING_DISABLED: LazyLock<bool> =
    LazyLock::new(|| std::env::var("OXEN_DISABLE_MERKLE_CACHE").is_ok());

static CACHE_SIZE: LazyLock<NonZeroUsize> = LazyLock::new(|| {
    std::env::var("OXEN_MERKLE_CACHE_SIZE")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .and_then(NonZeroUsize::new)
        .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap())
});

/// Get or create a node cache for a repository
pub fn get_node_cache(repo: &LocalRepository) -> NodeCache {
    let mut caches = NODE_CACHES.lock();
    caches
        .entry(repo.path.clone())
        .or_insert_with(|| Arc::new(Mutex::new(LruCache::new(*CACHE_SIZE))))
        .clone()
}

/// Get or create a children cache for a repository
pub fn get_children_cache(repo: &LocalRepository) -> ChildrenCache {
    let mut caches = CHILDREN_CACHES.lock();
    caches
        .entry(repo.path.clone())
        .or_insert_with(|| Arc::new(Mutex::new(LruCache::new(*CACHE_SIZE))))
        .clone()
}

/// Get a node from cache
pub fn get_cached_node(repo: &LocalRepository, hash: &MerkleHash) -> Option<Arc<MerkleTreeNode>> {
    if *CACHING_DISABLED || THREAD_CACHE_DISABLED.with(|c| c.get()) {
        return None;
    }
    let cache = get_node_cache(repo);
    let mut cache_guard = cache.lock();
    cache_guard.get(hash).cloned()
}

/// Put a node in cache
pub fn cache_node(
    repo: &LocalRepository,
    hash: MerkleHash,
    node: MerkleTreeNode,
) -> Arc<MerkleTreeNode> {
    let arc_node = Arc::new(node);
    if *CACHING_DISABLED || THREAD_CACHE_DISABLED.with(|c| c.get()) {
        return arc_node;
    }
    let cache = get_node_cache(repo);
    let mut cache_guard = cache.lock();
    cache_guard.put(hash, arc_node.clone());
    arc_node
}

/// Get children from cache
pub fn get_cached_children(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Option<Arc<Vec<(MerkleHash, MerkleTreeNode)>>> {
    if *CACHING_DISABLED || THREAD_CACHE_DISABLED.with(|c| c.get()) {
        return None;
    }
    let cache = get_children_cache(repo);
    let mut cache_guard = cache.lock();
    cache_guard.get(hash).cloned()
}

/// Put children in cache
pub fn cache_children(
    repo: &LocalRepository,
    hash: MerkleHash,
    children: Vec<(MerkleHash, MerkleTreeNode)>,
) -> Arc<Vec<(MerkleHash, MerkleTreeNode)>> {
    let arc_children = Arc::new(children);
    if *CACHING_DISABLED || THREAD_CACHE_DISABLED.with(|c| c.get()) {
        return arc_children;
    }
    let cache = get_children_cache(repo);
    let mut cache_guard = cache.lock();
    cache_guard.put(hash, arc_children.clone());
    arc_children
}

/// Remove a repository's caches
pub fn remove_from_cache(repository_path: impl AsRef<std::path::Path>) -> Result<(), OxenError> {
    let path = repository_path.as_ref().to_path_buf();

    // Remove from node caches
    {
        let mut caches = NODE_CACHES.lock();
        caches.remove(&path);
    }

    // Remove from children caches
    {
        let mut caches = CHILDREN_CACHES.lock();
        caches.remove(&path);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{repositories, test};

    #[test]
    fn test_cache_disable_with_closure() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let repo = repositories::init(dir)?;

            // Create a dummy node for testing
            let hash = MerkleHash::new(11111);
            let node = MerkleTreeNode::default();

            // Cache should work normally
            cache_node(&repo, hash, node.clone());
            assert!(get_cached_node(&repo, &hash).is_some());

            // Run operations with cache disabled
            let result = with_cache_disabled(|| {
                // Cache should be disabled in this closure
                assert!(get_cached_node(&repo, &hash).is_none());

                // Try to cache another node - it shouldn't be cached
                let hash2 = MerkleHash::new(22222);
                let node2 = MerkleTreeNode::default();
                cache_node(&repo, hash2, node2);
                assert!(get_cached_node(&repo, &hash2).is_none());

                "test_result"
            });

            // Verify the closure returned the expected value
            assert_eq!(result, "test_result");

            // Cache should work again after closure completes
            assert!(get_cached_node(&repo, &hash).is_some());

            Ok(())
        })
    }

    #[test]
    fn test_cache_disable_is_thread_local() -> Result<(), OxenError> {
        use std::sync::Arc;
        use std::thread;

        test::run_empty_dir_test(|dir| {
            let repo = Arc::new(repositories::init(dir)?);
            let hash = MerkleHash::new(33333);
            let node = MerkleTreeNode::default();

            // Cache a node in the main thread
            cache_node(&repo, hash, node.clone());

            // Disable cache in main thread
            let _guard = disable_merkle_cache_for_scope();
            assert!(get_cached_node(&repo, &hash).is_none());

            // Spawn a new thread where cache should still work
            let repo_clone = Arc::clone(&repo);
            let handle = thread::spawn(move || {
                // Cache should work in this thread
                get_cached_node(&repo_clone, &hash).is_some()
            });

            // Wait for thread to complete
            let other_thread_has_cache = handle.join().unwrap();
            assert!(other_thread_has_cache);

            // Main thread should still have cache disabled
            assert!(get_cached_node(&repo, &hash).is_none());

            Ok(())
        })
    }
}
