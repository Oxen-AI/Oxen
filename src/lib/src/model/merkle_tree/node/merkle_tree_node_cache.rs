//! Caching layer for MerkleTreeNode operations to improve performance when loading large trees.
//!
//! This module provides LRU caching for merkle tree node disk reads, specifically for the
//! `from_hash` and `read_children_from_hash` operations. Each repository gets its own set
//! of caches to avoid cross-repository contamination.
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

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use lru::LruCache;
use parking_lot::Mutex;

use super::MerkleTreeNode;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash};

const CACHE_SIZE: NonZeroUsize = NonZeroUsize::new(10_000_000).unwrap();

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

/// Get or create a node cache for a repository
pub fn get_node_cache(repo: &LocalRepository) -> NodeCache {
    let mut caches = NODE_CACHES.lock();
    caches
        .entry(repo.path.clone())
        .or_insert_with(|| Arc::new(Mutex::new(LruCache::new(CACHE_SIZE))))
        .clone()
}

/// Get or create a children cache for a repository
pub fn get_children_cache(repo: &LocalRepository) -> ChildrenCache {
    let mut caches = CHILDREN_CACHES.lock();
    caches
        .entry(repo.path.clone())
        .or_insert_with(|| Arc::new(Mutex::new(LruCache::new(CACHE_SIZE))))
        .clone()
}

/// Get a node from cache
pub fn get_cached_node(repo: &LocalRepository, hash: &MerkleHash) -> Option<Arc<MerkleTreeNode>> {
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
