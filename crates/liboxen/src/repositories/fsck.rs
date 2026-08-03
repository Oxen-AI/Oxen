//! # oxen fsck
//!
//! Integrity checks and repair utilities for a repository.
//!
//! Today this covers two distinct operations:
//!
//! 1. **Version-blob scan** — detect (and optionally remove) content-addressed version files that
//!    don't hash to their filename. Implemented in the `version_store` layer via
//!    [`VersionStore::clean_corrupted_versions`][crate::storage::version_store::VersionStore::clean_corrupted_versions];
//!    the CLI wraps that directly. No module-level public API is exposed here yet, but this
//!    module is the natural home for future wrappers.
//!
//! 2. **`dir_hash_db` rebuild** — rebuild a commit's path→dir-hash cache from its merkle tree
//!    (the authoritative representation). Path-based endpoints like
//!    `/api/repos/{ns}/{repo}/dir/{resource}` look paths up in that cache; when the cache drifts
//!    from the tree (e.g. from the commit-writer bug fixed in
//!    [PR #411](https://github.com/Oxen-AI/Oxen/pull/411), which could leave stale entries
//!    for removed nested directories), those endpoints return "Resource not found" for
//!    directories that are still present in the tree. See [`rebuild_dir_hash_db`].

use rocksdb::{DBWithThreadMode, SingleThreaded};
use serde::Serialize;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::core::db;
use crate::core::db::dir_hashes::dir_hashes_db::{
    dir_hash_db_path_from_commit_id, with_exclusive_access,
};
use crate::core::db::key_val::str_val_db;
use crate::core::db::merkle_node::merkle_node_db::{
    MerkleDbError, MerkleNodeDB, suppress_retired_format_logging,
};
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode, MerkleTreeNodeType};
use crate::model::{Commit, LocalRepository, MerkleHash};
use crate::repositories;
use crate::util;

/// What a [`scan_node_format`] pass found in one repository.
#[derive(Debug, Default, Clone, Serialize)]
pub struct NodeFormatReport {
    /// Every node the store holds, whatever its format.
    pub total_nodes: usize,
    /// Nodes written before the v0.25.0 on-disk format, counted per node type. Absent types
    /// simply have no such nodes.
    pub pre_v025: HashMap<MerkleTreeNodeType, usize>,
    /// Nodes that failed to decode for some other reason. Counted rather than returned so one
    /// damaged node doesn't abort a fleet-wide scan; a non-zero value wants looking at by hand.
    pub undecodable: usize,
}

impl NodeFormatReport {
    /// Nodes predating v0.25.0, across every node type.
    pub fn pre_v025_total(&self) -> usize {
        self.pre_v025.values().sum()
    }

    /// Whether this repository holds any pre-v0.25.0 nodes. Deliberately narrower than "holds
    /// anything unreadable": [`Self::undecodable`] nodes are damage, a separate condition with a
    /// separate remedy, and conflating the two overstates the population awaiting migration.
    pub fn is_affected(&self) -> bool {
        self.pre_v025_total() > 0
    }
}

/// Count a repository's Merkle nodes that predate the v0.25.0 on-disk format, by node type.
///
/// Read-only, and reads every node in the store, so cost scales with tree size. Works on either
/// merkle-node backend, since it goes through the store rather than the on-disk file layout.
///
/// This asks the same decoder the server uses, so its answer cannot drift from what a read
/// actually does — which is the point of having it rather than matching bytes externally.
pub fn scan_node_format(repo: &LocalRepository) -> Result<NodeFormatReport, OxenError> {
    // The report below names every node this would log, so leaving the warning on buries the
    // result under one line per affected node — order 10^5 across a fleet.
    let _quiet = suppress_retired_format_logging();

    let store = repo.merkle_node_store();
    let hashes = store.list_hashes()?;

    let mut report = NodeFormatReport {
        total_nodes: hashes.len(),
        ..Default::default()
    };

    for hash in hashes {
        // Decoding this node's own payload is what classifies it; a node whose *children* are
        // legacy is counted when the scan reaches those children by their own hashes.
        let decoded = MerkleNodeDB::open_read_only(store.clone(), &hash).and_then(|db| db.node());
        match decoded {
            Ok(_) => {}
            Err(MerkleDbError::PreV025Node { dtype, .. }) => {
                *report.pre_v025.entry(dtype).or_default() += 1;
            }
            Err(err) => {
                log::warn!("Node {hash} in {:?} did not decode: {err}", repo.path);
                report.undecodable += 1;
            }
        }
    }

    Ok(report)
}

/// Result of rebuilding a commit's `dir_hash_db`.
#[derive(Debug, Clone, Serialize)]
pub struct RebuildDirHashesStats {
    /// The commit whose `dir_hash_db` was rebuilt.
    pub commit_id: String,
    /// Number of directory entries written into the fresh `dir_hash_db`.
    pub dirs_written: usize,
}

/// Rebuild a commit's `dir_hash_db` from its merkle tree.
///
/// Walks every `Directory` node reachable from the commit's root and writes `(repo-relative
/// path, dir hash)` into a fresh RocksDB, replacing any existing `dir_hash_db` for the commit.
/// The root directory is keyed with an empty path, matching the convention used by the commit
/// writer.
///
/// Strategy: build the new database in a sibling temp directory, then hand off to
/// [`with_exclusive_access`] for the swap. That helper takes the per-repo write barrier —
/// waiting for in-flight readers to drop their `Arc<DB>`, which is what actually closes the
/// RocksDB and releases the OS file handles Windows needs before rename — runs our rename
/// dance, and releases the barrier.
pub fn rebuild_dir_hash_db(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<RebuildDirHashesStats, OxenError> {
    log::info!(
        "rebuild_dir_hash_db: repo={:?} commit={}",
        repo.path,
        commit.id
    );

    let root = repositories::tree::get_root_with_children(repo, commit)?
        .ok_or_else(|| OxenError::RevisionNotFound(commit.id.clone().into()))?;

    let pairs = collect_dir_hashes(&root);

    let db_path = dir_hash_db_path_from_commit_id(repo, &commit.id);
    // Suffix by commit id: each commit has its own temp paths. Concurrent rebuilds of the
    // same commit will fail at RocksDB open (LOCK contention on the shared temp path); that's
    // acceptable — this is a rare admin operation. A *previous* rebuild of this same commit
    // may have crashed partway through and left one of these paths behind, so clean up any
    // leftovers before we start; otherwise the RocksDB open below would reuse stale data,
    // or the later rename would step on a non-empty target.
    let new_path = db_path.with_file_name(format!("dir_hashes.new.{}", commit.id));
    let old_path = db_path.with_file_name(format!("dir_hashes.old.{}", commit.id));
    for path in [&new_path, &old_path] {
        if path.exists() {
            util::fs::remove_dir_all(path)?;
        }
    }

    // 1. Write every (path, hash) into a fresh db at the temp location. Done outside the
    //    exclusive-access block so we don't hold the slot lock while doing RocksDB writes.
    let mut successful_writes: usize = 0;
    {
        let opts = db::key_val::opts::default();
        let new_db: DBWithThreadMode<SingleThreaded> =
            DBWithThreadMode::open(&opts, dunce::simplified(&new_path))?;
        for (path, hash) in &pairs {
            let Some(path_str) = path.to_str() else {
                log::error!("Skipping non-UTF-8 path during rebuild: {path:?}");
                continue;
            };
            str_val_db::put(&new_db, path_str, &hash.to_string())?;
            successful_writes += 1;
        }
        // Drop the handle before renaming so RocksDB releases file locks.
    }

    // 2. Swap under the per-repo write barrier. In-flight readers finish and drop their
    //    `Arc<DB>` before we enter the closure, so the RocksDB handle is already closed and
    //    Windows will permit the renames; the next reader after the closure exits reopens.
    let swap_db_path = db_path.clone();
    with_exclusive_access(repo, move || {
        let had_existing = swap_db_path.exists();
        if had_existing {
            util::fs::rename(&swap_db_path, &old_path)?;
        }
        util::fs::rename(&new_path, &swap_db_path)?;

        if had_existing
            && old_path.exists()
            && let Err(err) = util::fs::remove_dir_all(&old_path)
        {
            log::warn!(
                "rebuild_dir_hash_db: could not remove previous dir_hashes at \
                 {old_path:?}: {err}"
            );
        }
        Ok(())
    })?;

    Ok(RebuildDirHashesStats {
        commit_id: commit.id.clone(),
        dirs_written: successful_writes,
    })
}

/// Walk the tree and collect `(repo-relative path, dir hash)` for every `Directory` node. The
/// root directory is recorded with an empty path.
fn collect_dir_hashes(root: &MerkleTreeNode) -> Vec<(PathBuf, MerkleHash)> {
    let mut out = Vec::new();
    let mut stack: Vec<(&MerkleTreeNode, PathBuf)> = vec![(root, PathBuf::new())];
    while let Some((node, path)) = stack.pop() {
        if matches!(&node.node, EMerkleTreeNode::Directory(_)) {
            out.push((path.clone(), node.hash));
        }
        for child in &node.children {
            let next_path = if let EMerkleTreeNode::Directory(dir) = &child.node {
                path.join(dir.name())
            } else {
                path.clone()
            };
            stack.push((child, next_path));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::repositories;
    use crate::storage::version_store::VersionLocation;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_fsck_dry_run_detects_corrupted_version() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Add and commit a file so we have a version in the store
            let file_path = repo.path.join("hello.txt");
            test::write_txt_file_to_path(&file_path, "hello world")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding hello.txt")?;

            let version_store = repo.version_store();
            let versions = version_store.list_versions().await?;
            assert!(!versions.is_empty());

            // Corrupt a version file by overwriting its data.
            // This test relies on writing directly to the version store's
            // on-disk path, which only works with LocalVersionStore.
            let hash = &versions[0];
            let VersionLocation::Local(path) = version_store.version_location(hash).await? else {
                panic!("Expected a local version store (Local path). This test only works with local storage.");
            };
            std::fs::write(&path, b"corrupted data")?;

            // Dry run should detect corruption but not delete
            let result = version_store.clean_corrupted_versions(true).await?;
            assert!(result.corrupted > 0);
            assert_eq!(result.cleaned, 0);

            // The corrupted file should still exist
            assert!(version_store.version_exists(hash).await?);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_fsck_clean_removes_corrupted_version() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Add and commit a file so we have a version in the store
            let file_path = repo.path.join("hello.txt");
            test::write_txt_file_to_path(&file_path, "hello world")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding hello.txt")?;

            let version_store = repo.version_store();
            let versions = version_store.list_versions().await?;
            assert!(!versions.is_empty());

            // Corrupt a version file by overwriting its data.
            // This test relies on writing directly to the version store's
            // on-disk path, which only works with LocalVersionStore.
            let hash = &versions[0];
            let VersionLocation::Local(path) = version_store.version_location(hash).await? else {
                panic!("Expected a local version store (Local path). This test only works with local storage.");
            };
            std::fs::write(&path, b"corrupted data")?;

            // Clean should detect and remove the corrupted file
            let result = version_store.clean_corrupted_versions(false).await?;
            assert!(result.corrupted > 0);
            assert!(result.cleaned > 0);

            // The corrupted file should be gone
            assert!(!version_store.version_exists(hash).await?);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_fsck_no_corruption_on_clean_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Add and commit a file
            let file_path = repo.path.join("hello.txt");
            test::write_txt_file_to_path(&file_path, "hello world")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding hello.txt")?;

            let version_store = repo.version_store();

            // No corruption on a clean repo
            let result = version_store.clean_corrupted_versions(true).await?;
            assert_eq!(result.corrupted, 0);
            assert!(result.scanned > 0);

            Ok(())
        })
        .await
    }

    /// Regression test for the dir_hash_db ↔ merkle-tree desync bug: when a directory entry is
    /// missing from `dir_hash_db` but still present in the tree, path-based lookups miss. The
    /// rebuild must restore the entry from the tree.
    #[tokio::test]
    async fn test_rebuild_dir_hash_db_restores_missing_entry() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create nested directories with at least one file so each dir shows up in the tree.
            let parent_dir = repo.path.join("features").join("fbimg");
            let child_dir = parent_dir.join("dinov3_vits16");
            util::fs::create_dir_all(&child_dir)?;
            let file = child_dir.join("note.txt");
            test::write_txt_file_to_path(&file, "hello")?;

            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "seed nested dirs")?;

            let child_rel = PathBuf::from("features/fbimg/dinov3_vits16");

            // Sanity: path-based lookup works on a healthy db.
            let ok = repositories::tree::get_dir_with_children(&repo, &commit, &child_rel, None)?;
            assert!(
                ok.is_some(),
                "expected dir_with_children to find {child_rel:?} on a healthy repo"
            );

            // Simulate a corrupted dir_hash_db by evicting the cached handle and removing the
            // on-disk directory entirely. This is a stronger corruption than production (where
            // individual entries go missing) but exercises the same recovery path: the rebuild
            // walks the merkle tree and writes a fresh dir_hash_db in place. We use this coarser
            // corruption because removing a single entry requires opening a separate RW
            // RocksDB on the same path — behavior that isn't reliably reflected in subsequent
            // reads on Windows, even after evicting the cached handle.
            let db_path =
                crate::core::db::dir_hashes::dir_hashes_db::dir_hash_db_path_from_commit_id(
                    &repo, &commit.id,
                );
            crate::core::db::dir_hashes::dir_hashes_db::remove_from_cache_with_children(&db_path)?;
            util::fs::remove_dir_all(&db_path)?;

            // With the db gone, path-based lookup now errors.
            let broken =
                repositories::tree::get_dir_with_children(&repo, &commit, &child_rel, None);
            assert!(
                matches!(broken, Err(OxenError::PathDoesNotExist(_))),
                "expected PathDoesNotExist after dir_hash_db was removed, got {broken:?}"
            );

            // Rebuild from the merkle tree.
            let stats = rebuild_dir_hash_db(&repo, &commit)?;
            assert_eq!(stats.commit_id, commit.id);
            assert!(
                stats.dirs_written >= 3,
                "expected at least root + features + fbimg + dinov3_vits16 entries, got {}",
                stats.dirs_written
            );

            // The path-based lookup works again.
            let repaired =
                repositories::tree::get_dir_with_children(&repo, &commit, &child_rel, None)?;
            assert!(
                repaired.is_some(),
                "expected dir_with_children to find {child_rel:?} after rebuild"
            );

            Ok(())
        })
        .await
    }

    /// The pre-0.25 shape of a vnode: the same data with no enum envelope and no `num_entries`.
    #[derive(serde::Serialize)]
    struct LegacyVNodeData {
        hash: MerkleHash,
        node_type: MerkleTreeNodeType,
    }

    #[tokio::test]
    async fn test_scan_node_format_finds_planted_pre_v0_25_node() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let clean = scan_node_format(&repo)?;
            assert!(
                !clean.is_affected(),
                "a freshly written repo holds no pre-0.25 nodes, got {clean:?}"
            );
            assert!(clean.total_nodes > 0, "fixture repo should have nodes");
            assert_eq!(clean.undecodable, 0);

            // Rewrite one vnode into the pre-0.25 shape, in place.
            let nodes_dir = util::fs::oxen_hidden_dir(&repo.path)
                .join(crate::constants::TREE_DIR)
                .join(crate::constants::NODES_DIR);
            let mut planted = false;
            for prefix in util::fs::list_dirs_in_dir(&nodes_dir)? {
                for node_dir in util::fs::list_dirs_in_dir(&prefix)? {
                    let node_file = node_dir.join("node");
                    let blob = std::fs::read(&node_file)?;
                    // [dtype u8][parent_id u128 LE][data_len u32 LE][payload][child entries...]
                    if blob.first() != Some(&MerkleTreeNodeType::VNode.to_u8()) {
                        continue;
                    }
                    let data_len =
                        u32::from_le_bytes(blob[17..21].try_into().expect("4 bytes")) as usize;
                    let vnode = crate::model::merkle_tree::node::VNode::deserialize(
                        &blob[21..21 + data_len],
                    )
                    .expect("fixture vnode should decode before rewriting");
                    let legacy = rmp_serde::to_vec(&LegacyVNodeData {
                        hash: *vnode.hash(),
                        node_type: MerkleTreeNodeType::VNode,
                    })
                    .expect("legacy vnode should serialize");

                    let mut rewritten = blob[..17].to_vec();
                    rewritten.extend_from_slice(&(legacy.len() as u32).to_le_bytes());
                    rewritten.extend_from_slice(&legacy);
                    rewritten.extend_from_slice(&blob[21 + data_len..]);
                    std::fs::write(&node_file, rewritten)?;
                    planted = true;
                    break;
                }
                if planted {
                    break;
                }
            }
            assert!(planted, "fixture repo should contain a vnode to rewrite");

            let scanned = scan_node_format(&repo)?;
            assert_eq!(
                scanned.pre_v025.get(&MerkleTreeNodeType::VNode).copied(),
                Some(1),
                "the planted vnode should be counted against its own type, got {scanned:?}"
            );
            assert_eq!(scanned.pre_v025_total(), 1);
            assert_eq!(
                scanned.total_nodes, clean.total_nodes,
                "node count is stable"
            );
            assert_eq!(
                scanned.undecodable, 0,
                "a retired format is not the same as an undecodable node"
            );

            Ok(())
        })
        .await
    }
}
