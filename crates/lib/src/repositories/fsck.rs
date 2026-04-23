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
use std::path::PathBuf;

use crate::core::db;
use crate::core::db::dir_hashes::dir_hashes_db::{
    dir_hash_db_path_from_commit_id, with_entry_evicted,
};
use crate::core::db::key_val::str_val_db;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Commit, LocalRepository, MerkleHash};
use crate::repositories;
use crate::util;

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
/// [`with_entry_evicted`] for the swap. That helper takes the per-path cache slot
/// for writing (waiting for in-flight readers to drop their guards), drops the cached RocksDB
/// handle — which on Windows is required before the directory can be renamed — runs our
/// rename dance, and releases the lock.
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
        }
        // Drop the handle before renaming so RocksDB releases file locks.
    }

    // 2. Swap under the slot's write lock. The cached RocksDB handle is closed inside this
    //    helper so Windows will permit the renames; the helper also reopens afterwards.
    let swap_db_path = db_path.clone();
    with_entry_evicted(repo, &commit.id, move || {
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
        dirs_written: pairs.len(),
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

    use rocksdb::{DBWithThreadMode, SingleThreaded};
    use std::path::PathBuf;

    use crate::core::db;
    use crate::core::db::key_val::str_val_db;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::storage::version_store::LocalFilePath;
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

            let version_store = repo.version_store()?;
            let versions = version_store.list_versions().await?;
            assert!(!versions.is_empty());

            // Corrupt a version file by overwriting its data.
            // This test relies on writing directly to the version store's
            // on-disk path, which only works with LocalVersionStore.
            let hash = &versions[0];
            let version_path = version_store.get_version_path(hash).await?;
            let LocalFilePath::Stable(ref path) = version_path else {
                panic!("Expected LocalVersionStore (Stable path), got a Temp path. This test only works with local storage.");
            };
            std::fs::write(path, b"corrupted data")?;

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

            let version_store = repo.version_store()?;
            let versions = version_store.list_versions().await?;
            assert!(!versions.is_empty());

            // Corrupt a version file by overwriting its data.
            // This test relies on writing directly to the version store's
            // on-disk path, which only works with LocalVersionStore.
            let hash = &versions[0];
            let version_path = version_store.get_version_path(hash).await?;
            let LocalFilePath::Stable(ref path) = version_path else {
                panic!("Expected LocalVersionStore (Stable path), got a Temp path. This test only works with local storage.");
            };
            std::fs::write(path, b"corrupted data")?;

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

            let version_store = repo.version_store()?;

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

            // Corrupt the dir_hash_db by removing the entry directly.
            let db_path =
                crate::core::db::dir_hashes::dir_hashes_db::dir_hash_db_path_from_commit_id(
                    &repo, &commit.id,
                );
            {
                let opts = db::key_val::opts::default();
                let db: DBWithThreadMode<SingleThreaded> =
                    DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
                str_val_db::delete(&db, child_rel.to_str().unwrap())?;
            }
            // Evict the LRU so the next read reopens from disk.
            crate::core::db::dir_hashes::dir_hashes_db::remove_from_cache_with_children(&db_path)?;

            // Now the lookup should miss, reproducing the production symptom.
            let broken =
                repositories::tree::get_dir_with_children(&repo, &commit, &child_rel, None)?;
            assert!(
                broken.is_none(),
                "expected dir_with_children to miss after dir_hash_db entry was removed"
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
}
