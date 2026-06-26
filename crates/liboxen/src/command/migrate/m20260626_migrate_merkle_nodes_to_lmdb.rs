//! Transcode the Merkle node store between the filesystem and LMDB backends.
//!
//! This is an opt-in, reversible migration (run with `--run-optional`):
//!   - **up** copies an FS-backed repo's nodes into an LMDB env and publishes it, *keeping* the
//!     filesystem node tree as a backup.
//!   - **down** copies an LMDB-backed repo's nodes back into the FS node tree, then removes the env.
//!
//! After `up`, `create_merkle_node_store` resolves the repo to the LMDB backend (it prefers an
//! existing LMDB env over the FS tree), so the repo runs on LMDB from then on while the source
//! directory stays untouched. The node blobs themselves are identical across backends — only their
//! storage location changes.
//!
//! Neither direction cleans up on error: a failed run leaves its partial artifacts on disk for
//! inspection rather than rolling them back. Because the source backend always remains the repo's
//! resolved backend until the destination is fully built and verified, a crash never strands the
//! repo, and re-running completes the move.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::{Direction, Migrate};
use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::db::merkle_node::MerkleNodeStore;
use crate::core::db::merkle_node::fs_merkle_node_store::FsMerkleNodeStore;
use crate::core::db::merkle_node::lmdb_merkle_node_store::LmdbMerkleNodeStore;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash};
use crate::util;

pub struct MerkleNodesToLmdbMigration;

impl Migrate for MerkleNodesToLmdbMigration {
    fn name(&self) -> &'static str {
        "merkle_nodes_to_lmdb"
    }

    fn description(&self) -> &'static str {
        "Transcode the Merkle node store between backends (up: filesystem → LMDB, down: LMDB → filesystem)"
    }

    fn is_needed(&self, _repo: &LocalRepository) -> Result<bool, OxenError> {
        // Opt-in: a repo works on either backend, so this is never required — run it explicitly
        // with `--run-optional`.
        Ok(false)
    }

    fn is_applicable(
        &self,
        direction: Direction,
        repo: &LocalRepository,
    ) -> Result<bool, OxenError> {
        let on_lmdb = LmdbMerkleNodeStore::exists_on_disk(&repo.path);
        Ok(match direction {
            // Can move to LMDB only if the repo isn't already on it.
            Direction::Up => !on_lmdb,
            // Can move back to the filesystem only if the repo is currently on LMDB.
            Direction::Down => on_lmdb,
        })
    }

    fn up(&self, repo: LocalRepository) -> Result<(), OxenError> {
        migrate_fs_to_lmdb(&repo)
    }

    fn down(&self, repo: LocalRepository) -> Result<(), OxenError> {
        migrate_lmdb_to_fs(repo)
    }
}

/// The filesystem-backend node tree (`.oxen/tree/nodes`).
fn fs_nodes_dir(repo_path: &Path) -> PathBuf {
    repo_path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR)
}

/// FS → LMDB. Build the LMDB env in a temp sibling directory, populate and verify it, then
/// atomically rename it into place. The filesystem node tree is kept as a backup.
///
/// Building in a temp dir is what makes a crash safe: `create_merkle_node_store` prefers an existing
/// LMDB env over the FS tree, so a half-populated env at the final path would be selected and
/// strand the repo. Until the rename, the complete FS tree remains the repo's backend.
///
/// Nothing is cleaned up on error. A leftover temp env from a previous failed run is reported rather
/// than deleted, and a failure mid-build leaves the partial temp env in place for inspection.
fn migrate_fs_to_lmdb(repo: &LocalRepository) -> Result<(), OxenError> {
    let final_dir = LmdbMerkleNodeStore::env_dir(&repo.path);
    let temp_dir = final_dir.with_extension("building");

    // Don't silently delete a leftover temp env from a previous failed run — this migration never
    // cleans up on error, so its presence means a prior attempt left state worth inspecting.
    if temp_dir.exists() {
        return Err(OxenError::basic_str(format!(
            "Found a partial LMDB env from a previous migration attempt at {}. \
             Inspect and remove it manually before retrying.",
            temp_dir.display()
        )));
    }

    let fs = FsMerkleNodeStore::new(&repo.path);
    let expected: HashSet<MerkleHash> = fs.list_hashes()?.into_iter().collect();

    // Populate the env in the temp dir. On any error we return without removing temp_dir, leaving
    // the partial env in place for inspection.
    {
        let lmdb = LmdbMerkleNodeStore::new_at(&temp_dir)?;
        for hash in &expected {
            let node = fs.read_node(hash)?;
            let children = fs.read_children(hash)?;
            lmdb.write_node(hash, node, children)?;
        }
        verify_migrated(&lmdb.list_hashes()?, &expected, "FS→LMDB")?;
    } // drop the temp env so its directory can be renamed

    // Atomically publish the populated env. Until this rename, the complete FS node tree remains the
    // repo's backend, so a crash before it never strands the repo on a half-built env.
    util::fs::rename(&temp_dir, &final_dir)?;

    // The FS node tree is intentionally kept as a backup. `create_merkle_node_store` prefers the
    // LMDB env, so the repo runs on LMDB from now on while the source directory stays untouched.
    log::info!(
        "Migrated {} merkle nodes from the filesystem backend to LMDB (filesystem node tree kept at {})",
        expected.len(),
        fs_nodes_dir(&repo.path).display()
    );
    Ok(())
}

/// LMDB → FS. Copy every node into the FS backend and verify before removing the LMDB env.
///
/// The LMDB env stays the repo's backend (and source of truth) until the very end, so a crash
/// mid-migration leaves it intact and `create_merkle_node_store` keeps preferring it; re-running
/// completes the move idempotently. Nothing is cleaned up on error.
fn migrate_lmdb_to_fs(repo: LocalRepository) -> Result<(), OxenError> {
    // Release the repo's own (LMDB-resolved) store handle up front so we control the env's lifetime
    // and can remove its directory at the end on every platform (an open env can't be removed on
    // Windows).
    let repo_path = repo.path.clone();
    drop(repo);

    let lmdb_dir = LmdbMerkleNodeStore::env_dir(&repo_path);
    let fs = FsMerkleNodeStore::new(&repo_path);

    let expected: HashSet<MerkleHash> = {
        let lmdb = LmdbMerkleNodeStore::new(&repo_path)?;
        let hashes = lmdb.list_hashes()?;
        for hash in &hashes {
            let node = lmdb.read_node(hash)?;
            let children = lmdb.read_children(hash)?;
            fs.write_node(hash, node, children)?;
        }
        hashes.into_iter().collect()
    }; // drop the lmdb env so its directory can be removed

    verify_migrated(&fs.list_hashes()?, &expected, "LMDB→FS")?;

    // Only now remove the env, so `create_merkle_node_store` falls back to the filesystem backend.
    if lmdb_dir.exists() {
        util::fs::remove_dir_all(&lmdb_dir)?;
    }
    log::info!(
        "Migrated {} merkle nodes from LMDB to the filesystem backend",
        expected.len()
    );
    Ok(())
}

/// Error out if the destination backend doesn't hold exactly the expected set of node hashes,
/// before any destructive step (rename / delete) relies on the copy being complete.
fn verify_migrated(
    migrated: &[MerkleHash],
    expected: &HashSet<MerkleHash>,
    label: &str,
) -> Result<(), OxenError> {
    let migrated: HashSet<MerkleHash> = migrated.iter().copied().collect();
    if &migrated != expected {
        return Err(OxenError::basic_str(format!(
            "{label} merkle node migration verification failed: destination holds {} of {} nodes",
            migrated.len(),
            expected.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::model::MerkleHash;
    use crate::repositories;
    use crate::test;

    /// A committed FS-backed repo migrates up to LMDB with the source kept: the LMDB env holds every
    /// node, the filesystem node tree is still present, and the tree still reads back. Then it
    /// migrates back down to the filesystem — with the same node set throughout.
    #[tokio::test]
    async fn test_merkle_nodes_migrate_fs_to_lmdb_keeps_source_and_back() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Commit a file so the repo has filesystem-backed merkle nodes.
            let file = repo.path.join("a.txt");
            util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "first")?;

            let fs_hashes: HashSet<MerkleHash> = FsMerkleNodeStore::new(&repo.path)
                .list_hashes()?
                .into_iter()
                .collect();
            assert!(
                !fs_hashes.is_empty(),
                "repo should have FS nodes to migrate"
            );

            let migration = MerkleNodesToLmdbMigration;
            assert!(migration.is_applicable(Direction::Up, &repo)?);
            assert!(!migration.is_applicable(Direction::Down, &repo)?);

            // --- up: FS → LMDB (source kept) ---
            migration.up(repo.clone())?;

            assert!(LmdbMerkleNodeStore::exists_on_disk(&repo.path));
            assert!(
                fs_nodes_dir(&repo.path).exists(),
                "FS node tree should be kept after up"
            );
            {
                let lmdb = LmdbMerkleNodeStore::new(&repo.path)?;
                let lmdb_hashes: HashSet<MerkleHash> = lmdb.list_hashes()?.into_iter().collect();
                assert_eq!(
                    lmdb_hashes, fs_hashes,
                    "LMDB should hold every migrated node"
                );
            }

            // Reloading the repo resolves to LMDB (preferred over the kept FS tree) and the tree
            // still reads back.
            {
                let reloaded = LocalRepository::from_dir(&repo.path)?;
                let root = repositories::tree::get_root_with_children(&reloaded, &commit)?
                    .expect("root readable through lmdb after migration");
                assert!(!root.children.is_empty());
            }

            // --- down: LMDB → FS ---
            let to_down = LocalRepository::from_dir(&repo.path)?;
            assert!(migration.is_applicable(Direction::Down, &to_down)?);
            assert!(!migration.is_applicable(Direction::Up, &to_down)?);
            migration.down(to_down)?;

            assert!(
                !LmdbMerkleNodeStore::exists_on_disk(&repo.path),
                "LMDB env should be removed after down"
            );
            let restored: HashSet<MerkleHash> = FsMerkleNodeStore::new(&repo.path)
                .list_hashes()?
                .into_iter()
                .collect();
            assert_eq!(
                restored, fs_hashes,
                "FS backend should be restored after down"
            );

            Ok(())
        })
        .await
    }

    /// A leftover temp env from a previous failed run is reported, not deleted: `up` errors and the
    /// temp dir is left in place for inspection.
    #[tokio::test]
    async fn test_up_reports_leftover_temp_env_without_cleanup() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("a.txt");
            util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "first")?;

            // Simulate a partial env left behind by a previous aborted run.
            let temp_dir = LmdbMerkleNodeStore::env_dir(&repo.path).with_extension("building");
            LmdbMerkleNodeStore::new_at(&temp_dir)?;

            let result = MerkleNodesToLmdbMigration.up(repo.clone());
            assert!(
                result.is_err(),
                "up should refuse to run over a leftover temp env"
            );
            assert!(
                temp_dir.exists(),
                "leftover temp env should be kept for inspection, not cleaned up"
            );
            assert!(
                !LmdbMerkleNodeStore::exists_on_disk(&repo.path),
                "no env should be published when up bails on a leftover temp env"
            );

            Ok(())
        })
        .await
    }
}
