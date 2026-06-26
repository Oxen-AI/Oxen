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
use crate::config::RepositoryConfig;
use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::db::merkle_node::fs_merkle_node_store::FsMerkleNodeStore;
use crate::core::db::merkle_node::lmdb_merkle_node_store::LmdbMerkleNodeStore;
use crate::core::db::merkle_node::{MerkleNodeBackend, MerkleNodeStore};
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
        // Gate on the repo's resolved backend (config first), not raw on-disk evidence: `up` keeps
        // the FS tree as a backup and `down` may leave an orphan env on a crash, so disk presence
        // alone doesn't tell us which backend the repo is actually using.
        let on_lmdb = repo.merkle_node_backend() == MerkleNodeBackend::Lmdb;
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

/// FS → LMDB. Build the LMDB env in a temp sibling directory, populate and verify it, atomically
/// rename it into place, then record LMDB as the repo's backend in `config.toml`. The filesystem
/// node tree is kept as a backup.
///
/// `config.toml` is the authoritative backend record, so the repo only actually switches to LMDB on
/// the config write at the end. Until then it still resolves to the complete, kept FS tree — so a
/// crash before the config write leaves the repo safely on the filesystem, and re-running finishes
/// the switch via the already-published-env fast path below.
///
/// Building in a temp dir keeps the env at its final path complete-or-absent (an atomic rename
/// publishes it), so config and on-disk evidence never disagree in a way that strands the repo.
///
/// Nothing is cleaned up on error. A leftover temp env from a previous failed run is reported rather
/// than deleted, and a failure mid-build leaves the partial temp env in place for inspection.
fn migrate_fs_to_lmdb(repo: &LocalRepository) -> Result<(), OxenError> {
    let final_dir = LmdbMerkleNodeStore::env_dir(&repo.path);

    // Idempotent fast path: if the env is already published (e.g. a previous run crashed after the
    // rename but before the config write), the data move is done — just make config authoritative.
    if LmdbMerkleNodeStore::exists_on_disk(&repo.path) {
        set_config_backend(&repo.path, MerkleNodeBackend::Lmdb)?;
        return Ok(());
    }

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

    // Switch the repo to LMDB. config is authoritative, so this is the step that actually takes
    // effect; the FS node tree is intentionally kept as a backup.
    set_config_backend(&repo.path, MerkleNodeBackend::Lmdb)?;

    log::info!(
        "Migrated {} merkle nodes from the filesystem backend to LMDB (filesystem node tree kept at {})",
        expected.len(),
        fs_nodes_dir(&repo.path).display()
    );
    Ok(())
}

/// LMDB → FS. Copy every node into the FS backend, verify, record Filesystem as the backend in
/// `config.toml`, then remove the LMDB env.
///
/// The config write happens *before* removing the env, and that ordering is load-bearing: config is
/// authoritative, so writing Filesystem first means a crash after it leaves config=Filesystem with
/// the FS tree already verified-complete (the orphan env is harmless and ignored). Removing the env
/// first would risk a crash leaving config=Lmdb with no env, which the next load would resolve by
/// creating a fresh *empty* env. Until the config write, the repo stays resolved to LMDB, so a crash
/// before it leaves the env intact and re-running completes the move. Nothing is cleaned up on error.
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

    // Switch the repo back to the filesystem backend before removing the env (see the ordering note
    // above). config is authoritative, so this is the step that actually takes effect.
    set_config_backend(&repo_path, MerkleNodeBackend::Filesystem)?;

    if lmdb_dir.exists() {
        util::fs::remove_dir_all(&lmdb_dir)?;
    }
    log::info!(
        "Migrated {} merkle nodes from LMDB to the filesystem backend",
        expected.len()
    );
    Ok(())
}

/// Persist the repo's Merkle node backend to `config.toml`, leaving the rest of the config intact.
/// This is the authoritative record `create_merkle_node_store` resolves from, so each direction
/// writes it as the step that actually switches the backend.
fn set_config_backend(repo_path: &Path, backend: MerkleNodeBackend) -> Result<(), OxenError> {
    let config_path = util::fs::config_filepath(repo_path);
    let mut config = RepositoryConfig::from_file(&config_path)?;
    config.merkle_node_backend = Some(backend);
    config.save(&config_path)?;
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

            // `up` recorded LMDB in config, so a reloaded repo actually resolves to the LMDB
            // backend (not just "the env exists on disk") and the tree reads back through it.
            {
                let reloaded = LocalRepository::from_dir(&repo.path)?;
                assert_eq!(
                    reloaded.merkle_node_backend(),
                    MerkleNodeBackend::Lmdb,
                    "config must make the repo resolve to LMDB after up"
                );
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
            // config records Filesystem again, so a reloaded repo resolves back to the FS backend.
            assert_eq!(
                LocalRepository::from_dir(&repo.path)?.merkle_node_backend(),
                MerkleNodeBackend::Filesystem,
                "config must make the repo resolve to Filesystem after down"
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
