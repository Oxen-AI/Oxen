use std::path::{Path, PathBuf};

use heed::EnvOpenOptions;

use super::{Direction, Migrate};

use crate::config::RepositoryConfig;
use crate::config::repository_config::MerkleStoreKind;
use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::db::merkle_node::LmdbBackend;
use crate::core::db::merkle_node::file_backend::FileBackend;
use crate::core::db::merkle_node::lmdb::lmdb_dir_location;
use crate::error::OxenError;
use crate::model::merkle_tree::MerkleReader;
use crate::model::merkle_tree::merkle_writer::{MerkleWriteSession, MerkleWriter};
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::{LocalRepository, MerkleHash};
use crate::util::progress_bar::{ProgressBarType, oxen_progress_bar};
use crate::{repositories, util};

/// Map-size hint for the destination LMDB env when migrating into LMDB. LMDB
/// only consumes what's actually written (the file is sparse), so this is a
/// ceiling rather than an allocation. 64 GiB comfortably accommodates the
/// largest Merkle trees we've seen in practice; users can grow it later by
/// re-opening the env with a larger map.
const LMDB_MIGRATION_MAP_SIZE_BYTES: usize = 64 * 1024 * 1024 * 1024;

/// Transcodes the repository's Merkle tree node store between the file-based backend
/// ([`FileBackend`]) and the LMDB backend ([`LmdbBackend`]).
///
/// - `up`: file → LMDB. Reads every node out of `.oxen/tree/nodes/...`, writes it into
///   a freshly-opened LMDB env at `.oxen/lmdb_merkle_tree_store/`, flips
///   `RepositoryConfig::merkle_store_kind` from `File` to `Lmdb`, then removes the
///   old tree node directory.
/// - `down`: LMDB → file. The mirror image: reads from LMDB, writes the file-based
///   layout, flips the config back to `File`, and removes the LMDB env directory.
///
/// Both directions are idempotent: re-running on an already-migrated repo is a no-op
/// guarded by the per-repo helpers' explicit kind check.
pub struct SwitchMerkleStoreToLmdbMigration;

impl Migrate for SwitchMerkleStoreToLmdbMigration {
    fn name(&self) -> &'static str {
        "switch_merkle_store_to_lmdb"
    }

    fn description(&self) -> &'static str {
        "Transcode the repository's Merkle tree node store from the file-based \
         backend to LMDB. `down` reverts to the file-based backend."
    }

    /// Migrate from [`FileBackend`] to [`LmdbBackend`].
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            let mr = run_up_on_all_repos(path)?;
            print_status_all(&mr)
        } else {
            run_up_on_one_repo_at(path)
        }
    }

    /// Migrate from [`LmdbBackend`] to [`FileBackend`].
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            let mr = run_down_on_all_repos(path)?;
            print_status_all(&mr)
        } else {
            run_down_on_one_repo_at(path)
        }
    }

    /// This migration is optional — it is never auto-required. Use
    /// `oxen migrate up/down switch_merkle_store_to_lmdb --run-optional` to invoke it
    /// explicitly; the CLI consults [`Self::is_applicable`] for that path.
    fn is_needed(&self, _: &LocalRepository) -> Result<bool, OxenError> {
        Ok(false)
    }

    /// Up is applicable iff the repo is on the file backend (so we have something to
    /// transcode forward); down is applicable iff the repo is on LMDB (so we have
    /// something to transcode back).
    fn is_applicable(
        &self,
        direction: Direction,
        repo: &LocalRepository,
    ) -> Result<bool, OxenError> {
        let kind = repo.merkle_store_kind();
        Ok(match direction {
            Direction::Up => kind == MerkleStoreKind::File,
            Direction::Down => kind == MerkleStoreKind::Lmdb,
        })
    }
}

fn print_status_all(migration_result: &MigrateResult) -> Result<(), OxenError> {
    let MigrateResult {
        failures,
        successes,
        ignored,
    } = migration_result;

    let extra_store: String;
    let extra = if !ignored.is_empty() {
        extra_store = format!(" (ignored {})", ignored.len());
        &extra_store
    } else {
        ""
    };

    if !failures.is_empty() {
        println!(
            "\u{274C} Migration completed with {} success(es) and {} failure(s){extra}:",
            successes.len(),
            failures.len(),
        );
        for (repo_path, err) in failures {
            println!("  • {:?}: {}", repo_path, err);
        }
        Err(OxenError::MigrationFail)
    } else {
        println!(
            "\u{2705} Migration completed: successfully migrated {} repositories{extra}",
            successes.len(),
        );
        Ok(())
    }
}

/// Driver for a single repo at the given path. Loads the repo, calls
/// [`run_up_on_one_repo`], then drops the repo (closing any held merkle store
/// handles) and finally deletes the now-empty file-based tree directory.
fn run_up_on_one_repo_at(path: &Path) -> Result<(), OxenError> {
    let repo = LocalRepository::from_dir(path)?;
    let repo_path = repo.path.clone();
    let did_migrate = run_up_on_one_repo(&repo)?;
    drop(repo);
    if did_migrate {
        cleanup_old_file_tree_dir(&repo_path)?;
    }
    Ok(())
}

/// Driver for a single repo at the given path; mirrors [`run_up_on_one_repo_at`].
fn run_down_on_one_repo_at(path: &Path) -> Result<(), OxenError> {
    let repo = LocalRepository::from_dir(path)?;
    let repo_path = repo.path.clone();
    let did_migrate = run_down_on_one_repo(&repo)?;
    drop(repo);
    if did_migrate {
        cleanup_lmdb_env_dir(&repo_path)?;
    }
    Ok(())
}

/// Perform the file → LMDB transcode for a single repository. Returns `Ok(true)`
/// when a transcode actually happened (and the caller still needs to clean up
/// the old file-tree directory), or `Ok(false)` when the repo was already on
/// LMDB and this is a no-op.
///
/// Does **not** delete the old file-tree directory — that has to happen after the
/// caller has dropped `repo` so any held merkle store handles are gone. See
/// [`run_up_on_one_repo_at`] for the full lifecycle.
fn run_up_on_one_repo(repo: &LocalRepository) -> Result<bool, OxenError> {
    if !SwitchMerkleStoreToLmdbMigration.is_applicable(Direction::Up, repo)? {
        log::info!(
            "File → LMDB migration not applicable for repo {:?}",
            repo.path
        );
        println!(
            "Warning: skipping `switch_merkle_store_to_lmdb` up on {:?}: \
             repo is not on the file-based merkle store.",
            repo.path
        );
        return Ok(false);
    }

    log::info!(
        "Migrating Merkle store: file → LMDB for repo {:?}",
        repo.path
    );

    // heed requires the env directory to exist before `open` is called.
    let env_dir = lmdb_dir_location(&repo.path);
    util::fs::create_dir_all(&env_dir)?;

    {
        let mut options = EnvOpenOptions::new();
        options.map_size(LMDB_MIGRATION_MAP_SIZE_BYTES);
        let dest = LmdbBackend::new(repo.path.clone(), options)?;
        let source = repo.merkle_store()?;
        copy_all_nodes(repo, source, &dest)?;
    }

    let mut config = RepositoryConfig::from_repo(repo)?;
    config.merkle_store_kind = MerkleStoreKind::Lmdb;
    config.save(util::fs::config_filepath(&repo.path))?;

    Ok(true)
}

/// Perform the LMDB → file transcode for a single repository. See
/// [`run_up_on_one_repo`] for the lifecycle.
fn run_down_on_one_repo(repo: &LocalRepository) -> Result<bool, OxenError> {
    if !SwitchMerkleStoreToLmdbMigration.is_applicable(Direction::Down, repo)? {
        log::info!(
            "LMDB → File migration not applicable for repo {:?}",
            repo.path
        );
        println!(
            "Warning: skipping `switch_merkle_store_to_lmdb` down on {:?}: \
             repo is not on the LMDB merkle store.",
            repo.path
        );
        return Ok(false);
    }

    log::info!(
        "Migrating Merkle store: LMDB → File for repo {:?}",
        repo.path
    );

    {
        let dest = FileBackend::new(repo);
        let source = repo.merkle_store()?;
        copy_all_nodes(repo, source, &dest)?;
    }

    let mut config = RepositoryConfig::from_repo(repo)?;
    config.merkle_store_kind = MerkleStoreKind::File;
    config.save(util::fs::config_filepath(&repo.path))?;

    Ok(true)
}

/// Delete the old `.oxen/tree/nodes/` directory after a successful file → LMDB
/// migration. The directory is missing in normal post-migration state, so a
/// missing dir is not an error.
fn cleanup_old_file_tree_dir(repo_path: &Path) -> Result<(), OxenError> {
    let dir = repo_path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);
    if dir.exists() {
        util::fs::remove_dir_all(&dir)?;
    }
    Ok(())
}

/// Delete the LMDB env directory after a successful LMDB → file migration.
fn cleanup_lmdb_env_dir(repo_path: &Path) -> Result<(), OxenError> {
    let dir = lmdb_dir_location(repo_path);
    if dir.exists() {
        util::fs::remove_dir_all(&dir)?;
    }
    Ok(())
}

struct MigrateResult {
    failures: Vec<(PathBuf, OxenError)>,
    successes: Vec<PathBuf>,
    ignored: Vec<PathBuf>,
}

fn run_up_on_all_repos(path: &Path) -> Result<MigrateResult, OxenError> {
    println!("🐂 Collecting namespaces to migrate (file backend → LMDB)...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());

    let mut failures = vec![];
    let mut successes = vec![];
    let mut ignored = vec![];

    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        for repo in repositories::list_repos_in_namespace(&namespace_path) {
            let repo_path = util::fs::canonicalize(&repo.path)?;
            match run_up_on_one_repo(&repo) {
                Ok(did_migrate) => {
                    drop(repo);
                    if did_migrate {
                        if let Err(err) = cleanup_old_file_tree_dir(&repo_path) {
                            log::error!(
                                "Could not clean up old file-tree dir for repo {}\nErr: {}",
                                repo_path.display(),
                                err
                            );
                            failures.push((repo_path, err));
                        } else {
                            successes.push(repo_path);
                        }
                    } else {
                        ignored.push(repo_path);
                    }
                }
                Err(err) => {
                    log::error!(
                        "Could not migrate merkle store (file → LMDB) for repo {}\nErr: {}",
                        repo_path.display(),
                        err
                    );
                    failures.push((repo_path, err));
                }
            }
        }
        bar.inc(1);
    }
    Ok(MigrateResult {
        failures,
        successes,
        ignored,
    })
}

fn run_down_on_all_repos(path: &Path) -> Result<MigrateResult, OxenError> {
    println!("🐂 Collecting namespaces to migrate (LMDB → file)...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Reverting {} namespaces", namespaces.len());

    let mut failures = vec![];
    let mut successes = vec![];
    let mut ignored = vec![];

    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            let repo_path = util::fs::canonicalize(&repo.path)?;
            match run_down_on_one_repo(&repo) {
                Ok(did_migrate) => {
                    drop(repo);
                    if did_migrate {
                        if let Err(err) = cleanup_lmdb_env_dir(&repo_path) {
                            log::error!(
                                "Could not clean up LMDB env dir for repo {}\nErr: {}",
                                repo_path.display(),
                                err
                            );
                            failures.push((repo_path, err));
                        } else {
                            successes.push(repo_path);
                        }
                    } else {
                        ignored.push(repo_path);
                    }
                }
                Err(err) => {
                    log::error!(
                        "Could not migrate merkle store (LMDB → file) for repo {}\nErr: {}",
                        repo_path.display(),
                        err
                    );
                    failures.push((repo_path, err));
                }
            }
        }
        bar.inc(1);
    }
    Ok(MigrateResult {
        failures,
        successes,
        ignored,
    })
}

/// Transcode every Merkle tree node reachable from any of `repo`'s commits from
/// `source` to `dest`. Opens one [`MerkleWriteSession`] per commit subtree so each
/// LMDB write transaction stays bounded to a single commit's nodes — both for
/// memory and for clean restart granularity if the migration is interrupted.
fn copy_all_nodes(
    repo: &LocalRepository,
    source: &dyn MerkleReader,
    dest: &dyn MerkleWriter,
) -> Result<(), OxenError> {
    let commits = repositories::commits::list_all(repo)?;
    let bar = oxen_progress_bar(commits.len() as u64, ProgressBarType::Counter);
    for commit in commits {
        let commit_hash = commit.hash()?;
        let session = dest.begin()?;
        walk_subtree(source, &*session, &commit_hash)?;
        session.finish()?;
        bar.inc(1);
    }
    Ok(())
}

/// Recursively walk the subtree rooted at `node_hash` from `source` and re-write
/// each non-leaf node — plus its immediate children — into `session`.
///
/// `MerkleReader::get_node` returns `None` for `File` and `FileChunk` nodes by
/// trait contract; those leaves are persisted to the destination as `add_child`
/// calls on their parent's `NodeWriteSession`, so this function never recurses
/// into them.
///
/// The stored `parent_id` for the re-written node comes from `entry.parent_id`
/// (i.e., the value already stored in the source), not from our recursion path —
/// matching the established pattern in `m20250111083535_add_child_counts_to_nodes`.
fn walk_subtree(
    source: &dyn MerkleReader,
    session: &dyn MerkleWriteSession,
    node_hash: &MerkleHash,
) -> Result<(), OxenError> {
    let Some(entry) = source.get_node(node_hash)? else {
        return Ok(());
    };
    let children = source.get_children(node_hash)?;

    {
        let mut ns = session.create_node(entry.node.as_t_node(), entry.parent_id)?;
        for (_child_hash, child_node) in &children {
            ns.add_child(child_node.node.as_t_node())?;
        }
        ns.finish()?;
    }

    for (child_hash, child_node) in children {
        if matches!(
            child_node.node,
            EMerkleTreeNode::Commit(_) | EMerkleTreeNode::Directory(_) | EMerkleTreeNode::VNode(_)
        ) {
            walk_subtree(source, session, &child_hash)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use crate::config::repository_config::MerkleStoreKind;
    use crate::model::merkle_tree::MerkleReader;
    use crate::model::merkle_tree::node::EMerkleTreeNode;
    use crate::test;

    /// A deterministic snapshot of every reachable Merkle tree node in a repo,
    /// keyed by node hash. Two repos that observe the same set of nodes — same
    /// node bodies AND same `parent_id` AND same child-hash list — will produce
    /// equal snapshots. Used to assert round-trip integrity.
    type NodeSnapshot = BTreeMap<u128, NodeRecord>;

    #[derive(Debug, PartialEq, Eq)]
    struct NodeRecord {
        node: EMerkleTreeNode,
        parent_id: Option<u128>,
        // Hashes of the immediate children, as exposed by `get_children`. Sorted
        // for determinism — child ordering inside a vnode is implementation-dependent.
        children: Vec<u128>,
    }

    fn snapshot_all_reachable_nodes(repo: &LocalRepository) -> Result<NodeSnapshot, OxenError> {
        let mut snap = NodeSnapshot::new();
        let store = repo.merkle_store()?;
        let commits = repositories::commits::list_all(repo)?;
        for commit in commits {
            let commit_hash = commit.hash()?;
            collect(store, &commit_hash, &mut snap)?;
        }
        Ok(snap)
    }

    fn collect(
        store: &dyn MerkleReader,
        node_hash: &MerkleHash,
        snap: &mut NodeSnapshot,
    ) -> Result<(), OxenError> {
        if snap.contains_key(&node_hash.to_u128()) {
            return Ok(());
        }
        let Some(entry) = store.get_node(node_hash)? else {
            return Ok(());
        };
        let children = store.get_children(node_hash)?;
        let mut child_hashes: Vec<u128> = children.iter().map(|(h, _)| h.to_u128()).collect();
        child_hashes.sort_unstable();
        snap.insert(
            node_hash.to_u128(),
            NodeRecord {
                node: entry.node,
                parent_id: entry.parent_id.map(|h| h.to_u128()),
                children: child_hashes,
            },
        );
        for (child_hash, child_node) in children {
            if matches!(
                child_node.node,
                EMerkleTreeNode::Commit(_)
                    | EMerkleTreeNode::Directory(_)
                    | EMerkleTreeNode::VNode(_)
            ) {
                collect(store, &child_hash, snap)?;
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_round_trip_file_to_lmdb_to_file_preserves_every_node() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            // LMDB only permits a single open env per path per process, so each
            // phase below drops the prior `LocalRepository` (and its merkle
            // store handle) before constructing the next one — otherwise the
            // migration's internal `from_dir` would race against a live env.
            let repo_path = repo.path.clone();

            // Sanity: fresh repo defaults to the file backend.
            assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::File);
            let before = snapshot_all_reachable_nodes(&repo)?;
            assert!(
                !before.is_empty(),
                "fresh one-commit repo should have at least one merkle node"
            );
            drop(repo);

            // Up: file → LMDB
            SwitchMerkleStoreToLmdbMigration.up(&repo_path, false)?;
            let mid = {
                let repo_after_up = LocalRepository::from_dir(&repo_path)?;
                assert_eq!(
                    repo_after_up.merkle_store_kind(),
                    MerkleStoreKind::Lmdb,
                    "config should report Lmdb after up"
                );
                assert!(
                    !repo_after_up
                        .path
                        .join(OXEN_HIDDEN_DIR)
                        .join(TREE_DIR)
                        .join(NODES_DIR)
                        .exists(),
                    "old file-tree node dir should be gone after up"
                );
                assert!(
                    lmdb_dir_location(&repo_after_up.path).exists(),
                    "LMDB env dir should exist after up"
                );
                snapshot_all_reachable_nodes(&repo_after_up)?
            };
            assert_eq!(before, mid, "LMDB backend must observe identical tree");

            // Down: LMDB → file
            SwitchMerkleStoreToLmdbMigration.down(&repo_path, false)?;
            let after = {
                let repo_after_down = LocalRepository::from_dir(&repo_path)?;
                assert_eq!(
                    repo_after_down.merkle_store_kind(),
                    MerkleStoreKind::File,
                    "config should report File after down"
                );
                assert!(
                    !lmdb_dir_location(&repo_after_down.path).exists(),
                    "LMDB env dir should be gone after down"
                );
                snapshot_all_reachable_nodes(&repo_after_down)?
            };
            assert_eq!(before, after, "round-trip must preserve every node");

            Ok(())
        })
        .await
    }

    #[test]
    fn test_is_needed_always_false() -> Result<(), OxenError> {
        let repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::File)?;
        assert!(!SwitchMerkleStoreToLmdbMigration.is_needed(&repo)?);
        drop(repo);
        let repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        assert!(!SwitchMerkleStoreToLmdbMigration.is_needed(&repo)?);
        Ok(())
    }

    #[test]
    fn test_is_applicable_tracks_current_backend() -> Result<(), OxenError> {
        let repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::File)?;
        assert!(SwitchMerkleStoreToLmdbMigration.is_applicable(Direction::Up, &repo)?);
        assert!(!SwitchMerkleStoreToLmdbMigration.is_applicable(Direction::Down, &repo)?);
        drop(repo);
        let repo = test::init_test_repo_with_merkle_store(MerkleStoreKind::Lmdb)?;
        assert!(!SwitchMerkleStoreToLmdbMigration.is_applicable(Direction::Up, &repo)?);
        assert!(SwitchMerkleStoreToLmdbMigration.is_applicable(Direction::Down, &repo)?);
        Ok(())
    }

    #[tokio::test]
    async fn test_up_is_idempotent_after_first_run() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            let repo_path = repo.path.clone();
            drop(repo);
            SwitchMerkleStoreToLmdbMigration.up(&repo_path, false)?;
            // Re-running on an already-Lmdb repo should be a no-op.
            SwitchMerkleStoreToLmdbMigration.up(&repo_path, false)?;
            let reloaded = LocalRepository::from_dir(&repo_path)?;
            assert_eq!(reloaded.merkle_store_kind(), MerkleStoreKind::Lmdb);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_down_on_file_backend_is_a_noop() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test(|repo| {
            assert_eq!(repo.merkle_store_kind(), MerkleStoreKind::File);
            SwitchMerkleStoreToLmdbMigration.down(&repo.path, false)?;
            let reloaded = LocalRepository::from_dir(&repo.path)?;
            assert_eq!(reloaded.merkle_store_kind(), MerkleStoreKind::File);
            Ok(())
        })
        .await
    }
}
