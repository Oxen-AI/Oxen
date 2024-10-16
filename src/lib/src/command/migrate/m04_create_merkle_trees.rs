use rocksdb::{DBWithThreadMode, MultiThreaded};

use super::Migrate;

use std::path::{Path, PathBuf};

use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::core::v0_10_0::index::{CommitEntryWriter, CommitReader, CommitWriter};
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{constants, repositories};

pub struct CreateMerkleTreesMigration;
impl Migrate for CreateMerkleTreesMigration {
    fn name(&self) -> &'static str {
        "create_merkle_trees"
    }

    fn description(&self) -> &'static str {
        "Reformats the underlying data model into merkle trees for storage and lookup efficiency"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            create_merkle_trees_for_all_repos_up(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            create_merkle_trees_up(&repo)?;
        }
        Ok(())
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            create_merkle_trees_for_all_repos_down(path)?;
        } else {
            println!("Running down migration");
            let repo = LocalRepository::new(path)?;
            create_merkle_trees_down(&repo)?;
        }
        Ok(())
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        let objects_dir = repo
            .path
            .join(constants::OXEN_HIDDEN_DIR)
            .join(constants::OBJECTS_DIR);
        if !objects_dir.exists() && repo.min_version() <= MinOxenVersion::V0_10_0 {
            return Ok(true);
        }
        // This may need a more elaborate check for migrations that are aborted with a single repo...
        // but it's too computationally expensive to parse through all the trees.
        Ok(false)
    }
}

pub fn create_merkle_trees_for_all_repos_up(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        // Show the canonical namespace path
        log::debug!(
            "This is the namespace path we're walking: {:?}",
            namespace_path.canonicalize()?
        );
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match create_merkle_trees_up(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate merkle trees for repo {:?}\nErr: {}",
                        repo.path.canonicalize(),
                        err
                    )
                }
            }
        }
        bar.inc(1);
    }
    Ok(())
}

pub fn create_merkle_trees_for_all_repos_down(_path: &Path) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}

pub fn create_merkle_trees_up(repo: &LocalRepository) -> Result<(), OxenError> {
    // Get all commits in repo, then construct merkle tree for each commit
    let reader = CommitReader::new(repo)?;
    let all_commits = reader.list_all_sorted_by_timestamp()?;

    let bar = oxen_progress_bar(all_commits.len() as u64, ProgressBarType::Counter);
    let commit_writer = CommitWriter::new(repo)?;
    for commit in all_commits {
        // Create the merkle tree for each commit
        match construct_commit_merkle_tree_from_legacy(repo, &commit) {
            Ok(_) => {}
            Err(err) => {
                log::error!(
                    "Could not construct merkle tree for commit {:?}\nErr: {}",
                    commit.id,
                    err
                )
            }
        }
        // Then we need to associate the root hash of the merkle tree with the commit
        let mut commit_to_update = commit.clone();
        let dir_hashes_db_dir = CommitEntryWriter::commit_dir_hash_db(&repo.path, &commit.id);
        let dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
            &db::key_val::opts::default(),
            &dir_hashes_db_dir,
            false,
        )?;

        let root_hash: String = path_db::get_entry(&dir_hashes_db, PathBuf::from(""))?.unwrap();

        commit_to_update.update_root_hash(root_hash);

        commit_writer.add_commit_to_db(&commit_to_update)?;

        bar.inc(1);
    }

    // runtime check: commit root hash is properly updated for all commits
    let updated_commits = reader.list_all()?;

    for commit in updated_commits {
        let dir_hashes_db_dir = CommitEntryWriter::commit_dir_hash_db(&repo.path, &commit.id);
        let dir_hashes_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_for_read_only(
            &db::key_val::opts::default(),
            &dir_hashes_db_dir,
            false,
        )?;
        let maybe_root_hash: Option<String> = path_db::get_entry(&dir_hashes_db, "")?;
        let Some(root_hash) = maybe_root_hash else {
            return Err(OxenError::basic_str(format!(
                "Could not find root hash for dir hashes db {:?} in repo {:?}",
                commit, repo.path
            )));
        };

        let Some(db_root_hash) = commit.root_hash.clone() else {
            return Err(OxenError::basic_str(format!(
                "Could not find root hash in commit db from commit {:?} in repo {:?}",
                commit, repo.path
            )));
        };

        if root_hash != db_root_hash {
            return Err(OxenError::basic_str(format!(
                "Root hash in commit db {:?} does not match root hash in dir hashes db {:?} in repo {:?}",
                db_root_hash, root_hash, repo.path
            )));
        }

        log::debug!(
            "Root hash for commit {:?} is correct in repo {:?}",
            commit,
            repo
        );
    }

    Ok(())
}

pub fn create_merkle_trees_down(_repo: &LocalRepository) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}

pub fn construct_commit_merkle_tree_from_legacy(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    let commit_writer = CommitEntryWriter::new(repo, commit)?;
    commit_writer.construct_merkle_tree_from_legacy_commit(&repo.path)?;
    Ok(())
}
