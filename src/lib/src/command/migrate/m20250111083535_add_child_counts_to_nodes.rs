use std::path::Path;

use super::Migrate;

use crate::config::RepositoryConfig;
use crate::core::db::merkle_node::MerkleNodeDB;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merkle_tree::merkle_tree_node_cache;
use crate::model::merkle_tree::node::vnode::VNodeOpts;
use crate::model::merkle_tree::node::{
    CommitNode, DirNode, EMerkleTreeNode, MerkleTreeNode, VNode,
};
use crate::model::{Commit, LocalRepository, MerkleHash};

use crate::util::hasher;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{repositories, util};

pub struct AddChildCountsToNodesMigration;
impl AddChildCountsToNodesMigration {}

impl Migrate for AddChildCountsToNodesMigration {
    fn name(&self) -> &'static str {
        "add_child_counts_to_nodes"
    }

    fn description(&self) -> &'static str {
        "Re-writes merkle tree with child counts for all directories and vnode nodes"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            run_on_all_repos(path)?;
        } else {
            let repo = LocalRepository::from_dir(path)?;
            run_on_one_repo(&repo)?;
        }
        Ok(())
    }

    fn down(&self, _path: &Path, _all: bool) -> Result<(), OxenError> {
        panic!("Not implemented");
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        let min_version = repo.min_version();
        log::debug!(
            "checking if migration is needed for repo: {:?}, min_version: {}",
            repo.path,
            min_version
        );
        Ok(min_version == MinOxenVersion::V0_19_0)
    }
}

pub fn run_on_all_repos(path: &Path) -> Result<(), OxenError> {
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
            match run_on_one_repo(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate version files for repo {:?}\nErr: {}",
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

fn run_on_one_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    let commits = repositories::commits::list_all(repo)?;

    merkle_tree_node_cache::with_cache_disabled(|| -> Result<(), OxenError> {
        for commit in commits {
            run_on_commit(repo, &commit)?;
        }

        Ok(())
    })
}

fn run_on_commit(repository: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::info!(
        "Running add_child_counts_to_nodes on commit: {} for repo: {:?}",
        commit,
        repository.path
    );
    let old_repo = repository.clone();

    // println!("old tree for commit {}", commit);
    // repositories::tree::print_tree(&old_repo, commit)?;

    // Iterate over the nodes, find the VNode and DirNode, and add the child counts
    let mut new_repo = repository.clone();
    new_repo.set_min_version(MinOxenVersion::from_string("0.25.0")?);

    // *******************************************************************
    // We need to load all the children of all the VNodes for each DirNode
    // Then re-split the children to new VNodes based on their path name
    // this way we can look up files by path faster, which is what this
    // migration is all about.
    // *******************************************************************

    let Some(root_node) = repositories::tree::get_root_with_children(&old_repo, commit)? else {
        return Err(OxenError::basic_str("Root node not found"));
    };

    let root_dir_node = repositories::tree::get_root_dir(&root_node)?;
    let EMerkleTreeNode::Directory(dir_node) = root_dir_node.node.clone() else {
        return Err(OxenError::basic_str("Root node must be CommitNode"));
    };

    // ✍️ Do all the rewriting
    let num_children = root_dir_node.children.len();
    log::debug!(
        "setting num children {} for root dir on commit {}",
        num_children,
        commit
    );
    let mut dir_node_opts = dir_node.get_opts();
    dir_node_opts.num_entries = num_children as u64;
    let dir_node = DirNode::new(&new_repo, dir_node_opts)?;

    // Write a new commit db
    let commit_node = CommitNode::from_commit(commit.clone());
    let mut root_commit_db =
        MerkleNodeDB::open_read_write(&old_repo, &commit_node, root_node.parent_id)?;
    root_commit_db.add_child(&dir_node)?;

    let current_path = Path::new("");
    rewrite_nodes(&old_repo, &new_repo, &root_node, current_path)?;

    // println!("new tree for commit {}", commit);
    // repositories::tree::print_tree(&new_repo, commit)?;

    // Set the oxen version to 0.25.0
    let mut config = RepositoryConfig::from_repo(&new_repo)?;
    config.min_version = Some("0.25.0".to_string());
    let path = util::fs::config_filepath(&new_repo.path);
    config.save(&path)?;

    Ok(())
}

// Forgive me if you are reading this for reference, we don't have great writers for the
// merkle tree yet - so there is a lot of duplicate logic with `commit_writer.rs`
fn rewrite_nodes(
    old_repo: &LocalRepository,
    new_repo: &LocalRepository,
    node: &MerkleTreeNode,
    current_dir: &Path,
) -> Result<(), OxenError> {
    for child in node.children.iter() {
        match &child.node {
            EMerkleTreeNode::Directory(dir) => {
                // Load all the children of children (files and folders)
                // Then redistribute into buckets...
                // and then just use the MerkleNodeDB to write the nodes
                // to the new tree
                let dir_children = repositories::tree::list_files_and_folders(child)?;
                let current_dir = current_dir.join(dir.name());

                // log::debug!(
                //     "rewrite_nodes {} children on current_dir {:?} DIRECTORY {} {}",
                //     dir_children.len(),
                //     current_dir,
                //     dir.hash(),
                //     dir
                // );

                let total_children = dir_children.len();
                let vnode_size = old_repo.vnode_size();
                let num_vnodes = (total_children as f32 / vnode_size as f32).ceil() as u128;

                // Create our new DirNode
                let mut dir_node_opts = dir.get_opts();
                dir_node_opts.num_entries = total_children as u64;
                let dir = DirNode::new(new_repo, dir_node_opts)?;
                let mut dir_db = MerkleNodeDB::open_read_write(old_repo, &dir, node.parent_id)?;

                // log::debug!(
                //     "rewrite_nodes {} VNodes for {} children in {} with vnode size {}",
                //     num_vnodes,
                //     total_children,
                //     dir,
                //     vnode_size
                // );

                // Compute buckets
                let mut buckets: Vec<Vec<MerkleTreeNode>> = vec![vec![]; num_vnodes as usize];
                for dir_child in dir_children {
                    let path = current_dir.join(dir_child.maybe_path().unwrap());
                    let hash = hasher::hash_buffer_128bit(path.to_str().unwrap().as_bytes());
                    let bucket_idx = hash % num_vnodes;
                    // log::debug!(
                    //     "\trewrite_nodes dir_child {:?} bucket {} num_vnodes {} hash {} {}",
                    //     path,
                    //     bucket_idx,
                    //     num_vnodes,
                    //     hash,
                    //     dir_child
                    // );
                    buckets[bucket_idx as usize].push(dir_child);
                }

                // Compute hashes and sort entries to get vnode buckets
                let mut vnodes: Vec<(MerkleHash, Vec<MerkleTreeNode>)> = vec![];
                for bucket in buckets.iter_mut() {
                    // Sort the entries in the vnode by path
                    // to make searching for entries faster
                    bucket.sort_by_key(|a| a.maybe_path().unwrap());

                    // Compute hash for the vnode
                    let mut vnode_hasher = xxhash_rust::xxh3::Xxh3::new();
                    vnode_hasher.update(b"vnode");
                    // add the dir name to the vnode hash
                    vnode_hasher.update(dir.name().as_bytes());

                    for entry in bucket.iter() {
                        if let EMerkleTreeNode::File(file_node) = &entry.node {
                            vnode_hasher.update(&file_node.combined_hash().to_le_bytes());
                        } else if let EMerkleTreeNode::Directory(dir_node) = &entry.node {
                            vnode_hasher.update(&dir_node.hash().to_le_bytes());
                        }
                    }

                    let vnode_id = MerkleHash::new(vnode_hasher.digest128());
                    vnodes.push((vnode_id, bucket.clone()));
                }

                // log::debug!("rewrite_nodes count vnodes: {}", vnodes.len());
                for (hash, entries) in vnodes.iter() {
                    // create a new vnode obj and add the the db
                    let opts = VNodeOpts {
                        hash: *hash,
                        num_entries: entries.len() as u64,
                    };
                    let vnode_obj = VNode::new(new_repo, opts)?;
                    // log::debug!("rewrite_nodes adding VNode to DirNode! {:?}", vnode_obj);
                    dir_db.add_child(&vnode_obj)?;

                    let mut vnode_db =
                        MerkleNodeDB::open_read_write(new_repo, &vnode_obj, Some(dir_db.node_id))?;

                    // log::debug!("rewrite_nodes count entries {}", entries.len());
                    for entry in entries {
                        match &entry.node {
                            EMerkleTreeNode::File(f_node) => {
                                // log::debug!("rewrite_nodes adding FileNode to VNode! {}", f_node);
                                vnode_db.add_child(f_node)?;
                            }
                            EMerkleTreeNode::Directory(d_node) => {
                                let mut d_node_opts = d_node.get_opts();
                                let d_children = repositories::tree::list_files_and_folders(entry)?;
                                d_node_opts.num_entries = d_children.len() as u64;
                                // log::debug!(
                                //     "rewrite_nodes adding DirNode to VNode with {} num_entries {}",
                                //     d_node_opts.num_entries,
                                //     d_node
                                // );
                                let d_node = DirNode::new(new_repo, d_node_opts)?;
                                vnode_db.add_child(&d_node)?;
                            }
                            _ => {
                                panic!("Shouldn't reach here.")
                            }
                        }
                    }
                }

                rewrite_nodes(old_repo, new_repo, child, &current_dir)?;
            }
            EMerkleTreeNode::VNode(_) => {
                // VNode just needs to traverse to the next dirnode
                rewrite_nodes(old_repo, new_repo, child, current_dir)?;
            }
            _ => {
                // pass, FileNode was not changed, so it is on the latest version
            }
        }
    }

    Ok(())
}

// Add tests
#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        model::{merkle_tree::node::EMerkleTreeNode, MerkleHash},
        test,
    };
    use std::{path::PathBuf, str::FromStr};

    #[tokio::test]
    async fn test_add_child_counts_to_nodes_migration() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate an older repository
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Populate the repo with some files
            test::populate_dir_with_training_data(&repo.path)?;
            // Make a variety of commits
            test::make_many_commits(&repo).await?;

            // Test that the root commit
            let latest_commit = repositories::commits::latest_commit(&repo)?;
            let commit_hash = MerkleHash::from_str(&latest_commit.id)?;
            let Some(old_root_node) =
                repositories::tree::get_node_by_id_with_children(&repo, &commit_hash)?
            else {
                return Err(OxenError::basic_str("Root node not found"));
            };

            repositories::tree::print_tree(&repo, &latest_commit)?;

            old_root_node.walk_tree(|node| {
                println!("test_add_child_counts_to_nodes node: {}", node);
                match &node.node {
                    EMerkleTreeNode::Commit(commit) => {
                        assert_eq!(commit.version(), MinOxenVersion::V0_19_0);
                    }
                    EMerkleTreeNode::Directory(dir) => {
                        assert_eq!(dir.version(), MinOxenVersion::V0_19_0);
                    }
                    EMerkleTreeNode::VNode(vnode) => {
                        assert_eq!(vnode.version(), MinOxenVersion::V0_19_0);
                    }
                    _ => {
                        // pass, FileNode was not changed, so it is on the latest version
                    }
                }
            });

            // Run the migration
            run_on_one_repo(&repo)?;

            // Clear the cache after the migration
            merkle_tree_node_cache::remove_from_cache(&repo.path)?;

            let repo = LocalRepository::from_dir(&repo.path)?;
            let latest_commit = repositories::commits::latest_commit(&repo)?;
            let commit_node_version =
                repositories::tree::get_commit_node_version(&repo, &latest_commit)?;
            let node_version_str = commit_node_version.to_string();
            assert_eq!(node_version_str, "0.36.0");

            let commit_hash = MerkleHash::from_str(&latest_commit.id)?;
            let Some(new_root_node) =
                repositories::tree::get_node_by_id_with_children(&repo, &commit_hash)?
            else {
                return Err(OxenError::basic_str("Root node not found"));
            };

            new_root_node.walk_tree(|node| {
                println!("test_add_child_counts_to_nodes node: {}", node);
                match &node.node {
                    EMerkleTreeNode::Commit(commit) => {
                        assert_eq!(
                            commit.version(),
                            MinOxenVersion::from_string("0.36.0").unwrap()
                        );
                    }
                    EMerkleTreeNode::Directory(dir) => {
                        assert_eq!(
                            dir.version(),
                            MinOxenVersion::from_string("0.36.0").unwrap()
                        );
                    }
                    EMerkleTreeNode::VNode(vnode) => {
                        assert_eq!(
                            vnode.version(),
                            MinOxenVersion::from_string("0.36.0").unwrap()
                        );
                    }
                    _ => {
                        // pass, FileNode was not changed, so it is on the latest version
                    }
                }
            });

            // Make sure that the repo version is updated
            let repo = LocalRepository::from_dir(&repo.path)?;
            let version_str = repo.min_version().to_string();
            assert_eq!(version_str, "0.25.0");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_add_child_counts_migration_with_many_vnodes() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Instantiate an older repository
            let mut repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;
            // Set the vnode size to 3
            repo.set_vnode_size(3);

            // Populate the repo with some files
            test::populate_dir_with_training_data(&repo.path)?;
            // Make a variety of commits
            test::make_many_commits(&repo).await?;

            // Test that the root commit
            let latest_commit = repositories::commits::latest_commit(&repo)?;
            let commit_hash = MerkleHash::from_str(&latest_commit.id)?;
            let Some(old_root_node) =
                repositories::tree::get_node_by_id_with_children(&repo, &commit_hash)?
            else {
                return Err(OxenError::basic_str("Root node not found"));
            };

            repositories::tree::print_tree(&repo, &latest_commit)?;

            old_root_node.walk_tree(|node| {
                println!("test_add_child_counts_to_nodes node: {}", node);
                match &node.node {
                    EMerkleTreeNode::Commit(commit) => {
                        assert_eq!(commit.version(), MinOxenVersion::V0_19_0);
                    }
                    EMerkleTreeNode::Directory(dir) => {
                        assert_eq!(dir.version(), MinOxenVersion::V0_19_0);
                    }
                    EMerkleTreeNode::VNode(vnode) => {
                        assert_eq!(vnode.version(), MinOxenVersion::V0_19_0);
                    }
                    _ => {
                        // pass, FileNode was not changed, so it is on the latest version
                    }
                }
            });

            // Run the migration
            run_on_one_repo(&repo)?;

            // Clear the cache after the migration
            merkle_tree_node_cache::remove_from_cache(&repo.path)?;

            let mut repo = LocalRepository::from_dir(&repo.path)?;
            repo.set_vnode_size(3);

            let latest_commit = repositories::commits::latest_commit(&repo)?;
            let commit_node_version =
                repositories::tree::get_commit_node_version(&repo, &latest_commit)?;
            let node_version_str = commit_node_version.to_string();
            assert_eq!(node_version_str, "0.36.0");

            let commit_hash = MerkleHash::from_str(&latest_commit.id)?;
            let Some(new_root_node) =
                repositories::tree::get_node_by_id_with_children(&repo, &commit_hash)?
            else {
                return Err(OxenError::basic_str("Root node not found"));
            };

            new_root_node.walk_tree(|node| {
                println!("test_add_child_counts_to_nodes node: {}", node);
                match &node.node {
                    EMerkleTreeNode::Commit(commit) => {
                        assert_eq!(
                            commit.version(),
                            MinOxenVersion::from_string("0.36.0").unwrap()
                        );
                    }
                    EMerkleTreeNode::Directory(dir) => {
                        assert_eq!(
                            dir.version(),
                            MinOxenVersion::from_string("0.36.0").unwrap()
                        );
                    }
                    EMerkleTreeNode::VNode(vnode) => {
                        assert_eq!(
                            vnode.version(),
                            MinOxenVersion::from_string("0.36.0").unwrap()
                        );
                    }
                    _ => {
                        // pass, FileNode was not changed, so it is on the latest version
                    }
                }
            });

            println!("Checking files on latest_commit: {}", latest_commit);

            // Make sure we can get an individual file
            let file_node = repositories::tree::get_node_by_path(
                &repo,
                &latest_commit,
                PathBuf::from("README.md"),
            )?;
            assert!(file_node.is_some());

            for i in 1..3 {
                let path = PathBuf::from("train").join(format!("cat_{}.jpg", i));
                log::debug!("LOOKING UP CAT: {:?}", path);
                let file_node = repositories::tree::get_node_by_path(&repo, &latest_commit, &path)?;
                assert!(file_node.is_some());
            }

            for i in 1..4 {
                let path = PathBuf::from("train").join(format!("dog_{}.jpg", i));
                log::debug!("LOOKING UP DOG: {:?}", path);
                let file_node = repositories::tree::get_node_by_path(&repo, &latest_commit, &path)?;
                assert!(file_node.is_some());
            }

            // Make sure that the repo version is updated
            let repo = LocalRepository::from_dir(&repo.path)?;
            let version_str = repo.min_version().to_string();
            assert_eq!(version_str, "0.25.0");

            Ok(())
        })
        .await
    }
}
