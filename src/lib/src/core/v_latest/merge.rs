use crate::core::db;
pub use crate::core::merge::entry_merge_conflict_db_reader::EntryMergeConflictDBReader;
pub use crate::core::merge::node_merge_conflict_db_reader::NodeMergeConflictDBReader;
use crate::core::merge::node_merge_conflict_reader::NodeMergeConflictReader;
use crate::core::merge::{db_path, node_merge_conflict_writer};
use crate::core::refs::with_ref_manager;
use crate::core::v_latest::commits::{get_commit_or_head, list_between};
use crate::core::v_latest::index::CommitMerkleTree;
use crate::core::v_latest::{add, rm};
use crate::error::OxenError;
use crate::model::merge_conflict::NodeMergeConflict;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Branch, Commit, LocalRepository};
use crate::model::{MerkleHash, PartialNode};
use crate::opts::RmOpts;
use crate::repositories;
use crate::repositories::commits::commit_writer;
use crate::repositories::merge::MergeCommits;
use crate::util;

use rocksdb::DB;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str;

use super::index::restore;
use super::index::restore::FileToRestore;

// entries_to_restore: files that ought to be restored from the currently traversed tree
// I.e., In r_ff_merge_commit, it contains merge commit files that are not present in or have changed from the base tree
// cannot_overwrite_entries: files that would be restored, but are modified from the from_tree, and thus would erase work if overwritten

// for r_ff_base_dir, the 'entries to restore' are actually entries being removed
struct MergeResult {
    pub entries_to_restore: Vec<FileToRestore>,
    pub cannot_overwrite_entries: Vec<PathBuf>,
}

impl MergeResult {
    pub fn new() -> Self {
        MergeResult {
            entries_to_restore: vec![],
            cannot_overwrite_entries: vec![],
        }
    }
}

pub fn has_conflicts(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<bool, OxenError> {
    let base_commit =
        repositories::commits::get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit =
        repositories::commits::get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    let res = can_merge_commits(repo, &base_commit, &merge_commit)?;
    Ok(!res)
}

pub fn list_conflicts(repo: &LocalRepository) -> Result<Vec<NodeMergeConflict>, OxenError> {
    match NodeMergeConflictReader::new(repo) {
        Ok(reader) => reader.list_conflicts(),
        Err(e) => {
            log::debug!("Error creating NodeMergeConflictReader: {e}");
            Ok(Vec::new())
        }
    }
}

pub fn mark_conflict_as_resolved(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    node_merge_conflict_writer::mark_conflict_as_resolved_in_db(repo, path)
}

/// Check if there are conflicts between the merge commit and the base commit
/// Returns true if there are no conflicts, false if there are conflicts
pub fn can_merge_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<bool, OxenError> {
    let lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?;
    let merge_commits = MergeCommits {
        lca,
        base: base_commit.clone(),
        merge: merge_commit.clone(),
    };

    if merge_commits.is_fast_forward_merge() {
        // If it is fast forward merge, there are no merge conflicts
        return Ok(true);
    }

    let write_to_disk = false;
    let conflicts = find_merge_conflicts(repo, &merge_commits, write_to_disk)?;
    Ok(conflicts.is_empty())
}

pub fn list_conflicts_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<Vec<PathBuf>, OxenError> {
    let base_commit = get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit = get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    list_conflicts_between_commits(repo, &base_commit, &merge_commit)
}

pub fn list_commits_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    head_branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!(
        "list_commits_between_branches() base: {:?} head: {:?}",
        base_branch,
        head_branch
    );
    let base_commit = get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let head_commit = get_commit_or_head(repo, Some(head_branch.commit_id.clone()))?;

    let lca = lowest_common_ancestor_from_commits(repo, &base_commit, &head_commit)?;
    log::debug!(
        "list_commits_between_branches {:?} -> {:?} found lca {:?}",
        base_commit,
        head_commit,
        lca
    );
    list_between(repo, &lca, &head_commit)
}

pub fn list_commits_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!(
        "list_commits_between_commits()\nbase: {}\nhead: {}",
        base_commit,
        head_commit
    );

    let lca = lowest_common_ancestor_from_commits(repo, base_commit, head_commit)?;

    log::debug!(
        "For commits {:?} -> {:?} found lca {:?}",
        base_commit,
        head_commit,
        lca
    );

    log::debug!("Reading history from lca to head");
    list_between(repo, &lca, head_commit)
}

pub fn list_conflicts_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Vec<PathBuf>, OxenError> {
    let lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?;
    let merge_commits = MergeCommits {
        lca,
        base: base_commit.clone(),
        merge: merge_commit.clone(),
    };
    let write_to_disk = false;
    let conflicts = find_merge_conflicts(repo, &merge_commits, write_to_disk)?;
    Ok(conflicts
        .iter()
        .map(|c| {
            let (_, path) = &c.base_entry;
            path.to_owned()
        })
        .collect())
}

/// Merge a branch into a base branch, returns the merge commit if successful, and None if there is conflicts
pub fn merge_into_base(
    repo: &LocalRepository,
    merge_branch: &Branch,
    base_branch: &Branch,
) -> Result<Option<Commit>, OxenError> {
    log::debug!(
        "merge_into_base merge {} into {}",
        merge_branch,
        base_branch
    );

    if merge_branch.commit_id == base_branch.commit_id {
        // If the merge branch is the same as the base branch, there is nothing to merge
        return Ok(None);
    }

    let base_commit = get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit = get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    let lca = lowest_common_ancestor_from_commits(repo, &base_commit, &merge_commit)?;
    log::debug!(
        "merge_into_base base: {:?} merge: {:?} lca: {:?}",
        base_commit,
        merge_commit,
        lca
    );

    let commits = MergeCommits {
        lca,
        base: base_commit,
        merge: merge_commit,
    };

    merge_commits(repo, &commits)
}

/// Merge into the current branch, returns the merge commit if successful, and None if there is conflicts
pub fn merge(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let branch_name = branch_name.as_ref();

    let merge_branch = repositories::branches::get_by_name(repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;

    let base_commit = repositories::commits::head_commit(repo)?;
    let merge_commit = get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;
    let lca = lowest_common_ancestor_from_commits(repo, &base_commit, &merge_commit)?;
    let commits = MergeCommits {
        lca,
        base: base_commit,
        merge: merge_commit,
    };
    merge_commits(repo, &commits)
}

pub fn merge_commit_into_base(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
) -> Result<Option<Commit>, OxenError> {
    let lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?;
    log::debug!(
        "merge_commit_into_base has lca {:?} for merge commit {:?} and base {:?}",
        lca,
        merge_commit,
        base_commit
    );
    let commits = MergeCommits {
        lca,
        base: base_commit.to_owned(),
        merge: merge_commit.to_owned(),
    };

    merge_commits(repo, &commits)
}

pub fn merge_commit_into_base_on_branch(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
    branch: &Branch,
) -> Result<Option<Commit>, OxenError> {
    let lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?;

    log::debug!(
        "merge_commit_into_branch has lca {:?} for merge commit {:?} and base {:?}",
        lca,
        merge_commit,
        base_commit
    );

    let merge_commits = MergeCommits {
        lca,
        base: base_commit.to_owned(),
        merge: merge_commit.to_owned(),
    };

    merge_commits_on_branch(repo, &merge_commits, branch)
}

pub fn has_file(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let db_path = db_path(repo);
    log::debug!("Merger::new() DB {:?}", db_path);
    let opts = db::key_val::opts::default();
    let merge_db = DB::open(&opts, dunce::simplified(&db_path))?;

    NodeMergeConflictDBReader::has_file(&merge_db, path)
}

pub fn remove_conflict_path(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let db_path = db_path(repo);
    log::debug!("Merger::new() DB {:?}", db_path);
    let opts = db::key_val::opts::default();
    let merge_db = DB::open(&opts, dunce::simplified(&db_path))?;

    let path_str = path.to_str().unwrap();
    let key = path_str.as_bytes();
    merge_db.delete(key)?;
    Ok(())
}

pub fn find_merge_commits<S: AsRef<str>>(
    repo: &LocalRepository,
    branch_name: S,
) -> Result<MergeCommits, OxenError> {
    let branch_name = branch_name.as_ref();

    let current_branch = repositories::branches::current_branch(repo)?
        .ok_or(OxenError::basic_str("No current branch"))?;

    let head_commit =
        repositories::commits::get_commit_or_head(repo, Some(current_branch.name.clone()))?;

    let merge_commit = get_commit_or_head(repo, Some(branch_name))?;

    let lca = lowest_common_ancestor_from_commits(repo, &head_commit, &merge_commit)?;

    Ok(MergeCommits {
        lca,
        base: head_commit,
        merge: merge_commit,
    })
}

fn merge_commits_on_branch(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
    branch: &Branch,
) -> Result<Option<Commit>, OxenError> {
    // User output
    println!(
        "merge_commits_on_branch {} -> {}",
        merge_commits.base.id, merge_commits.merge.id
    );

    log::debug!(
        "FOUND MERGE COMMITS:\nLCA: {} -> {}\nBASE: {} -> {}\nMerge: {} -> {}",
        merge_commits.lca.id,
        merge_commits.lca.message,
        merge_commits.base.id,
        merge_commits.base.message,
        merge_commits.merge.id,
        merge_commits.merge.message,
    );

    // Check which type of merge we need to do
    if merge_commits.is_fast_forward_merge() {
        let commit = fast_forward_merge(repo, &merge_commits.base, &merge_commits.merge)?;
        Ok(Some(commit))
    } else {
        log::debug!(
            "Three way merge! {} -> {}",
            merge_commits.base.id,
            merge_commits.merge.id
        );

        let write_to_disk = true;
        let conflicts = find_merge_conflicts(repo, merge_commits, write_to_disk)?;
        log::debug!("Got {} conflicts", conflicts.len());

        if conflicts.is_empty() {
            log::debug!("creating merge commit on branch {:?}", branch);
            let commit = create_merge_commit_on_branch(repo, merge_commits, branch)?;
            Ok(Some(commit))
        } else {
            println!(
                r"
Found {} conflicts, please resolve them before merging.

  oxen checkout --theirs path/to/file_1.txt
  oxen checkout --ours path/to/file_2.txt
  oxen add path/to/file_1.txt path/to/file_2.txt
  oxen commit -m 'Merge conflict resolution'

",
                conflicts.len()
            );
            let db_path = db_path(repo);
            log::debug!("Merger::new() DB {:?}", db_path);
            let opts = db::key_val::opts::default();
            let merge_db = DB::open(&opts, dunce::simplified(&db_path))?;

            node_merge_conflict_writer::write_conflicts_to_disk(
                repo,
                &merge_db,
                &merge_commits.merge,
                &merge_commits.base,
                &conflicts,
            )?;
            Ok(None)
        }
    }
}

/// Check if HEAD is in the direct parent chain of the merge commit. If it is a direct parent, we can just fast forward
pub fn lowest_common_ancestor(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let current_branch = repositories::branches::current_branch(repo)?
        .ok_or(OxenError::basic_str("No current branch"))?;

    let base_commit =
        repositories::commits::get_commit_or_head(repo, Some(current_branch.name.clone()))?;
    let merge_commit = repositories::commits::get_commit_or_head(repo, Some(branch_name))?;

    lowest_common_ancestor_from_commits(repo, &base_commit, &merge_commit)
}

fn fast_forward_merge(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Commit, OxenError> {
    log::debug!("FF merge!");

    // Collect all dir and vnode hashes while loading the merge tree
    // This is done to identify shared dirs/vnodes between the merge and base trees while loading the base tree
    let mut merge_hashes = HashSet::new();
    let Some(merge_tree) =
        CommitMerkleTree::root_with_children_and_hashes(repo, merge_commit, &mut merge_hashes)?
    else {
        return Err(OxenError::basic_str("Cannot get root node for base commit"));
    };

    // Collect every shared dir/vnode hash between the trees, load the base tree's unique nodes and collect them as 'partial nodes'
    // These are done to skip checks for shared dirs/vnodes and avoid slow tree traversals when comparing files in the recursvie functions respectively
    let mut shared_hashes = HashSet::new();
    let mut partial_nodes = HashMap::new();
    let Some(base_tree) = CommitMerkleTree::root_with_unique_children(
        repo,
        base_commit,
        &mut merge_hashes,
        &mut shared_hashes,
        &mut partial_nodes,
    )?
    else {
        return Err(OxenError::basic_str(
            "Cannot get root node for merge commit",
        ));
    };

    // Stop early if there are conflicts
    let mut merge_tree_results = MergeResult::new();
    let mut seen_files = HashSet::new();

    r_ff_merge_commit(
        repo,
        &merge_tree,
        PathBuf::from(""),
        &mut merge_tree_results,
        &mut partial_nodes,
        &mut shared_hashes,
        &mut seen_files,
    )?;
    // If there are no conflicts, restore the entries
    if merge_tree_results.cannot_overwrite_entries.is_empty() {
        let version_store = repo.version_store()?;
        for entry in merge_tree_results.entries_to_restore.iter() {
            restore::restore_file(repo, &entry.file_node, &entry.path, &version_store)?;
        }
    } else {
        // If there are conflicts, return an error without restoring anything
        return Err(OxenError::cannot_overwrite_files(
            &merge_tree_results.cannot_overwrite_entries,
        ));
    }

    let mut base_tree_results = MergeResult::new();

    r_ff_base_dir(
        repo,
        &base_tree,
        PathBuf::from(""),
        &mut base_tree_results,
        &mut shared_hashes,
        &mut seen_files,
    )?;

    // If there are no conflicts, remove the entries
    if base_tree_results.cannot_overwrite_entries.is_empty() {
        for entry in base_tree_results.entries_to_restore.iter() {
            util::fs::remove_file(&entry.path)?;
        }
    } else {
        // If there are conflicts, return an error without removing anything
        return Err(OxenError::cannot_overwrite_files(
            &base_tree_results.cannot_overwrite_entries,
        ));
    }

    // Move the HEAD forward to this commit
    with_ref_manager(repo, |manager| manager.set_head_commit_id(&merge_commit.id))?;

    Ok(merge_commit.clone())
}

fn r_ff_merge_commit(
    repo: &LocalRepository,
    merge_node: &MerkleTreeNode,
    path: impl AsRef<Path>,
    results: &mut MergeResult,
    base_files: &mut HashMap<PathBuf, PartialNode>,
    shared_hashes: &mut HashSet<MerkleHash>,
    seen_files: &mut HashSet<PathBuf>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    match &merge_node.node {
        EMerkleTreeNode::File(merge_file_node) => {
            let file_path = path.join(merge_file_node.name());
            seen_files.insert(file_path.clone());
            // log::debug!("r_ff_merge_commit file_path {:?}", file_path);
            // log::debug!("merge_node {}", merge_node);
            // log::debug!("merge_file_node {}", merge_file_node);

            // If file_path found in base_tree, get the corresponding PartialNode
            // A PartialNode is the minimal representation of a file necessary to determine whether it should be restored
            // To properly handle moved files, the partial nodes are associated with their path in the base tree
            // I.e., if a file has been moved in the merge tree, this code will find that it shouldn't be restored

            if base_files.contains_key(&file_path) {
                // if found, use to determine whether the file should be restored
                let base_file_node = &base_files[&file_path];

                // determine if file should be restored from its hash and last modified time
                let should_restore = restore::should_restore_partial(
                    repo,
                    Some(base_file_node.clone()),
                    merge_file_node,
                    &file_path,
                )?;
                if merge_node.hash != base_file_node.hash {
                    if should_restore {
                        results.entries_to_restore.push(FileToRestore {
                            file_node: merge_file_node.clone(),
                            path: file_path.clone(),
                        });
                    } else {
                        results.cannot_overwrite_entries.push(file_path.clone());
                    }
                } else {
                    log::debug!(
                        "Merge entry has not changed, but still !restore: {:?}",
                        file_path
                    );
                    if !should_restore {
                        results.cannot_overwrite_entries.push(file_path.clone());
                    }
                }
            } else if restore::should_restore_file(repo, None, merge_file_node, &file_path)? {
                results.entries_to_restore.push(FileToRestore {
                    file_node: merge_file_node.clone(),
                    path: file_path.clone(),
                });
            } else {
                results.cannot_overwrite_entries.push(file_path.clone());
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            let dir_path = path.join(dir_node.name());
            // Early exit if the directory is the same in the from and target trees
            if shared_hashes.contains(&merge_node.hash) {
                return Ok(());
            };
            let merge_children = {
                // Get vnodes for the from dir node
                let dir_vnodes = &merge_node.children;

                // Only iterate through vnodes not shared between the trees
                let mut unique_nodes = Vec::new();
                for vnode in dir_vnodes {
                    if !shared_hashes.contains(&vnode.hash) {
                        unique_nodes.extend(vnode.children.iter().cloned());
                    }
                }

                unique_nodes
            };

            for child in merge_children.iter() {
                log::debug!("r_ff_merge_commit child_path {}", child);
                r_ff_merge_commit(
                    repo,
                    child,
                    &dir_path,
                    results,
                    base_files,
                    shared_hashes,
                    seen_files,
                )?;
            }
        }
        EMerkleTreeNode::Commit(_) => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = repositories::tree::get_root_dir(merge_node)?;
            r_ff_merge_commit(
                repo,
                root_dir,
                path,
                results,
                base_files,
                shared_hashes,
                seen_files,
            )?;
        }
        _ => {
            return Err(OxenError::basic_str(
                "Got an unexpected node type during checkout",
            ));
        }
    }

    Ok(())
}

fn r_ff_base_dir(
    repo: &LocalRepository,
    base_node: &MerkleTreeNode,
    path: impl AsRef<Path>,
    results: &mut MergeResult,
    shared_hashes: &mut HashSet<MerkleHash>,
    merge_files: &mut HashSet<PathBuf>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    match &base_node.node {
        EMerkleTreeNode::File(base_file_node) => {
            let file_path = path.join(base_file_node.name());
            // Remove all entries that are in HEAD but not in merge entries
            if !merge_files.contains(&file_path) {
                // Here, we don't need partial node representation, as we're only concerned with restoring files that aren't in the merge tree
                let path = repo.path.join(file_path.clone());
                if path.exists() {
                    if restore::should_restore_file(repo, None, base_file_node, &file_path)? {
                        results.entries_to_restore.push(FileToRestore {
                            file_node: base_file_node.clone(),
                            path: path.clone(),
                        });
                    } else {
                        results.cannot_overwrite_entries.push(file_path);
                    }
                }
            }
        }
        EMerkleTreeNode::Directory(dir_node) => {
            let dir_path = path.join(dir_node.name());

            if shared_hashes.contains(&base_node.hash) {
                return Ok(());
            };

            let base_children = {
                // Get vnodes for the from dir node
                let dir_vnodes = &base_node.children;

                // Only iterate through vnodes not shared between the trees
                let mut unique_nodes = Vec::new();
                for vnode in dir_vnodes {
                    if !shared_hashes.contains(&vnode.hash) {
                        unique_nodes.extend(vnode.children.iter().cloned());
                    }
                }

                unique_nodes
            };

            for child in base_children.iter() {
                //log::debug!("r_ff_base_dir child_path {}", child);
                r_ff_base_dir(repo, child, &dir_path, results, shared_hashes, merge_files)?;
            }
        }
        EMerkleTreeNode::Commit(_) => {
            // If we get a commit node, we need to skip to the root directory
            let root_dir = repositories::tree::get_root_dir(base_node)?;
            r_ff_base_dir(repo, root_dir, path, results, shared_hashes, merge_files)?;
        }
        _ => {
            log::debug!("r_ff_base_dir unknown node type");
        }
    }
    Ok(())
}

fn merge_commits(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
) -> Result<Option<Commit>, OxenError> {
    // User output
    println!(
        "Merge commits {} -> {}",
        merge_commits.base.id, merge_commits.merge.id
    );

    log::debug!(
        "FOUND MERGE COMMITS:\nLCA: {} -> {}\nBASE: {} -> {}\nMerge: {} -> {}",
        merge_commits.lca.id,
        merge_commits.lca.message,
        merge_commits.base.id,
        merge_commits.base.message,
        merge_commits.merge.id,
        merge_commits.merge.message,
    );

    // Check which type of merge we need to do
    if merge_commits.is_fast_forward_merge() {
        // User output
        let commit = fast_forward_merge(repo, &merge_commits.base, &merge_commits.merge)?;
        Ok(Some(commit))
    } else {
        log::debug!(
            "Three way merge! {} -> {}",
            merge_commits.base.id,
            merge_commits.merge.id
        );

        let write_to_disk = true;
        let conflicts = find_merge_conflicts(repo, merge_commits, write_to_disk)?;

        if !conflicts.is_empty() {
            println!(
                r"
Found {} conflicts, please resolve them before merging.

  oxen checkout --theirs path/to/file_1.txt
  oxen checkout --ours path/to/file_2.txt
  oxen add path/to/file_1.txt path/to/file_2.txt
  oxen commit -m 'Merge conflict resolution'

",
                conflicts.len()
            );
        }

        log::debug!("Got {} conflicts", conflicts.len());

        if conflicts.is_empty() {
            let commit = create_merge_commit(repo, merge_commits)?;
            Ok(Some(commit))
        } else {
            let db_path = db_path(repo);
            log::debug!("Merger::new() DB {:?}", db_path);
            let opts = db::key_val::opts::default();
            let merge_db = DB::open(&opts, dunce::simplified(&db_path))?;

            node_merge_conflict_writer::write_conflicts_to_disk(
                repo,
                &merge_db,
                &merge_commits.merge,
                &merge_commits.base,
                &conflicts,
            )?;
            Ok(None)
        }
    }
}

fn create_merge_commit(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
) -> Result<Commit, OxenError> {
    // Stage changes
    // let stager = Stager::new(repo)?;
    // stager.add(&repo.path, &reader, &schema_reader, &ignore)?;
    let head_commit = repositories::commits::head_commit(repo)?;
    add::add_dir(repo, &Some(head_commit), repo.path.clone())?;

    let commit_msg = format!(
        "Merge commit {} into {}",
        merge_commits.merge.id, merge_commits.base.id
    );

    log::debug!("create_merge_commit {}", commit_msg);

    let parent_ids: Vec<String> = vec![
        merge_commits.base.id.to_owned(),
        merge_commits.merge.id.to_owned(),
    ];

    let commit = commit_writer::commit_with_parent_ids(repo, &commit_msg, parent_ids)?;

    // rm::remove_staged(repo, &HashSet::from([PathBuf::from("/")]))?;

    Ok(commit)
}

fn create_merge_commit_on_branch(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
    branch: &Branch,
) -> Result<Commit, OxenError> {
    // Stage changes
    let head_commit = repositories::commits::head_commit(repo)?;
    add::add_dir(repo, &Some(head_commit), repo.path.clone())?;

    let commit_msg = format!(
        "Merge commit {} into {} on branch {}",
        merge_commits.merge.id, merge_commits.base.id, branch.name
    );

    log::debug!("create_merge_commit_on_branch {}", commit_msg);

    // Create a commit with both parents
    // let commit_writer = CommitWriter::new(repo)?;
    let parent_ids: Vec<String> = vec![
        merge_commits.base.id.to_owned(),
        merge_commits.merge.id.to_owned(),
    ];

    // The author in this case is the pusher - the author of the merge commit

    let commit = commit_writer::commit_with_parent_ids(repo, &commit_msg, parent_ids)?;
    let mut opts = RmOpts::from_path(PathBuf::from("/"));
    opts.staged = true;
    opts.recursive = true;
    rm::remove_staged(repo, &HashSet::from([PathBuf::from("/")]), &opts)?;

    Ok(commit)
}

pub fn lowest_common_ancestor_from_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Commit, OxenError> {
    log::debug!(
        "lowest_common_ancestor_from_commits: base: {} merge: {}",
        base_commit.id,
        merge_commit.id
    );
    // Traverse the base commit back to start, keeping map of Commit -> Depth(int)
    let commit_depths_from_head =
        repositories::commits::list_from_with_depth(repo, base_commit.id.as_str())?;

    // Traverse the merge commit back
    //   check at each step if ID is in the HEAD commit history
    //   The lowest Depth Commit in HEAD should be the LCA
    let commit_depths_from_merge =
        repositories::commits::list_from_with_depth(repo, merge_commit.id.as_str())?;

    let mut min_depth = usize::MAX;
    let mut lca: Commit = commit_depths_from_head.keys().next().unwrap().clone();
    for (commit, _) in commit_depths_from_merge.iter() {
        if let Some(depth) = commit_depths_from_head.get(commit) {
            if depth < &min_depth {
                min_depth = *depth;
                log::debug!("setting new lca, {:?}", commit);
                lca = commit.clone();
            }
        }
    }

    Ok(lca)
}

/// Will try a three way merge and return conflicts if there are any to indicate that the merge was unsuccessful
pub fn find_merge_conflicts(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
    write_to_disk: bool,
) -> Result<Vec<NodeMergeConflict>, OxenError> {
    log::debug!("finding merge conflicts");
    /*
    https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge

    C = LCA
    A = Base
    B = Merge
    D = Resulting merge commit

    C - A - D
      \   /
        B

    The three-way merge looks for sections which are the same in only two of the three files.
    In this case, there are two versions of the section,
        and the version which is in the common ancestor "C" is discarded,
        while the version that differs is preserved in the output.
    If "A" and "B" agree, that is what appears in the output.
    A section that is the same in "A" and "C" outputs the changed version in "B",
    and likewise a section that is the same in "B" and "C" outputs the version in "A".

    Sections that are different in all three files are marked as a conflict situation and left for the user to resolve.
    */

    // We will return conflicts if there are any
    let mut conflicts: Vec<NodeMergeConflict> = vec![];
    let mut entries_to_restore: Vec<FileToRestore> = vec![];
    let mut cannot_overwrite_entries: Vec<PathBuf> = vec![];

    // Read all the entries from each commit into sets we can compare to one another
    let mut lca_hashes = HashSet::new();
    let mut base_hashes = HashSet::new();
    let mut shared_hashes = HashSet::new();

    let mut _partial_nodes = HashMap::new();

    // Use the same functions from the fast forward merge to load in only the entries found to be unique to each tree
    // First, we load in every node from the LCA tree
    let lca_commit_tree =
        CommitMerkleTree::root_with_children_and_hashes(repo, &merge_commits.lca, &mut lca_hashes)?
            .unwrap();

    // Then, we load in only the nodes of the base commit tree that weren't in the LCA tree
    // We also track the shared hashes between them
    let base_commit_tree = CommitMerkleTree::root_with_unique_children(
        repo,
        &merge_commits.base,
        &mut lca_hashes,
        &mut base_hashes,
        &mut _partial_nodes,
    )?
    .unwrap();

    // Then, we load in only the nodes of the merge tree that weren't in the Base tree (or the LCA tree)
    // After this, 'shared hashes' will have all the dir/vnode hashes shared between all 3 trees
    let merge_commit_tree = CommitMerkleTree::root_with_unique_children(
        repo,
        &merge_commits.merge,
        &mut base_hashes,
        &mut shared_hashes,
        &mut _partial_nodes,
    )?
    .unwrap();

    // TODO: Remove this unless debugging
    // log::debug!("lca_hashes: {lca_hashes:?}");
    //lca_commit_tree.print();
    // log::debug!("base_hashes: {base_hashes:?}");
    //base_commit_tree.print();
    // log::debug!("merge_hashes: {merge_hashes:?}");

    let starting_path = PathBuf::from("");

    let lca_entries =
        repositories::tree::unique_dir_entries(&starting_path, &lca_commit_tree, &shared_hashes)?;
    let base_entries =
        repositories::tree::unique_dir_entries(&starting_path, &base_commit_tree, &shared_hashes)?;
    let merge_entries =
        repositories::tree::unique_dir_entries(&starting_path, &merge_commit_tree, &shared_hashes)?;

    log::debug!("lca_entries.len() {}", lca_entries.len());
    log::debug!("base_entries.len() {}", base_entries.len());
    log::debug!("merge_entries.len() {}", merge_entries.len());

    // Check all the entries in the candidate merge
    for merge_entry in merge_entries.iter() {
        let entry_path = merge_entry.0;
        let merge_file_node = merge_entry.1;
        // log::debug!("Considering entry {}", entry_path.to_string_lossy());
        // Check if the entry exists in all 3 commits
        if base_entries.contains_key(entry_path) {
            let base_file_node = &base_entries[entry_path];
            if lca_entries.contains_key(entry_path) {
                let lca_file_node = &lca_entries[entry_path];
                // If Base and LCA are the same but Merge is different, take merge
                /*log::debug!(
                    "Comparing hashes merge_entry {:?} BASE {} LCA {} MERGE {}",
                    entry_path,
                    merge_file_node,
                    base_file_node,
                    lca_file_node,

                );*/
                if base_file_node.hash() == lca_file_node.hash()
                    && base_file_node.hash() != merge_file_node.hash()
                    && write_to_disk
                {
                    log::debug!("top update entry");
                    if restore::should_restore_file(
                        repo,
                        Some(base_file_node.clone()),
                        merge_file_node,
                        entry_path,
                    )? {
                        entries_to_restore.push(FileToRestore {
                            file_node: merge_file_node.clone(),
                            path: entry_path.clone(),
                        });
                    } else {
                        cannot_overwrite_entries.push(merge_entry.0.clone());
                    }
                }

                // If all three are different, mark as conflict
                if base_file_node.hash() != lca_file_node.hash()
                    && lca_file_node.hash() != merge_file_node.hash()
                    && base_file_node.hash() != merge_file_node.hash()
                {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: (lca_file_node.to_owned(), entry_path.to_path_buf()),
                        base_entry: (base_file_node.to_owned(), entry_path.to_path_buf()),
                        merge_entry: (merge_file_node.to_owned(), entry_path.to_path_buf()),
                    });
                }
            } else {
                // merge entry doesn't exist in LCA, so just check if it's different from base
                if base_file_node.hash() != merge_file_node.hash() {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: (base_file_node.to_owned(), entry_path.to_path_buf()),
                        base_entry: (base_file_node.to_owned(), entry_path.to_path_buf()),
                        merge_entry: (merge_file_node.to_owned(), entry_path.to_path_buf()),
                    });
                }
            }
        } else if write_to_disk {
            // merge entry does not exist in base, so create it
            log::debug!("bottom update entry");
            if restore::should_restore_file(repo, None, merge_file_node, entry_path)? {
                entries_to_restore.push(FileToRestore {
                    file_node: merge_file_node.clone(),
                    path: entry_path.to_path_buf(),
                });
            } else {
                cannot_overwrite_entries.push(entry_path.clone());
            }
        }
    }
    log::debug!("three_way_merge conflicts.len() {}", conflicts.len());

    // If there are no conflicts, restore the entries
    if cannot_overwrite_entries.is_empty() {
        let version_store = repo.version_store()?;
        for entry in entries_to_restore.iter() {
            restore::restore_file(repo, &entry.file_node, &entry.path, &version_store)?;
        }
    } else {
        // If there are conflicts, return an error without restoring anything
        return Err(OxenError::cannot_overwrite_files(&cannot_overwrite_entries));
    }

    Ok(conflicts)
}
