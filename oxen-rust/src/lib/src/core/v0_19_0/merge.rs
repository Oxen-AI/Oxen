use crate::config::UserConfig;
use crate::core::db;
pub use crate::core::merge::entry_merge_conflict_db_reader::EntryMergeConflictDBReader;
pub use crate::core::merge::node_merge_conflict_db_reader::NodeMergeConflictDBReader;
use crate::core::merge::node_merge_conflict_reader::NodeMergeConflictReader;
use crate::core::merge::{db_path, node_merge_conflict_writer};
use crate::core::refs::RefWriter;
use crate::core::v0_10_0::index::CommitWriter;
use crate::core::v0_19_0::commits::{get_commit_or_head, list_between};
use crate::core::v0_19_0::{add, rm, status};
use crate::error::OxenError;
use crate::model::merge_conflict::NodeMergeConflict;
use crate::model::merkle_tree::node::FileNode;
use crate::model::{Branch, Commit, LocalRepository};
use crate::repositories;
use crate::repositories::merge::MergeCommits;
use crate::util;

use rocksdb::DB;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;

use super::index::{self, CommitMerkleTree};

pub fn has_conflicts(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<bool, OxenError> {
    let base_commit =
        repositories::commits::get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit =
        repositories::commits::get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    can_merge_commits(repo, &base_commit, &merge_commit)
}

pub fn list_conflicts(repo: &LocalRepository) -> Result<Vec<NodeMergeConflict>, OxenError> {
    let reader = NodeMergeConflictReader::new(repo)?;
    reader.list_conflicts()
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
    list_between(repo, &lca, &head_commit)
}

pub fn list_commits_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!(
        "list_commits_between_commits() base: {:?} head: {:?}",
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
    println!(
        "merge_into_base merge {} into {}",
        merge_branch, base_branch
    );

    if merge_branch.commit_id == base_branch.commit_id {
        // If the merge branch is the same as the base branch, there is nothing to merge
        return Ok(None);
    }

    let base_commit = get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit = get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    let lca = lowest_common_ancestor_from_commits(repo, &base_commit, &merge_commit)?;
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
        "Updating {} -> {}",
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
        println!("Fast-forward");
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

    lowest_common_ancestor_from_commits(&repo, &base_commit, &merge_commit)
}

fn fast_forward_merge(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Commit, OxenError> {
    log::debug!("FF merge!");
    let base_commit_node = CommitMerkleTree::from_commit(repo, base_commit)?;
    let merge_commit_node = CommitMerkleTree::from_commit(repo, merge_commit)?;

    let base_entries =
        CommitMerkleTree::dir_entries_with_paths(&base_commit_node.root, &PathBuf::from(""))?;
    let merge_entries =
        CommitMerkleTree::dir_entries_with_paths(&merge_commit_node.root, &PathBuf::from(""))?;

    // Make sure files_db is dropped before the merge commit is written
    {
        // Can just copy over all new versions since it is fast forward
        for merge_entry in merge_entries.iter() {
            let (merge_file_node, merge_path) = merge_entry;
            log::debug!("Merge entry: {:?}", merge_path);
            // Only copy over if hash is different or it doesn't exist for performance
            if let Some(base_entry) = base_entries.get(merge_entry) {
                let (base_file_node, base_path) = base_entry;
                if base_file_node.hash != merge_file_node.hash {
                    log::debug!("Merge entry has changed, restore: {:?}", merge_path);
                    update_entry(repo, merge_entry)?;
                }
            } else {
                log::debug!("Merge entry is new, restore: {:?}", merge_path);
                update_entry(repo, merge_entry)?;
            }
        }

        // Remove all entries that are in HEAD but not in merge entries
        for base_entry in base_entries.iter() {
            let (base_file_node, base_path) = base_entry;
            log::debug!("Base entry: {:?}", base_path);
            if !merge_entries.iter().any(|entry| entry.1 == base_entry.1) {
                log::debug!("Removing Base Entry: {:?}", base_path);

                let path = repo.path.join(base_path);
                if path.exists() {
                    util::fs::remove_file(path)?;
                }
            }
        }
    }

    // Move the HEAD forward to this commit
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_head_commit_id(&merge_commit.id)?;

    Ok(merge_commit.clone())
}

fn merge_commits(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
) -> Result<Option<Commit>, OxenError> {
    // User output
    println!(
        "Updating {} -> {}",
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
        println!("Fast-forward");
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

    let status = status::status(repo)?;
    let commit_writer = CommitWriter::new(repo)?;
    let parent_ids: Vec<String> = vec![
        merge_commits.base.id.to_owned(),
        merge_commits.merge.id.to_owned(),
    ];
    let commit = commit_writer.commit_with_parent_ids(&status, parent_ids, &commit_msg)?;
    rm::remove_staged(repo, &HashSet::from([PathBuf::from("/")]))?;

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
    let status = status::status(repo)?;
    let commit_writer = CommitWriter::new(repo)?;
    let parent_ids: Vec<String> = vec![
        merge_commits.base.id.to_owned(),
        merge_commits.merge.id.to_owned(),
    ];

    // The author in this case is the pusher - the author of the merge commit

    let cfg = UserConfig {
        name: merge_commits.merge.author.clone(),
        email: merge_commits.merge.email.clone(),
    };

    let commit = commit_writer.commit_with_parent_ids_on_branch(
        &status,
        parent_ids,
        &commit_msg,
        branch.clone(),
        cfg,
    )?;

    rm::remove_staged(repo, &HashSet::from([PathBuf::from("/")]))?;

    Ok(commit)
}

pub fn lowest_common_ancestor_from_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Commit, OxenError> {
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

    // Read all the entries from each commit into sets we can compare to one another
    let lca_commit_node = CommitMerkleTree::from_commit(repo, &merge_commits.lca)?;
    let base_commit_node = CommitMerkleTree::from_commit(repo, &merge_commits.base)?;
    let merge_commit_node = CommitMerkleTree::from_commit(repo, &merge_commits.merge)?;

    let lca_entries =
        CommitMerkleTree::dir_entries_with_paths(&lca_commit_node.root, &PathBuf::from("/"))?;
    let base_entries =
        CommitMerkleTree::dir_entries_with_paths(&base_commit_node.root, &PathBuf::from("/"))?;
    let merge_entries =
        CommitMerkleTree::dir_entries_with_paths(&merge_commit_node.root, &PathBuf::from("/"))?;

    log::debug!("lca_entries.len() {}", lca_entries.len());
    log::debug!("base_entries.len() {}", base_entries.len());
    log::debug!("merge_entries.len() {}", merge_entries.len());

    // Check all the entries in the candidate merge
    for merge_entry in merge_entries.iter() {
        // log::debug!("Considering entry {}", merge_entries.len());
        // Check if the entry exists in all 3 commits
        if let Some(base_entry) = base_entries.get(merge_entry) {
            if let Some(lca_entry) = lca_entries.get(merge_entry) {
                // If Base and LCA are the same but Merge is different, take merge
                // log::debug!(
                //     "Comparing hashes merge_entry {:?} BASE {} LCA {} MERGE {}",
                //     merge_entry.path,
                //     base_entry.hash,
                //     lca_entry.hash,
                //     merge_entry.hash
                // );
                if base_entry.0.hash == lca_entry.0.hash
                    && base_entry.0.hash != merge_entry.0.hash
                    && write_to_disk
                {
                    log::debug!("top update entry");
                    update_entry(repo, merge_entry)?;
                }

                // If all three are different, mark as conflict
                if base_entry.0.hash != lca_entry.0.hash
                    && lca_entry.0.hash != merge_entry.0.hash
                    && base_entry.0.hash != merge_entry.0.hash
                {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: lca_entry.to_owned(),
                        base_entry: base_entry.to_owned(),
                        merge_entry: merge_entry.to_owned(),
                    });
                }
            } else {
                // merge entry doesn't exist in LCA, so just check if it's different from base
                if base_entry.0.hash != merge_entry.0.hash {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: base_entry.to_owned(),
                        base_entry: base_entry.to_owned(),
                        merge_entry: merge_entry.to_owned(),
                    });
                }
            }
        } else if write_to_disk {
            // merge entry does not exist in base, so create it
            log::debug!("bottom update entry");
            update_entry(repo, merge_entry)?;
        }
    }
    log::debug!("three_way_merge conflicts.len() {}", conflicts.len());

    Ok(conflicts)
}

fn update_entry(
    repo: &LocalRepository,
    merge_entry: &(FileNode, PathBuf),
) -> Result<(), OxenError> {
    let (file_node, path) = merge_entry;
    index::restore::restore_file(repo, file_node, path)?;
    Ok(())
}
