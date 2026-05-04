use crate::constants;
use crate::core::db;
use crate::core::db::merkle_node::MerkleNodeDB;
pub use crate::core::merge::entry_merge_conflict_db_reader::EntryMergeConflictDBReader;
pub use crate::core::merge::node_merge_conflict_db_reader::NodeMergeConflictDBReader;
use crate::core::merge::node_merge_conflict_reader::NodeMergeConflictReader;
use crate::core::merge::{db_path, node_merge_conflict_writer};
use crate::core::refs::with_ref_manager;
use crate::core::v_latest::add;
use crate::core::v_latest::branches::OnConflict;
use crate::core::v_latest::commits::{get_commit_or_head, list_between};
use crate::core::v_latest::merge_marker;
use crate::error::OxenError;
use crate::model::NewCommit;
use crate::model::StagedEntryStatus;
use crate::model::merge_conflict::NodeMergeConflict;
use crate::model::merkle_tree::node::CommitNode;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;
use crate::model::merkle_tree::node::commit_node::CommitNodeOpts;
use crate::model::merkle_tree::node::file_node::FileNode;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Branch, Commit, LocalRepository};
use crate::model::{MerkleHash, PartialNode};
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
// I.e., In walk_merge_commit, it contains merge commit files that are not present in or have changed from the base tree
// cannot_overwrite_entries: files that would be restored, but are modified from the from_tree, and thus would erase work if overwritten

// for walk_base_dir, the 'entries to restore' are actually entries being removed
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

/// Whether a merge is attached to a local working tree.
#[derive(Debug, Clone, Copy)]
pub enum LocalCheckout {
    /// Server-side merge: operates on tree data only and does not touch the working tree.
    Absent,
    /// Client-side merge: restores files on disk and advances HEAD, bracketed by MERGE_IN_PROGRESS.
    ///
    /// `is_resume` is true when a prior attempt at this target was interrupted; the restore path
    /// then force-restores instead of conflict-checking.
    Present { is_resume: bool },
}

impl LocalCheckout {
    pub fn writes_to_disk(self) -> bool {
        matches!(self, Self::Present { .. })
    }

    pub fn is_resume(self) -> bool {
        matches!(self, Self::Present { is_resume: true })
    }
}

/// Result of a three-way merge conflict analysis. Contains both the conflicts found and the files
/// from the merge branch that should be included in the merge result.
pub struct MergeConflictAnalysis {
    pub conflicts: Vec<NodeMergeConflict>,
    /// Files added, modified, or deleted on the merge branch (relative to LCA) that should be
    /// applied to the base tree. Each entry carries its `StagedEntryStatus`.
    pub entries: Vec<(PathBuf, FileNode, StagedEntryStatus)>,
}

pub async fn has_conflicts(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<bool, OxenError> {
    let base_commit =
        repositories::commits::get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit =
        repositories::commits::get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    let res = can_merge_commits(repo, &base_commit, &merge_commit).await?;
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
pub async fn can_merge_commits(
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

    let mut _hashes = HashSet::new();
    let analysis =
        find_merge_conflicts(repo, &merge_commits, LocalCheckout::Absent, &mut _hashes).await?;
    Ok(analysis.conflicts.is_empty())
}

pub async fn list_conflicts_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<Vec<PathBuf>, OxenError> {
    let base_commit = get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit = get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    list_conflicts_between_commits(repo, &base_commit, &merge_commit).await
}

pub fn list_commits_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    head_branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!("list_commits_between_branches() base: {base_branch:?} head: {head_branch:?}");
    let base_commit = get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let head_commit = get_commit_or_head(repo, Some(head_branch.commit_id.clone()))?;

    let Some(lca) = lowest_common_ancestor_from_commits(repo, &base_commit, &head_commit)? else {
        return Err(OxenError::basic_str(format!(
            "Error: head commit {:?} and base commit {:?} have no common ancestor",
            head_commit.id, base_commit.id
        )));
    };

    log::debug!(
        "list_commits_between_branches {base_commit:?} -> {head_commit:?} found lca {lca:?}"
    );
    list_between(repo, &lca, &head_commit)
}

pub fn list_commits_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!("list_commits_between_commits()\nbase: {base_commit}\nhead: {head_commit}");

    let Some(lca) = lowest_common_ancestor_from_commits(repo, base_commit, head_commit)? else {
        return Err(OxenError::basic_str(format!(
            "Error: head commit {:?} and base commit {:?} have no common ancestor",
            head_commit.id, base_commit.id
        )));
    };

    log::debug!("For commits {base_commit:?} -> {head_commit:?} found lca {lca:?}");

    log::debug!("Reading history from lca to head");
    list_between(repo, &lca, head_commit)
}

pub async fn list_conflicts_between_commits(
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
    let mut _hashes = HashSet::new();
    let analysis =
        find_merge_conflicts(repo, &merge_commits, LocalCheckout::Absent, &mut _hashes).await?;
    Ok(analysis
        .conflicts
        .iter()
        .map(|c| {
            let (_, path) = &c.base_entry;
            path.to_owned()
        })
        .collect())
}

/// Server-side merge: merge a branch into a base branch.
/// Updates the base branch ref on success. Does not modify the working directory or HEAD.
///
/// See the docs for merge_commits for details on how three-way merging works, including definitions
/// of the terms used.
///
/// # Errors
/// - `OxenError::UpstreamMergeConflict` if the branches have conflicting changes.
/// - Other `OxenError` variants for internal failures (missing commits, tree corruption, etc.).
pub async fn merge_into_base(
    repo: &LocalRepository,
    merge_branch: &Branch,
    base_branch: &Branch,
) -> Result<Commit, OxenError> {
    log::debug!("merge_into_base merge {merge_branch} into {base_branch}");

    let base_commit = get_commit_or_head(repo, Some(base_branch.commit_id.clone()))?;
    let merge_commit = get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;

    let lca = lowest_common_ancestor_from_commits(repo, &base_commit, &merge_commit)?
        .ok_or_else(|| OxenError::basic_str("Cannot merge branches with no common ancestor"))?;
    log::debug!("merge_into_base base: {base_commit:?} merge: {merge_commit:?} lca: {lca:?}");

    let commits = MergeCommits {
        lca: Some(lca),
        base: base_commit,
        merge: merge_commit,
    };

    let result = if commits.is_already_up_to_date() {
        // Merge branch is an ancestor of base (or equal tips) — base already contains everything
        // from merge. Mirrors `git merge`'s "Already up to date" outcome: no merge commit, base
        // unchanged.
        Ok(commits.base.clone())
    } else if commits.is_fast_forward_merge() {
        Ok(commits.merge)
    } else {
        server_three_way_merge(repo, &commits).await
    };

    if let Ok(ref commit) = result {
        repositories::branches::update(repo, &base_branch.name, &commit.id)?;
    }

    result
}

/// Server-side three-way merge: creates a merge commit from tree data without touching the working
/// directory. Returns `Err(UpstreamMergeConflict)` if there are conflicts.
async fn server_three_way_merge(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
) -> Result<Commit, OxenError> {
    log::debug!(
        "server_three_way_merge: base commit {} -> merge commit {}",
        merge_commits.base.id,
        merge_commits.merge.id
    );

    // 1. Find conflicts and collect merge entries in a single tree traversal
    let mut shared_hashes = HashSet::new();
    let analysis = find_merge_conflicts(
        repo,
        merge_commits,
        LocalCheckout::Absent,
        &mut shared_hashes,
    )
    .await?;

    if !analysis.conflicts.is_empty() {
        return Err(OxenError::UpstreamMergeConflict(
            format!(
                "Unable to merge {} into {} due to {} conflicts.",
                merge_commits.merge.id,
                merge_commits.base.id,
                analysis.conflicts.len()
            )
            .into(),
        ));
    }

    if analysis.entries.is_empty() {
        // The merge branch contributes no new content on top of base — its changes are already
        // present in base via a separate path (e.g. base squash-replayed the merge branch). Build
        // an empty merge commit (two parents, base's tree) so the merge is preserved in commit
        // history. Mirrors `git merge`'s behavior on a 3-way merge with no content delta.
        log::info!(
            "server_three_way_merge: merge {} contributes no new content to base {}; creating empty merge commit",
            merge_commits.merge.id,
            merge_commits.base.id,
        );
        return create_empty_merge_commit(repo, merge_commits);
    }

    // 2. Build dir_entries HashMap (parent dir -> staged nodes) for the commit writer
    let mut dir_entries: HashMap<PathBuf, Vec<StagedMerkleTreeNode>> = HashMap::new();
    for (path, file_node, status) in &analysis.entries {
        let parent = path.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
        // The commit writer's existing children use full relative paths (e.g. "data/b.txt") as
        // their names, so our staged entries must match.
        let mut named_node = file_node.clone();
        named_node.set_name(path.to_str().unwrap());
        dir_entries
            .entry(parent)
            .or_default()
            .push(StagedMerkleTreeNode {
                status: status.clone(),
                node: MerkleTreeNode::from_file(named_node),
            });
        // Ensure all ancestor directories are present in dir_entries
        let mut ancestor = path.to_path_buf();
        while let Some(p) = ancestor.parent() {
            ancestor = p.to_path_buf();
            dir_entries.entry(ancestor.clone()).or_default();
            if ancestor == Path::new("") {
                break;
            }
        }
    }

    // TODO: This is reading the server's local user config, but we should use the user/email
    // that initiated the merge request. If initiated from the client, the client should send it's
    // local user. If initiated from the hub, the hub should send the user/email of the user who
    // initiated the merge request.
    let cfg = crate::config::UserConfig::get()?;
    let new_commit = crate::model::NewCommitBody {
        message: merge_commits.commit_message(),
        author: cfg.name.clone(),
        email: cfg.email.clone(),
    };

    let parent_ids = vec![
        merge_commits.base.id.clone(),
        merge_commits.merge.id.clone(),
    ];

    // Pass the base commit ID as the target revision so the existing tree comes from the base
    // commit (revisions::get resolves commit IDs)
    let progress = indicatif::ProgressBar::hidden();
    let commit = commit_writer::commit_dir_entries_with_parents(
        repo,
        parent_ids,
        dir_entries,
        &new_commit,
        &progress,
        &merge_commits.base.id,
    )?;

    Ok(commit)
}

/// Client-side merge that alters the local checkout. Merge into the current branch. Returns the
/// merge commit if successful, and None if there are conflicts
pub async fn merge(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let branch_name = branch_name.as_ref();

    let merge_branch = repositories::branches::get_by_name(repo, branch_name)?;

    let base_commit = repositories::commits::head_commit(repo)?;
    let merge_commit = get_commit_or_head(repo, Some(merge_branch.commit_id.clone()))?;
    let lca = lowest_common_ancestor_from_commits(repo, &base_commit, &merge_commit)?
        .ok_or_else(|| OxenError::basic_str("Cannot merge branches with no common ancestor"))?;
    let commits = MergeCommits {
        lca: Some(lca),
        base: base_commit,
        merge: merge_commit,
    };
    merge_commits(repo, &commits, LocalCheckout::Present { is_resume: false }).await
}

/// Server-safe merge of two commits. Does not touch the working directory or
/// HEAD — the caller is responsible for updating the branch ref.
///
/// Use this variant from server code paths that must not mutate on-disk files.
/// For the client-side equivalent that updates the checkout and HEAD, see
/// [`merge_commit_into_base`].
pub async fn merge_commit_into_base_server_safe(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
) -> Result<Option<Commit>, OxenError> {
    let maybe_lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?;
    log::debug!(
        "merge_commit_into_base_server_safe has lca {maybe_lca:?} for merge commit {merge_commit:?} and base {base_commit:?}"
    );

    let commits = MergeCommits {
        lca: maybe_lca,
        base: base_commit.to_owned(),
        merge: merge_commit.to_owned(),
    };

    merge_commits(repo, &commits, LocalCheckout::Absent).await
}

/// Client-side merge of two commits. Updates files on disk and advances HEAD.
///
/// For the server-side equivalent that never touches the working directory,
/// see [`merge_commit_into_base_server_safe`].
pub async fn merge_commit_into_base(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
) -> Result<Option<Commit>, OxenError> {
    let maybe_lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?;
    log::debug!(
        "merge_commit_into_base has lca {maybe_lca:?} for merge commit {merge_commit:?} and base {base_commit:?}"
    );

    let commits = MergeCommits {
        lca: maybe_lca,
        base: base_commit.to_owned(),
        merge: merge_commit.to_owned(),
    };

    merge_commits(repo, &commits, LocalCheckout::Present { is_resume: false }).await
}

/// Server-side merge: merge a commit into a base commit on a specific branch.
/// Updates the branch ref on success. Does not modify the working directory or HEAD.
///
/// # Errors
/// - `OxenError::UpstreamMergeConflict` if the branches have conflicting changes.
/// - Other `OxenError` variants for internal failures (missing commits, tree corruption, etc.).
pub async fn merge_commit_into_base_on_branch(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
    branch: &Branch,
) -> Result<Commit, OxenError> {
    let lca = lowest_common_ancestor_from_commits(repo, base_commit, merge_commit)?
        .ok_or_else(|| OxenError::basic_str("Cannot merge commits with no common ancestor"))?;

    log::debug!(
        "merge_commit_into_branch has lca {lca:?} for merge commit {merge_commit:?} and base {base_commit:?}"
    );

    let merge_commits = MergeCommits {
        lca: Some(lca),
        base: base_commit.to_owned(),
        merge: merge_commit.to_owned(),
    };

    let result = if merge_commits.is_already_up_to_date() {
        // Merge is an ancestor of base (or equal tips) — base already contains everything from
        // merge. Return base unchanged rather than fabricating an empty merge commit via
        // `server_three_way_merge`. Mirrors `git merge`'s "Already up to date" outcome.
        Ok(merge_commits.base.clone())
    } else if merge_commits.is_fast_forward_merge() {
        Ok(merge_commits.merge)
    } else {
        server_three_way_merge(repo, &merge_commits).await
    };

    if let Ok(ref commit) = result {
        repositories::branches::update(repo, &branch.name, &commit.id)?;
    }

    result
}

pub fn has_file(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    let db_path = db_path(repo);
    log::debug!("Merger::new() DB {db_path:?}");
    let opts = db::key_val::opts::default();
    let merge_db = DB::open(&opts, dunce::simplified(&db_path))?;

    NodeMergeConflictDBReader::has_file(&merge_db, path)
}

pub fn remove_conflict_path(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    let db_path = db_path(repo);
    log::debug!("Merger::new() DB {db_path:?}");
    let opts = db::key_val::opts::default();
    let merge_db = DB::open(&opts, dunce::simplified(&db_path))?;

    let path_str = path.to_str().unwrap();
    let key = path_str.as_bytes();
    merge_db.delete(key)?;
    Ok(())
}

/// Abandon an interrupted or in-conflict merge, restoring the working tree to HEAD
/// and clearing every piece of merge state on disk.
///
/// Returns `OxenError::NoMergeInProgress` if there is nothing to abort.
pub async fn abort_merge(repo: &LocalRepository) -> Result<(), OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let merge_head_path = hidden_dir.join(constants::MERGE_HEAD_FILE);
    let orig_head_path = hidden_dir.join(constants::ORIG_HEAD_FILE);
    let conflict_db_path = db_path(repo);

    // Gather the identifier of whatever target the in-progress merge was
    // restoring toward. Either the MERGE_IN_PROGRESS marker (fast-forward
    // mid-restore) or MERGE_HEAD (3-way conflict state) will tell us. If
    // neither is present, there's no merge to abort. (The conflicts DB
    // directory can exist as an empty rocksdb dir left over from a previous
    // merge, so it is not a reliable "in progress" signal on its own.)
    let target_id = match merge_marker::read(repo).await? {
        Some(id) => Some(id),
        None if merge_head_path.exists() => Some(
            util::fs::read_from_path(&merge_head_path)?
                .trim()
                .to_string(),
        ),
        None => None,
    };

    if target_id.is_none() {
        return Err(OxenError::NoMergeInProgress);
    }

    let head_commit = repositories::commits::head_commit(repo)?;

    // If we know what the working tree was being pushed toward, synthesize that
    // as the "from" commit so checkout_commit performs the reverse restore.
    let from_commit = target_id
        .as_deref()
        .and_then(|id| repositories::commits::get_by_id(repo, id).ok().flatten());

    repositories::branches::checkout_commit_from_commit(
        repo,
        &head_commit,
        &from_commit,
        OnConflict::Overwrite,
    )
    .await?;

    merge_marker::clear(repo).await?;
    if merge_head_path.exists() {
        tokio::fs::remove_file(&merge_head_path).await?;
    }
    if orig_head_path.exists() {
        tokio::fs::remove_file(&orig_head_path).await?;
    }
    if conflict_db_path.exists() {
        tokio::fs::remove_dir_all(&conflict_db_path).await?;
    }

    println!("Merge aborted. HEAD is {}", head_commit.id);
    Ok(())
}

pub fn find_merge_commits<S: AsRef<str>>(
    repo: &LocalRepository,
    branch_name: S,
) -> Result<MergeCommits, OxenError> {
    let branch_name = branch_name.as_ref();

    let current_branch = repositories::branches::current_branch(repo)?
        .ok_or_else(|| OxenError::basic_str("No current branch"))?;

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

/// Check if HEAD is in the direct parent chain of the merge commit. If it is a direct parent, we can just fast forward
pub fn lowest_common_ancestor(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let branch_name = branch_name.as_ref();
    let current_branch = repositories::branches::current_branch(repo)?
        .ok_or_else(|| OxenError::basic_str("No current branch"))?;

    let base_commit =
        repositories::commits::get_commit_or_head(repo, Some(current_branch.name.clone()))?;
    let merge_commit = repositories::commits::get_commit_or_head(repo, Some(branch_name))?;

    lowest_common_ancestor_from_commits(repo, &base_commit, &merge_commit)
}

/// Fast-forward merge.
///
/// When `update_working_dir` is `true` (client-side), the function checks for
/// uncommitted local changes that would be overwritten, restores/removes files
/// on disk, and advances HEAD.
///
/// When `update_working_dir` is `false` (server-side), no working-directory or
/// HEAD operations are performed — only the merge commit is returned so the
/// caller can update the branch ref.
async fn fast_forward_merge(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
    checkout: LocalCheckout,
) -> Result<Option<Commit>, OxenError> {
    log::debug!("FF merge!");

    if base_commit == merge_commit {
        // If the base commit is the same as the merge commit, there is nothing to merge
        return Ok(None);
    }

    if !checkout.writes_to_disk() {
        // Server-side: no checkout to update, just return the merge commit.
        return Ok(Some(merge_commit.clone()));
    }

    let is_resume = checkout.is_resume();

    // Collect all dir and vnode hashes while loading the merge tree
    // This is done to identify shared dirs/vnodes between the merge and base trees while loading the base tree
    let mut merge_hashes = HashSet::new();
    let Some(merge_tree) = repositories::tree::get_root_with_children_and_node_hashes(
        repo,
        merge_commit,
        None,
        Some(&mut merge_hashes),
        None,
    )?
    else {
        return Err(OxenError::basic_str("Cannot get root node for base commit"));
    };

    // Collect every shared dir/vnode hash between the trees, load the base tree's unique nodes and collect them as 'partial nodes'
    // These are done to skip checks for shared dirs/vnodes and avoid slow tree traversals when comparing files in the recursvie functions respectively
    let mut shared_hashes = HashSet::new();
    let mut partial_nodes = HashMap::new();
    let Some(base_tree) = repositories::tree::get_root_with_children_and_partial_nodes(
        repo,
        base_commit,
        Some(&merge_hashes),
        None,
        Some(&mut shared_hashes),
        &mut partial_nodes,
    )?
    else {
        return Err(OxenError::basic_str(
            "Cannot get root node for merge commit",
        ));
    };

    // Stop early if there are conflicts
    let (merge_tree_results, seen_files) =
        walk_merge_commit(repo, &merge_tree, &partial_nodes, &shared_hashes, is_resume).await?;

    if !merge_tree_results.cannot_overwrite_entries.is_empty() {
        return Err(OxenError::cannot_overwrite_files(
            &merge_tree_results.cannot_overwrite_entries,
        ));
    }

    let base_tree_results =
        walk_base_dir(repo, &base_tree, &shared_hashes, &seen_files, is_resume).await?;

    // If there are no conflicts, restore the entries
    // Grouping the processing of merge_tree_results and base_tree_results like this ensures no files are modified if the merge doesn't complete
    if base_tree_results.cannot_overwrite_entries.is_empty() {
        // All conflict checks have passed; the next lines mutate the working
        // tree. Write the resume marker *now* so a SIGTERM mid-restore is
        // recoverable, but no marker is left behind when the merge errors out
        // earlier for an unrelated reason.
        merge_marker::write(repo, &merge_commit.id).await?;

        let version_store = repo.version_store()?;
        for entry in merge_tree_results.entries_to_restore.iter() {
            restore::restore_file(repo, &entry.file_node, &entry.path, &version_store).await?;
        }

        // TODO: Make a new struct called 'BaseResults' that's exactly like MergeResults, but with 'entries_to_remove' instead
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

    // Mutation complete; the marker is no longer load-bearing.
    merge_marker::clear(repo).await?;

    Ok(Some(merge_commit.clone()))
}

/// Walk the merge tree and decide, file by file, whether each entry can be safely restored
/// from the version store or whether the working copy looks like a local edit we shouldn't
/// stomp on. Returns the populated `MergeResult` plus the set of paths actually seen on the
/// merge side, which `walk_base_dir` consumes to find files HEAD has but the merge target
/// doesn't (i.e., deletions to apply).
async fn walk_merge_commit<'a>(
    repo: &LocalRepository,
    root: &'a MerkleTreeNode,
    base_files: &HashMap<PathBuf, PartialNode>,
    shared_hashes: &HashSet<MerkleHash>,
    is_resume: bool,
) -> Result<(MergeResult, HashSet<PathBuf>), OxenError> {
    let mut results = MergeResult::new();
    let mut seen_files: HashSet<PathBuf> = HashSet::new();
    let mut stack: Vec<(PathBuf, &'a MerkleTreeNode)> = vec![(PathBuf::new(), root)];

    while let Some((path, node)) = stack.pop() {
        match &node.node {
            EMerkleTreeNode::File(merge_file_node) => {
                let file_path = path.join(merge_file_node.name());
                seen_files.insert(file_path.clone());

                // If file_path is found in the base tree, look up the corresponding PartialNode —
                // the minimal representation needed to decide whether to restore. PartialNodes are
                // keyed by base-tree path so that a file moved between base and merge is correctly
                // detected as "shouldn't restore".
                if is_resume {
                    // Resuming an interrupted merge targeting this same commit: trust the target
                    // and force-restore every file the merge wants, regardless of working-tree state.
                    results.entries_to_restore.push(FileToRestore {
                        file_node: merge_file_node.clone(),
                        path: file_path.clone(),
                    });
                } else if let Some(base_file_node) = base_files.get(&file_path) {
                    if node.hash != base_file_node.hash {
                        let should_restore = restore::should_restore_partial_node(
                            repo,
                            Some(base_file_node.clone()),
                            merge_file_node,
                            &file_path,
                        )
                        .await?;
                        if should_restore {
                            results.entries_to_restore.push(FileToRestore {
                                file_node: merge_file_node.clone(),
                                path: file_path.clone(),
                            });
                        } else {
                            results.cannot_overwrite_entries.push(file_path.clone());
                        }
                    } else {
                        // Merge target matches base for this path — nothing to apply, so a
                        // locally modified working copy is not an overwrite conflict.
                        log::debug!("Merge entry has not changed: {file_path:?}");
                    }
                } else if restore::should_restore_file(repo, None, merge_file_node, &file_path)
                    .await?
                {
                    results.entries_to_restore.push(FileToRestore {
                        file_node: merge_file_node.clone(),
                        path: file_path.clone(),
                    });
                } else {
                    results.cannot_overwrite_entries.push(file_path.clone());
                }
            }
            EMerkleTreeNode::Directory(dir_node) => {
                // Early exit if the directory is the same in the from and target trees.
                if shared_hashes.contains(&node.hash) {
                    continue;
                }
                let dir_path = path.join(dir_node.name());
                // Only enqueue children of vnodes not shared between the trees.
                for vnode in &node.children {
                    if !shared_hashes.contains(&vnode.hash) {
                        for child in &vnode.children {
                            log::debug!("walk_merge_commit child_path {child}");
                            stack.push((dir_path.clone(), child));
                        }
                    }
                }
            }
            EMerkleTreeNode::Commit(_) => {
                // Skip the commit wrapper to its root directory.
                let root_dir = repositories::tree::get_root_dir(node)?;
                stack.push((path, root_dir));
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Got an unexpected node type during checkout",
                ));
            }
        }
    }

    Ok((results, seen_files))
}

/// Walk the base (HEAD) tree to find files that exist in HEAD but not in the merge target —
/// those are deletions the FF merge must apply on disk. Same iterative async depth-first
/// search shape as `walk_merge_commit`. `merge_files` is the set of paths the merge walker
/// visited.
async fn walk_base_dir<'a>(
    repo: &LocalRepository,
    root: &'a MerkleTreeNode,
    shared_hashes: &HashSet<MerkleHash>,
    merge_files: &HashSet<PathBuf>,
    is_resume: bool,
) -> Result<MergeResult, OxenError> {
    let mut results = MergeResult::new();
    let mut stack: Vec<(PathBuf, &'a MerkleTreeNode)> = vec![(PathBuf::new(), root)];

    while let Some((path, node)) = stack.pop() {
        match &node.node {
            EMerkleTreeNode::File(base_file_node) => {
                let file_path = path.join(base_file_node.name());
                // Only consider paths in HEAD that aren't also in the merge tree — those are
                // the deletions to apply.
                if !merge_files.contains(&file_path) {
                    let full_path = repo.path.join(&file_path);
                    if full_path.exists() {
                        if is_resume
                            || restore::should_restore_file(repo, None, base_file_node, &file_path)
                                .await?
                        {
                            results.entries_to_restore.push(FileToRestore {
                                file_node: base_file_node.clone(),
                                path: full_path,
                            });
                        } else {
                            results.cannot_overwrite_entries.push(file_path);
                        }
                    }
                }
            }
            EMerkleTreeNode::Directory(dir_node) => {
                if shared_hashes.contains(&node.hash) {
                    continue;
                }
                let dir_path = path.join(dir_node.name());
                for vnode in &node.children {
                    if !shared_hashes.contains(&vnode.hash) {
                        for child in &vnode.children {
                            stack.push((dir_path.clone(), child));
                        }
                    }
                }
            }
            EMerkleTreeNode::Commit(_) => {
                let root_dir = repositories::tree::get_root_dir(node)?;
                stack.push((path, root_dir));
            }
            _ => {
                log::debug!("walk_base_dir unknown node type");
            }
        }
    }

    Ok(results)
}

/// Perform a merge between commits.
///
/// With `LocalCheckout::Present { .. }` (client-side), working-directory files
/// are checked, restored/removed, and HEAD is advanced.
///
/// With `LocalCheckout::Absent` (server-side), no working-directory or HEAD
/// operations are performed. For three-way merges the server-safe
/// `server_three_way_merge` path is used instead.
async fn merge_commits(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
    checkout: LocalCheckout,
) -> Result<Option<Commit>, OxenError> {
    // User output
    println!(
        "Merge commits {} -> {}",
        merge_commits.base.id, merge_commits.merge.id
    );

    log::debug!(
        "FOUND MERGE COMMITS:\nLCA: {} -> {}\nBASE: {} -> {}\nMerge: {} -> {}",
        merge_commits.lca.as_ref().map_or("None", |c| c.id.as_str()),
        merge_commits
            .lca
            .as_ref()
            .map_or("None", |c| c.message.as_str()),
        merge_commits.base.id,
        merge_commits.base.message,
        merge_commits.merge.id,
        merge_commits.merge.message,
    );

    // Inspect the resume marker. If a prior mutation was interrupted for this
    // same target, we'll force-restore merge-target files; if it named a
    // different target, abort. The marker itself is written/cleared inside the
    // restore loops (fast_forward_merge / find_merge_conflicts) so that a
    // merge which errors out cleanly on the conflict check never leaves a
    // stale marker behind.
    let checkout = if checkout.writes_to_disk() {
        match merge_marker::read(repo).await? {
            Some(existing) if existing != merge_commits.merge.id => {
                return Err(OxenError::MergeInProgressMismatch {
                    expected: existing,
                    found: merge_commits.merge.id.clone(),
                });
            }
            Some(_) => {
                log::info!(
                    "Resuming interrupted merge for target commit {}",
                    merge_commits.merge.id
                );
                LocalCheckout::Present { is_resume: true }
            }
            None => LocalCheckout::Present { is_resume: false },
        }
    } else {
        LocalCheckout::Absent
    };

    // Check which type of merge we need to do.
    // "Already up to date" must be checked before the fast-forward case: when base == merge both
    // predicates are true, and we want the no-op outcome (return base unchanged) rather than
    // calling `fast_forward_merge`, which would return Ok(None).
    if merge_commits.is_already_up_to_date() {
        // Merge branch is an ancestor of base (or equal tips) — `git merge`'s "Already up to
        // date" outcome. No merge commit, working tree and HEAD unchanged.
        println!("Already up to date.");
        Ok(Some(merge_commits.base.clone()))
    } else if merge_commits.is_fast_forward_merge() {
        let commit =
            fast_forward_merge(repo, &merge_commits.base, &merge_commits.merge, checkout).await?;
        Ok(commit)
    } else {
        log::debug!(
            "Three way merge! {} -> {}",
            merge_commits.base.id,
            merge_commits.merge.id
        );

        if !checkout.writes_to_disk() {
            // Server-safe: use the server three-way merge path which operates
            // only on tree data and never touches the working directory.
            return server_three_way_merge(repo, merge_commits).await.map(Some);
        }

        let mut shared_hashes = HashSet::new();
        let analysis =
            find_merge_conflicts(repo, merge_commits, checkout, &mut shared_hashes).await?;

        if !analysis.conflicts.is_empty() {
            println!(
                r"
Found {} conflicts, please resolve them before merging.

  oxen checkout --theirs path/to/file_1.txt
  oxen checkout --ours path/to/file_2.txt
  oxen add path/to/file_1.txt path/to/file_2.txt
  oxen commit -m 'Merge conflict resolution'

",
                analysis.conflicts.len()
            );
        }

        log::debug!("Got {} conflicts", analysis.conflicts.len());

        if analysis.conflicts.is_empty() {
            let commit = if analysis.entries.is_empty() {
                // Same case as server_three_way_merge: merge contributes no new content on top of
                // base. Build an empty merge commit aliasing base's tree (matches `git merge`'s
                // 3-way no-delta behavior). Skips the staging path since there's nothing to stage,
                // and advances HEAD afterward since `commit_dir_entries_with_parents` and the
                // empty-merge-commit helper don't update HEAD on their own.
                let commit = create_empty_merge_commit(repo, merge_commits)?;
                with_ref_manager(repo, |manager| manager.set_head_commit_id(&commit.id))?;
                commit
            } else {
                create_merge_commit(repo, merge_commits, shared_hashes).await?
            };
            Ok(Some(commit))
        } else {
            let db_path = db_path(repo);
            log::debug!("Merger::new() DB {db_path:?}");
            let opts = db::key_val::opts::default();
            let merge_db = DB::open(&opts, dunce::simplified(&db_path))?;

            node_merge_conflict_writer::write_conflicts_to_disk(
                repo,
                &merge_db,
                &merge_commits.merge,
                &merge_commits.base,
                &analysis.conflicts,
            )?;
            Ok(None)
        }
    }
}

async fn create_merge_commit(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
    shared_hashes: HashSet<MerkleHash>,
) -> Result<Commit, OxenError> {
    let base_commit = merge_commits.base.clone();
    add::add_dir_except(repo, &Some(base_commit), repo.path.clone(), shared_hashes).await?;

    let commit_msg = merge_commits.commit_message();
    log::debug!("create_merge_commit {commit_msg}");

    let parent_ids: Vec<String> = vec![
        merge_commits.base.id.to_owned(),
        merge_commits.merge.id.to_owned(),
    ];

    let commit = commit_writer::commit_with_parent_ids(repo, &commit_msg, parent_ids)?;

    // rm::remove_staged(repo, &HashSet::from([PathBuf::from("/")]))?;

    Ok(commit)
}

/// Create an empty merge commit: a commit with two parents whose tree is identical to base's
/// tree. Used when a 3-way merge analysis produces no entries (the merge branch contributes no
/// new content on top of base) but the merge isn't a fast-forward / "already up to date" case
/// either. Mirrors `git merge`'s empty-merge-commit behavior on a 3-way merge with no content
/// delta.
///
/// This aliases base's existing root DirNode by hash — Merkle storage is content-addressed, so
/// the new commit and base share the underlying tree storage. Only a new CommitNode and a copy
/// of base's `dir_hash_db` are written. The caller is responsible for advancing the appropriate
/// ref (server-side: the base branch ref via `merge_into_base`; client-side: HEAD via the
/// `with_ref_manager` helper).
fn create_empty_merge_commit(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
) -> Result<Commit, OxenError> {
    let cfg = crate::config::UserConfig::get()?;
    let timestamp = time::OffsetDateTime::now_utc();
    let new_commit_data = NewCommit {
        parent_ids: vec![
            merge_commits.base.id.clone(),
            merge_commits.merge.id.clone(),
        ],
        message: merge_commits.commit_message(),
        author: cfg.name.clone(),
        email: cfg.email.clone(),
        timestamp,
    };
    let commit_id = commit_writer::compute_commit_id(&new_commit_data)?;

    let base_commit_hash: MerkleHash = merge_commits.base.id.parse()?;
    let merge_commit_hash: MerkleHash = merge_commits.merge.id.parse()?;

    let commit_node = CommitNode::new(
        repo,
        CommitNodeOpts {
            hash: commit_id,
            // CommitNode.parent_ids holds parent commit hashes (matches `create_empty_commit`
            // in core/v_latest/commits.rs).
            parent_ids: vec![base_commit_hash, merge_commit_hash],
            email: cfg.email.clone(),
            author: cfg.name.clone(),
            message: merge_commits.commit_message(),
            timestamp,
        },
    )?;

    // Open the new commit's MerkleNodeDB and add base's existing root DirNode as the only child.
    // Tree is content-addressed, so the new commit's tree equals base's tree without any tree
    // rebuild or copy.
    let base_node = repositories::tree::get_node_by_id_with_children(repo, &base_commit_hash)?
        .ok_or_else(|| {
            OxenError::resource_not_found(format!(
                "Merkle tree node not found for base commit '{}'",
                merge_commits.base.id
            ))
        })?;
    let mut commit_db = MerkleNodeDB::open_read_write(repo, &commit_node, Some(base_node.hash))?;
    let root_dir = base_node
        .children
        .first()
        .expect("base commit must have a root dir as its first child")
        .dir()?;
    commit_db.add_child(&root_dir)?;

    // Copy base's path -> dir hash mapping so path-based tree lookups work for the new commit.
    repositories::tree::cp_dir_hashes_to(repo, &base_commit_hash, commit_node.hash())?;

    Ok(commit_node.to_commit())
}

pub fn lowest_common_ancestor_from_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Option<Commit>, OxenError> {
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

    // If the commits have no common ancestor (new repository), return None
    let mut has_common_ancestor = false;
    let mut min_depth = usize::MAX;
    let mut lca: Commit = commit_depths_from_head.keys().next().unwrap().clone();
    for (commit, _) in commit_depths_from_merge.iter() {
        if let Some(depth) = commit_depths_from_head.get(commit) {
            has_common_ancestor = true;

            if depth < &min_depth {
                min_depth = *depth;
                log::debug!("setting new lca, {commit:?}");
                lca = commit.clone();
            }
        }
    }

    if has_common_ancestor {
        Ok(Some(lca))
    } else {
        Ok(None)
    }
}

/// Analyze a three-way merge between commits. Always returns conflicts and files changed on the
/// merge branch. When `write_to_disk` is true, also restores non-conflicting merge files to the
/// working directory (for client-side merges).
pub async fn find_merge_conflicts(
    repo: &LocalRepository,
    merge_commits: &MergeCommits,
    checkout: LocalCheckout,
    shared_hashes: &mut HashSet<MerkleHash>,
) -> Result<MergeConflictAnalysis, OxenError> {
    log::debug!("finding merge conflicts");
    let write_to_disk = checkout.writes_to_disk();
    let is_resume = checkout.is_resume();
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

    use StagedEntryStatus::*;

    let mut conflicts: Vec<NodeMergeConflict> = vec![];
    let mut entries: Vec<(PathBuf, FileNode, StagedEntryStatus)> = vec![];
    let mut entries_to_restore: Vec<FileToRestore> = vec![];
    let mut cannot_overwrite_entries: Vec<PathBuf> = vec![];

    // Read all the entries from each commit into sets we can compare to one another
    let mut lca_hashes = HashSet::new();
    let mut base_hashes = HashSet::new();

    // Load in every node from the LCA tree (if there is one)
    let lca_commit_tree = if let Some(lca) = &merge_commits.lca {
        repositories::tree::get_root_with_children_and_node_hashes(
            repo,
            lca,
            None,
            Some(&mut lca_hashes),
            None,
        )?
    } else {
        None
    };

    // Then, load in only the nodes of the base commit tree that weren't in the LCA tree
    let base_commit_tree = repositories::tree::get_root_with_children_and_node_hashes(
        repo,
        &merge_commits.base,
        Some(&lca_hashes),
        Some(&mut base_hashes),
        Some(shared_hashes),
    )?
    .unwrap();

    // Then, we load in only the nodes of the merge tree that weren't in the Base tree (or the LCA tree)
    // After this, 'shared hashes' will have all the dir/vnode hashes shared between all 3 trees
    // This lets us skip checking these nodes, and also lets us skip adding them when creating the merge commit later

    let merge_commit_tree = repositories::tree::get_root_with_children_and_node_hashes(
        repo,
        &merge_commits.merge,
        Some(&base_hashes),
        None,
        Some(shared_hashes),
    )?
    .unwrap();

    // Note: Remove this unless debugging
    // log::debug!("lca_hashes: {lca_hashes:?}");
    //lca_commit_tree.print();
    // log::debug!("base_hashes: {base_hashes:?}");
    //base_commit_tree.print();
    // log::debug!("merge_hashes: {merge_hashes:?}");

    let starting_path = PathBuf::from("");

    // Walk the full LCA tree without the shared_hashes filter. The LCA tree is fully loaded
    // (no exclusions), and pruning it would hide files in directories shared between LCA and
    // base — causing missed deletions and incorrect "not in LCA" classifications.
    let no_filter = HashSet::new();
    let lca_entries = if let Some(lca_tree) = &lca_commit_tree {
        repositories::tree::unique_dir_entries(&starting_path, lca_tree, &no_filter)?
    } else {
        HashMap::new()
    };
    let base_entries =
        repositories::tree::unique_dir_entries(&starting_path, &base_commit_tree, shared_hashes)?;
    let merge_tree_entries =
        repositories::tree::unique_dir_entries(&starting_path, &merge_commit_tree, shared_hashes)?;

    log::debug!("lca_entries.len() {}", lca_entries.len());
    log::debug!("base_entries.len() {}", base_entries.len());
    log::debug!("merge_tree_entries.len() {}", merge_tree_entries.len());

    // Check all the entries in the candidate merge
    for (entry_path, merge_file_node) in &merge_tree_entries {
        // log::debug!("Considering entry {}", entry_path.to_string_lossy());
        // Check if the entry exists in all 3 commits
        if let Some(base_file_node) = base_entries.get(entry_path) {
            if let Some(lca_file_node) = lca_entries.get(entry_path) {
                // File exists in all three trees
                let base_eq_lca = base_file_node.combined_hash() == lca_file_node.combined_hash();
                let base_eq_merge =
                    base_file_node.combined_hash() == merge_file_node.combined_hash();

                if base_eq_lca && !base_eq_merge {
                    // Changed only on merge branch — include in merge result
                    entries.push((entry_path.clone(), merge_file_node.clone(), Modified));
                    if write_to_disk {
                        if is_resume
                            || restore::should_restore_file(
                                repo,
                                Some(base_file_node.clone()),
                                merge_file_node,
                                entry_path,
                            )
                            .await?
                        {
                            entries_to_restore.push(FileToRestore {
                                file_node: merge_file_node.clone(),
                                path: entry_path.clone(),
                            });
                        } else {
                            cannot_overwrite_entries.push(entry_path.clone());
                        }
                    }
                }

                // If all three are different, mark as conflict
                let lca_eq_merge = lca_file_node.combined_hash() == merge_file_node.combined_hash();
                if !base_eq_lca && !lca_eq_merge && !base_eq_merge {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: (lca_file_node.to_owned(), entry_path.to_path_buf()),
                        base_entry: (base_file_node.to_owned(), entry_path.to_path_buf()),
                        merge_entry: (merge_file_node.to_owned(), entry_path.to_path_buf()),
                    });
                }
            } else {
                // File in base and merge but not LCA — conflict if different
                if base_file_node.combined_hash() != merge_file_node.combined_hash() {
                    conflicts.push(NodeMergeConflict {
                        lca_entry: (base_file_node.to_owned(), entry_path.to_path_buf()),
                        base_entry: (base_file_node.to_owned(), entry_path.to_path_buf()),
                        merge_entry: (merge_file_node.to_owned(), entry_path.to_path_buf()),
                    });
                }
            }
        } else {
            // File in merge_tree_entries but not in base_entries. This could mean:
            // (a) base actually deleted it, or
            // (b) the file's parent dir was shared between LCA and base, so
            //     unique_dir_entries skipped it — the file still exists in base.
            // Look up the file in the full base commit tree (not the optimized one which
            // skips shared directories).
            let base_file_node =
                repositories::tree::get_file_by_path(repo, &merge_commits.base, entry_path)?;

            if base_file_node.is_some() {
                // Case (b): file exists in base via a shared directory.
                // It's changed on merge but not on base — include merge version.
                entries.push((entry_path.clone(), merge_file_node.clone(), Modified));
                if write_to_disk {
                    // Remove the parent dir from shared_hashes so add_dir_except will
                    // scan this directory when creating the merge commit.
                    if let Some(parent) = entry_path.parent()
                        && let Some(lca_tree) = &lca_commit_tree
                        && let Some(dir_node) = lca_tree.get_by_path(parent)?
                    {
                        shared_hashes.remove(&dir_node.hash);
                    }

                    if is_resume
                        || restore::should_restore_file(
                            repo,
                            base_file_node,
                            merge_file_node,
                            entry_path,
                        )
                        .await?
                    {
                        entries_to_restore.push(FileToRestore {
                            file_node: merge_file_node.clone(),
                            path: entry_path.clone(),
                        });
                    } else {
                        cannot_overwrite_entries.push(entry_path.clone());
                    }
                }
            } else if let Some(lca_node) = lca_entries.get(entry_path) {
                // Case (a): file was in LCA, base deleted it.
                if lca_node.combined_hash() != merge_file_node.combined_hash() {
                    // Merge modified it and base deleted it — conflict
                    conflicts.push(NodeMergeConflict {
                        lca_entry: (lca_node.to_owned(), entry_path.to_path_buf()),
                        base_entry: (lca_node.to_owned(), entry_path.to_path_buf()),
                        merge_entry: (merge_file_node.to_owned(), entry_path.to_path_buf()),
                    });
                }
                // If merge didn't change it from LCA, base's deletion wins
            } else {
                // Truly new file on merge branch
                entries.push((entry_path.clone(), merge_file_node.clone(), Added));
                if write_to_disk {
                    if is_resume
                        || restore::should_restore_file(repo, None, merge_file_node, entry_path)
                            .await?
                    {
                        entries_to_restore.push(FileToRestore {
                            file_node: merge_file_node.clone(),
                            path: entry_path.to_path_buf(),
                        });
                    } else {
                        cannot_overwrite_entries.push(entry_path.clone());
                    }
                }
            }
        }
    }

    // Detect deletions: files in the LCA that are absent from the merge tree. `base_entries` is
    // pruned (shared dirs skipped), so we use `get_file_by_path` for base lookups.
    for (entry_path, lca_file_node) in &lca_entries {
        if merge_tree_entries.contains_key(entry_path)
            || repositories::tree::get_file_by_path(repo, &merge_commits.merge, entry_path)?
                .is_some()
        {
            continue;
        }
        // File is in LCA but absent from merge — check base to decide delete vs conflict.
        let base_file_node =
            repositories::tree::get_file_by_path(repo, &merge_commits.base, entry_path)?;
        if let Some(base_file_node) = base_file_node {
            if base_file_node.combined_hash() == lca_file_node.combined_hash() {
                // Unchanged on base, deleted on merge — delete it
                entries.push((entry_path.clone(), lca_file_node.clone(), Removed));
            } else {
                // Base modified it, merge deleted it — conflict
                conflicts.push(NodeMergeConflict {
                    lca_entry: (lca_file_node.to_owned(), entry_path.to_path_buf()),
                    base_entry: (base_file_node.to_owned(), entry_path.to_path_buf()),
                    merge_entry: (lca_file_node.to_owned(), entry_path.to_path_buf()),
                });
            }
        }
    }

    log::debug!("three_way_merge conflicts.len() {}", conflicts.len());

    // If there are no conflicts, restore the entries
    if cannot_overwrite_entries.is_empty() {
        // Working-tree mutation starts here. Bracket it with the resume marker
        // so a SIGTERM mid-restore can be recovered by a subsequent merge
        // against the same target. Only when write_to_disk=true — the
        // server-safe paths leave the working tree untouched.
        if write_to_disk {
            merge_marker::write(repo, &merge_commits.merge.id).await?;
        }

        let version_store = repo.version_store()?;
        for entry in entries_to_restore.iter() {
            restore::restore_file(repo, &entry.file_node, &entry.path, &version_store).await?;

            // If it's a tabular file, we also need to stage the schema
            if util::fs::is_tabular(&entry.path)
                && let Some(schema) = repositories::data_frames::schemas::get_by_path(
                    repo,
                    &merge_commits.merge,
                    &entry.path,
                )?
            {
                for field in schema.fields {
                    if let Some(metadata) = field.metadata {
                        let _ = repositories::data_frames::schemas::add_column_metadata(
                            repo,
                            &entry.path,
                            &field.name,
                            &metadata,
                        )?;
                    }
                }

                if let Some(metadata) = schema.metadata {
                    let _ = repositories::data_frames::schemas::add_schema_metadata(
                        repo,
                        &entry.path,
                        &metadata,
                    )?;
                }
            }
        }

        if write_to_disk {
            merge_marker::clear(repo).await?;
        }
    } else {
        // If there are conflicts, return an error without restoring anything
        return Err(OxenError::cannot_overwrite_files(&cannot_overwrite_entries));
    }

    Ok(MergeConflictAnalysis { conflicts, entries })
}
