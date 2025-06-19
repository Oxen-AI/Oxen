use std::collections::{HashMap, HashSet};
use std::path::Path;

use glob::Pattern;
use time::OffsetDateTime;

use crate::core;
use crate::core::refs::with_ref_manager;
use crate::error::OxenError;
use crate::model::merkle_tree::node::commit_node::CommitNodeOpts;
use crate::model::merkle_tree::node::{CommitNode, EMerkleTreeNode};
use crate::model::{Commit, LocalRepository, MerkleHash, User};
use crate::opts::PaginateOpts;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{repositories, util};

use std::path::PathBuf;
use std::str;
use std::str::FromStr;

use crate::core::db::merkle_node::MerkleNodeDB;

pub fn commit(repo: &LocalRepository, message: impl AsRef<str>) -> Result<Commit, OxenError> {
    repositories::commits::commit_writer::commit(repo, message)
}

pub fn commit_with_user(
    repo: &LocalRepository,
    message: impl AsRef<str>,
    user: &User,
) -> Result<Commit, OxenError> {
    repositories::commits::commit_writer::commit_with_user(repo, message, user)
}

pub fn get_commit_or_head<S: AsRef<str> + Clone>(
    repo: &LocalRepository,
    commit_id_or_branch_name: Option<S>,
) -> Result<Commit, OxenError> {
    match commit_id_or_branch_name {
        Some(ref_name) => {
            log::debug!("get_commit_or_head: ref_name: {:?}", ref_name.as_ref());
            get_commit_by_ref(repo, ref_name)
        }
        None => {
            log::debug!("get_commit_or_head: calling head_commit");
            head_commit(repo)
        }
    }
}

fn get_commit_by_ref<S: AsRef<str> + Clone>(
    repo: &LocalRepository,
    ref_name: S,
) -> Result<Commit, OxenError> {
    get_by_id(repo, ref_name.clone())?
        .or_else(|| get_commit_by_branch(repo, ref_name.as_ref()))
        .ok_or_else(|| OxenError::basic_str("Commit not found"))
}

fn get_commit_by_branch(repo: &LocalRepository, branch_name: &str) -> Option<Commit> {
    repositories::branches::get_by_name(repo, branch_name)
        .ok()
        .flatten()
        .and_then(|branch| get_by_id(repo, &branch.commit_id).ok().flatten())
}

pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let branches = with_ref_manager(repo, |manager| manager.list_branches())?;
    let mut latest_commit: Option<Commit> = None;
    for branch in branches {
        let commit = get_by_id(repo, &branch.commit_id)?;
        if let Some(commit) = commit {
            if latest_commit.is_none()
                || commit.timestamp < latest_commit.as_ref().unwrap().timestamp
            {
                latest_commit = Some(commit);
            }
        }
    }
    latest_commit.ok_or(OxenError::no_commits_found())
}

fn head_commit_id(repo: &LocalRepository) -> Result<MerkleHash, OxenError> {
    let commit_id = with_ref_manager(repo, |manager| manager.head_commit_id())?;
    match commit_id {
        Some(commit_id) => MerkleHash::from_str(&commit_id),
        None => Err(OxenError::head_not_found()),
    }
}

pub fn head_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    let commit_id = with_ref_manager(repo, |manager| manager.head_commit_id())?;
    match commit_id {
        Some(commit_id) => {
            let commit_id = MerkleHash::from_str(&commit_id)?;
            get_by_hash(repo, &commit_id)
        }
        None => Ok(None),
    }
}

pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let head_commit_id = head_commit_id(repo)?;
    log::debug!("head_commit: head_commit_id: {:?}", head_commit_id);

    let node = repositories::tree::get_node_by_id(repo, &head_commit_id)?.ok_or(
        OxenError::basic_str(format!(
            "Merkle tree node not found for head commit: '{}'",
            head_commit_id
        )),
    )?;
    let commit = node.commit()?;
    Ok(commit.to_commit())
}

/// Get the root commit of the repository or None
pub fn root_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    // Try to get a branch ref and follow it to the root
    // We only need to look at one ref as all branches will have the same root
    let branches = with_ref_manager(repo, |manager| manager.list_branches())?;

    if let Some(branch) = branches.first() {
        if let Some(commit) = get_by_id(repo, &branch.commit_id)? {
            let mut seen = HashSet::new();
            let root_commit =
                root_commit_recursive(repo, MerkleHash::from_str(&commit.id)?, &mut seen)?;
            return Ok(Some(root_commit));
        }
    }
    log::debug!("root_commit_maybe: no root commit found");
    Ok(None)
}

fn root_commit_recursive(
    repo: &LocalRepository,
    commit_id: MerkleHash,
    seen: &mut HashSet<String>,
) -> Result<Commit, OxenError> {
    // Check if we've already seen this commit
    if !seen.insert(commit_id.to_string()) {
        return Err(OxenError::basic_str("Cycle detected in commit history"));
    }

    if let Some(commit) = get_by_hash(repo, &commit_id)? {
        if commit.parent_ids.is_empty() {
            return Ok(commit);
        }

        // Only need to check the first parent, as all paths lead to the root
        if let Some(parent_id) = commit.parent_ids.first() {
            let parent_id = MerkleHash::from_str(parent_id)?;
            return root_commit_recursive(repo, parent_id, seen);
        }
    }
    Err(OxenError::basic_str("No root commit found"))
}

pub fn get_by_id(
    repo: &LocalRepository,
    commit_id_str: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_id_str = commit_id_str.as_ref();
    let Ok(commit_id) = MerkleHash::from_str(commit_id_str) else {
        // log::debug!(
        //     "get_by_id could not create commit_id from [{}]",
        //     commit_id_str
        // );
        return Ok(None);
    };
    get_by_hash(repo, &commit_id)
}

pub fn get_by_hash(repo: &LocalRepository, hash: &MerkleHash) -> Result<Option<Commit>, OxenError> {
    let Some(node) = repositories::tree::get_node_by_id(repo, hash)? else {
        return Ok(None);
    };
    let commit = node.commit()?;
    Ok(Some(commit.to_commit()))
}

pub fn create_empty_commit(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
    new_commit: &Commit,
) -> Result<Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let Some(existing_commit) = repositories::revisions::get(repo, branch_name)? else {
        return Err(OxenError::revision_not_found(branch_name.into()));
    };
    let existing_commit_id = MerkleHash::from_str(&existing_commit.id)?;
    let existing_node =
        repositories::tree::get_node_by_id_with_children(repo, &existing_commit_id)?.ok_or(
            OxenError::basic_str(format!(
                "Merkle tree node not found for commit: '{}'",
                existing_commit.id
            )),
        )?;
    let timestamp = OffsetDateTime::now_utc();
    let commit_node = CommitNode::new(
        repo,
        CommitNodeOpts {
            hash: MerkleHash::from_str(&new_commit.id)?,
            parent_ids: vec![existing_commit_id],
            email: new_commit.email.clone(),
            author: new_commit.author.clone(),
            message: new_commit.message.clone(),
            timestamp,
        },
    )?;

    let parent_id = Some(existing_node.hash);
    let mut commit_db = MerkleNodeDB::open_read_write(repo, &commit_node, parent_id)?;
    // There should always be one child, the root directory
    let dir_node = existing_node.children.first().unwrap().dir()?;
    commit_db.add_child(&dir_node)?;

    // Copy the dir hashes db to the new commit
    repositories::tree::cp_dir_hashes_to(repo, &existing_commit_id, commit_node.hash())?;

    // Update the ref
    with_ref_manager(repo, |manager| {
        manager.set_branch_commit_id(branch_name, commit_node.hash().to_string())
    })?;

    Ok(commit_node.to_commit())
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    let mut results = vec![];
    let mut visited = HashSet::new();
    if let Some(commit) = head_commit_maybe(repo)? {
        list_recursive(repo, commit, &mut results, None, &mut visited)?;
    }
    Ok(results)
}

/// List commits recursively from the head commit
/// Commits will be returned in reverse chronological order
fn list_recursive(
    repo: &LocalRepository,
    head_commit: Commit,
    results: &mut Vec<Commit>,
    stop_at_base: Option<&Commit>,
    visited: &mut HashSet<String>,
) -> Result<(), OxenError> {
    recurse_commit(repo, head_commit, results, stop_at_base, visited)?;
    results.reverse();
    Ok(())
}

// post-order traversal of the commit tree
// returns a Topological sort with priority to timestamp in case of multiple parents
fn recurse_commit(
    repo: &LocalRepository,
    head_commit: Commit,
    results: &mut Vec<Commit>,
    stop_at_base: Option<&Commit>,
    visited: &mut HashSet<String>,
) -> Result<(), OxenError> {
    // Check if we've already visited this commit
    if !visited.insert(head_commit.id.clone()) {
        return Ok(());
    }

    if let Some(base) = stop_at_base {
        if head_commit.id == base.id {
            results.push(head_commit.clone());
            return Ok(());
        }
    }

    let mut parent_commits: Vec<Commit> = Vec::new();
    for parent_id in head_commit.parent_ids.clone() {
        let parent_id = MerkleHash::from_str(&parent_id)?;
        if let Some(c) = get_by_hash(repo, &parent_id)? {
            parent_commits.push(c);
        }
    }

    parent_commits.sort_by_key(|c| c.timestamp);
    for parent_commit in parent_commits {
        recurse_commit(repo, parent_commit, results, stop_at_base, visited)?;
    }
    results.push(head_commit.clone());

    Ok(())
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    let branches = with_ref_manager(repo, |manager| manager.list_branches())?;
    let mut commits = HashSet::new();
    for branch in branches {
        let commit = get_by_id(repo, &branch.commit_id)?;
        if let Some(commit) = commit {
            list_all_recursive(repo, commit, &mut commits)?;
        }
    }
    Ok(commits)
}

pub fn list_unsynced_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<HashSet<Commit>, OxenError> {
    let revision = revision.as_ref();
    let all_commits: HashSet<Commit> = list_from(repo, revision)?.into_iter().collect();
    filter_unsynced(repo, all_commits)
}

pub fn list_unsynced(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    let all_commits = list_all(repo)?;
    filter_unsynced(repo, all_commits)
}

fn filter_unsynced(
    repo: &LocalRepository,
    commits: HashSet<Commit>,
) -> Result<HashSet<Commit>, OxenError> {
    log::debug!("filter_unsynced filtering down from {}", commits.len());
    let mut unsynced_commits = HashSet::new();
    for commit in commits {
        if !core::commit_sync_status::commit_is_synced(repo, &MerkleHash::from_str(&commit.id)?) {
            unsynced_commits.insert(commit);
        }
    }
    log::debug!("list_unsynced filtered down to {}", unsynced_commits.len());
    Ok(unsynced_commits)
}

fn list_all_recursive(
    repo: &LocalRepository,
    commit: Commit,
    commits: &mut HashSet<Commit>,
) -> Result<(), OxenError> {
    commits.insert(commit.clone());
    for parent_id in commit.parent_ids {
        let parent_id = MerkleHash::from_str(&parent_id)?;
        if let Some(parent_commit) = get_by_hash(repo, &parent_id)? {
            list_all_recursive(repo, parent_commit, commits)?;
        }
    }
    Ok(())
}

/// Get commit history given a revision (branch name or commit id)
pub fn list_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let revision = revision.as_ref();
    if revision.contains("..") {
        let split: Vec<&str> = revision.split("..").collect();
        let base = split[0];
        let head = split[1];
        let base_commit = repositories::commits::get_by_id(repo, base)?
            .ok_or(OxenError::revision_not_found(base.into()))?;
        let head_commit = repositories::commits::get_by_id(repo, head)?
            .ok_or(OxenError::revision_not_found(head.into()))?;
        return list_between(repo, &base_commit, &head_commit);
    }
    let mut results = vec![];
    let commit = repositories::revisions::get(repo, revision)?;
    if let Some(commit) = commit {
        list_recursive(repo, commit, &mut results, None, &mut HashSet::new())?;
    }

    Ok(results)
}

/// Get commit history given a revision (branch name or commit id)
pub fn list_from_with_depth(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<HashMap<Commit, usize>, OxenError> {
    let mut results = HashMap::new();
    let commit = repositories::revisions::get(repo, revision)?;
    if let Some(commit) = commit {
        list_recursive_with_depth(repo, commit, &mut results, 0)?;
    }
    Ok(results)
}

fn list_recursive_with_depth(
    repo: &LocalRepository,
    commit: Commit,
    results: &mut HashMap<Commit, usize>,
    depth: usize,
) -> Result<(), OxenError> {
    // Check if we've already visited this commit at a shallower or equal depth
    if let Some(&existing_depth) = results.get(&commit) {
        if existing_depth <= depth {
            // We've already processed this commit, skip it
            return Ok(());
        }
    }

    // Insert or update with the current (shallower) depth
    results.insert(commit.clone(), depth);

    for parent_id in commit.parent_ids {
        let parent_id = MerkleHash::from_str(&parent_id)?;
        if let Some(parent_commit) = get_by_hash(repo, &parent_id)? {
            list_recursive_with_depth(repo, parent_commit, results, depth + 1)?;
        }
    }
    Ok(())
}

/// List the history between two commits
pub fn list_between(
    repo: &LocalRepository,
    base: &Commit,
    head: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!("list_between()\nbase: {}\nhead: {}", base, head);
    let mut results = vec![];
    list_recursive(
        repo,
        head.clone(),
        &mut results,
        Some(base),
        &mut HashSet::new(),
    )?;
    Ok(results)
}

/// Retrieve entries with filepaths matching a provided glob pattern
pub fn search_entries(
    repo: &LocalRepository,
    commit: &Commit,
    pattern: impl AsRef<str>,
) -> Result<HashSet<PathBuf>, OxenError> {
    let pattern = pattern.as_ref();
    let pattern = Pattern::new(pattern)?;

    let mut results = HashSet::new();
    let tree = repositories::tree::get_root_with_children(repo, commit)?
        .ok_or(OxenError::basic_str("Root not found"))?;
    let (files, _) = repositories::tree::list_files_and_dirs(&tree)?;
    for file in files {
        let path = file.dir.join(file.file_node.name());
        if pattern.matches_path(&path) {
            results.insert(path);
        }
    }
    Ok(results)
}

/// List commits by path (directory or file) recursively
pub fn list_by_path_recursive(
    repo: &LocalRepository,
    path: &Path,
    commit: &Commit,
    commits: &mut Vec<Commit>,
) -> Result<(), OxenError> {
    let mut visited = HashSet::new();
    list_by_path_recursive_impl(repo, path, commit, commits, &mut visited)
}

fn list_by_path_recursive_impl(
    repo: &LocalRepository,
    path: &Path,
    commit: &Commit,
    commits: &mut Vec<Commit>,
    visited: &mut HashSet<String>,
) -> Result<(), OxenError> {
    let node = repositories::tree::get_node_by_path(repo, commit, path)?;

    let last_commit = if let Some(node) = node {
        let last_commit_id = node.latest_commit_id()?;

        // Check if the commit already exists in the commits vector, if so, skip it
        if visited.contains(&last_commit_id.to_string()) {
            return Ok(());
        }

        repositories::revisions::get(repo, last_commit_id.to_string())?.ok_or_else(|| {
            OxenError::basic_str(format!(
                "Commit not found for last_commit_id: {}",
                last_commit_id
            ))
        })?
    } else {
        return Ok(());
    };

    // Mark last_commit as visited and add to results
    visited.insert(last_commit.id.clone());
    commits.push(last_commit.clone());

    let parent_ids = last_commit.parent_ids;

    for parent_id in parent_ids {
        let parent_commit = repositories::revisions::get(repo, parent_id)?;
        if let Some(parent_commit_obj) = parent_commit {
            list_by_path_recursive_impl(repo, path, &parent_commit_obj, commits, visited)?;
        }
    }

    Ok(())
}

/// Get paginated list of commits by path (directory or file)
pub fn list_by_path_from_paginated(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
    pagination: PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    // Check if the path is a directory or file
    let node = repositories::tree::get_node_by_path(repo, commit, path)?.ok_or(
        OxenError::basic_str(format!("Merkle tree node not found for path: {:?}", path)),
    )?;
    let last_commit_id = match &node.node {
        EMerkleTreeNode::File(file_node) => file_node.last_commit_id(),
        EMerkleTreeNode::Directory(dir_node) => dir_node.last_commit_id(),
        _ => {
            return Err(OxenError::basic_str(format!(
                "Merkle tree node not found for path: {:?}",
                path
            )));
        }
    };
    let last_commit_id = last_commit_id.to_string();
    let mut commits: Vec<Commit> = Vec::new();
    list_by_path_recursive(repo, path, commit, &mut commits)?;
    log::info!(
        "list_by_path_from_paginated {} got {} commits before pagination",
        last_commit_id,
        commits.len()
    );
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}
