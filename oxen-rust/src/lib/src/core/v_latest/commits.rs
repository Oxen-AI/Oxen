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

use crate::constants::COMMIT_COUNT_DIR;
use crate::core::db::key_val::str_val_db;
use crate::core::db::merkle_node::MerkleNodeDB;
use rocksdb::{DBWithThreadMode, MultiThreaded};

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

pub fn commit_allow_empty(
    repo: &LocalRepository,
    message: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let message = message.as_ref();

    // Check if there are staged changes
    let status = crate::core::v_latest::status::status(repo)?;
    let has_changes = !status.staged_files.is_empty() || !status.staged_dirs.is_empty();

    if has_changes {
        // If there are changes, commit normally
        repositories::commits::commit_writer::commit(repo, message)
    } else {
        // No changes, create an empty commit
        let cfg = crate::config::UserConfig::get()?;
        let branch = repositories::branches::current_branch(repo)?
            .ok_or_else(|| OxenError::basic_str("No current branch found"))?;

        let head_commit = head_commit(repo)?;

        // Create a new commit with the same tree as parent
        let timestamp = OffsetDateTime::now_utc();
        let new_commit_data = crate::model::NewCommit {
            parent_ids: vec![head_commit.id.clone()],
            message: message.to_string(),
            author: cfg.name.clone(),
            email: cfg.email.clone(),
            timestamp,
        };

        // Compute the commit hash
        let mut hasher = xxhash_rust::xxh3::Xxh3::new();
        hasher.update(b"commit");
        hasher.update(format!("{:?}", new_commit_data.parent_ids).as_bytes());
        hasher.update(new_commit_data.message.as_bytes());
        hasher.update(new_commit_data.author.as_bytes());
        hasher.update(new_commit_data.email.as_bytes());
        hasher.update(&new_commit_data.timestamp.unix_timestamp().to_le_bytes());
        let commit_hash = MerkleHash::new(hasher.digest128());

        let new_commit = Commit::from_new_and_id(&new_commit_data, commit_hash.to_string());

        // Use the existing create_empty_commit function
        let result = create_empty_commit(repo, &branch.name, &new_commit)?;

        println!("🐂 commit {result} (empty)");

        Ok(result)
    }
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
        Some(commit_id) => Ok(commit_id.parse()?),
        None => Err(OxenError::head_not_found()),
    }
}

pub fn head_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    let commit_id = with_ref_manager(repo, |manager| manager.head_commit_id())?;
    match commit_id {
        Some(commit_id) => {
            let commit_id = commit_id.parse()?;
            get_by_hash(repo, &commit_id)
        }
        None => Ok(None),
    }
}

pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let head_commit_id = head_commit_id(repo)?;
    log::debug!("head_commit: head_commit_id: {head_commit_id:?}");

    let node =
        repositories::tree::get_node_by_id(repo, &head_commit_id)?.ok_or(OxenError::basic_str(
            format!("Merkle tree node not found for head commit: '{head_commit_id}'"),
        ))?;
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
            let root_commit = root_commit_recursive(repo, commit.id.parse()?, &mut seen)?;
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
    let mut current_id = commit_id;

    loop {
        // Check if we've already seen this commit
        if !seen.insert(current_id.to_string()) {
            return Err(OxenError::basic_str("Cycle detected in commit history"));
        }

        if let Some(commit) = get_by_hash(repo, &current_id)? {
            if commit.parent_ids.is_empty() {
                return Ok(commit);
            }

            // Only need to check the first parent, as all paths lead to the root
            if let Some(parent_id) = commit.parent_ids.first() {
                current_id = parent_id.parse()?;
                continue;
            }
        }

        return Err(OxenError::basic_str("No root commit found"));
    }
}

pub fn get_by_id(
    repo: &LocalRepository,
    commit_id_str: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_id_str = commit_id_str.as_ref();
    let Ok(commit_id) = commit_id_str.parse() else {
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
    let existing_commit_id = existing_commit.id.parse()?;
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
            hash: new_commit.id.parse()?,
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

/// Fast forward-only traversal for paginated commits
/// This is much faster than full topological sort for recent commits
/// Follows the first parent chain, which works well for linear histories (99% of cases)
fn list_forward_paginated(
    repo: &LocalRepository,
    head_commit: Commit,
    skip: usize,
    limit: usize,
) -> Result<Vec<Commit>, OxenError> {
    let mut results = Vec::new();
    let mut current = Some(head_commit);
    let mut count = 0;
    let end_idx = skip + limit;

    while let Some(commit) = current {
        // Collect commits within the pagination range
        if count >= skip && count < end_idx {
            results.push(commit.clone());
        }
        count += 1;

        // Early exit once we've collected enough commits
        if count >= end_idx {
            break;
        }

        // Follow the first parent (main branch)
        current = if let Some(parent_id) = commit.parent_ids.first() {
            let parent_id: MerkleHash = parent_id.parse()?;
            get_by_hash(repo, &parent_id)?
        } else {
            None
        };
    }

    Ok(results)
}

/// List commits recursively with pagination support
/// Returns (commits, total_count)
/// Only collects commits within the skip..skip+limit range
/// If known_total_count is provided, enables early exit optimization
fn list_recursive_paginated(
    repo: &LocalRepository,
    head_commit: Commit,
    skip: usize,
    limit: usize,
    stop_at_base: Option<&Commit>,
    known_total_count: Option<usize>,
) -> Result<(Vec<Commit>, usize), OxenError> {
    let mut results = vec![];
    let mut visited = HashSet::new();
    let total_count = recurse_commit_paginated(
        repo,
        head_commit,
        &mut results,
        stop_at_base,
        &mut visited,
        skip,
        limit,
        known_total_count,
    )?;
    results.reverse();
    Ok((results, total_count))
}

// post-order traversal of the commit tree
// returns a Topological sort with priority to timestamp in case of multiple parents
// Uses iterative approach to avoid stack overflow in debug builds
fn recurse_commit(
    repo: &LocalRepository,
    head_commit: Commit,
    results: &mut Vec<Commit>,
    stop_at_base: Option<&Commit>,
    visited: &mut HashSet<String>,
) -> Result<(), OxenError> {
    // Stack to simulate recursion: (commit, processing_state)
    // processing_state: false = need to process children, true = children processed, add to results
    let mut stack: Vec<(Commit, bool)> = vec![(head_commit, false)];

    while let Some((commit, children_processed)) = stack.pop() {
        // Check if we've already visited this commit
        if visited.contains(&commit.id) {
            continue;
        }

        if children_processed {
            // All children have been processed, now add this commit to results
            visited.insert(commit.id.clone());
            results.push(commit);
        } else {
            // Check if this is the base commit we should stop at
            if let Some(base) = stop_at_base {
                if commit.id == base.id {
                    visited.insert(commit.id.clone());
                    results.push(commit);
                    continue;
                }
            }

            // Mark this commit for re-processing after children
            stack.push((commit.clone(), true));

            // Get and sort parent commits
            let mut parent_commits: Vec<Commit> = Vec::new();
            for parent_id in commit.parent_ids.clone() {
                let parent_id = parent_id.parse()?;
                if let Some(c) = get_by_hash(repo, &parent_id)? {
                    parent_commits.push(c);
                }
            }

            // Sort by timestamp and push in reverse order (so they're processed in correct order)
            parent_commits.sort_by_key(|c| std::cmp::Reverse(c.timestamp));
            for parent_commit in parent_commits {
                if !visited.contains(&parent_commit.id) {
                    stack.push((parent_commit, false));
                }
            }
        }
    }

    Ok(())
}

/// Mark a commit and all its ancestors as visited
/// This is used when we find a cached count to prevent double-counting
fn mark_ancestors_visited(
    repo: &LocalRepository,
    commit: &Commit,
    visited: &mut HashSet<String>,
) -> Result<(), OxenError> {
    let mut stack = vec![commit.clone()];

    while let Some(current) = stack.pop() {
        if visited.contains(&current.id) {
            continue;
        }
        visited.insert(current.id.clone());

        for parent_id in current.parent_ids.clone() {
            let parent_id: MerkleHash = parent_id.parse()?;
            if let Some(parent) = get_by_hash(repo, &parent_id)? {
                if !visited.contains(&parent.id) {
                    stack.push(parent);
                }
            }
        }
    }

    Ok(())
}

/// Count commits recursively without collecting them into memory
/// This is much more efficient than list_from for counting purposes
/// Leverages the commit count cache during traversal - when we encounter a commit
/// with a cached count, we use that count and skip traversing its entire history
fn count_with_cache(
    repo: &LocalRepository,
    db: &DBWithThreadMode<MultiThreaded>,
    head_commit: Commit,
    stop_at_base: Option<&Commit>,
    visited: &mut HashSet<String>,
) -> Result<usize, OxenError> {
    let mut count = 0;

    let mut stack: Vec<(Commit, bool)> = vec![(head_commit, false)];

    while let Some((commit, children_processed)) = stack.pop() {
        if visited.contains(&commit.id) {
            continue;
        }

        if children_processed {
            visited.insert(commit.id.clone());
            count += 1;
        } else {
            if let Some(base) = stop_at_base {
                if commit.id == base.id {
                    visited.insert(commit.id.clone());
                    count += 1;
                    continue;
                }
            }

            if let Some(cached_count) = get_cached_count(db, &commit.id)? {
                log::debug!(
                    "Found cached count for commit {}: {} commits",
                    &commit.id[..8],
                    cached_count
                );
                mark_ancestors_visited(repo, &commit, visited)?;
                count += cached_count;
                continue;
            }

            // Mark this commit for re-processing after children
            stack.push((commit.clone(), true));

            // Get and sort parent commits
            let mut parent_commits: Vec<Commit> = Vec::new();
            for parent_id in commit.parent_ids.clone() {
                let parent_id = parent_id.parse()?;
                if let Some(c) = get_by_hash(repo, &parent_id)? {
                    parent_commits.push(c);
                }
            }

            // Sort by timestamp and push in reverse order (so they're processed in correct order)
            parent_commits.sort_by_key(|c| std::cmp::Reverse(c.timestamp));
            for parent_commit in parent_commits {
                if !visited.contains(&parent_commit.id) {
                    stack.push((parent_commit, false));
                }
            }
        }
    }

    Ok(count)
}

#[allow(clippy::too_many_arguments)]
// Paginated version of recurse_commit
// Only collects commits within the skip..skip+limit range
// If known_total_count is provided, enables early exit after collecting enough commits
// Returns the total count of commits (either computed or the known value)
fn recurse_commit_paginated(
    repo: &LocalRepository,
    head_commit: Commit,
    results: &mut Vec<Commit>,
    stop_at_base: Option<&Commit>,
    visited: &mut HashSet<String>,
    skip: usize,
    limit: usize,
    known_total_count: Option<usize>,
) -> Result<usize, OxenError> {
    // Stack to simulate recursion: (commit, processing_state)
    let mut stack: Vec<(Commit, bool)> = vec![(head_commit, false)];
    let mut count = 0;
    let end_idx = skip + limit;
    let can_early_exit = known_total_count.is_some();

    while let Some((commit, children_processed)) = stack.pop() {
        // Check if we've already visited this commit
        if visited.contains(&commit.id) {
            continue;
        }

        if children_processed {
            // All children have been processed, now add this commit to results
            visited.insert(commit.id.clone());

            // Only collect commits within the pagination range
            if count >= skip && count < end_idx {
                results.push(commit);
            }
            count += 1;

            // Early exit if we've collected enough commits and we know the total count
            if can_early_exit && count >= end_idx {
                log::debug!(
                    "Early exit: collected {} commits (skip={}, limit={})",
                    results.len(),
                    skip,
                    limit
                );
                break;
            }
        } else {
            // Check if this is the base commit we should stop at
            if let Some(base) = stop_at_base {
                if commit.id == base.id {
                    visited.insert(commit.id.clone());
                    if count >= skip && count < end_idx {
                        results.push(commit);
                    }
                    count += 1;
                    continue;
                }
            }

            // Mark this commit for re-processing after children
            stack.push((commit.clone(), true));

            // Get and sort parent commits
            let mut parent_commits: Vec<Commit> = Vec::new();
            for parent_id in commit.parent_ids.clone() {
                let parent_id = parent_id.parse()?;
                if let Some(c) = get_by_hash(repo, &parent_id)? {
                    parent_commits.push(c);
                }
            }

            // Sort by timestamp and push in reverse order (so they're processed in correct order)
            parent_commits.sort_by_key(|c| std::cmp::Reverse(c.timestamp));
            for parent_commit in parent_commits {
                if !visited.contains(&parent_commit.id) {
                    stack.push((parent_commit, false));
                }
            }
        }
    }

    // Return known count if available, otherwise return computed count
    Ok(known_total_count.unwrap_or(count))
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
        if !core::commit_sync_status::commit_is_synced(repo, &commit.id.parse()?) {
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
    let mut stack = vec![commit];

    while let Some(current_commit) = stack.pop() {
        // Skip if already processed
        if commits.contains(&current_commit) {
            continue;
        }

        commits.insert(current_commit.clone());

        for parent_id in current_commit.parent_ids {
            let parent_id = parent_id.parse()?;
            if let Some(parent_commit) = get_by_hash(repo, &parent_id)? {
                if !commits.contains(&parent_commit) {
                    stack.push(parent_commit);
                }
            }
        }
    }
    Ok(())
}

/// Get commit history given a revision (branch name or commit id)
pub fn list_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let _perf = crate::perf_guard!("core::commits::list_from");

    let revision = revision.as_ref();
    if revision.contains("..") {
        let _perf_between = crate::perf_guard!("core::commits::list_between_range");
        let split: Vec<&str> = revision.split("..").collect();
        let base = split[0];
        let head = split[1];
        let base_commit = repositories::commits::get_by_id(repo, base)?
            .ok_or(OxenError::revision_not_found(base.into()))?;
        let head_commit = repositories::commits::get_by_id(repo, head)?
            .ok_or(OxenError::revision_not_found(head.into()))?;
        return list_between(repo, &base_commit, &head_commit);
    }

    let _perf_get = crate::perf_guard!("core::commits::get_revision");
    let commit = repositories::revisions::get(repo, revision)?;
    drop(_perf_get);

    let mut results = vec![];
    if let Some(commit) = commit {
        let _perf_recursive = crate::perf_guard!("core::commits::list_recursive");
        list_recursive(repo, commit, &mut results, None, &mut HashSet::new())?;
    }

    Ok(results)
}

/// Get commit history given a revision with pagination
/// Returns (commits, total_count, count_was_cached)
/// Only collects commits within the requested page range
pub fn list_from_paginated_impl(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
    skip: usize,
    limit: usize,
) -> Result<(Vec<Commit>, usize, bool), OxenError> {
    let _perf = crate::perf_guard!("core::commits::list_from_paginated_impl");

    let revision = revision.as_ref();
    if revision.contains("..") {
        let _perf_between = crate::perf_guard!("core::commits::list_between_range");
        let split: Vec<&str> = revision.split("..").collect();
        let base = split[0];
        let head = split[1];
        let base_commit = repositories::commits::get_by_id(repo, base)?
            .ok_or(OxenError::revision_not_found(base.into()))?;
        let head_commit = repositories::commits::get_by_id(repo, head)?
            .ok_or(OxenError::revision_not_found(head.into()))?;

        // For range queries, we need to compute the count (no cache for ranges)
        let (commits, total_count) =
            list_recursive_paginated(repo, head_commit, skip, limit, Some(&base_commit), None)?;
        return Ok((commits, total_count, false));
    }

    let _perf_get = crate::perf_guard!("core::commits::get_revision");
    let commit = repositories::revisions::get(repo, revision)?;
    drop(_perf_get);

    if let Some(commit) = commit {
        // Try to get cached count first
        let _perf_count = crate::perf_guard!("core::commits::get_cached_count");
        let (total_count, cached) = count_from(repo, &commit.id)?;
        drop(_perf_count);

        log::info!(
            "list_from_paginated_impl: total_count={total_count}, cached={cached}, skip={skip}, limit={limit}"
        );

        // Fast path: if requesting a small number of recent commits, use simple forward traversal
        // This is much faster than the full topological sort for early pages
        if skip + limit <= 100 {
            let _perf_fast = crate::perf_guard!("core::commits::list_forward_paginated");
            let commits = list_forward_paginated(repo, commit, skip, limit)?;
            drop(_perf_fast);
            return Ok((commits, total_count, cached));
        }

        // Slow path: full topological sort with early exit
        let _perf_recursive = crate::perf_guard!("core::commits::list_recursive_paginated");
        let (commits, _) =
            list_recursive_paginated(repo, commit, skip, limit, None, Some(total_count))?;
        drop(_perf_recursive);

        return Ok((commits, total_count, cached));
    }

    Ok((vec![], 0, false))
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
    let mut stack = vec![(commit, depth)];

    while let Some((current_commit, current_depth)) = stack.pop() {
        // Check if we've already visited this commit at a shallower or equal depth
        if let Some(&existing_depth) = results.get(&current_commit) {
            if existing_depth <= current_depth {
                // We've already processed this commit, skip it
                continue;
            }
        }

        // Insert or update with the current (shallower) depth
        results.insert(current_commit.clone(), current_depth);

        for parent_id in current_commit.parent_ids {
            let parent_id = parent_id.parse()?;
            if let Some(parent_commit) = get_by_hash(repo, &parent_id)? {
                stack.push((parent_commit, current_depth + 1));
            }
        }
    }
    Ok(())
}

/// Open the commit count cache database
fn open_commit_count_db(
    repo: &LocalRepository,
) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(COMMIT_COUNT_DIR);
    let opts = crate::core::db::key_val::opts::default();
    Ok(DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?)
}

/// Get cached commit count from the database
fn get_cached_count(
    db: &DBWithThreadMode<MultiThreaded>,
    commit_id: &str,
) -> Result<Option<usize>, OxenError> {
    str_val_db::get(db, commit_id)
}

/// Cache a commit count in the database
fn cache_count(
    db: &DBWithThreadMode<MultiThreaded>,
    commit_id: &str,
    count: usize,
) -> Result<(), OxenError> {
    str_val_db::put(db, commit_id, &count)
}

/// Get the number of commits in the history from a given revision (including the commit itself)
/// This function uses a cache to avoid recomputing counts for commits we've already seen
pub fn count_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<(usize, bool), OxenError> {
    let revision = revision.as_ref();

    let commit = repositories::revisions::get(repo, revision)?
        .ok_or_else(|| OxenError::revision_not_found(revision.into()))?;

    let db = open_commit_count_db(repo)?;

    if let Some(cached_count) = get_cached_count(&db, &commit.id)? {
        return Ok((cached_count, true));
    }

    let count = count_with_cache(repo, &db, commit.clone(), None, &mut HashSet::new())?;

    cache_count(&db, &commit.id, count)?;

    Ok((count, false))
}

/// List the history between two commits
pub fn list_between(
    repo: &LocalRepository,
    base: &Commit,
    head: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    log::debug!("list_between()\nbase: {base}\nhead: {head}");
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
    let mut stack = vec![commit.clone()];

    while let Some(current_commit) = stack.pop() {
        let node = repositories::tree::get_node_by_path(repo, &current_commit, path)?;

        let last_commit = if let Some(node) = node {
            let last_commit_id = node.latest_commit_id()?;

            // Check if the commit already exists in the commits vector, if so, skip it
            if visited.contains(&last_commit_id.to_string()) {
                continue;
            }

            repositories::revisions::get(repo, last_commit_id.to_string())?.ok_or_else(|| {
                OxenError::basic_str(format!(
                    "Commit not found for last_commit_id: {last_commit_id}"
                ))
            })?
        } else {
            continue;
        };

        // Mark last_commit as visited and add to results
        visited.insert(last_commit.id.clone());
        commits.push(last_commit.clone());

        let parent_ids = last_commit.parent_ids;

        for parent_id in parent_ids {
            let parent_commit = repositories::revisions::get(repo, parent_id)?;
            if let Some(parent_commit_obj) = parent_commit {
                if !visited.contains(&parent_commit_obj.id) {
                    stack.push(parent_commit_obj);
                }
            }
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
    let _perf = crate::perf_guard!("core::commits::list_by_path_from_paginated");

    // Check if the path is a directory or file
    let _perf_node = crate::perf_guard!("core::commits::get_node_by_path");
    let node = repositories::tree::get_node_by_path(repo, commit, path)?.ok_or(
        OxenError::basic_str(format!("Merkle tree node not found for path: {path:?}")),
    )?;
    let last_commit_id = match &node.node {
        EMerkleTreeNode::File(file_node) => file_node.last_commit_id(),
        EMerkleTreeNode::Directory(dir_node) => dir_node.last_commit_id(),
        _ => {
            return Err(OxenError::basic_str(format!(
                "Merkle tree node not found for path: {path:?}"
            )));
        }
    };
    let last_commit_id = last_commit_id.to_string();
    drop(_perf_node);

    let _perf_recursive = crate::perf_guard!("core::commits::list_by_path_recursive");
    let mut commits: Vec<Commit> = Vec::new();
    list_by_path_recursive(repo, path, commit, &mut commits)?;
    log::info!(
        "list_by_path_from_paginated {} got {} commits before pagination",
        last_commit_id,
        commits.len()
    );
    drop(_perf_recursive);

    let _perf_paginate = crate::perf_guard!("core::commits::paginate_path_commits");
    let (commits, pagination) = util::paginate(commits, pagination.page_num, pagination.page_size);
    drop(_perf_paginate);

    Ok(PaginatedCommits {
        status: StatusMessage::resource_found(),
        commits,
        pagination,
    })
}
