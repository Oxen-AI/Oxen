use std::collections::{HashMap, HashSet};
use std::path::Path;

use glob::Pattern;
use time::OffsetDateTime;

use crate::core;
use crate::core::refs::with_ref_manager;
use crate::error::OxenError;
use crate::model::merkle_tree::node::commit_node::CommitNodeOpts;
use crate::model::merkle_tree::node::dir_node::DirNodeOpts;
use crate::model::merkle_tree::node::{CommitNode, DirNode, EMerkleTreeNode};
use crate::model::{Commit, LocalRepository, MerkleHash, User};
use crate::opts::PaginateOpts;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{repositories, util};

use std::path::PathBuf;
use std::str;

use crate::constants::COMMIT_COUNT_DIR;
use crate::core::db::key_val::{opts, str_val_db};
use crate::core::db::merkle_node::MerkleNodeDB;
use crate::core::v_latest::index::CommitMerkleTree;
use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};

/// Configuration for commit traversal operations
struct CommitTraversalConfig<'a> {
    /// Repository to traverse
    repo: &'a LocalRepository,
    /// Starting commit for traversal
    head_commit: Commit,
    /// Optional base commit to stop at (exclusive)
    stop_at_base: Option<&'a Commit>,
    /// Set of visited commit IDs to avoid cycles
    visited: &'a mut HashSet<String>,
    /// Number of commits to skip (for pagination)
    skip: usize,
    /// Maximum number of commits to collect (for pagination)
    limit: usize,
    /// Optional cache database for count lookups
    cache_db: Option<&'a DBWithThreadMode<MultiThreaded>>,
    /// Known total count for early exit optimization
    known_total_count: Option<usize>,
}

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
        let commit_hash =
            repositories::commits::commit_writer::compute_commit_id(&new_commit_data)?;

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

/// Create an initial empty commit for an empty repository.
/// This creates the first commit with an empty tree and sets up the branch.
/// Returns an error if the repository already has commits.
pub fn create_initial_commit(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
    user: &User,
    message: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let message = message.as_ref();

    // Ensure the repository is actually empty
    if head_commit_maybe(repo)?.is_some() {
        return Err(OxenError::basic_str(
            "Cannot create initial commit: repository already has commits",
        ));
    }

    let timestamp = OffsetDateTime::now_utc();

    // Create commit data with no parents
    let new_commit = crate::model::NewCommit {
        parent_ids: vec![],
        message: message.to_string(),
        author: user.name.clone(),
        email: user.email.clone(),
        timestamp,
    };

    // Compute the commit hash
    let commit_id = repositories::commits::commit_writer::compute_commit_id(&new_commit)?;

    // Create the commit node
    let commit_node = CommitNode::new(
        repo,
        CommitNodeOpts {
            hash: commit_id,
            parent_ids: vec![],
            email: user.email.clone(),
            author: user.name.clone(),
            message: message.to_string(),
            timestamp,
        },
    )?;

    // Create an empty root directory node
    let empty_dir_hash = MerkleHash::new(0); // Empty hash for empty directory
    let dir_node = DirNode::new(
        repo,
        DirNodeOpts {
            name: String::new(), // Root directory has empty name
            hash: empty_dir_hash,
            num_entries: 0,
            num_bytes: 0,
            last_commit_id: commit_id,
            last_modified_seconds: timestamp.unix_timestamp(),
            last_modified_nanoseconds: timestamp.nanosecond(),
            data_type_counts: HashMap::new(),
            data_type_sizes: HashMap::new(),
        },
    )?;

    // Open the commit database and add the root directory
    let mut commit_db = MerkleNodeDB::open_read_write(repo, &commit_node, None)?;
    commit_db.add_child(&dir_node)?;

    // Initialize the dir_hash_db with the root directory hash
    let commit_id_string = commit_id.to_string();
    let dir_hash_db_path = CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, &commit_id_string);
    let dir_hash_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts::default(), dunce::simplified(&dir_hash_db_path))?;
    str_val_db::put(&dir_hash_db, "", &dir_node.hash().to_string())?;

    // Create the branch pointing to this commit
    with_ref_manager(repo, |manager| {
        manager.create_branch(branch_name, commit_id.to_string())
    })?;

    // Set HEAD to the new branch
    with_ref_manager(repo, |manager| {
        manager.set_head(branch_name);
        Ok(())
    })?;

    Ok(commit_node.to_commit())
}

/// List commits on the current branch from HEAD
pub fn list(repo: &LocalRepository) -> Result<Vec<Commit>, OxenError> {
    if let Some(commit) = head_commit_maybe(repo)? {
        let (results, _) = list_recursive_paginated(repo, commit, 0, usize::MAX, None, None)?;
        Ok(results)
    } else {
        Ok(vec![])
    }
}

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
        if count >= skip && count < end_idx {
            results.push(commit.clone());
        }
        count += 1;

        if count >= end_idx {
            break;
        }

        current = if let Some(parent_id) = commit.parent_ids.first() {
            let parent_id: MerkleHash = parent_id.parse()?;
            get_by_hash(repo, &parent_id)?
        } else {
            None
        };
    }

    Ok(results)
}

pub fn list_recursive_paginated(
    repo: &LocalRepository,
    head_commit: Commit,
    skip: usize,
    limit: usize,
    stop_at_base: Option<&Commit>,
    known_total_count: Option<usize>,
) -> Result<(Vec<Commit>, usize), OxenError> {
    let mut results = vec![];
    let mut visited = HashSet::new();

    let config = CommitTraversalConfig {
        repo,
        head_commit,
        stop_at_base,
        visited: &mut visited,
        skip,
        limit,
        cache_db: None,
        known_total_count,
    };

    let total_count = traverse_commits(config, Some(&mut results))?;
    Ok((results, total_count))
}

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

fn traverse_commits(
    config: CommitTraversalConfig,
    mut results: Option<&mut Vec<Commit>>,
) -> Result<usize, OxenError> {
    let mut count = 0;
    let mut stack: Vec<Commit> = vec![config.head_commit];
    let end_idx = config.skip + config.limit;
    let can_early_exit = config.known_total_count.is_some();
    let collect_results = results.is_some();

    while let Some(commit) = stack.pop() {
        if config.visited.contains(&commit.id) {
            continue;
        }

        config.visited.insert(commit.id.clone());

        // Check for base case
        if let Some(base) = config.stop_at_base {
            if commit.id == base.id {
                if count >= config.skip && count < end_idx {
                    if let Some(ref mut res) = results {
                        res.push(commit);
                    }
                }
                count += 1;
                continue;
            }
        }

        // Check cache
        if let Some(db) = config.cache_db {
            if let Some(cached_count) = get_cached_count(db, &commit.id)? {
                log::debug!(
                    "Found cached count for commit {}: {} commits",
                    &commit.id[..8],
                    cached_count
                );
                mark_ancestors_visited(config.repo, &commit, config.visited)?;
                count += cached_count;
                continue;
            }
        }

        // Process commit in pre-order (newest-first)
        if count >= config.skip && count < end_idx {
            if let Some(ref mut res) = results {
                res.push(commit.clone());
            }
        }
        count += 1;

        if can_early_exit && collect_results && count >= end_idx {
            log::debug!(
                "Early exit: collected {} commits (skip={}, limit={})",
                results.as_ref().map(|r| r.len()).unwrap_or(0),
                config.skip,
                config.limit
            );
            break;
        }

        // Push children to stack (sorted so newest is processed first)
        let mut parent_commits: Vec<Commit> = Vec::new();
        for parent_id in commit.parent_ids.clone() {
            let parent_id = parent_id.parse()?;
            if let Some(c) = get_by_hash(config.repo, &parent_id)? {
                parent_commits.push(c);
            }
        }

        // Sort by timestamp ascending, so when we push to stack, newest is on top
        parent_commits.sort_by_key(|c| c.timestamp);
        for parent_commit in parent_commits {
            if !config.visited.contains(&parent_commit.id) {
                stack.push(parent_commit);
            }
        }
    }

    Ok(config.known_total_count.unwrap_or(count))
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
    // Create a temporary Vec to collect results, then add to HashSet
    let mut visited_ids = HashSet::new();
    let mut results = Vec::new();

    let config = CommitTraversalConfig {
        repo,
        head_commit: commit,
        stop_at_base: None,
        visited: &mut visited_ids,
        skip: 0,
        limit: usize::MAX,
        cache_db: None,
        known_total_count: None,
    };

    traverse_commits(config, Some(&mut results))?;
    commits.extend(results);
    Ok(())
}

/// Get commit history given a revision (branch name or commit id)
pub fn list_from(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
) -> Result<Vec<Commit>, OxenError> {
    let (commits, _, _) = list_from_paginated_impl(repo, revision, 0, usize::MAX)?;
    Ok(commits)
}

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

        let (commits, total_count) =
            list_recursive_paginated(repo, head_commit, skip, limit, Some(&base_commit), None)?;
        return Ok((commits, total_count, false));
    }

    let _perf_get = crate::perf_guard!("core::commits::get_revision");
    let commit = repositories::revisions::get(repo, revision)?;
    drop(_perf_get);

    if let Some(commit) = commit {
        let _perf_count = crate::perf_guard!("core::commits::get_cached_count");
        let (total_count, cached) = count_from(repo, &commit.id)?;
        drop(_perf_count);

        log::info!(
            "list_from_paginated_impl: total_count={total_count}, cached={cached}, skip={skip}, limit={limit}"
        );

        if skip + limit <= 10 {
            let _perf_fast = crate::perf_guard!("core::commits::list_forward_paginated");
            let commits = list_forward_paginated(repo, commit, skip, limit)?;
            drop(_perf_fast);
            return Ok((commits, total_count, cached));
        }

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

fn open_commit_count_db(
    repo: &LocalRepository,
) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(COMMIT_COUNT_DIR);
    util::fs::create_dir_all(&db_path)?;
    let opts = crate::core::db::key_val::opts::default();
    Ok(DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?)
}

fn get_cached_count(
    db: &DBWithThreadMode<MultiThreaded>,
    commit_id: &str,
) -> Result<Option<usize>, OxenError> {
    str_val_db::get(db, commit_id)
}

fn cache_count(
    db: &DBWithThreadMode<MultiThreaded>,
    commit_id: &str,
    count: usize,
) -> Result<(), OxenError> {
    str_val_db::put(db, commit_id, &count)
}

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

    let config = CommitTraversalConfig {
        repo,
        head_commit: commit.clone(),
        stop_at_base: None,
        visited: &mut HashSet::new(),
        skip: 0,
        limit: usize::MAX,
        cache_db: Some(&db),
        known_total_count: None,
    };
    let count = traverse_commits(config, None)?;

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
    let (results, _) =
        list_recursive_paginated(repo, head.clone(), 0, usize::MAX, Some(base), None)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_pagination_order_with_more_than_10_commits() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 15 commits to trigger the slow path (skip + limit > 10)
            let mut commit_ids = Vec::new();

            for i in 0..15 {
                let filename = format!("file_{i}.txt");
                let file_path = repo.path.join(&filename);
                test::write_txt_file_to_path(&file_path, format!("Content {i}"))?;

                repositories::add(&repo, &file_path).await?;
                let commit = repositories::commit(&repo, &format!("Commit {i}"))?;
                commit_ids.push(commit.id.clone());
            }

            // Commits should be ordered newest-first (C14, C13, C12, ...)
            // Test: skip=9, limit=2 (total 11 > 10) to trigger the slow path
            // This should return [C5, C4] (skip 9 newest, then take 2)
            let (paginated_commits, _total, _cached) =
                list_from_paginated_impl(&repo, "main", 9, 2)?;

            assert_eq!(
                paginated_commits.len(),
                2,
                "Should return exactly 2 commits"
            );

            // Expected: skip 9 newest (C14 down to C6), then take [C5, C4]
            let expected_first = &commit_ids[5]; // C5 (0-indexed)
            let expected_second = &commit_ids[4]; // C4

            println!("Total commits: {}", commit_ids.len());
            println!("Expected first: {expected_first} (C5 - Commit 5)");
            println!("Expected second: {expected_second} (C4 - Commit 4)");
            println!(
                "Actual first: {} ({})",
                paginated_commits[0].id, paginated_commits[0].message
            );
            println!(
                "Actual second: {} ({})",
                paginated_commits[1].id, paginated_commits[1].message
            );

            assert_eq!(
                &paginated_commits[0].id, expected_first,
                "First result should be C5 (Commit 5), but got {}",
                paginated_commits[0].message
            );
            assert_eq!(
                &paginated_commits[1].id, expected_second,
                "Second result should be C4 (Commit 4), but got {}",
                paginated_commits[1].message
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_pagination_with_forward_path() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create exactly 10 commits - this should use forward pagination (fast path)
            let mut commit_ids = Vec::new();

            for i in 0..10 {
                let filename = format!("file_{i}.txt");
                let file_path = repo.path.join(&filename);
                test::write_txt_file_to_path(&file_path, format!("Content {i}"))?;

                repositories::add(&repo, &file_path).await?;
                let commit = repositories::commit(&repo, &format!("Commit {i}"))?;
                commit_ids.push(commit.id.clone());
            }

            // With skip=1, limit=2, and total commits <= 10, should use forward path
            let (paginated_commits, _total, _cached) =
                list_from_paginated_impl(&repo, "main", 1, 2)?;

            assert_eq!(
                paginated_commits.len(),
                2,
                "Should return exactly 2 commits"
            );

            // Forward path should work correctly: skip C9, return [C8, C7]
            let expected_first = &commit_ids[8]; // C8
            let expected_second = &commit_ids[7]; // C7

            println!("Forward path test:");
            println!("Expected first: {expected_first} (C8)");
            println!("Expected second: {expected_second} (C7)");
            println!(
                "Actual first: {} ({})",
                paginated_commits[0].id, paginated_commits[0].message
            );
            println!(
                "Actual second: {} ({})",
                paginated_commits[1].id, paginated_commits[1].message
            );

            assert_eq!(
                &paginated_commits[0].id, expected_first,
                "First result should be C8, got {}",
                paginated_commits[0].message
            );
            assert_eq!(
                &paginated_commits[1].id, expected_second,
                "Second result should be C7, got {}",
                paginated_commits[1].message
            );

            Ok(())
        })
        .await
    }
}
