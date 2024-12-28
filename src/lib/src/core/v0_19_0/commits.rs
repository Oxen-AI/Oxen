use std::collections::{HashMap, HashSet};
use std::path::Path;

use glob::Pattern;
use time::OffsetDateTime;

use crate::core;
use crate::core::refs::{RefReader, RefWriter};
use crate::core::v0_10_0::cache::cacher_status::CacherStatusType;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{CommitNode, EMerkleTreeNode};
use crate::model::{Commit, LocalRepository, MerkleHash, User};
use crate::opts::PaginateOpts;
use crate::view::{PaginatedCommits, StatusMessage};
use crate::{repositories, util};

use std::path::PathBuf;
use std::str;
use std::str::FromStr;

use crate::core::v0_19_0::index::CommitMerkleTree;

use super::index::MerkleNodeDB;

pub fn commit(repo: &LocalRepository, message: impl AsRef<str>) -> Result<Commit, OxenError> {
    super::index::commit_writer::commit(repo, message)
}

pub fn commit_with_user(
    repo: &LocalRepository,
    message: impl AsRef<str>,
    user: &User,
) -> Result<Commit, OxenError> {
    super::index::commit_writer::commit_with_user(repo, message, user)
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
        .and_then(|branch| get_by_id(repo, branch.commit_id).ok().flatten())
}

pub fn latest_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
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
    let ref_reader = RefReader::new(repo)?;
    match ref_reader.head_commit_id() {
        Ok(Some(commit_id)) => MerkleHash::from_str(&commit_id),
        Ok(None) => Err(OxenError::head_not_found()),
        Err(err) => Err(err),
    }
}

pub fn head_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    match ref_reader.head_commit_id() {
        Ok(Some(commit_id)) => {
            let commit_id = MerkleHash::from_str(&commit_id)?;
            get_by_hash(repo, &commit_id)
        }
        Ok(None) => Ok(None),
        Err(err) => Err(err),
    }
}

pub fn head_commit(repo: &LocalRepository) -> Result<Commit, OxenError> {
    let head_commit_id = head_commit_id(repo)?;
    log::debug!("head_commit: head_commit_id: {:?}", head_commit_id);
    let commit_data = CommitMerkleTree::read_node(repo, &head_commit_id, false)?.ok_or(
        OxenError::basic_str(format!(
            "Merkle tree node not found for head commit: '{}'",
            head_commit_id
        )),
    )?;
    let commit = commit_data.commit()?;
    Ok(commit.to_commit())
}

/// Get the root commit of the repository or None
pub fn root_commit_maybe(repo: &LocalRepository) -> Result<Option<Commit>, OxenError> {
    // Try to get a branch ref and follow it to the root
    // We only need to look at one ref as all branches will have the same root
    let ref_reader = RefReader::new(repo)?;
    if let Some(branch) = ref_reader.list_branches()?.first() {
        if let Some(commit) = get_by_id(repo, &branch.commit_id)? {
            let root_commit = root_commit_recursive(repo, MerkleHash::from_str(&commit.id)?)?;
            return Ok(Some(root_commit));
        }
    }
    log::debug!("root_commit_maybe: no root commit found");
    Ok(None)
}

fn root_commit_recursive(
    repo: &LocalRepository,
    commit_id: MerkleHash,
) -> Result<Commit, OxenError> {
    if let Some(commit) = get_by_hash(repo, &commit_id)? {
        if commit.parent_ids.is_empty() {
            return Ok(commit);
        }

        // Only need to check the first parent, as all paths lead to the root
        if let Some(parent_id) = commit.parent_ids.first() {
            let parent_id = MerkleHash::from_str(parent_id)?;
            return root_commit_recursive(repo, parent_id);
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
    let Some(commit_data) = CommitMerkleTree::read_node(repo, hash, false)? else {
        return Ok(None);
    };
    let commit = commit_data.commit()?;
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
    let existing_node = CommitMerkleTree::read_depth(repo, &existing_commit_id, 1)?.ok_or(
        OxenError::basic_str(format!(
            "Merkle tree node not found for commit: '{}'",
            existing_commit.id
        )),
    )?;
    let timestamp = OffsetDateTime::now_utc();
    let commit_node = CommitNode {
        hash: MerkleHash::from_str(&new_commit.id)?,
        node_type: existing_node.node.node_type(),
        parent_ids: vec![existing_commit_id],
        message: new_commit.message.clone(),
        author: new_commit.author.clone(),
        email: new_commit.email.clone(),
        timestamp,
    };
    let parent_id = Some(existing_node.hash);
    let mut commit_db = MerkleNodeDB::open_read_write(repo, &commit_node, parent_id)?;
    // There should always be one child, the root directory
    let dir_node = existing_node.children.first().unwrap().dir()?;
    commit_db.add_child(&dir_node)?;

    // Copy the dir hashes db to the new commit
    let old_dir_hashes_path =
        CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, existing_commit_id.to_owned());
    let new_dir_hashes_path =
        CommitMerkleTree::dir_hash_db_path_from_commit_id(repo, commit_node.hash.to_owned());
    util::fs::copy_dir_all(old_dir_hashes_path, new_dir_hashes_path)?;

    // Update the ref
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_branch_commit_id(branch_name, commit_node.hash.to_string())?;

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

fn list_recursive(
    repo: &LocalRepository,
    commit: Commit,
    results: &mut Vec<Commit>,
    stop_at: Option<&Commit>,
    visited: &mut HashSet<String>,
) -> Result<(), OxenError> {
    // Check if we've already visited this commit
    if !visited.insert(commit.id.clone()) {
        return Ok(());
    }

    log::debug!("list_recursive: commit: {}", commit);
    results.push(commit.clone());

    if stop_at.is_some() && &commit == stop_at.unwrap() {
        return Ok(());
    }

    for parent_id in commit.parent_ids {
        let parent_id = MerkleHash::from_str(&parent_id)?;
        if let Some(parent_commit) = get_by_hash(repo, &parent_id)? {
            list_recursive(repo, parent_commit, results, stop_at, visited)?;
        }
    }
    Ok(())
}

/// List commits for the repository in no particular order
pub fn list_all(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    let branches = ref_reader.list_branches()?;
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
    Ok(filter_unsynced(repo, all_commits))
}

pub fn list_unsynced(repo: &LocalRepository) -> Result<HashSet<Commit>, OxenError> {
    let all_commits = list_all(repo)?;
    Ok(filter_unsynced(repo, all_commits))
}

fn filter_unsynced(repo: &LocalRepository, commits: HashSet<Commit>) -> HashSet<Commit> {
    log::debug!("filter_unsynced filtering down from {}", commits.len());
    let mut unsynced_commits = HashSet::new();
    for commit in commits {
        if !core::commit_sync_status::commit_is_synced(repo, &commit) {
            unsynced_commits.insert(commit);
        }
    }
    log::debug!("list_unsynced filtered down to {}", unsynced_commits.len());
    unsynced_commits
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
    log::debug!("list_between() base: {} head: {}", base, head);
    let mut results = vec![];
    list_recursive(
        repo,
        base.clone(),
        &mut results,
        Some(head),
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
    let tree = repositories::tree::get_by_commit(repo, commit)?;
    let (files, _) = repositories::tree::list_files_and_dirs(&tree)?;
    for file in files {
        let path = file.dir.join(file.file_node.name);
        if pattern.matches_path(&path) {
            results.insert(path);
        }
    }
    Ok(results)
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
        EMerkleTreeNode::File(file_node) => file_node.last_commit_id,
        EMerkleTreeNode::Directory(dir_node) => dir_node.last_commit_id,
        _ => {
            return Err(OxenError::basic_str(format!(
                "Merkle tree node not found for path: {:?}",
                path
            )));
        }
    };
    let last_commit_id = last_commit_id.to_string();
    let commits = list_from(repo, &last_commit_id)?;
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

// TODO: Temporary function until after v0.19.0, see repositories::commits::get_commit_status_tmp
pub fn get_commit_status_tmp(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<CacherStatusType>, OxenError> {
    match get_by_id(repo, &commit.id)? {
        Some(_commit) => Ok(Some(CacherStatusType::Success)),
        None => Ok(None),
    }
}

// TODO: Temporary function until after v0.19.0, see repositories::commits::is_commit_valid_tmp
pub fn is_commit_valid_tmp(_repo: &LocalRepository, _commit: &Commit) -> Result<bool, OxenError> {
    Ok(true)
}
