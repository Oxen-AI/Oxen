use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;

use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, MerkleHash, RemoteRepository};
use crate::{api, repositories};

use crate::core::v0_19_0::index::CommitMerkleTree;
use crate::core::v0_19_0::structs::push_progress::PushProgress;
use crate::model::merkle_tree::node::MerkleTreeNode;

pub async fn push(repo: &LocalRepository) -> Result<Branch, OxenError> {
    let Some(current_branch) = repositories::branches::current_branch(repo)? else {
        log::debug!("Push, no current branch found");
        return Err(OxenError::must_be_on_valid_branch());
    };
    push_remote_branch(repo, DEFAULT_REMOTE_NAME, current_branch.name).await
}

pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: impl AsRef<str>,
    branch_name: impl AsRef<str>,
) -> Result<Branch, OxenError> {
    // start a timer
    let start = std::time::Instant::now();

    let remote = remote.as_ref();
    let branch_name = branch_name.as_ref();

    let Some(local_branch) = repositories::branches::get_by_name(repo, branch_name)? else {
        return Err(OxenError::local_branch_not_found(branch_name));
    };

    println!(
        "🐂 oxen push {} {} -> {}",
        remote, local_branch.name, local_branch.commit_id
    );

    let remote = repo
        .get_remote(remote)
        .ok_or(OxenError::remote_not_set(remote))?;

    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    push_local_branch_to_remote_repo(repo, &remote_repo, &local_branch).await?;
    let duration = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);
    println!(
        "🐂 push complete 🎉 took {}",
        humantime::format_duration(duration)
    );
    Ok(local_branch)
}

async fn push_local_branch_to_remote_repo(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_branch: &Branch,
) -> Result<(), OxenError> {
    // Get the commit from the branch
    let Some(commit) = repositories::commits::get_by_id(repo, &local_branch.commit_id)? else {
        return Err(OxenError::revision_not_found(
            local_branch.commit_id.clone().into(),
        ));
    };

    // Notify the server that we are starting a push
    api::client::repositories::pre_push(remote_repo, local_branch, &commit.id).await?;

    // Check if the remote branch exists, and either push to it or create a new one
    match api::client::branches::get_by_name(remote_repo, &local_branch.name).await? {
        Some(remote_branch) => {
            push_to_existing_branch(repo, &commit, remote_repo, &remote_branch).await?
        }
        None => push_to_new_branch(repo, remote_repo, local_branch, &commit).await?,
    }

    // Notify the server that we are done pushing
    api::client::repositories::post_push(remote_repo, local_branch, &commit.id).await?;

    Ok(())
}

async fn push_to_new_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: &Branch,
    commit: &Commit,
) -> Result<(), OxenError> {
    // We need to find all the commits that need to be pushed
    let history = repositories::commits::list_from(repo, &commit.id)?;

    // Push the commits
    push_commits(repo, remote_repo, &history).await?;

    // Create the remote branch from the commit
    api::client::branches::create_from_commit(remote_repo, &branch.name, commit).await?;

    Ok(())
}

fn collect_missing_files(
    node: &MerkleTreeNode,
    hashes: &HashSet<MerkleHash>,
    entries: &mut HashSet<Entry>,
) -> Result<(), OxenError> {
    for child in &node.children {
        if let EMerkleTreeNode::File(file_node) = &child.node {
            if !hashes.contains(&child.hash) {
                continue;
            }
            entries.insert(Entry::CommitEntry(CommitEntry {
                commit_id: file_node.last_commit_id.to_string(),
                path: PathBuf::from(&file_node.name),
                hash: child.hash.to_string(),
                num_bytes: file_node.num_bytes,
                last_modified_seconds: file_node.last_modified_seconds,
                last_modified_nanoseconds: file_node.last_modified_nanoseconds,
            }));
        }
    }
    Ok(())
}

async fn push_to_existing_branch(
    repo: &LocalRepository,
    commit: &Commit,
    remote_repo: &RemoteRepository,
    remote_branch: &Branch,
) -> Result<(), OxenError> {
    // Check if the latest commit on the remote is the same as the local branch
    if remote_branch.commit_id == commit.id {
        println!("Everything is up to date");
        return Ok(());
    }

    // Check if the remote branch is ahead or behind the local branch
    // If we don't have the commit locally, we are behind
    let Some(latest_remote_commit) =
        repositories::commits::get_by_id(repo, &remote_branch.commit_id)?
    else {
        let err_str = format!(
            "Branch {} is behind {} must pull.\n\nRun `oxen pull` to update your local branch",
            remote_branch.name, remote_branch.commit_id
        );
        return Err(OxenError::basic_str(err_str));
    };

    // If we do have the commit locally, we are ahead
    // We need to find all the commits that need to be pushed
    let mut commits = repositories::commits::list_between(repo, commit, &latest_remote_commit)?;
    commits.reverse();

    push_commits(repo, remote_repo, &commits).await?;

    // Update the remote branch to point to the latest commit
    api::client::branches::update(remote_repo, &remote_branch.name, commit).await?;

    Ok(())
}

async fn push_commits(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    history: &[Commit],
) -> Result<(), OxenError> {
    // We need to find all the commits that need to be pushed
    let node_hashes = history
        .iter()
        .map(|c| c.hash().unwrap())
        .collect::<HashSet<MerkleHash>>();

    // Given the missing commits on the server, filter the history
    let missing_commit_hashes =
        api::client::commits::list_missing_hashes(remote_repo, node_hashes).await?;
    let commits: Vec<Commit> = history
        .iter()
        .filter(|c| missing_commit_hashes.contains(&c.hash().unwrap()))
        .map(|c| c.to_owned())
        .collect();

    // Collect all the nodes that could be missing from the server
    let progress = Arc::new(PushProgress::new());
    progress.set_message("Collecting missing nodes...");
    let mut candidate_nodes: HashSet<MerkleTreeNode> = HashSet::new();
    for commit in &commits {
        log::debug!("push_commits adding candidate nodes for commit: {}", commit);
        let commit_hash = commit.hash()?;
        let commit_node = CommitMerkleTree::read_depth(repo, &commit_hash, 0)?.unwrap();
        candidate_nodes.insert(commit_node);
        let sub_tree = CommitMerkleTree::from_commit(repo, commit)?;
        sub_tree.walk_tree_without_leaves(|node| {
            candidate_nodes.insert(node.clone());
            progress.set_message(format!(
                "Collecting missing nodes... {}",
                candidate_nodes.len()
            ));
        });
    }
    log::debug!(
        "push_commits candidate_nodes count: {}",
        candidate_nodes.len()
    );

    // Check which of the candidate nodes are missing from the server (just use the hashes)
    let candidate_node_hashes = candidate_nodes
        .iter()
        .map(|n| n.hash)
        .collect::<HashSet<MerkleHash>>();
    progress.set_message(format!(
        "Considering {} nodes...",
        candidate_node_hashes.len()
    ));
    let missing_node_hashes =
        api::client::tree::list_missing_node_hashes(remote_repo, candidate_node_hashes).await?;
    log::debug!(
        "push_commits missing_node_hashes count: {}",
        missing_node_hashes.len()
    );

    // Filter the candidate nodes to only include the missing ones
    let missing_nodes: HashSet<MerkleTreeNode> = candidate_nodes
        .into_iter()
        .filter(|n| missing_node_hashes.contains(&n.hash))
        .collect();
    log::debug!("push_commits missing_nodes count: {}", missing_nodes.len());
    progress.set_message(format!("Pushing {} nodes...", missing_nodes.len()));
    api::client::tree::create_nodes(repo, remote_repo, missing_nodes.clone(), &progress).await?;

    // Create the dir hashes for the missing commits
    api::client::commits::post_commits_dir_hashes_to_server(repo, remote_repo, &commits).await?;

    // Check which file hashes are missing from the server
    progress.set_message("Checking for missing files...".to_string());
    let missing_file_hashes = api::client::tree::list_missing_file_hashes_from_commits(
        repo,
        remote_repo,
        missing_commit_hashes.clone(),
    )
    .await?;
    progress.set_message(format!("Pushing {} files...", missing_file_hashes.len()));

    let mut missing_files: HashSet<Entry> = HashSet::new();
    for node in missing_nodes {
        collect_missing_files(&node, &missing_file_hashes, &mut missing_files)?;
    }

    let missing_files: Vec<Entry> = missing_files.into_iter().collect();
    let total_bytes = missing_files.iter().map(|e| e.num_bytes()).sum();
    progress.finish();
    let progress = Arc::new(PushProgress::new_with_totals(
        missing_files.len() as u64,
        total_bytes,
    ));
    log::debug!("pushing {} entries", missing_files.len());
    let commit = &history.last().unwrap();
    core::v0_10_0::index::pusher::push_entries(
        repo,
        remote_repo,
        &missing_files,
        commit,
        &progress,
    )
    .await?;
    progress.finish();

    Ok(())
}
