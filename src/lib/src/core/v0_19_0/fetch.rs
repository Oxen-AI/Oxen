use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::api;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::core;
use crate::core::refs::RefWriter;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Branch, Commit, CommitEntry};
use crate::model::{LocalRepository, MerkleHash, RemoteBranch, RemoteRepository};
use crate::repositories;

use crate::core::v0_19_0::index::commit_merkle_tree::CommitMerkleTree;
use crate::core::v0_19_0::structs::pull_progress::PullProgress;

pub async fn fetch_remote_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    remote_branch: &RemoteBranch,
    all: bool,
) -> Result<(), OxenError> {
    log::debug!(
        "fetching remote branch {} --all {}",
        remote_branch.branch,
        all
    );

    // Start the timer
    let start = std::time::Instant::now();

    // Find the head commit on the remote branch
    let Some(remote_branch) =
        api::client::branches::get_by_name(remote_repo, &remote_branch.branch).await?
    else {
        return Err(OxenError::remote_branch_not_found(&remote_branch.branch));
    };

    // Write the new branch commit id to the local repo
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_branch_commit_id(&remote_branch.name, &remote_branch.commit_id)?;

    if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
        if head_commit.id == remote_branch.commit_id {
            println!("Repository is up to date.");
            return Ok(());
        }
    }

    fetch_full_tree_and_hashes(repo, remote_repo, &remote_branch).await?;
    let commits = repositories::commits::list_unsynced_from(repo, &remote_branch.commit_id)?;

    // Keep track of how many bytes we have downloaded
    let pull_progress = PullProgress::new();

    // Recursively download the entries
    if all {
        log::debug!("fetching all {} commits", commits.len());
        for commit in commits {
            log::debug!("fetching all commits {}", commit);
            let hash = MerkleHash::from_str(&commit.id)?;
            api::client::tree::download_tree(repo, remote_repo).await?;
            let commit_node = CommitMerkleTree::read_node(repo, &hash, true)?.unwrap();
            r_download_entries(
                repo,
                remote_repo,
                &commit_node,
                &PathBuf::from(""),
                &pull_progress,
            )
            .await?;
        }
    } else {
        let hash = MerkleHash::from_str(&remote_branch.commit_id)?;
        let commit_node = CommitMerkleTree::read_node(repo, &hash, true)?.unwrap();
        let directory = PathBuf::from("");
        r_download_entries(repo, remote_repo, &commit_node, &directory, &pull_progress).await?;
    }

    // If we fetched all the data, we're no longer shallow
    repo.write_is_shallow(false)?;

    pull_progress.finish();
    let duration = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);

    println!(
        "🐂 oxen fetched {} ({} files) in {}",
        bytesize::ByteSize::b(pull_progress.get_num_bytes()),
        pull_progress.get_num_files(),
        humantime::format_duration(duration)
    );

    Ok(())
}

pub async fn fetch_tree_and_hashes_for_commit_id(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<(), OxenError> {
    let repo_hidden_dir = repo.path.join(OXEN_HIDDEN_DIR);
    api::client::commits::download_dir_hashes_db_to_path(remote_repo, commit_id, &repo_hidden_dir)
        .await?;

    let hash = MerkleHash::from_str(commit_id)?;
    api::client::tree::download_tree_from(repo, remote_repo, &hash).await?;

    api::client::commits::download_dir_hashes_from_commit(remote_repo, commit_id, &repo_hidden_dir)
        .await?;

    Ok(())
}

pub async fn fetch_full_tree_and_hashes(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    remote_branch: &Branch,
) -> Result<(), OxenError> {
    // Download the latest merkle tree
    // Must do this before downloading the commit node
    // because the commit node references the merkle tree
    let repo_hidden_dir = repo.path.join(OXEN_HIDDEN_DIR);
    api::client::commits::download_dir_hashes_db_to_path(
        remote_repo,
        &remote_branch.commit_id,
        &repo_hidden_dir,
    )
    .await?;

    // Download the latest merkle tree
    // let hash = MerkleHash::from_str(&remote_branch.commit_id)?;
    api::client::tree::download_tree(repo, remote_repo).await?;
    // let commit_node = CommitMerkleTree::read_node(repo, &hash, true)?.unwrap();

    // Download the commit history
    // Check what our HEAD commit is locally
    if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
        // Remote is not guaranteed to have our head commit
        // If it doesn't, we will download all commits from the remote branch commit
        if api::client::tree::has_node(remote_repo, MerkleHash::from_str(&head_commit.id)?).await? {
            // Download the commits between the head commit and the remote branch commit
            let base_commit_id = head_commit.id;
            let head_commit_id = &remote_branch.commit_id;

            api::client::commits::download_base_head_dir_hashes(
                remote_repo,
                &base_commit_id,
                head_commit_id,
                &repo_hidden_dir,
            )
            .await?;
        } else {
            // Download the dir hashes from the remote branch commit
            api::client::commits::download_dir_hashes_from_commit(
                remote_repo,
                &remote_branch.commit_id,
                &repo_hidden_dir,
            )
            .await?;
        }
    } else {
        // Download the dir hashes from the remote branch commit
        api::client::commits::download_dir_hashes_from_commit(
            remote_repo,
            &remote_branch.commit_id,
            &repo_hidden_dir,
        )
        .await?;
    };
    Ok(())
}

/// Fetch missing entries for a commit
/// If there is no remote, or we can't find the remote, this will *not* error
pub async fn maybe_fetch_missing_entries(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    // If we don't have a remote, there are no missing entries, so return
    let rb = RemoteBranch::default();
    let remote = repo.get_remote(&rb.remote);
    let Some(remote) = remote else {
        log::debug!("No remote, no missing entries to fetch");
        return Ok(());
    };

    let commit_merkle_tree = CommitMerkleTree::from_commit(repo, commit)?;

    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => {
            log::warn!("Remote repo not found: {}", remote.url);
            return Ok(());
        }
        Err(err) => {
            log::warn!("Error getting remote repo: {}", err);
            return Ok(());
        }
    };

    // TODO: what should we print here? If there is nothing to pull, we
    // shouldn't show the PullProgress

    // Keep track of how many bytes we have downloaded
    let pull_progress = PullProgress::new();

    // Recursively download the entries
    let directory = PathBuf::from("");
    r_download_entries(
        repo,
        &remote_repo,
        &commit_merkle_tree.root,
        &directory,
        &pull_progress,
    )
    .await?;

    Ok(())
}

async fn r_download_entries(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node: &MerkleTreeNode,
    directory: &Path,
    pull_progress: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    for child in &node.children {
        let mut new_directory = directory.to_path_buf();
        if let EMerkleTreeNode::Directory(dir_node) = &child.node {
            new_directory.push(&dir_node.name);
        }

        if child.has_children() {
            Box::pin(r_download_entries(
                repo,
                remote_repo,
                child,
                &new_directory,
                pull_progress,
            ))
            .await?;
        }
    }

    if let EMerkleTreeNode::VNode(_) = &node.node {
        // Figure out which entries need to be downloaded
        let mut missing_entries: Vec<Entry> = vec![];
        let missing_hashes = repositories::tree::list_missing_file_hashes(repo, &node.hash)?;

        for child in &node.children {
            if let EMerkleTreeNode::File(file_node) = &child.node {
                if !missing_hashes.contains(&child.hash) {
                    continue;
                }

                missing_entries.push(Entry::CommitEntry(CommitEntry {
                    commit_id: file_node.last_commit_id.to_string(),
                    path: directory.join(&file_node.name),
                    hash: child.hash.to_string(),
                    num_bytes: file_node.num_bytes,
                    last_modified_seconds: file_node.last_modified_seconds,
                    last_modified_nanoseconds: file_node.last_modified_nanoseconds,
                }));
            }
        }

        core::v0_10_0::index::puller::pull_entries_to_versions_dir(
            remote_repo,
            &missing_entries,
            &repo.path,
            pull_progress,
        )
        .await?;
    }

    if let EMerkleTreeNode::Commit(commit_node) = &node.node {
        // Mark the commit as synced
        let commit_id = commit_node.hash.to_string();
        let commit = repositories::commits::get_by_id(repo, &commit_id)?.unwrap();
        core::commit_sync_status::mark_commit_as_synced(repo, &commit)?;
    }

    Ok(())
}
