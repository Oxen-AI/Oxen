use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::constants::OXEN_HIDDEN_DIR;
use crate::core;
use crate::core::refs::RefWriter;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::{EMerkleTreeNode, FileNodeWithDir, MerkleTreeNode};
use crate::model::{Branch, Commit, CommitEntry};
use crate::model::{LocalRepository, MerkleHash, RemoteBranch, RemoteRepository};
use crate::repositories;
use crate::{api, util};

use crate::core::v0_19_0::index::commit_merkle_tree::CommitMerkleTree;
use crate::core::v0_19_0::structs::pull_progress::PullProgress;
use crate::opts::fetch_opts::FetchOpts;

pub async fn fetch_remote_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    fetch_opts: &FetchOpts,
) -> Result<(), OxenError> {
    log::debug!(
        "fetching remote branch {} --all {} --subtree {:?} --depth {:?}",
        fetch_opts.branch,
        fetch_opts.all,
        fetch_opts.subtree_paths,
        fetch_opts.depth,
    );

    // Start the timer
    let start = std::time::Instant::now();

    // Keep track of how many bytes we have downloaded
    let pull_progress = Arc::new(PullProgress::new());
    pull_progress.set_message(format!("Fetching remote branch {}", fetch_opts.branch));

    // Find the head commit on the remote branch
    let Some(remote_branch) =
        api::client::branches::get_by_name(remote_repo, &fetch_opts.branch).await?
    else {
        return Err(OxenError::remote_branch_not_found(&fetch_opts.branch));
    };

    // We may not have a head commit if the repo is empty (initial clone)
    if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
        log::debug!("Head commit: {}", head_commit);
        log::debug!("Remote branch commit: {}", remote_branch.commit_id);
        // If the head commit is the same as the remote branch commit, we are up to date
        if head_commit.id == remote_branch.commit_id {
            println!("Repository is up to date.");
            let ref_writer = RefWriter::new(repo)?;
            ref_writer.set_branch_commit_id(&remote_branch.name, &remote_branch.commit_id)?;
            return Ok(());
        }

        // Download the nodes from the commits between the head and the remote head
        sync_from_head(
            repo,
            remote_repo,
            fetch_opts,
            &remote_branch,
            &head_commit,
            &pull_progress,
        )
        .await?;
    } else {
        // If there is no head commit, we are fetching all commits from the remote branch commit
        log::debug!(
            "Fetching all commits from remote branch {}",
            remote_branch.commit_id
        );
        sync_tree_from_commit(
            repo,
            remote_repo,
            &remote_branch.commit_id,
            fetch_opts,
            &pull_progress,
        )
        .await?;
    }

    // If all, fetch all the missing entries from all the commits
    // Otherwise, fetch the missing entries from the head commit
    let commits = if fetch_opts.all {
        repositories::commits::list_unsynced_from(repo, &remote_branch.commit_id)?
    } else {
        let hash = MerkleHash::from_str(&remote_branch.commit_id)?;
        let recurse = false;
        let commit_node = CommitMerkleTree::read_node(repo, &hash, recurse)?.unwrap();
        HashSet::from([commit_node.commit()?.to_commit()])
    };
    log::debug!("Fetch got {} commits", commits.len());

    let missing_entries =
        collect_missing_entries(repo, &commits, &fetch_opts.subtree_paths, &fetch_opts.depth)?;
    log::debug!("Fetch got {} missing entries", missing_entries.len());
    let missing_entries: Vec<Entry> = missing_entries.into_iter().collect();
    pull_progress.finish();
    let total_bytes = missing_entries.iter().map(|e| e.num_bytes()).sum();
    let pull_progress = Arc::new(PullProgress::new_with_totals(
        missing_entries.len() as u64,
        total_bytes,
    ));
    core::v0_10_0::index::puller::pull_entries_to_versions_dir(
        remote_repo,
        &missing_entries,
        &repo.path,
        &pull_progress,
    )
    .await?;

    // If we fetched the data, we're no longer shallow
    repo.write_is_shallow(false)?;

    // Mark the commits as synced
    for commit in commits {
        core::commit_sync_status::mark_commit_as_synced(repo, &commit)?;
    }

    // Write the new branch commit id to the local repo
    log::debug!(
        "Setting branch {} commit id to {}",
        remote_branch.name,
        remote_branch.commit_id
    );
    let ref_writer = RefWriter::new(repo)?;
    ref_writer.set_branch_commit_id(&remote_branch.name, &remote_branch.commit_id)?;

    pull_progress.finish();
    let duration = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);

    println!(
        "🐂 oxen downloaded {} ({} files) in {}",
        bytesize::ByteSize::b(pull_progress.get_num_bytes()),
        pull_progress.get_num_files(),
        humantime::format_duration(duration)
    );

    Ok(())
}

async fn sync_from_head(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    fetch_opts: &FetchOpts,
    branch: &Branch,
    head_commit: &Commit,
    pull_progress: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    let repo_hidden_dir = util::fs::oxen_hidden_dir(&repo.path);

    // If HEAD commit is not on the remote server, that means we are ahead of the remote branch
    if api::client::tree::has_node(remote_repo, MerkleHash::from_str(&head_commit.id)?).await? {
        pull_progress.set_message(format!(
            "Downloading commits from {} to {}",
            head_commit.id, branch.commit_id
        ));
        api::client::tree::download_trees_between(
            repo,
            remote_repo,
            &head_commit.id,
            &branch.commit_id,
            fetch_opts,
        )
        .await?;
        api::client::commits::download_base_head_dir_hashes(
            remote_repo,
            &branch.commit_id,
            &head_commit.id,
            &repo_hidden_dir,
        )
        .await?;
    } else {
        // If the node does not exist on the remote server,
        // we need to sync all the commits from the commit id and their parents
        sync_tree_from_commit(
            repo,
            remote_repo,
            &branch.commit_id,
            fetch_opts,
            pull_progress,
        )
        .await?;
    }
    Ok(())
}

// Sync all the commits from the commit (and their parents)
async fn sync_tree_from_commit(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit_id: impl AsRef<str>,
    fetch_opts: &FetchOpts,
    pull_progress: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    let repo_hidden_dir = util::fs::oxen_hidden_dir(&repo.path);

    pull_progress.set_message(format!("Downloading commits from {}", commit_id.as_ref()));
    api::client::tree::download_trees_from(repo, remote_repo, &commit_id.as_ref(), fetch_opts)
        .await?;
    api::client::commits::download_dir_hashes_from_commit(
        remote_repo,
        commit_id.as_ref(),
        &repo_hidden_dir,
    )
    .await?;
    Ok(())
}

fn collect_missing_entries(
    repo: &LocalRepository,
    commits: &HashSet<Commit>,
    subtree_paths: &Option<Vec<PathBuf>>,
    depth: &Option<i32>,
) -> Result<HashSet<Entry>, OxenError> {
    let mut missing_entries: HashSet<Entry> = HashSet::new();
    for commit in commits {
        if let Some(subtree_paths) = subtree_paths {
            log::debug!(
                "collect_missing_entries for {:?} subtree paths and depth {:?}",
                subtree_paths,
                depth
            );
            for subtree_path in subtree_paths {
                let tree = repositories::tree::get_subtree_by_depth(
                    repo,
                    commit,
                    &Some(subtree_path.clone()),
                    depth,
                )?;
                collect_missing_entries_for_subtree(&tree, &mut missing_entries)?;
            }
        } else {
            let tree = repositories::tree::get_subtree_by_depth(repo, commit, &None, depth)?;
            collect_missing_entries_for_subtree(&tree, &mut missing_entries)?;
        }
    }
    Ok(missing_entries)
}

fn collect_missing_entries_for_subtree(
    tree: &CommitMerkleTree,
    missing_entries: &mut HashSet<Entry>,
) -> Result<(), OxenError> {
    let files: HashSet<FileNodeWithDir> = repositories::tree::list_all_files(tree)?;
    for file in files {
        missing_entries.insert(Entry::CommitEntry(CommitEntry {
            commit_id: file.file_node.last_commit_id.to_string(),
            path: file.dir.join(&file.file_node.name),
            hash: file.file_node.hash.to_string(),
            num_bytes: file.file_node.num_bytes,
            last_modified_seconds: file.file_node.last_modified_seconds,
            last_modified_nanoseconds: file.file_node.last_modified_nanoseconds,
        }));
    }
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
    log::debug!("Fetching missing entries for commit {}", commit);

    // Keep track of how many bytes we have downloaded
    let pull_progress = Arc::new(PullProgress::new());

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
    log::debug!(
        "fetch r_download_entries ({}) {:?} {:?}",
        node.children.len(),
        node.hash,
        node.node
    );
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
