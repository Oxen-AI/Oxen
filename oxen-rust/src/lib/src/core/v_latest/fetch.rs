use futures::future;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::constants::{AVG_CHUNK_SIZE, OXEN_HIDDEN_DIR};
use crate::core;
use crate::core::refs::with_ref_manager;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Branch, Commit, CommitEntry, MerkleHash};
use crate::model::{LocalRepository, RemoteBranch, RemoteRepository};
use crate::repositories;
use crate::storage::VersionStore;
use crate::util::concurrency;
use crate::{api, util};

use crate::core::progress::pull_progress::PullProgress;
use crate::opts::fetch_opts::FetchOpts;

pub async fn fetch_remote_branch(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    fetch_opts: &FetchOpts,
) -> Result<Branch, OxenError> {
    log::debug!("fetching remote branch with opts {fetch_opts:?}");

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
        log::debug!("Head commit: {head_commit}");
        log::debug!("Remote branch commit: {}", remote_branch.commit_id);
        // If the head commit is the same as the remote branch commit, we are up to date
        if head_commit.id == remote_branch.commit_id {
            println!("Repository is up to date.");
            with_ref_manager(repo, |manager| {
                manager.set_branch_commit_id(&remote_branch.name, &remote_branch.commit_id)
            })?;
            return Ok(remote_branch);
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
        if fetch_opts.all {
            fetch_full_tree_and_hashes(repo, remote_repo, &remote_branch, &pull_progress).await?;
        } else {
            sync_tree_from_commit(
                repo,
                remote_repo,
                &remote_branch.commit_id,
                fetch_opts,
                &pull_progress,
            )
            .await?;
        }
    }

    // Early exit for remote repo
    if repo.is_remote_mode() {
        // Write the new branch commit id to the local repo
        if fetch_opts.should_update_branch_head {
            repositories::branches::update(repo, &fetch_opts.branch, &remote_branch.commit_id)?;
        }
        return Ok(remote_branch);
    }

    // If all, fetch all the missing entries from all the commits
    // Otherwise, fetch the missing entries from the head commit
    let commits = if fetch_opts.all {
        repositories::commits::list_unsynced_from(repo, &remote_branch.commit_id)?
    } else {
        let hash = remote_branch.commit_id.parse()?;
        let commit_node = repositories::tree::get_node_by_id(repo, &hash)?
            .ok_or(OxenError::basic_str("Commit node not found"))?;

        if core::commit_sync_status::commit_is_synced(repo, &hash) {
            HashSet::new()
        } else {
            HashSet::from([commit_node.commit()?.to_commit()])
        }
    };

    log::debug!("Fetch got {} commits", commits.len());

    let mut total_bytes = 0;
    let missing_entries = collect_missing_entries(
        repo,
        &commits,
        &fetch_opts.subtree_paths,
        &fetch_opts.depth,
        &mut total_bytes,
    )?;
    log::debug!(
        "Fetch got {} potentially missing entries",
        missing_entries.len()
    );
    let missing_entries: Vec<Entry> = missing_entries.into_iter().collect();
    pull_progress.finish();
    let pull_progress = Arc::new(PullProgress::new_with_totals(
        missing_entries.len() as u64,
        total_bytes,
    ));
    pull_entries_to_versions_dir(repo, remote_repo, &missing_entries, &pull_progress).await?;

    // If we fetched the data, we're no longer shallow
    repo.write_is_shallow(false)?;

    // Mark the commits as synced
    for commit in commits {
        core::commit_sync_status::mark_commit_as_synced(repo, &commit.id.parse()?)?;
    }

    // Write the new branch commit id to the local repo
    if fetch_opts.should_update_branch_head {
        repositories::branches::update(repo, &fetch_opts.branch, &remote_branch.commit_id)?;
    }

    pull_progress.finish();
    let duration = std::time::Duration::from_millis(start.elapsed().as_millis() as u64);

    println!(
        "🐂 oxen downloaded {} ({} files) in {}",
        bytesize::ByteSize::b(pull_progress.get_num_bytes()),
        pull_progress.get_num_files(),
        humantime::format_duration(duration)
    );

    Ok(remote_branch)
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
    log::debug!("sync_from_head head_commit: {head_commit}");
    log::debug!("sync_from_head branch: {branch}");

    // If HEAD commit IS on the remote server, that means we are behind the remote branch
    if api::client::tree::has_node(remote_repo, head_commit.id.parse()?).await? {
        log::debug!("sync_from_head has head commit: {head_commit}");
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
            &head_commit.id,
            &branch.commit_id,
            &repo_hidden_dir,
        )
        .await?;
    } else {
        // If HEAD commit is NOT on the remote server, that means we are ahead of the remote branch
        // If the node does not exist on the remote server,
        // we need to sync all the commits from the commit id and their parents
        // TODO: This logic doesn't seem correct. Revisit this.
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
    total_bytes: &mut u64,
) -> Result<HashSet<Entry>, OxenError> {
    let mut missing_entries = HashSet::new();

    let mut shared_hashes =
        if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
            let mut starting_node_hashes = HashSet::new();
            repositories::tree::populate_starting_hashes(
                repo,
                &head_commit,
                &None,
                &None,
                &mut starting_node_hashes,
            )?;
            starting_node_hashes
        } else {
            HashSet::new()
        };

    let mut file_hashes_seen = HashSet::new();

    for commit in commits {
        if let Some(subtree_paths) = subtree_paths {
            log::debug!(
                "collect_missing_entries for {subtree_paths:?} subtree paths and depth {depth:?}"
            );
            for subtree_path in subtree_paths {
                let mut unique_hashes = HashSet::new();
                let Some(tree) = repositories::tree::get_subtree_by_depth_with_unique_children(
                    repo,
                    commit,
                    subtree_path,
                    Some(&shared_hashes),
                    Some(&mut unique_hashes),
                    None,
                    depth.unwrap_or(-1),
                )?
                else {
                    log::warn!("get_subtree_by_depth returned None for path: {subtree_path:?}");
                    continue;
                };

                shared_hashes.extend(unique_hashes);

                collect_missing_entries_for_subtree(
                    &tree,
                    &mut missing_entries,
                    &mut file_hashes_seen,
                    total_bytes,
                )?;
            }
        } else {
            let mut unique_hashes = HashSet::new();
            let Some(tree) = repositories::tree::get_subtree_by_depth_with_unique_children(
                repo,
                commit,
                PathBuf::from("."),
                Some(&shared_hashes),
                Some(&mut unique_hashes),
                None,
                depth.unwrap_or(-1),
            )?
            else {
                log::warn!("get_subtree_by_depth returned None for commit: {commit:?}");
                continue;
            };

            shared_hashes.extend(unique_hashes);

            collect_missing_entries_for_subtree(
                &tree,
                &mut missing_entries,
                &mut file_hashes_seen,
                total_bytes,
            )?;
        }
    }
    Ok(missing_entries)
}

fn collect_missing_entries_for_subtree(
    tree: &MerkleTreeNode,
    missing_entries: &mut HashSet<Entry>,
    file_hashes_seen: &mut HashSet<MerkleHash>,
    total_bytes: &mut u64,
) -> Result<(), OxenError> {
    use crate::model::MerkleTreeNodeType;

    tree.walk_tree(|node: &MerkleTreeNode| {
        let t = node.node.node_type();
        if t == MerkleTreeNodeType::File || t == MerkleTreeNodeType::FileChunk {
            let file_hash = *node.node.hash();
            // Only add files we haven't seen before
            if file_hashes_seen.insert(file_hash) {
                let entry = Entry::CommitEntry(CommitEntry::from_node(&node.node));
                *total_bytes += entry.num_bytes();
                missing_entries.insert(entry);
            }
        }
    });
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

    let hash = commit_id.parse()?;
    api::client::tree::download_tree_from(repo, remote_repo, &hash).await?;

    api::client::commits::download_dir_hashes_from_commit(remote_repo, commit_id, &repo_hidden_dir)
        .await?;

    Ok(())
}

pub async fn fetch_full_tree_and_hashes(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    remote_branch: &Branch,
    pull_progress: &Arc<PullProgress>,
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

    pull_progress.set_message(format!(
        "Downloading all commits from {}",
        remote_branch.commit_id
    ));
    // Download the latest merkle tree
    // let hash = MerkleHash::from_str(&remote_branch.commit_id)?;
    api::client::tree::download_tree(repo, remote_repo).await?;
    // let commit_node = CommitMerkleTree::read_node(repo, &hash, true)?.unwrap();

    // Download the commit history
    // Check what our HEAD commit is locally
    if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
        // Remote is not guaranteed to have our head commit
        // If it doesn't, we will download all commit dir hashes from the remote branch commit
        if api::client::tree::has_node(remote_repo, head_commit.id.parse()?).await? {
            // Download the dir_hashes between the head commit and the remote branch commit
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

    let Some(commit_merkle_tree) = repositories::tree::get_root_with_children(repo, commit)? else {
        log::warn!("get_root_with_children returned None for commit: {commit:?}");
        return Ok(());
    };

    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => {
            log::warn!("Remote repo not found: {}", remote.url);
            return Ok(());
        }
        Err(err) => {
            log::warn!("Error getting remote repo: {err}");
            return Ok(());
        }
    };

    // TODO: what should we print here? If there is nothing to pull, we
    // shouldn't show the PullProgress
    log::debug!("Fetching missing entries for commit {commit}");

    // Keep track of how many bytes we have downloaded
    let pull_progress = Arc::new(PullProgress::new());

    // Recursively download the entries
    let directory = PathBuf::from("");
    r_download_entries(
        repo,
        &remote_repo,
        &commit_merkle_tree,
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
            new_directory.push(dir_node.name());
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
                    commit_id: file_node.last_commit_id().to_string(),
                    path: directory.join(file_node.name()),
                    hash: child.hash.to_string(),
                    num_bytes: file_node.num_bytes(),
                    last_modified_seconds: file_node.last_modified_seconds(),
                    last_modified_nanoseconds: file_node.last_modified_nanoseconds(),
                }));
            }
        }

        pull_entries_to_versions_dir(repo, remote_repo, &missing_entries, pull_progress).await?;
    }

    if let EMerkleTreeNode::Commit(commit_node) = &node.node {
        // Mark the commit as synced
        let commit_id = commit_node.hash().to_string();
        let commit = repositories::commits::get_by_id(repo, &commit_id)?.unwrap();
        core::commit_sync_status::mark_commit_as_synced(repo, &commit.id.parse()?)?;
    }

    Ok(())
}

// pull entries from remote repo to versions dir
pub async fn pull_entries_to_versions_dir(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: &[Entry],
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    log::debug!("entries.len() {}", entries.len());
    if entries.is_empty() {
        return Ok(());
    }

    let version_store = repo.version_store()?;
    let missing_entries = get_missing_entries_for_pull(&version_store, entries)?;
    log::debug!("Pulling {} missing entries", missing_entries.len());

    if missing_entries.is_empty() {
        return Ok(());
    }

    // Some files may be much larger than others....so we can't just download them within a single body
    // Hence we chunk and send the big ones, and bundle and download the small ones

    // For files smaller than AVG_CHUNK_SIZE, we are going to group them, zip them up, and transfer them
    let smaller_entries: Vec<Entry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes() <= AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // For files larger than AVG_CHUNK_SIZE, we are going break them into chunks and download the chunks in parallel
    let larger_entries: Vec<Entry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes() > AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    let large_entries_sync = pull_large_entries(repo, remote_repo, larger_entries, progress_bar);

    let small_entries_sync = pull_small_entries(repo, remote_repo, smaller_entries, progress_bar);

    match tokio::join!(large_entries_sync, small_entries_sync) {
        (Ok(_), Ok(_)) => {
            log::debug!("Successfully synced entries!");
        }
        (Err(err), Ok(_)) => {
            let err = format!("Error syncing large entries: {err}");
            return Err(OxenError::basic_str(err));
        }
        (Ok(_), Err(err)) => {
            let err = format!("Error syncing small entries: {err}");
            return Err(OxenError::basic_str(err));
        }
        _ => return Err(OxenError::basic_str("Unknown error syncing entries")),
    }

    Ok(())
}

async fn pull_large_entries(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }
    // Pull the large entries in parallel
    type PieceOfWork = (LocalRepository, RemoteRepository, Entry);
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;

    log::debug!("Chunking and sending {} larger files", entries.len());
    let entries: Vec<PieceOfWork> = entries
        .iter()
        .map(|e| (repo.to_owned(), remote_repo.to_owned(), e.to_owned()))
        .collect();

    let queue = Arc::new(TaskQueue::new(entries.len()));
    for entry in entries.iter() {
        queue.try_push(entry.to_owned()).unwrap();
    }

    let worker_count = concurrency::num_threads_for_items(entries.len());
    log::debug!(
        "worker_count {} entries len {}",
        worker_count,
        entries.len()
    );
    let mut handles = vec![];

    for worker in 0..worker_count {
        let queue = queue.clone();
        let progress_bar = Arc::clone(progress_bar);
        let handle = tokio::spawn(async move {
            loop {
                let Some((repo, remote_repo, entry)) = queue.try_pop() else {
                    // reached end of queue
                    break;
                };
                log::debug!("worker[{worker}] processing task...");

                // Chunk and individual files
                let remote_path = &entry.path();

                // Download to the tmp path, then copy over to the entries dir
                match api::client::entries::pull_large_entry(
                    &repo,
                    &remote_repo,
                    &remote_path,
                    &entry,
                )
                .await
                {
                    Ok(_) => {
                        log::debug!("Pulled large entry {remote_path:?} to versions dir");
                        progress_bar.add_bytes(entry.num_bytes());
                        progress_bar.add_files(1);
                    }
                    Err(err) => {
                        log::error!("Could not download chunk... {err}")
                    }
                }
            }
        });
        handles.push(handle);
    }
    let join_results = future::join_all(handles).await;
    for res in join_results {
        if let Err(e) = res {
            return Err(OxenError::basic_str(format!("worker task panicked: {e}")));
        }
    }

    log::debug!("All large file tasks done. :-)");

    Ok(())
}

async fn pull_small_entries(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    let total_size = repositories::entries::compute_generic_entries_size(&entries)?;

    // Compute num chunks
    let num_chunks = ((total_size / AVG_CHUNK_SIZE) + 1) as usize;

    let mut chunk_size = entries.len() / num_chunks;
    if num_chunks > entries.len() {
        chunk_size = entries.len();
    }

    log::debug!(
        "pull_small_entries got {} missing content IDs",
        entries.len()
    );

    // Split into chunks, zip up, and post to server
    type PieceOfWork = (RemoteRepository, Vec<String>, LocalRepository);
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;

    log::debug!("pull_small_entries creating {num_chunks} chunks from {total_size} bytes with size {chunk_size}");

    let chunks: Vec<PieceOfWork> = entries
        .chunks(chunk_size)
        .map(|chunk| {
            let hashes = chunk.iter().map(|entry| entry.hash().to_string()).collect();
            (remote_repo.to_owned(), hashes, repo.to_owned())
        })
        .collect();

    let worker_count = concurrency::num_threads_for_items(entries.len());
    let queue = Arc::new(TaskQueue::new(chunks.len()));
    for chunk in chunks {
        queue.try_push(chunk).unwrap();
    }
    let mut handles = vec![];

    for worker in 0..worker_count {
        let queue = queue.clone();
        let progress_bar = Arc::clone(progress_bar);
        let handle = tokio::spawn(async move {
            loop {
                let Some((remote_repo, chunk, local_repo)) = queue.try_pop() else {
                    // reached end of queue
                    break;
                };
                log::debug!("worker[{worker}] processing task...");

                match api::client::versions::download_data_from_version_paths(
                    &remote_repo,
                    &chunk,
                    &local_repo,
                )
                .await
                {
                    Ok(download_size) => {
                        progress_bar.add_bytes(download_size);
                        progress_bar.add_files(chunk.len() as u64);
                    }
                    Err(err) => {
                        log::error!("Could not pull entries... {err}")
                    }
                }
            }
        });
        handles.push(handle);
    }
    let join_results = future::join_all(handles).await;
    for res in join_results {
        if let Err(e) = res {
            return Err(OxenError::basic_str(format!("worker task panicked: {e}")));
        }
    }

    log::debug!("All tasks done. :-)");

    Ok(())
}

// download entries to working dir
pub async fn download_entries_to_working_dir(
    remote_repo: &RemoteRepository,
    entries: &[Entry],
    dst: &Path,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    log::debug!(
        "download_entries_to_working_dir entries.len() {}",
        entries.len()
    );

    if entries.is_empty() {
        return Ok(());
    }

    let missing_entries = get_missing_entries_for_download(entries, dst);
    log::debug!("Pulling {} missing entries", missing_entries.len());

    if missing_entries.is_empty() {
        return Ok(());
    }

    // Some files may be much larger than others....so we can't just download them within a single body
    // Hence we chunk and send the big ones, and bundle and download the small ones

    // For files smaller than AVG_CHUNK_SIZE, we are going to group them, zip them up, and transfer them
    let smaller_entries: Vec<Entry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes() <= AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // For files larger than AVG_CHUNK_SIZE, we are going break them into chunks and download the chunks in parallel
    let larger_entries: Vec<Entry> = missing_entries
        .iter()
        .filter(|e| e.num_bytes() > AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    let large_entries_sync =
        download_large_entries(remote_repo, larger_entries, &dst, progress_bar);
    log::info!("Downloaded large entries");
    let small_entries_sync =
        download_small_entries(remote_repo, smaller_entries, &dst, progress_bar);
    log::info!("Downloaded small entries");

    match tokio::join!(large_entries_sync, small_entries_sync) {
        (Ok(_), Ok(_)) => {
            log::debug!("Successfully synced entries!");
        }
        (Err(err), Ok(_)) => {
            let err = format!("Error syncing large entries: {err}");
            return Err(OxenError::basic_str(err));
        }
        (Ok(_), Err(err)) => {
            let err = format!("Error syncing small entries: {err}");
            return Err(OxenError::basic_str(err));
        }
        _ => return Err(OxenError::basic_str("Unknown error syncing entries")),
    }
    log::info!("Finished download_entries_to_working_dir");
    Ok(())
}

async fn download_large_entries(
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    dst: impl AsRef<Path>,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }
    // Pull the large entries in parallel
    type PieceOfWork = (RemoteRepository, Entry, PathBuf, PathBuf);
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;

    log::debug!("Chunking and sending {} larger files", entries.len());
    let large_entry_paths = working_dir_paths_from_large_entries(&entries, dst.as_ref());
    let entries: Vec<PieceOfWork> = entries
        .iter()
        .zip(large_entry_paths.iter())
        .map(|(e, path)| {
            (
                remote_repo.to_owned(),
                e.to_owned(),
                dst.as_ref().to_owned(),
                path.to_owned(),
            )
        })
        .collect();

    let queue = Arc::new(TaskQueue::new(entries.len()));
    for entry in entries.iter() {
        queue.try_push(entry.to_owned()).unwrap();
    }

    let worker_count = concurrency::num_threads_for_items(entries.len());
    log::debug!(
        "worker_count {} entries len {}",
        worker_count,
        entries.len()
    );
    let mut handles = vec![];
    let tmp_dir = util::fs::oxen_hidden_dir(dst).join("tmp").join("pulled");
    log::debug!("Backing up pulls to tmp dir: {:?}", &tmp_dir);
    for worker in 0..worker_count {
        let queue = queue.clone();
        let progress_bar = Arc::clone(progress_bar);
        let handle = tokio::spawn(async move {
            loop {
                let Some((remote_repo, entry, _dst, download_path)) = queue.try_pop() else {
                    // reached end of queue
                    break;
                };
                log::debug!("worker[{worker}] processing task...");

                // Chunk and individual files
                let remote_path = &entry.path();

                // Download to the tmp path, then copy over to the entries dir
                match api::client::entries::download_large_entry(
                    &remote_repo,
                    &remote_path,
                    &download_path,
                    &entry.commit_id(),
                    entry.num_bytes(),
                )
                .await
                {
                    Ok(_) => {
                        // log::debug!("Downloaded large entry {:?} to versions dir", remote_path);
                        progress_bar.add_bytes(entry.num_bytes());
                        progress_bar.add_files(1);
                    }
                    Err(err) => {
                        log::error!("Could not download chunk... {err}")
                    }
                }
            }
        });
        handles.push(handle);
    }
    let join_results = future::join_all(handles).await;
    for res in join_results {
        if let Err(e) = res {
            return Err(OxenError::basic_str(format!("worker task panicked: {e}")));
        }
    }

    log::debug!("All large file tasks done. :-)");

    Ok(())
}

async fn download_small_entries(
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    dst: impl AsRef<Path>,
    progress_bar: &Arc<PullProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    let total_size = repositories::entries::compute_generic_entries_size(&entries)?;

    // Compute num chunks
    let num_chunks = ((total_size / AVG_CHUNK_SIZE) + 1) as usize;

    let mut chunk_size = entries.len() / num_chunks;
    if num_chunks > entries.len() {
        chunk_size = entries.len();
    }

    log::debug!(
        "download_small_entries got {} missing entries",
        entries.len()
    );

    // Split into chunks, zip up, and post to server
    type PieceOfWork = (RemoteRepository, Vec<(String, PathBuf)>, PathBuf);
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;

    let chunks: Vec<PieceOfWork> = entries
        .chunks(chunk_size)
        .map(|chunk| {
            let mut content_ids: Vec<(String, PathBuf)> = vec![];
            for e in chunk {
                content_ids.push((e.hash(), e.path().to_owned()));
            }
            (remote_repo.to_owned(), content_ids, dst.as_ref().to_owned())
        })
        .collect();

    let worker_count = concurrency::num_threads_for_items(entries.len());
    let queue = Arc::new(TaskQueue::new(chunks.len()));
    for chunk in chunks {
        queue.try_push(chunk).unwrap();
    }
    let mut handles = vec![];

    for worker in 0..worker_count {
        let queue = queue.clone();
        let progress_bar = Arc::clone(progress_bar);
        let handle = tokio::spawn(async move {
            loop {
                let Some((remote_repo, chunk, path)) = queue.try_pop() else {
                    // reached end of queue
                    break;
                };
                log::debug!("worker[{worker}] processing task...");

                match api::client::entries::download_data_from_version_paths(
                    &remote_repo,
                    &chunk,
                    &path,
                )
                .await
                {
                    Ok(download_size) => {
                        progress_bar.add_bytes(download_size);
                        progress_bar.add_files(chunk.len() as u64);
                    }
                    Err(err) => {
                        log::error!("Could not download entries... {err}")
                    }
                }
            }
        });
        handles.push(handle);
    }

    let join_results = future::join_all(handles).await;
    for res in join_results {
        if let Err(e) = res {
            return Err(OxenError::basic_str(format!("worker task panicked: {e}")));
        }
    }
    log::debug!("All tasks done. :-)");

    Ok(())
}

fn get_missing_entries_for_download(entries: &[Entry], dst: &Path) -> Vec<Entry> {
    let mut missing_entries: Vec<Entry> = vec![];
    for entry in entries {
        let working_path = dst.join(entry.path());
        if !working_path.exists() {
            missing_entries.push(entry.to_owned())
        }
    }
    missing_entries
}

fn get_missing_entries_for_pull(
    version_store: &Arc<dyn VersionStore>,
    entries: &[Entry],
) -> Result<Vec<Entry>, OxenError> {
    let mut missing_entries: Vec<Entry> = vec![];
    for entry in entries {
        let version_path = version_store.get_version_path(&entry.hash().to_string())?;
        if !version_path.exists() {
            missing_entries.push(entry.to_owned())
        }
    }

    Ok(missing_entries)
}

fn working_dir_paths_from_large_entries(entries: &[Entry], dst: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = vec![];
    for entry in entries.iter() {
        let working_path = dst.join(entry.path());
        paths.push(working_path);
    }
    paths
}
