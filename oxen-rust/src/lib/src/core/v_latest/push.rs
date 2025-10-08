use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;

use crate::constants::AVG_CHUNK_SIZE;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core::progress::push_progress::PushProgress;
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, MerkleHash, RemoteRepository};
use crate::opts::PushOpts;
use crate::util::{self, concurrency};
use crate::{api, repositories};

pub async fn push(repo: &LocalRepository) -> Result<Branch, OxenError> {
    let Some(current_branch) = repositories::branches::current_branch(repo)? else {
        log::debug!("Push, no current branch found");
        return Err(OxenError::must_be_on_valid_branch());
    };
    let opts = PushOpts {
        remote: DEFAULT_REMOTE_NAME.to_string(),
        branch: current_branch.name,
        ..Default::default()
    };
    push_remote_branch(repo, &opts).await
}

pub async fn push_remote_branch(
    repo: &LocalRepository,
    opts: &PushOpts,
) -> Result<Branch, OxenError> {
    // start a timer
    let start = std::time::Instant::now();

    let Some(local_branch) = repositories::branches::get_by_name(repo, &opts.branch)? else {
        return Err(OxenError::local_branch_not_found(&opts.branch));
    };

    println!(
        "🐂 oxen push {} {} -> {}",
        opts.remote, local_branch.name, local_branch.commit_id
    );

    let remote = repo
        .get_remote(&opts.remote)
        .ok_or_else(|| OxenError::remote_not_set(&opts.remote))?;

    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    push_local_branch_to_remote_repo(repo, &remote_repo, &local_branch, opts).await?;
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
    opts: &PushOpts,
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
            push_to_existing_branch(repo, &commit, remote_repo, &remote_branch, opts).await?
        }
        None => push_to_new_branch(repo, remote_repo, local_branch, &commit, opts).await?,
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
    opts: &PushOpts,
) -> Result<(), OxenError> {
    // We need to find all the commits that need to be pushed
    let history = repositories::commits::list_from(repo, &commit.id)?;

    // Find the latest remote commit to use as a base for filtering out existing nodes
    let latest_remote_commit = find_latest_remote_commit(repo, remote_repo).await?;

    // Push the commits
    push_commits(repo, remote_repo, latest_remote_commit, &history, opts).await?;

    // Create the remote branch from the commit
    api::client::branches::create_from_commit(remote_repo, &branch.name, commit).await?;

    Ok(())
}

fn collect_missing_files(
    node: &MerkleTreeNode,
    hashes: &HashSet<MerkleHash>,
    entries: &mut HashSet<Entry>,
    total_bytes: &mut u64,
) -> Result<(), OxenError> {
    log::debug!(
        "collect_missing_files node: {} children: {}",
        node,
        node.children.len()
    );
    for child in &node.children {
        if let EMerkleTreeNode::File(file_node) = &child.node {
            if !hashes.contains(&child.hash) {
                continue;
            }
            *total_bytes += file_node.num_bytes();
            entries.insert(Entry::CommitEntry(CommitEntry {
                commit_id: file_node.last_commit_id().to_string(),
                path: PathBuf::from(file_node.name()),
                hash: child.hash.to_string(),
                num_bytes: file_node.num_bytes(),
                last_modified_seconds: file_node.last_modified_seconds(),
                last_modified_nanoseconds: file_node.last_modified_nanoseconds(),
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
    opts: &PushOpts,
) -> Result<(), OxenError> {
    // Check if the latest commit on the remote is the same as the local branch
    if remote_branch.commit_id == commit.id && !opts.missing_files {
        println!("Everything is up to date");
        return Ok(());
    }

    match repositories::commits::list_from(repo, &commit.id) {
        Ok(commits) => {
            if commits.iter().any(|c| c.id == remote_branch.commit_id) {
                //we're ahead

                let latest_remote_commit =
                    repositories::commits::get_by_id(repo, &remote_branch.commit_id)?.ok_or_else(
                        || OxenError::revision_not_found(remote_branch.commit_id.clone().into()),
                    )?;

                let mut commits =
                    repositories::commits::list_between(repo, &latest_remote_commit, commit)?;
                commits.reverse();

                push_commits(
                    repo,
                    remote_repo,
                    Some(latest_remote_commit),
                    &commits,
                    opts,
                )
                .await?;
                api::client::branches::update(remote_repo, &remote_branch.name, commit).await?;
            } else {
                //we're behind
                let err_str = format!(
                    "Branch {} is behind {} must pull.\n\nRun `oxen pull` to update your local branch",
                    remote_branch.name, remote_branch.commit_id
                );
                return Err(OxenError::basic_str(err_str));
            }
        }
        Err(err) => {
            return Err(err);
        }
    };

    Ok(())
}

async fn push_missing_files(
    repo: &LocalRepository,
    opts: &PushOpts,
    remote_repo: &RemoteRepository,
    latest_remote_commit: &Option<Commit>,
    commits: &[Commit],
) -> Result<(), OxenError> {
    let Some(head_commit) = commits.last() else {
        return Err(OxenError::basic_str(
            "Cannot push missing files without a head commit",
        ));
    };

    if let Some(commit_id) = &opts.missing_files_commit_id {
        let commit = repositories::commits::get_by_id(repo, commit_id)?
            .ok_or_else(|| OxenError::commit_id_does_not_exist(commit_id))?;
        list_and_push_missing_files(repo, remote_repo, None, &commit).await?;
    } else if head_commit.id == latest_remote_commit.clone().unwrap().id {
        //both remote and local are at same commit

        let history = repositories::commits::list_from(repo, &head_commit.id)?;

        for commit in history {
            // check missing files for each commit
            list_and_push_missing_files(repo, remote_repo, None, &commit).await?;
        }
    } else {
        list_and_push_missing_files(repo, remote_repo, latest_remote_commit.clone(), head_commit)
            .await?;
    }
    Ok(())
}

async fn list_and_push_missing_files(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    base_commit: Option<Commit>,
    head_commit: &Commit,
) -> Result<(), OxenError> {
    let missing_files =
        api::client::commits::list_missing_files(remote_repo, base_commit, &head_commit.id).await?;
    let missing_files: Vec<Entry> = missing_files.into_iter().map(Entry::CommitEntry).collect();

    let total_bytes = missing_files.iter().map(|e| e.num_bytes()).sum();

    let progress = Arc::new(PushProgress::new_with_totals(
        missing_files.len() as u64,
        total_bytes,
    ));

    push_entries(repo, remote_repo, &missing_files, head_commit, &progress).await?;
    Ok(())
}
async fn push_commits(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    latest_remote_commit: Option<Commit>,
    commits: &[Commit],
    opts: &PushOpts,
) -> Result<(), OxenError> {
    // We need to find all the commits that need to be pushed
    let node_hashes = commits
        .iter()
        .map(|c| c.hash().unwrap())
        .collect::<HashSet<MerkleHash>>();

    if opts.missing_files {
        return push_missing_files(repo, opts, remote_repo, &latest_remote_commit, commits).await;
    }
    // Given the missing commits on the server, filter the history
    let missing_commit_hashes =
        api::client::commits::list_missing_hashes(remote_repo, node_hashes).await?;
    log::debug!(
        "push_commits missing_commit_hashes count: {}",
        missing_commit_hashes.len()
    );

    let missing_commits: Vec<Commit> = commits
        .iter()
        .filter(|c| missing_commit_hashes.contains(&c.hash().unwrap()))
        .map(|c| c.to_owned())
        .collect();

    // Collect all the nodes that could be missing from the server
    let progress = Arc::new(PushProgress::new());
    progress.set_message("Collecting candidate nodes...");

    // Get the node hashes for the starting commit (if we have one)
    let mut starting_node_hashes = HashSet::new();
    if let Some(ref commit) = latest_remote_commit {
        repositories::tree::populate_starting_hashes(
            repo,
            commit,
            &None,
            &None,
            &mut starting_node_hashes,
        )?;
    }

    log::debug!("starting hashes: {:?}", starting_node_hashes.len());

    let mut shared_hashes = starting_node_hashes.clone();
    let mut unique_hashes = HashSet::new();

    let mut candidate_nodes: HashSet<MerkleTreeNode> = HashSet::new();
    for commit in &missing_commits {
        log::debug!("push_commits adding candidate nodes for commit: {}", commit);
        let Some(commit_node) = CommitMerkleTree::get_unique_children_for_commit(
            repo,
            commit,
            &mut shared_hashes,
            &mut unique_hashes,
        )?
        else {
            log::error!("push_commits commit node not found for commit: {}", commit);
            continue;
        };

        shared_hashes.extend(&unique_hashes);
        unique_hashes.clear();
        candidate_nodes.insert(commit_node.clone());

        commit_node.walk_tree_without_leaves(|node| {
            if !starting_node_hashes.contains(&node.hash) {
                candidate_nodes.insert(node.clone());
                progress.set_message(format!(
                    "Collecting candidate nodes... {}",
                    candidate_nodes.len()
                ));
            }
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

    log::debug!("Candidate Hashes: {candidate_node_hashes:?}");
    let missing_node_hashes =
        api::client::tree::list_missing_node_hashes(remote_repo, candidate_node_hashes).await?;
    log::debug!(
        "push_commits missing_node_hashes count: {:?}",
        missing_node_hashes.len()
    );

    // Separate the candidate nodes into present and missing nodes
    let mut missing_nodes: HashSet<MerkleTreeNode> = HashSet::new();
    let mut present_node_hashes: HashSet<MerkleHash> = HashSet::new();

    for node in candidate_nodes.into_iter() {
        if missing_node_hashes.contains(&node.hash) {
            missing_nodes.insert(node);
        } else {
            present_node_hashes.insert(node.hash);
        }
    }

    // As well, don't collect anything in the
    log::debug!(
        "push_commits missing_nodes count: {:?}",
        missing_nodes.len()
    );
    progress.set_message(format!("Pushing {} nodes...", missing_nodes.len()));
    api::client::tree::create_nodes(repo, remote_repo, missing_nodes.clone(), &progress).await?;

    // Create the dir hashes for the missing commits
    api::client::commits::post_commits_dir_hashes_to_server(repo, remote_repo, &missing_commits)
        .await?;

    progress.set_message("Checking for missing files...".to_string());

    starting_node_hashes.extend(present_node_hashes);
    let missing_file_hashes = api::client::tree::list_missing_file_hashes_from_nodes(
        repo,
        remote_repo,
        missing_commit_hashes.clone(),
        starting_node_hashes,
    )
    .await?;
    progress.set_message(format!("Pushing {} files...", missing_file_hashes.len()));
    let mut missing_files: HashSet<Entry> = HashSet::new();
    let mut total_bytes = 0;
    for node in missing_nodes {
        collect_missing_files(
            &node,
            &missing_file_hashes,
            &mut missing_files,
            &mut total_bytes,
        )?;
    }

    let missing_files: Vec<Entry> = missing_files.into_iter().collect();
    progress.finish();
    let progress = Arc::new(PushProgress::new_with_totals(
        missing_files.len() as u64,
        total_bytes,
    ));
    log::debug!("pushing {} entries", missing_files.len());
    let commit = &commits.last().unwrap();
    push_entries(repo, remote_repo, &missing_files, commit, &progress).await?;

    // Mark commits as synced on the server
    api::client::commits::mark_commits_as_synced(remote_repo, missing_commit_hashes).await?;

    // Mark dirs/vnodes as synced on the server
    // TODO

    progress.finish();

    Ok(())
}

pub async fn push_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: &[Entry],
    commit: &Commit,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    log::debug!(
        "PUSH ENTRIES {} -> {} -> '{}'",
        entries.len(),
        commit.id,
        commit.message
    );
    // Some files may be much larger than others....so we can't just zip them up and send them
    // since bodies will be too big. Hence we chunk and send the big ones, and bundle and send the small ones

    // For files smaller than AVG_CHUNK_SIZE, we are going to group them, zip them up, and transfer them
    let smaller_entries: Vec<Entry> = entries
        .iter()
        .filter(|e| e.num_bytes() <= AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    // For files larger than AVG_CHUNK_SIZE, we are going break them into chunks and send the chunks in parallel
    let larger_entries: Vec<Entry> = entries
        .iter()
        .filter(|e| e.num_bytes() > AVG_CHUNK_SIZE)
        .map(|e| e.to_owned())
        .collect();

    let large_entries_sync =
        chunk_and_send_large_entries(local_repo, remote_repo, larger_entries, progress);
    let small_entries_sync = bundle_and_send_small_entries(
        local_repo,
        remote_repo,
        smaller_entries,
        commit,
        AVG_CHUNK_SIZE,
        progress,
    );

    match tokio::join!(large_entries_sync, small_entries_sync) {
        (Ok(_), Ok(_)) => {
            log::debug!("Moving on to post-push validation");
            Ok(())
        }
        (Err(err), Ok(_)) => {
            let err = format!("Error syncing large entries: {err}");
            Err(OxenError::basic_str(err))
        }
        (Ok(_), Err(err)) => {
            let err = format!("Error syncing small entries: {err}");
            Err(OxenError::basic_str(err))
        }
        _ => Err(OxenError::basic_str("Unknown error syncing entries")),
    }
}

async fn chunk_and_send_large_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    use tokio::time::sleep;
    type PieceOfWork = (Entry, PathBuf, RemoteRepository);
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;

    log::debug!("Chunking and sending {} larger files", entries.len());
    let entries: Vec<PieceOfWork> = entries
        .iter()
        .map(|e| {
            (
                e.to_owned(),
                local_repo.path.clone(),
                remote_repo.to_owned(),
            )
        })
        .collect();

    let queue = Arc::new(TaskQueue::new(entries.len()));
    for entry in entries.iter() {
        queue.try_push(entry.to_owned()).unwrap();
    }
    let version_store = local_repo.version_store()?;

    let worker_count = concurrency::num_threads_for_items(entries.len());
    log::debug!(
        "worker_count {} entries len {}",
        worker_count,
        entries.len()
    );
    let should_stop = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<String>));
    let mut handles = vec![];

    for worker in 0..worker_count {
        let queue = queue.clone();
        let bar = Arc::clone(progress);
        let should_stop = should_stop.clone();
        let first_error = first_error.clone();
        let version_store = Arc::clone(&version_store);

        let handle = tokio::spawn(async move {
            loop {
                if should_stop.load(Ordering::Relaxed) {
                    break;
                }

                let Some((entry, repo_path, remote_repo)) = queue.try_pop() else {
                    // reached end of queue
                    break;
                };

                let version_path = match version_store.get_version_path(&entry.hash()) {
                    Ok(path) => path,
                    Err(e) => {
                        log::error!("Failed to get version path: {}", e);
                        should_stop.store(true, Ordering::Relaxed);
                        *first_error.lock().await = Some(e.to_string());
                        break;
                    }
                };
                let relative_path = util::fs::path_relative_to_dir(version_path, &repo_path)
                    .unwrap_or_else(|e| {
                        log::error!("Failed to get relative path: {}", e);
                        entry.path()
                    });
                let path = if relative_path.exists() {
                    relative_path
                } else {
                    // for test environment
                    repo_path.join(relative_path)
                };

                match api::client::versions::parallel_large_file_upload(
                    &remote_repo,
                    path,
                    None::<PathBuf>,
                    None,
                    Some(entry.clone()),
                    Some(&bar),
                )
                .await
                {
                    Ok(_) => {
                        log::debug!(
                            "worker[{}] successfully uploaded {:?}",
                            worker,
                            entry.path()
                        );
                    }
                    Err(err) => {
                        log::error!(
                            "worker[{}] failed to upload {:?}: {}",
                            worker,
                            entry.path(),
                            err
                        );
                        should_stop.store(true, Ordering::Relaxed);
                        *first_error.lock().await = Some(err.to_string());
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }

    let join_results = futures::future::join_all(handles).await;
    for res in join_results {
        if let Err(e) = res {
            return Err(OxenError::basic_str(format!("worker task panicked: {e}")));
        }
    }

    if let Some(err) = first_error.lock().await.clone() {
        return Err(OxenError::basic_str(err));
    }

    log::debug!("All large file tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Sends entries in tarballs of size ~chunk size
async fn bundle_and_send_small_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    entries: Vec<Entry>,
    commit: &Commit,
    avg_chunk_size: u64,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    // Compute size for this subset of entries
    let total_size = repositories::entries::compute_generic_entries_size(&entries)?;
    let num_chunks = ((total_size / avg_chunk_size) + 1) as usize;

    let mut chunk_size = entries.len() / num_chunks;
    if num_chunks > entries.len() {
        chunk_size = entries.len();
    }

    // Create a client for uploading chunks
    let client = Arc::new(api::client::builder_for_remote_repo(remote_repo)?.build()?);

    // Split into chunks, zip up, and post to server
    use tokio::time::sleep;
    type PieceOfWork = (
        Vec<Entry>,
        LocalRepository,
        Commit,
        RemoteRepository,
        Arc<reqwest::Client>,
    );
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!("Creating {num_chunks} chunks from {total_size} bytes with size {chunk_size}");
    let chunks: Vec<PieceOfWork> = entries
        .chunks(chunk_size)
        .map(|c| {
            (
                c.to_owned(),
                local_repo.to_owned(),
                commit.to_owned(),
                remote_repo.to_owned(),
                client.clone(),
            )
        })
        .collect();

    let worker_count = concurrency::num_threads_for_items(chunks.len());
    let queue = Arc::new(TaskQueue::new(chunks.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(chunks.len()));
    for chunk in chunks {
        queue.try_push(chunk).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    // Error handling similar to `chunk_and_send_large_entries`
    use std::sync::atomic::{AtomicBool, Ordering};
    let should_stop = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<String>));
    let mut handles = vec![];

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        let should_stop = should_stop.clone();
        let first_error = first_error.clone();
        let handle = tokio::spawn(async move {
            loop {
                log::debug!("worker[{worker}] processing task");
                if should_stop.load(Ordering::Relaxed) {
                    break;
                }

                let Some((chunk, repo, _commit, remote_repo, client)) = queue.try_pop() else {
                    // reached end of queue
                    break;
                };

                let chunk_size = match repositories::entries::compute_generic_entries_size(&chunk) {
                    Ok(size) => size,
                    Err(e) => {
                        log::error!("Failed to compute entries size: {}", e);
                        should_stop.store(true, Ordering::Relaxed);
                        *first_error.lock().await = Some(e.to_string());
                        finished_queue.pop().await;
                        break;
                    }
                };

                let _synced_nodes = HashSet::new();
                match api::client::versions::multipart_batch_upload_with_retry(
                    &repo,
                    &remote_repo,
                    &chunk,
                    &client,
                    &_synced_nodes,
                )
                .await
                {
                    Ok(_err_files) => {
                        bar.add_bytes(chunk_size);
                        bar.add_files(chunk.len() as u64);
                        finished_queue.pop().await;
                    }
                    Err(e) => {
                        should_stop.store(true, Ordering::Relaxed);
                        *first_error.lock().await = Some(e.to_string());
                        finished_queue.pop().await;
                        break;
                    }
                }
            }
        });
        handles.push(handle);
    }

    let join_results = futures::future::join_all(handles).await;
    for res in join_results {
        if let Err(e) = res {
            return Err(OxenError::basic_str(format!("worker task panicked: {e}")));
        }
    }

    if let Some(err) = first_error.lock().await.clone() {
        return Err(OxenError::basic_str(err));
    }

    sleep(Duration::from_millis(100)).await;

    Ok(())
}

async fn find_latest_remote_commit(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
) -> Result<Option<Commit>, OxenError> {
    // TODO: Revisit this and compute the latest commit from the LCA of the local and remote branches
    // Try to get remote branches
    let remote_branches = api::client::branches::list(remote_repo).await?;

    if remote_branches.is_empty() {
        // No remote branches exist - this is a new repo
        return Ok(None);
    }

    // First, try to find the default branch (main)
    let default_branch = remote_branches
        .iter()
        .find(|b| b.name == crate::constants::DEFAULT_BRANCH_NAME)
        .or_else(|| remote_branches.first());

    if let Some(remote_branch) = default_branch {
        // Get the commit from the remote branch
        if let Some(remote_commit) =
            repositories::commits::get_by_id(repo, &remote_branch.commit_id)?
        {
            // We have the remote commit locally, so use it
            Ok(Some(remote_commit))
        } else {
            // We don't have the remote commit locally - this shouldn't happen in normal flow
            // but can happen if we haven't fetched the remote branch
            Ok(None)
        }
    } else {
        // No branches found
        Ok(None)
    }
}
