use flate2::write::GzEncoder;
use flate2::Compression;
use futures::prelude::*;
use indicatif::ProgressBar;
use std::collections::HashSet;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::time::Duration;

use crate::api::client::commits::ChunkParams;
use crate::constants::AVG_CHUNK_SIZE;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core::progress::push_progress::PushProgress;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, MerkleHash, RemoteRepository};
use crate::util::{self, concurrency};
use crate::{api, repositories};

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
    let mut commits = repositories::commits::list_between(repo, &latest_remote_commit, commit)?;
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
    log::debug!(
        "push_commits missing_commit_hashes count: {}",
        missing_commit_hashes.len()
    );

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
        let Some(commit_node) = repositories::tree::get_root_with_children(repo, commit)? else {
            log::error!("push_commits commit node not found for commit: {}", commit);
            continue;
        };
        candidate_nodes.insert(commit_node.clone());
        commit_node.walk_tree_without_leaves(|node| {
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
    push_entries(repo, remote_repo, &missing_files, commit, &progress).await?;

    // Mark commits as synced on the server
    api::client::commits::mark_commits_as_synced(remote_repo, missing_commit_hashes).await?;

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

    let large_entries_sync = chunk_and_send_large_entries(
        local_repo,
        remote_repo,
        larger_entries,
        commit,
        AVG_CHUNK_SIZE,
        progress,
    );
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
    commit: &Commit,
    chunk_size: u64,
    progress: &Arc<PushProgress>,
) -> Result<(), OxenError> {
    if entries.is_empty() {
        return Ok(());
    }

    use tokio::time::sleep;
    type PieceOfWork = (Entry, LocalRepository, Commit, RemoteRepository);
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!("Chunking and sending {} larger files", entries.len());
    let entries: Vec<PieceOfWork> = entries
        .iter()
        .map(|e| {
            (
                e.to_owned(),
                local_repo.to_owned(),
                commit.to_owned(),
                remote_repo.to_owned(),
            )
        })
        .collect();

    let queue = Arc::new(TaskQueue::new(entries.len()));
    let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
    for entry in entries.iter() {
        queue.try_push(entry.to_owned()).unwrap();
        finished_queue.try_push(false).unwrap();
    }

    let worker_count = concurrency::num_threads_for_items(entries.len());
    log::debug!(
        "worker_count {} entries len {}",
        worker_count,
        entries.len()
    );
    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        tokio::spawn(async move {
            loop {
                let (entry, repo, commit, remote_repo) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                upload_large_file_chunks(entry, repo, commit, remote_repo, chunk_size, &bar).await;

                finished_queue.pop().await;
            }
        });
    }

    while !finished_queue.is_empty() {
        // log::debug!("Before waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All large file tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

/// Chunk and send large file in parallel
async fn upload_large_file_chunks(
    entry: Entry,
    repo: LocalRepository,
    commit: Commit,
    remote_repo: RemoteRepository,
    chunk_size: u64,
    progress: &Arc<PushProgress>,
) {
    // Open versioned file
    let version_store = repo.version_store().unwrap();
    let file = version_store.open_version(&entry.hash()).unwrap();
    let mut reader = BufReader::new(file);
    // The version path is just being used for compatibility with the server endpoint,
    // we aren't using it to read the file.
    // TODO: This should be migrated to use the new versions API
    let version_path = util::fs::version_path_for_entry(&repo, &entry);

    // These variables are the same for every chunk
    // let is_compressed = false;
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let path = util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();
    let file_name = Some(String::from(path.to_str().unwrap()));

    // Calculate chunk sizes
    let total_bytes = entry.num_bytes();
    let total_chunks = ((total_bytes / chunk_size) + 1) as usize;
    let mut total_bytes_read = 0;
    let mut chunk_size = chunk_size;

    // Create a client for uploading chunks
    let client = Arc::new(
        api::client::builder_for_remote_repo(&remote_repo)
            .unwrap()
            .build()
            .unwrap(),
    );

    // Create queues for sending data to workers
    type PieceOfWork = (
        Vec<u8>,
        u64,   // chunk size
        usize, // chunk num
        usize, // total chunks
        u64,   // total size
        Arc<reqwest::Client>,
        RemoteRepository,
        String, // entry hash
        Commit,
        Option<String>, // filename
    );

    // In order to upload chunks in parallel
    // We should only read N chunks at a time so that
    // the whole file does not get read into memory
    let sub_chunk_size = concurrency::num_threads_for_items(total_chunks);

    let mut total_chunk_idx = 0;
    let mut processed_chunk_idx = 0;
    let num_sub_chunks = (total_chunks / sub_chunk_size) + 1;
    log::debug!(
        "upload_large_file_chunks {:?} processing file in {} subchunks of size {} from total {} chunk size {} file size {}",
        entry.path(),
        num_sub_chunks,
        sub_chunk_size,
        total_chunks,
        chunk_size,
        total_bytes
    );
    for i in 0..num_sub_chunks {
        log::debug!(
            "upload_large_file_chunks Start reading subchunk {i}/{num_sub_chunks} of size {sub_chunk_size} from total {total_chunks} chunk size {chunk_size} file size {total_bytes_read}/{total_bytes}"
        );
        // Read and send the subset of buffers sequentially
        let mut sub_buffers: Vec<Vec<u8>> = Vec::new();
        for _ in 0..sub_chunk_size {
            // If we have read all the bytes, break
            if total_bytes_read >= total_bytes {
                break;
            }

            // Make sure we read the last size correctly
            if (total_bytes_read + chunk_size) > total_bytes {
                chunk_size = total_bytes % chunk_size;
            }

            let percent_read = (total_bytes_read as f64 / total_bytes as f64) * 100.0;
            log::debug!("upload_large_file_chunks has read {total_bytes_read}/{total_bytes} = {percent_read}% about to read {chunk_size}");

            // Only read as much as you need to send so we don't blow up memory on large files
            let mut buffer = vec![0u8; chunk_size as usize];
            match reader.read_exact(&mut buffer) {
                Ok(_) => {}
                Err(err) => {
                    log::error!("upload_large_file_chunks Error reading file {:?} chunk {total_chunk_idx}/{total_chunks} chunk size {chunk_size} total_bytes_read: {total_bytes_read} total_bytes: {total_bytes} {:?}", entry.path(), err);
                    return;
                }
            }
            total_bytes_read += chunk_size;
            total_chunk_idx += 1;

            sub_buffers.push(buffer);
        }
        log::debug!(
            "upload_large_file_chunks Done, have read subchunk {}/{} subchunk {}/{} of size {}",
            processed_chunk_idx,
            total_chunks,
            i,
            num_sub_chunks,
            sub_chunk_size
        );

        // Then send sub_buffers over network in parallel
        // let queue = Arc::new(TaskQueue::new(sub_buffers.len()));
        // let finished_queue = Arc::new(FinishedTaskQueue::new(sub_buffers.len()));
        let mut tasks: Vec<PieceOfWork> = Vec::new();
        for buffer in sub_buffers.iter() {
            tasks.push((
                buffer.to_owned(),
                chunk_size,
                processed_chunk_idx, // Needs to be the overall chunk num
                total_chunks,
                total_bytes,
                client.clone(),
                remote_repo.to_owned(),
                entry.hash().to_owned(),
                commit.to_owned(),
                file_name.to_owned(),
            ));
            // finished_queue.try_push(false).unwrap();
            processed_chunk_idx += 1;
        }

        // Setup the stream chunks in parallel
        let bodies = stream::iter(tasks)
            .map(|item| async move {
                let (
                    buffer,
                    chunk_size,
                    chunk_num,
                    total_chunks,
                    total_size,
                    client,
                    remote_repo,
                    entry_hash,
                    _commit,
                    file_name,
                ) = item;
                let size = buffer.len() as u64;
                log::debug!(
                    "upload_large_file_chunks Streaming entry buffer {}/{} of size {}",
                    chunk_num,
                    total_chunks,
                    size
                );

                let params = ChunkParams {
                    chunk_num,
                    total_chunks,
                    total_size: total_size as usize,
                };

                let is_compressed = false;
                match api::client::commits::upload_data_chunk_to_server_with_retry(
                    &client,
                    &remote_repo,
                    &buffer,
                    &entry_hash,
                    &params,
                    is_compressed,
                    &file_name,
                )
                .await
                {
                    Ok(_) => {
                        log::debug!(
                            "upload_large_file_chunks Successfully uploaded subchunk overall chunk {}/{}",
                            chunk_num,
                            total_chunks
                        );
                        Ok(chunk_size)
                    }
                    Err(err) => {
                        log::error!("Error uploading chunk: {err}");
                        Err(err)
                    }
                }
            })
            .buffer_unordered(sub_chunk_size);

        // Wait for all requests to finish
        bodies
            .for_each(|b| async {
                match b {
                    Ok(_) => {
                        progress.add_bytes(chunk_size);
                    }
                    Err(err) => {
                        log::error!("Error uploading chunk: {err}")
                    }
                }
            })
            .await;

        log::debug!("upload_large_file_chunks Subchunk {i}/{num_sub_chunks} tasks done. :-)");
    }
    progress.add_files(1);
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

    // TODO: this needs some more robust error handling. What should we do if a single item fails?
    // Currently no way to bubble up that error.
    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        tokio::spawn(async move {
            loop {
                let (chunk, repo, _commit, remote_repo, client) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                let enc = GzEncoder::new(Vec::new(), Compression::default());
                let mut tar = tar::Builder::new(enc);
                log::debug!("Chunk size {}", chunk.len());
                let chunk_size = match repositories::entries::compute_generic_entries_size(&chunk) {
                    Ok(size) => size,
                    Err(e) => {
                        log::error!("Failed to compute entries size: {}", e);
                        continue; // or break or decide on another error-handling strategy
                    }
                };

                for entry in &chunk {
                    log::trace!(
                        "bundle_and_send_small_entries adding entry to tarball: {:?}",
                        entry
                    );
                    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
                    let version_path = util::fs::version_path_for_entry(&repo, entry);
                    let name = util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();

                    match tar.append_path_with_name(version_path, name) {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!("Failed to add file to archive: {}", e);
                            continue; // TODO: error handling, same as above
                        }
                    };
                }

                let buffer = match tar.into_inner() {
                    Ok(gz_encoder) => match gz_encoder.finish() {
                        Ok(buffer) => {
                            let size = buffer.len() as u64;
                            log::debug!("Got tarball buffer of size {}", size);
                            buffer
                        }
                        Err(err) => {
                            panic!("Error creating tar.gz on entries: {}", err)
                        }
                    },
                    Err(err) => {
                        panic!("Error creating tar of entries: {}", err)
                    }
                };

                // Send tar.gz to server
                let is_compressed = true;
                let file_name = None;

                // TODO: Refactor where the bars are being passed so we don't need silent here
                let quiet_bar = Arc::new(ProgressBar::hidden());

                match api::client::commits::post_data_to_server_with_client(
                    &client,
                    &remote_repo,
                    buffer,
                    is_compressed,
                    &file_name,
                    quiet_bar,
                )
                .await
                {
                    Ok(_) => {
                        log::debug!("Successfully uploaded data!")
                    }
                    Err(err) => {
                        log::error!("Error uploading chunk: {:?}", err)
                    }
                }
                bar.add_bytes(chunk_size);
                bar.add_files(chunk.len() as u64);
                finished_queue.pop().await;
            }
        });
    }
    while !finished_queue.is_empty() {
        // log::debug!("Waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}
