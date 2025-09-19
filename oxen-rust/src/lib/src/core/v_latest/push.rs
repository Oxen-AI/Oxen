use futures::prelude::*;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;

use crate::api::client::commits::ChunkParams;
use crate::constants::AVG_CHUNK_SIZE;
use crate::constants::DEFAULT_REMOTE_NAME;
use crate::core::progress::push_progress::PushProgress;
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::{
    Branch, Commit, CommitEntry, LocalRepository, MerkleHash, MerkleTreeNodeType, RemoteRepository,
};
use crate::opts::PushOpts;
use crate::util::{self, concurrency};
use crate::{api, repositories};
use derive_more::FromStr;

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
    let missing_files = api::client::commits::list_missing_files(
        remote_repo,
        base_commit,
        Some(head_commit.clone()),
        None,
    )
    .await?;

    let total_bytes = missing_files.iter().map(|e| e.num_bytes()).sum();

    let progress = Arc::new(PushProgress::new_with_totals(
        missing_files.len() as u64,
        total_bytes,
    ));

    push_entries(repo, remote_repo, &missing_files, head_commit, &progress).await?;
    Ok(())
}

async fn get_commit_missing_hashes(
    repo: &LocalRepository,
    latest_remote_commit: Option<Commit>,
    commits: &[Commit],
) -> Result<HashMap<MerkleHash, PushCommitInfo>, OxenError> {
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

    let mut result = HashMap::new();

    for commit in commits.iter().rev() {
        let mut unique_hashes_and_type = HashSet::new();
        log::debug!("push_commits adding candidate nodes for commit: {}", commit);
        let Some(commit_node) = CommitMerkleTree::get_unique_children_for_commit(
            repo,
            commit,
            &mut shared_hashes,
            &mut unique_hashes_and_type,
        )?
        else {
            log::error!("push_commits commit node not found for commit: {}", commit);
            continue;
        };
        log::debug!("✅ unique_hashes_and_type: {:?}", unique_hashes_and_type);
        let (file_entries, _) = repositories::tree::list_files_and_dirs(&commit_node)?;
        log::debug!("✅ file_entries: {:?}", file_entries);
        let entries = file_entries
            .into_iter()
            .map(|entry| CommitEntry::from_file_node(&entry.file_node))
            .collect::<Vec<CommitEntry>>();
        log::debug!("✅ entries: {:?}", entries);

        let unique_files = unique_hashes_and_type
            .iter()
            .filter(|(_, t)| *t == MerkleTreeNodeType::File || *t == MerkleTreeNodeType::FileChunk)
            .map(|(h, _)| h.clone())
            .collect::<HashSet<MerkleHash>>();
        log::debug!("✅unique_files: {:#?}", unique_files);

        let unique_dir_nodes = unique_hashes_and_type
            .iter()
            .filter(|(_, t)| {
                !(*t == MerkleTreeNodeType::File || *t == MerkleTreeNodeType::FileChunk)
            })
            .map(|(h, _)| h.clone())
            .collect::<HashSet<MerkleHash>>();
        log::debug!("✅unique_dir_nodes: {:#?}", unique_dir_nodes);

        let unique_file_entries = entries
            .into_iter()
            .filter(|e| {
                let hash = MerkleHash::from_str(&e.hash).unwrap();
                log::debug!("✅hash: {:#?}", hash);
                let contains = unique_files.contains(&hash);
                log::debug!("✅contains: {:#?}", contains);
                contains
            })
            .collect::<HashSet<CommitEntry>>();
        log::debug!("✅unique_file_entries: {:#?}", unique_file_entries);

        shared_hashes.extend(
            &unique_dir_nodes
                .iter()
                .map(|h| (h.clone(), MerkleTreeNodeType::Dir))
                .collect::<HashSet<(MerkleHash, MerkleTreeNodeType)>>(),
        );
        shared_hashes.extend(
            &unique_files
                .iter()
                .map(|h| (h.clone(), MerkleTreeNodeType::File))
                .collect::<HashSet<(MerkleHash, MerkleTreeNodeType)>>(),
        );

        let push_commit_info = PushCommitInfo {
            unique_dir_nodes,
            // unique_files,
            unique_file_entries,
        };
        result.insert(commit.hash().unwrap(), push_commit_info);
    }

    Ok(result)
}

#[derive(Debug)]
struct PushCommitInfo {
    unique_dir_nodes: HashSet<MerkleHash>,
    // unique_files: HashSet<MerkleHash>,
    unique_file_entries: HashSet<CommitEntry>,
}

async fn push_commits(
    repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    latest_remote_commit: Option<Commit>,
    commits: &[Commit],
    opts: &PushOpts,
) -> Result<(), OxenError> {
    let progress = Arc::new(PushProgress::new());

    if opts.missing_files {
        return push_missing_files(repo, opts, remote_repo, &latest_remote_commit, commits).await;
    }

    // We need to find all the commits that need to be pushed
    let commit_hashes = commits
        .iter()
        .map(|c| c.hash().unwrap())
        .collect::<HashSet<MerkleHash>>();

    let commit_info = get_commit_missing_hashes(repo, latest_remote_commit, commits).await?;

    let missing_commit_hashes =
        api::client::commits::list_missing_hashes(remote_repo, commit_hashes).await?;

    let missing_commits: Vec<Commit> = commits
        .iter()
        .filter(|c| missing_commit_hashes.contains(&c.hash().unwrap()))
        .map(|c| c.to_owned())
        .collect();

    // Create the dir hashes for the missing commits
    api::client::commits::post_commits_dir_hashes_to_server(repo, remote_repo, &missing_commits)
        .await?;

    for commit in missing_commits {
        let commit_hash = commit.hash().unwrap();

        let commit_info = commit_info.get(&commit_hash).unwrap();

        // Get a list of missing file hashes from local
        // Call list_missing_files API to find the missing files from the given list of file hashes.
        // Push the missing files to the server
        let missing_files = api::client::commits::list_missing_files(
            remote_repo,
            None,
            None,
            Some(commit_info.unique_file_entries.clone()),
        )
        .await
        .unwrap();

        push_entries(repo, remote_repo, &missing_files, &commit, &progress)
            .await
            .unwrap();

        let missing_node_hashes = api::client::tree::list_missing_node_hashes(
            remote_repo,
            commit_info.unique_dir_nodes.clone(),
        )
        .await
        .unwrap();

        api::client::tree::create_nodes(repo, remote_repo, missing_node_hashes.clone(), &progress)
            .await
            .unwrap();

        api::client::commits::mark_commits_as_synced(remote_repo, HashSet::from([commit_hash]))
            .await
            .unwrap();
    }

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
    let should_stop = Arc::new(AtomicBool::new(false));
    let first_error = Arc::new(Mutex::new(None::<String>));

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        let should_stop = should_stop.clone();
        let first_error = first_error.clone();

        tokio::spawn(async move {
            loop {
                if should_stop.load(Ordering::Relaxed) {
                    break;
                }

                let (entry, repo, commit, remote_repo) = queue.pop().await;

                match upload_large_file_chunks(
                    entry.clone(),
                    repo,
                    commit,
                    remote_repo,
                    chunk_size,
                    &bar,
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

                finished_queue.pop().await;
            }
        });
    }

    while !finished_queue.is_empty() {
        // log::debug!("Before waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }

    if let Some(err) = first_error.lock().await.clone() {
        return Err(OxenError::basic_str(err));
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
) -> Result<(), OxenError> {
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
                    return Err(OxenError::from(err));
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

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        let bar = Arc::clone(progress);
        let should_stop = should_stop.clone();
        let first_error = first_error.clone();
        tokio::spawn(async move {
            use tokio::time::sleep as tokio_sleep;
            loop {
                log::debug!("worker[{worker}] processing task");
                if should_stop.load(Ordering::Relaxed) {
                    break;
                }

                // Pop work, but periodically check for stop signal
                let (chunk, repo, _commit, remote_repo, client) = tokio::select! {
                    item = queue.pop() => item,
                    _ = tokio_sleep(Duration::from_millis(200)) => {
                        if should_stop.load(Ordering::Relaxed) {
                            break;
                        }
                        continue;
                    }
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
    }

    while !finished_queue.is_empty() {
        sleep(Duration::from_secs(1)).await;
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
