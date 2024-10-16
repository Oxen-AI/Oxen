//! Pushes commits and entries to the remote repository
//!

use crate::api::client::commits::ChunkParams;
use crate::model::entry::commit_entry::{Entry, SchemaEntry};
use crate::model::entry::unsynced_commit_entry::UnsyncedCommitEntries;
use crate::repositories::entries::compute_generic_entries_size;
use crate::util::concurrency;
use crate::util::progress_bar::{oxen_progress_bar_with_msg, spinner_with_msg};
use crate::{core, repositories};

use flate2::write::GzEncoder;
use flate2::Compression;
use futures::prelude::*;
use indicatif::ProgressBar;
use std::collections::{HashSet, VecDeque};

use std::io::{BufReader, Read};
use std::sync::Arc;

use tokio::time::Duration;

use crate::constants::{self, AVG_CHUNK_SIZE, NUM_HTTP_RETRIES};

use crate::core::v0_10_0::index::{CommitReader, Merger};
use crate::error::OxenError;
use crate::model::{Branch, Commit, LocalRepository, RemoteBranch, RemoteRepository};

use crate::core::v0_19_0::structs::push_progress::PushProgress;
use crate::{api, util};

pub async fn push(
    repo: &LocalRepository,
    src: Branch,
    dst: RemoteBranch,
) -> Result<Branch, OxenError> {
    let branch = src;
    println!(
        "🐂 Oxen push {} {} -> {}",
        dst.remote, branch.name, branch.commit_id
    );
    let remote = repo
        .get_remote(&dst.remote)
        .ok_or(OxenError::remote_not_set(&dst.remote))?;

    log::debug!("Pushing to remote {:?}", remote);
    // Repo should be created before this step
    let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    push_remote_repo(repo, remote_repo, branch.clone()).await?;
    Ok(branch)
}

async fn validate_repo_is_pushable(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: &Branch,
    commit_reader: &CommitReader,
    head_commit: &Commit,
) -> Result<bool, OxenError> {
    if remote_is_ahead_of_local(head_commit, remote_repo, commit_reader, branch).await? {
        log::debug!("remote is ahead of local for commit {:#?}", head_commit);
        if api::client::commits::can_push(remote_repo, &branch.name, local_repo, head_commit)
            .await?
        {
            // log::debug!("can_push is true for commit {:#?}", head_commit);
            return Ok(true); // We need a merge commit
        } else {
            // log::debug!("can_push is false for commit {:#?}", head_commit);
            return Err(OxenError::upstream_merge_conflict());
        }
    }

    Ok(false)
}

pub async fn push_remote_repo(
    local_repo: &LocalRepository,
    remote_repo: RemoteRepository,
    branch: Branch,
) -> Result<RemoteRepository, OxenError> {
    // Lock the branch at the top, to avoid collisions from true simultaneous push
    // Returns a `remote_branch_locked` error if lock is already held
    api::client::branches::lock(&remote_repo, &branch.name).await?;

    let commit_reader = CommitReader::new(local_repo)?;
    let head_commit = commit_reader
        .get_commit_by_id(&branch.commit_id)?
        .ok_or(OxenError::must_be_on_valid_branch())?;

    // Lock successfully acquired
    api::client::repositories::pre_push(&remote_repo, &branch, &head_commit.id).await?;

    #[allow(unused_assignments)]
    let mut requires_merge = false;
    match validate_repo_is_pushable(
        local_repo,
        &remote_repo,
        &branch,
        &commit_reader,
        &head_commit,
    )
    .await
    {
        Ok(result) => {
            log::debug!(
                "push_remote_repo is pushable, result is {} for commit {:#?}",
                result,
                head_commit
            );
            requires_merge = result;
        }
        Err(err) => {
            api::client::branches::unlock(&remote_repo, &branch.name).await?;
            log::debug!(
                "push_remote_repo is not pushable for commit {:#?}",
                head_commit
            );
            return Err(err);
        }
    }

    let branch_clone = branch.clone();
    let branch_name = branch.name.clone();

    // Push the commits. If at any point during this process, we have errors or the user ctrl+c's, we release the branch lock.
    // TODO: Maybe we should only release the lock if we haven't yet started adding commits to the queue.
    // IF we've added commits to the queue, should we cede control of lock removal to when the queue is finished processing?
    let head_commit_clone = head_commit.clone();
    tokio::select! {
        result = try_push_remote_repo(local_repo, &remote_repo, branch, head_commit, requires_merge) => {
            match result {
                Ok(_) => {
                    // Unlock the branch
                    api::client::branches::unlock(&remote_repo, &branch_name).await?;
                }
                Err(err) => {
                    // Unlock the branch and handle error
                    api::client::branches::unlock(&remote_repo, &branch_name).await?;
                    // handle the error
                    return Err(err);
                }
            }
        },
        _ = tokio::signal::ctrl_c() => {
            // Ctrl+C was pressed
            println!("🐂 Received interrupt signal. Gracefully shutting down...");
            // Unlock the branch
            api::client::branches::unlock(&remote_repo, &branch_name).await?;
            println!("🐂 Shutdown successful.");
            // Exit the process
            std::process::exit(0);
        }
    }
    // TODO: Handle additional push complete / incomplete statuses on ctrl + c
    api::client::repositories::post_push(&remote_repo, &branch_clone, &head_commit_clone.id)
        .await?;
    Ok(remote_repo)
}

pub async fn try_push_remote_repo(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    branch: Branch,
    mut head_commit: Commit,
    requires_merge: bool,
) -> Result<(), OxenError> {
    let commits_to_push =
        get_commit_objects_to_sync(local_repo, remote_repo, &head_commit, &branch).await?;

    log::debug!("got these commits to push");

    if !commits_to_push_are_synced(local_repo, &commits_to_push)? {
        return Err(OxenError::incomplete_local_history());
    }

    log::debug!(
        "push_remote_repo commit order after get_commit_objects_to_sync {}",
        commits_to_push.len()
    );

    let maybe_remote_branch = api::client::branches::get_by_name(remote_repo, &branch.name).await?;

    let (unsynced_entries, _total_size) =
        push_missing_commit_objects(local_repo, remote_repo, &commits_to_push, &branch).await?;

    log::debug!("🐂 Identifying unsynced commits dbs...");
    let unsynced_db_commits =
        api::client::commits::get_commits_with_unsynced_dbs(remote_repo, &branch).await?;

    api::client::commits::post_tree_objects_to_server(local_repo, remote_repo, &head_commit)
        .await?;

    push_missing_commit_dbs(local_repo, remote_repo, unsynced_db_commits).await?;

    // Get commits with unsynced entries
    let mut unsynced_entries_commits =
        api::client::commits::get_commits_with_unsynced_entries(remote_repo, &branch).await?;

    log::debug!(
        "commits with unsynced entries before entries fn {:?}",
        unsynced_entries_commits
    );

    push_missing_commit_entries(
        local_repo,
        remote_repo,
        &unsynced_entries_commits,
        unsynced_entries,
    )
    .await?;

    if requires_merge {
        let remote_head_id = match maybe_remote_branch {
            Some(remote_branch) => remote_branch.commit_id,
            None => return Err(OxenError::remote_branch_not_found(&branch.name)),
        };

        let merge_commit = api::client::branches::maybe_create_merge(
            remote_repo,
            branch.name.as_str(),
            head_commit.id.as_str(),
            remote_head_id.as_str(),
        )
        .await?;

        log::debug!(
            "try_push_remote_repo found new merge head commit {:?}",
            head_commit
        );
        unsynced_entries_commits.push(merge_commit.clone());

        head_commit = merge_commit;
    } else {
        log::debug!(
            "try_push_remote_repo didn't find new merge head, old head is {:?}",
            head_commit
        );
    }

    // Even if there are no entries, there may still be commits we need to call post-push on (esp initial commits)
    api::client::commits::bulk_post_push_complete(remote_repo, &unsynced_entries_commits).await?;
    // Update the head...
    api::client::branches::update(remote_repo, &branch.name, &head_commit).await?;

    // update the branch after everything else is synced
    log::debug!(
        "updated remote branch {:?} to commit {:?}",
        &branch.name,
        &head_commit
    );

    // Remotely validate commit
    // This is an async process on the server so good to stall the user here so they don't push again
    // If they did push again before this is finished they would get a still syncing error
    let bar = oxen_progress_bar_with_msg(
        unsynced_entries_commits.len() as u64,
        "Remote validating commits",
    );
    poll_until_synced(remote_repo, &head_commit, &bar).await?;
    bar.finish_and_clear();

    log::debug!("Just finished push.");

    Ok(())
}

async fn get_commit_objects_to_sync(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    local_commit: &Commit,
    branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    let remote_branch = api::client::branches::get_by_name(remote_repo, &branch.name).await?;
    let commit_reader = CommitReader::new(local_repo)?;
    let mut commits_to_sync: Vec<Commit>;
    if let Some(remote_branch) = remote_branch {
        log::debug!(
            "get_commit_objects_to_sync found remote branch {:?}, calculating missing commits between local and remote heads", remote_branch
        );
        let remote_commit = api::client::commits::get_by_id(remote_repo, &remote_branch.commit_id)
            .await?
            .unwrap();

        let merger = Merger::new(local_repo)?;
        commits_to_sync =
            merger.list_commits_between_commits(&commit_reader, &remote_commit, local_commit)?;

        println!("🐂 Getting commit history...");
        let remote_history = api::client::commits::list_commit_history(remote_repo, &branch.name)
            .await
            .unwrap_or_else(|_| vec![]);
        log::debug!(
            "get_commit_objects_to_sync calculated {} commits",
            commits_to_sync.len()
        );

        // Filter out any commits_to_sync that are in the remote_history
        commits_to_sync.retain(|commit| {
            !remote_history
                .iter()
                .any(|remote_commit| remote_commit.id == commit.id)
        });
    } else {
        // Remote branch does not exist. Find commits to push with reference to whatever
        // remote branch head comes first in the local newbranch history, aka what it was branched off of.

        // Early return to avoid checking for remote commits: if full local history and no remote branch,
        // push full local branch history.
        if repositories::commits::commit_history_is_complete(local_repo, local_commit)? {
            return repositories::commits::list_from(local_repo, &local_commit.id);
        }

        // Otherwise, find the remote commit that the local branch was branched off of and push everything since then.
        let all_commits = api::client::commits::list_all(remote_repo).await?;
        log::debug!("got all remote commits as {:#?}", all_commits);
        let maybe_remote_commit =
            find_latest_local_commit_synced(local_repo, local_commit, &all_commits)?;

        if let Some(remote_commit) = maybe_remote_commit {
            let merger = Merger::new(local_repo)?;
            commits_to_sync = merger.list_commits_between_commits(
                &commit_reader,
                &remote_commit,
                local_commit,
            )?;
        } else {
            commits_to_sync = repositories::commits::list_from(local_repo, &local_commit.id)?;
        }
    }

    // Order from BASE to HEAD
    commits_to_sync.reverse();

    Ok(commits_to_sync)
}

fn get_unsynced_entries_for_commit(
    local_repo: &LocalRepository,
    commit: &Commit,
    commit_reader: &CommitReader,
) -> Result<(Vec<UnsyncedCommitEntries>, u64), OxenError> {
    let mut unsynced_commits: Vec<UnsyncedCommitEntries> = Vec::new();
    let mut total_size: u64 = 0;

    if commit.parent_ids.is_empty() {
        unsynced_commits.push(UnsyncedCommitEntries {
            commit: commit.to_owned(),
            entries: vec![],
        });
    }
    for parent_id in commit.parent_ids.iter() {
        let local_parent = commit_reader
            .get_commit_by_id(parent_id)?
            .ok_or_else(|| OxenError::local_parent_link_broken(&commit.id))?;
        let entries =
            repositories::entries::read_unsynced_entries(local_repo, &local_parent, commit)?;

        let schemas: Vec<SchemaEntry> =
            repositories::entries::read_unsynced_schemas(local_repo, &local_parent, commit)?;

        // Get the entries and schemas into one Vec<Entry>
        let mut entries: Vec<Entry> = entries.into_iter().map(Entry::from).collect();

        log::debug!(
            "got unsynced entries for commit {:#?}: {:#?}",
            commit,
            entries
        );
        let schemas: Vec<Entry> = schemas.into_iter().map(Entry::from).collect();
        log::debug!(
            "got unsynced schemas for commit {:#?}: {:#?}",
            commit,
            schemas
        );

        entries.extend(schemas);

        // Get size of these entries
        let entries_size = repositories::entries::compute_generic_entries_size(&entries)?;
        total_size += entries_size;

        unsynced_commits.push(UnsyncedCommitEntries {
            commit: commit.to_owned(),
            entries,
        })
    }

    Ok((unsynced_commits, total_size))
}

async fn push_missing_commit_objects(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commits: &Vec<Commit>,
    branch: &Branch,
) -> Result<(Vec<UnsyncedCommitEntries>, u64), OxenError> {
    let mut unsynced_commits: Vec<UnsyncedCommitEntries> = Vec::new();

    let spinner = spinner_with_msg(format!(
        "🐂 Finding unsynced data from {} commits",
        commits.len()
    ));
    let commit_reader = CommitReader::new(local_repo)?;
    let mut total_size: u64 = 0;

    for commit in commits {
        log::debug!("objects checker checking commit {:#?}", commit);
        let (commit_unsynced_commits, commit_size) =
            get_unsynced_entries_for_commit(local_repo, commit, &commit_reader)?;
        log::debug!(
            "objects checker got entries for commit {:#?} as {:?}",
            commit,
            commit_unsynced_commits
        );
        total_size += commit_size;
        unsynced_commits.extend(commit_unsynced_commits);
    }
    spinner.finish_and_clear();

    // Spin during async bulk create
    let spinner = spinner_with_msg(format!("🐂 Syncing {} commits", unsynced_commits.len()));

    api::client::commits::post_commits_to_server(
        local_repo,
        remote_repo,
        &unsynced_commits,
        branch.name.clone(),
    )
    .await?;

    spinner.finish_and_clear();
    Ok((unsynced_commits, total_size))
}

async fn remote_is_ahead_of_local(
    local_head: &Commit,
    remote_repo: &RemoteRepository,
    reader: &CommitReader,
    branch: &Branch,
) -> Result<bool, OxenError> {
    // Make sure that the branch has not progressed ahead of the commit
    let remote_branch = api::client::branches::get_by_name(remote_repo, &branch.name).await?;

    if remote_branch.is_none() {
        // If the remote branch does not exist then it is not ahead
        return Ok(false);
    }

    if !reader.commit_id_exists(&remote_branch.clone().unwrap().commit_id) {
        log::debug!("commit id does not exist for commit {:#?}", remote_branch);
        return Ok(true);
    } else {
        log::debug!("commit id exists for commit {:#?}", remote_branch);
        // return Ok(false);
    }

    // Get the commit
    let remote_commit = reader
        .get_commit_by_id(remote_branch.clone().unwrap().commit_id)?
        .ok_or(OxenError::local_parent_link_broken(
            remote_branch.unwrap().commit_id,
        ))?;

    Ok(!local_head.has_ancestor(&remote_commit.id, reader)?)
}

fn find_latest_local_commit_synced(
    local_repo: &LocalRepository,
    local_head: &Commit,
    remote_commits: &Vec<Commit>,
) -> Result<Option<Commit>, OxenError> {
    let commit_reader = CommitReader::new(local_repo).unwrap();
    let mut commits_set: HashSet<String> = HashSet::new();
    for remote_commit in remote_commits {
        commits_set.insert(remote_commit.id.clone());
    }
    // let mut current_commit = local_head.clone();
    let mut queue: VecDeque<Commit> = VecDeque::new();
    queue.push_back(local_head.clone());

    while !queue.is_empty() {
        let current_commit = queue.pop_front().unwrap();
        if commits_set.contains(&current_commit.id) {
            return Ok(Some(current_commit));
        }
        for parent_id in current_commit.parent_ids.iter() {
            let parent_commit = commit_reader.get_commit_by_id(parent_id)?;
            let Some(parent_commit) = parent_commit else {
                return Err(OxenError::local_parent_link_broken(&current_commit.id));
            };
            queue.push_back(parent_commit);
        }
    }
    Ok(None)
}

fn commits_to_push_are_synced(
    local_repo: &LocalRepository,
    commits_to_push: &Vec<Commit>,
) -> Result<bool, OxenError> {
    for commit in commits_to_push {
        if !core::commit_sync_status::commit_is_synced(local_repo, commit) {
            log::debug!("commit is not synced {:?}", commit);
            return Ok(false);
        }
    }
    Ok(true)
}

async fn poll_until_synced(
    remote_repo: &RemoteRepository,
    commit: &Commit,
    bar: &Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let commits_to_sync = bar.length().unwrap();

    let head_commit_id = &commit.id;

    let mut retries = 0;

    loop {
        match api::client::commits::latest_commit_synced(remote_repo, head_commit_id).await {
            Ok(sync_status) => {
                retries = 0;
                log::debug!("Got latest synced commit {:?}", sync_status.latest_synced);
                log::debug!("Got n unsynced commits {:?}", sync_status.num_unsynced);
                if commits_to_sync > sync_status.num_unsynced as u64 {
                    bar.set_position(commits_to_sync - sync_status.num_unsynced as u64);
                }
                if sync_status.num_unsynced == 0 {
                    bar.finish_and_clear();
                    println!("🎉 Push successful");
                    return Ok(());
                }
            }
            Err(err) => {
                retries += 1;
                // Back off, but don't want to go all the way to 100s
                let sleep_time = 2 * retries;
                if retries >= NUM_HTTP_RETRIES {
                    bar.finish_and_clear();
                    return Err(err);
                }
                log::warn!(
                    "Server error encountered, retrying... ({}/{})",
                    retries,
                    NUM_HTTP_RETRIES
                );
                // Extra sleep time in error cases
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
}

async fn push_missing_commit_dbs(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    unsynced_commits: Vec<Commit>,
) -> Result<(), OxenError> {
    let pieces_of_work = unsynced_commits.len();

    if pieces_of_work == 0 {
        return Ok(());
    }

    let pb = oxen_progress_bar_with_msg(pieces_of_work as u64, "Syncing databases");

    // Compute size for this subset of entries
    let num_chunks = concurrency::num_threads_for_items(unsynced_commits.len());
    let mut chunk_size = pieces_of_work / num_chunks;
    if num_chunks > pieces_of_work {
        chunk_size = pieces_of_work;
    }

    // Split into chunks, process in parallel, and post to server
    use tokio::time::sleep;
    type PieceOfWork = (
        LocalRepository,
        RemoteRepository,
        Vec<Commit>,
        Arc<ProgressBar>,
    );
    type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
    type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

    log::debug!(
        "Creating {num_chunks} chunks from {pieces_of_work} commits with size {chunk_size}"
    );
    let chunks: Vec<PieceOfWork> = unsynced_commits
        .chunks(chunk_size)
        .map(|commits| {
            (
                local_repo.to_owned(),
                remote_repo.to_owned(),
                commits.to_owned(),
                pb.to_owned(),
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

    for worker in 0..worker_count {
        let queue = queue.clone();
        let finished_queue = finished_queue.clone();
        tokio::spawn(async move {
            loop {
                let (local_repo, remote_repo, commits, bar) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);
                for commit in &commits {
                    match api::client::commits::post_commit_dir_hashes_to_server(
                        &local_repo,
                        &remote_repo,
                        commit,
                    )
                    .await
                    {
                        Ok(_) => {
                            log::debug!("worker[{}] posted commit to server", worker);
                            bar.inc(1);
                        }
                        Err(err) => {
                            log::error!(
                                "worker[{}] failed to post commit to server: {}",
                                worker,
                                err
                            );
                        }
                    }
                }
                finished_queue.pop().await;
            }
        });
    }
    while finished_queue.len() > 0 {
        // log::debug!("Waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_secs(1)).await;
    pb.finish_and_clear();
    Ok(())
}

async fn push_missing_commit_entries(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commits: &Vec<Commit>,
    mut unsynced_entries: Vec<UnsyncedCommitEntries>,
) -> Result<(), OxenError> {
    // If no commits, nothing to do here. If no entries, but still commits to sync, need to do this step
    // TODO: maybe factor validation into a separate fourth step so that this can be skipped if no entries
    if commits.is_empty() {
        return Ok(());
    }

    log::debug!("push_missing_commit_entries num unsynced {}", commits.len());

    let spinner = spinner_with_msg(format!(
        "\n🐂 Collecting files for {} commits",
        commits.len()
    ));

    // Find the commits that still have unsynced entries (some might already be synced)
    // Collect them and calculate the new size to send
    let commit_reader = CommitReader::new(local_repo)?;

    for commit in commits {
        // Only if the commit is not already accounted for in unsynced entries - avoid double counting
        if !unsynced_entries.iter().any(|u| u.commit.id == commit.id) {
            log::debug!("processing commit {:?}", commit);
            let (commit_unsynced_commits, _commit_size) =
                get_unsynced_entries_for_commit(local_repo, commit, &commit_reader)?;

            log::debug!(
                "got entries for commit {:#?} {:?}",
                commit,
                commit_unsynced_commits
            );

            unsynced_entries.extend(commit_unsynced_commits);
        } else {
            log::debug!("Skipping commit {:?}", commit);
        }
    }

    let mut unsynced_entries: Vec<Entry> = unsynced_entries
        .iter()
        .flat_map(|u: &UnsyncedCommitEntries| u.entries.clone())
        .collect();

    log::debug!(
        "pushing and we've collected these entries: {}",
        unsynced_entries.len()
    );

    spinner.finish_and_clear();

    // Dedupe unsynced_entries on hash and file extension to form unique version path names
    let mut seen_entries: HashSet<String> = HashSet::new();
    unsynced_entries.retain(|e| {
        let key = format!("{}{}", e.hash().clone(), e.extension());
        seen_entries.insert(key)
    });

    let total_size = compute_generic_entries_size(&unsynced_entries)?;

    println!("🐂 Pushing {}", bytesize::ByteSize::b(total_size));

    // TODO - we can probably take commits out of this flow entirely, but it disrupts a bit rn so want to make sure this is stable first
    // For now, will send the HEAD commit through for logging purposes
    if !unsynced_entries.is_empty() {
        let all_entries = UnsyncedCommitEntries {
            commit: commits[0].clone(), // New head commit. Guaranteed to be here by earlier guard
            entries: unsynced_entries,
        };

        let bar = PushProgress::new();
        push_entries(
            local_repo,
            remote_repo,
            &all_entries.entries,
            &all_entries.commit,
            &bar,
        )
        .await?;
    } else {
        println!("🐂 No entries to push");
    }
    log::debug!("push_missing_commit_entries done");

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
        .filter(|e| e.num_bytes() < AVG_CHUNK_SIZE)
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

    while finished_queue.len() > 0 {
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
    let version_path = util::fs::version_path_for_entry(&repo, &entry);
    let f = std::fs::File::open(&version_path).unwrap();
    let mut reader = BufReader::new(f);

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

    // Create queues for sending data to workers
    type PieceOfWork = (
        Vec<u8>,
        u64,   // chunk size
        usize, // chunk num
        usize, // total chunks
        u64,   // total size
        RemoteRepository,
        String, // entry hash
        Commit,
        Option<String>, // filename
    );

    // In order to upload chunks in parallel
    // We should only read N chunks at a time so that
    // the whole file does not get read into memory
    let sub_chunk_size = constants::DEFAULT_NUM_WORKERS;

    let mut total_chunk_idx = 0;
    let mut processed_chunk_idx = 0;
    let num_sub_chunks = (total_chunks / sub_chunk_size) + 1;
    log::debug!(
        "upload_large_file_chunks {:?} proccessing file in {} subchunks of size {} from total {} chunk size {} file size {}",
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
                    remote_repo,
                    entry_hash,
                    commit,
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
                    &remote_repo,
                    &commit,
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

    // Split into chunks, zip up, and post to server
    use tokio::time::sleep;
    type PieceOfWork = (Vec<Entry>, LocalRepository, Commit, RemoteRepository);
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
                let (chunk, repo, commit, remote_repo) = queue.pop().await;
                log::debug!("worker[{}] processing task...", worker);

                let enc = GzEncoder::new(Vec::new(), Compression::default());
                let mut tar = tar::Builder::new(enc);
                log::debug!("Chunk size {}", chunk.len());
                let chunk_size = match compute_generic_entries_size(&chunk) {
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

                match api::client::commits::post_data_to_server(
                    &remote_repo,
                    &commit,
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
    while finished_queue.len() > 0 {
        // log::debug!("Waiting for {} workers to finish...", queue.len());
        sleep(Duration::from_secs(1)).await;
    }
    log::debug!("All tasks done. :-)");

    // Sleep again to let things sync...
    sleep(Duration::from_millis(100)).await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::core::v0_10_0::index::pusher;

    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;

    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_push_missing_commit_dbs() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async_min_version(
            MinOxenVersion::V0_10_0,
            |mut repo| async move {
                // Set the proper remote
                let name = repo.dirname();
                let remote = test::repo_remote_url_from(&name);
                command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

                // Create remote repo
                let remote_repo = test::create_remote_repo(&repo).await?;

                // Get commits to sync...
                let head_commit = repositories::commits::head_commit(&repo)?;
                let branch = repositories::branches::current_branch(&repo)?.unwrap();

                // Create all commit objects
                let unsynced_commits =
                    pusher::get_commit_objects_to_sync(&repo, &remote_repo, &head_commit, &branch)
                        .await?;
                pusher::push_missing_commit_objects(
                    &repo,
                    &remote_repo,
                    &unsynced_commits,
                    &branch,
                )
                .await?;

                // Should have one missing commit db - root created on repo creation
                let unsynced_db_commits =
                    api::client::commits::get_commits_with_unsynced_dbs(&remote_repo, &branch)
                        .await?;
                assert_eq!(unsynced_db_commits.len(), 0);

                // Push to the remote
                pusher::push_missing_commit_dbs(&repo, &remote_repo, unsynced_db_commits).await?;

                // All commits should now have dbs
                let unsynced_db_commits =
                    api::client::commits::get_commits_with_unsynced_dbs(&remote_repo, &branch)
                        .await?;
                assert_eq!(unsynced_db_commits.len(), 0);

                Ok(())
            },
        )
        .await
    }
}
