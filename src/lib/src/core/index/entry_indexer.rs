use async_recursion::async_recursion;
use bytesize::ByteSize;
use filetime::FileTime;
use flate2::write::GzEncoder;
use flate2::Compression;
use indicatif::ProgressBar;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::fs;
use std::io::{BufReader, Read};
use std::path::Path;
use std::sync::Arc;

use crate::api::remote::commits::ChunkParams;
use crate::constants::{AVG_CHUNK_SIZE, HISTORY_DIR};
use crate::core::index::{self, puller};
use crate::core::index::{
    CommitDirEntryReader, CommitDirEntryWriter, CommitEntryReader, CommitReader, CommitWriter,
    RefReader, RefWriter,
};
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, LocalRepository, RemoteBranch, RemoteRepository};
use crate::util;
use crate::{api, current_function};

pub struct UnsyncedCommitEntries {
    commit: Commit,
    entries: Vec<CommitEntry>,
}

pub struct EntryIndexer {
    pub repository: LocalRepository,
}

impl EntryIndexer {
    pub fn new(repository: &LocalRepository) -> Result<EntryIndexer, OxenError> {
        Ok(EntryIndexer {
            repository: repository.clone(),
        })
    }

    pub async fn push(&self, rb: &RemoteBranch) -> Result<RemoteRepository, OxenError> {
        let ref_reader = RefReader::new(&self.repository)?;
        let branch = ref_reader.get_branch_by_name(&rb.branch)?;
        if branch.is_none() {
            return Err(OxenError::local_branch_not_found(&rb.branch));
        }

        let branch = branch.unwrap();

        println!(
            "🐂 Oxen push {} {} -> {}",
            rb.remote, branch.name, branch.commit_id
        );
        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or(OxenError::remote_not_set(&rb.remote))?;

        log::debug!("Pushing to remote {:?}", remote);
        // Repo should be created before this step
        let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
            Err(err) => return Err(err),
        };
        self.push_remote_repo(remote_repo, branch).await
    }

    pub async fn push_remote_repo(
        &self,
        remote_repo: RemoteRepository,
        branch: Branch,
    ) -> Result<RemoteRepository, OxenError> {
        // Push unsynced commit db and history dbs
        let commit_reader = CommitReader::new(&self.repository)?;
        let head_commit = commit_reader.get_commit_by_id(branch.commit_id)?.unwrap();

        // This method will check with server to find out what commits need to be pushed
        // will fill in commits that are not synced
        let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
        self.rpush_missing_commit_objects(&remote_repo, &head_commit, &mut unsynced_commits)
            .await?;

        // If there are any unsynced commits, sync their entries
        if !unsynced_commits.is_empty() {
            log::debug!(
                "Push entries for {} unsynced commits",
                unsynced_commits.len()
            );

            // recursively check commits against remote head
            // and sync ones that have not been synced
            self.rpush_entries(&remote_repo, &unsynced_commits).await?;

            // update the branch after everything else is synced
            log::debug!(
                "Updating remote branch {:?} to commit {:?}",
                &branch.name,
                &head_commit
            );

            // Remotely validate commit
            // This is an async process on the server so good to stall the user here so they don't push again
            // If they did push again before this is finished they would get a still syncing error
            self.poll_until_synced(&remote_repo, &head_commit).await?;
        }

        // Update the remote branch name last
        api::remote::branches::update(&remote_repo, &branch.name, &head_commit).await?;
        println!(
            "Updated remote branch {} -> {}",
            &branch.name, &head_commit.id
        );

        Ok(remote_repo)
    }

    async fn poll_until_synced(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        println!("Remote verifying commit...");
        let progress = ProgressBar::new_spinner();

        loop {
            progress.tick();
            match api::remote::commits::commit_is_synced(remote_repo, &commit.id).await {
                Ok(Some(sync_status)) => {
                    if sync_status.is_valid {
                        progress.finish();
                        println!("✅ push successful\n");
                        return Ok(());
                    }
                }
                Ok(None) => {
                    progress.finish();
                    return Err(OxenError::basic_str("Err: Commit never got pushed"));
                }
                Err(err) => {
                    progress.finish();
                    return Err(err);
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(750));
        }
    }

    #[async_recursion]
    async fn rpush_missing_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        local_commit: &Commit,
        unsynced_commits: &mut VecDeque<UnsyncedCommitEntries>,
    ) -> Result<(), OxenError> {
        log::debug!(
            "rpush_missing_commit_objects START, checking local {} -> '{}'",
            local_commit.id,
            local_commit.message
        );

        // check if commit exists on remote
        // if not, push the commit and it's dbs
        match api::remote::commits::commit_is_synced(remote_repo, &local_commit.id).await {
            Ok(Some(sync_status)) => {
                if sync_status.is_valid {
                    // Commit has been synced
                    let commit_reader = CommitReader::new(&self.repository)?;
                    let head_commit = commit_reader.head_commit()?;

                    // We have remote commit, stop syncing
                    log::debug!(
                        "rpush_missing_commit_objects STOP, we have remote parent {} -> '{}' head {} -> '{}'",
                        local_commit.id,
                        local_commit.message,
                        head_commit.id,
                        head_commit.message
                    );
                } else if sync_status.is_processing {
                    // Print that last commit is still processing (or we may be in still caching values on the server)
                    println!("Commit is still processing on server {}", local_commit.id);
                } else {
                    return Err(OxenError::basic_str(sync_status.status_description));
                }
            }
            Ok(None) => {
                log::debug!(
                    "rpush_missing_commit_objects CONTINUE Didn't find remote parent: {} -> '{}'",
                    local_commit.id,
                    local_commit.message
                );
                // We don't have remote commit
                // Recursively find local parent and remote parents
                for parent_id in local_commit.parent_ids.iter() {
                    // We should have a local parent if the local_commit has parent id
                    let local_parent = api::local::commits::get_by_id(&self.repository, parent_id)?
                        .ok_or_else(|| OxenError::local_parent_link_broken(&local_commit.id))?;

                    // Compute the diff from the parent that we are going to sync
                    let entries = self.read_unsynced_entries(&local_parent, local_commit)?;
                    let entries_size = api::local::entries::compute_entries_size(&entries)?;

                    self.rpush_missing_commit_objects(remote_repo, &local_parent, unsynced_commits)
                        .await?;

                    // Unroll and post commits
                    api::remote::commits::post_commit_to_server(
                        &self.repository,
                        remote_repo,
                        local_commit,
                        entries_size,
                    )
                    .await?;

                    log::debug!(
                        "rpush_missing_commit_objects unsynced_commits.push_back commit -> {:?} parent {:?}",
                        local_commit.id, local_parent.id
                    );

                    unsynced_commits.push_back(UnsyncedCommitEntries {
                        commit: local_commit.to_owned(),
                        entries,
                    });
                }

                log::debug!(
                    "rpush_missing_commit_objects stop, no more local parents {} -> '{}'",
                    local_commit.id,
                    local_commit.message
                );

                if local_commit.parent_ids.is_empty() {
                    // Create the root commit
                    api::remote::commits::post_commit_to_server(
                        &self.repository,
                        remote_repo,
                        local_commit,
                        0, // No entries
                    )
                    .await?;
                    log::debug!("unsynced_commits.push_back root {:?}", local_commit);
                    unsynced_commits.push_back(UnsyncedCommitEntries {
                        commit: local_commit.to_owned(),
                        entries: vec![],
                    });
                }
            }
            Err(err) => {
                let err = format!("{err}");
                return Err(OxenError::basic_str(err));
            }
        }

        Ok(())
    }

    async fn rpush_entries(
        &self,
        remote_repo: &RemoteRepository,
        unsynced_commits: &VecDeque<UnsyncedCommitEntries>,
    ) -> Result<(), OxenError> {
        log::debug!("rpush_entries num unsynced {}", unsynced_commits.len());
        for unsynced in unsynced_commits.iter() {
            let commit = &unsynced.commit;
            let entries = &unsynced.entries;

            println!(
                "Pushing commit {} entries: {} -> '{}'",
                entries.len(),
                commit.id,
                commit.message
            );

            self.push_entries(remote_repo, entries, commit).await?;
        }
        Ok(())
    }

    fn read_unsynced_entries(
        &self,
        last_commit: &Commit,
        this_commit: &Commit,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        println!("Computing delta {} -> {}", last_commit.id, this_commit.id);
        // Find and compare all entries between this commit and last
        let this_entry_reader = CommitEntryReader::new(&self.repository, this_commit)?;

        let this_entries = this_entry_reader.list_entries()?;
        let grouped = api::local::entries::group_entries_to_parent_dirs(&this_entries);
        log::debug!(
            "Checking {} entries in {} groups",
            this_entries.len(),
            grouped.len()
        );

        let bar = ProgressBar::new(this_entries.len() as u64);
        let mut entries_to_sync: Vec<CommitEntry> = vec![];
        for (dir, dir_entries) in grouped.iter() {
            log::debug!("Checking {} entries from {:?}", dir_entries.len(), dir);

            let last_entry_reader =
                CommitDirEntryReader::new(&self.repository, &last_commit.id, dir)?;
            let mut entries: Vec<CommitEntry> = dir_entries
                .into_par_iter()
                .filter(|entry| {
                    bar.inc(1);
                    // If hashes are different, or it is a new entry, we'll keep it
                    let filename = entry.path.file_name().unwrap().to_str().unwrap();
                    match last_entry_reader.get_entry(filename) {
                        Ok(Some(old_entry)) => {
                            if old_entry.hash != entry.hash {
                                return true;
                            }
                        }
                        Ok(None) => {
                            return true;
                        }
                        Err(err) => {
                            panic!("Error filtering entries to sync: {}", err)
                        }
                    }
                    false
                })
                .map(|e| e.to_owned())
                .collect();
            entries_to_sync.append(&mut entries);
        }
        bar.finish();

        log::debug!("Got {} entries to sync", entries_to_sync.len());

        Ok(entries_to_sync)
    }

    async fn push_entries(
        &self,
        remote_repo: &RemoteRepository,
        entries: &[CommitEntry],
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!(
            "PUSH ENTRIES {} -> {} -> '{}'",
            entries.len(),
            commit.id,
            commit.message
        );

        println!("🐂 push computing size...");
        let total_size = api::local::entries::compute_entries_size(entries)?;

        if !entries.is_empty() {
            println!(
                "Pushing {} files with size {}",
                entries.len(),
                ByteSize::b(total_size)
            );
        }

        let bar = Arc::new(ProgressBar::new(total_size));

        // Some files may be much larger than others....so we can't just zip them up and send them
        // since bodies will be too big. Hence we chunk and send the big ones, and bundle and send the small ones

        // For files smaller than AVG_CHUNK_SIZE, we are going to group them, zip them up, and transfer them
        let smaller_entries: Vec<CommitEntry> = entries
            .iter()
            .filter(|e| e.num_bytes < AVG_CHUNK_SIZE)
            .map(|e| e.to_owned())
            .collect();

        // For files larger than AVG_CHUNK_SIZE, we are going break them into chunks and send the chunks in parallel
        let larger_entries: Vec<CommitEntry> = entries
            .iter()
            .filter(|e| e.num_bytes > AVG_CHUNK_SIZE)
            .map(|e| e.to_owned())
            .collect();

        let large_entries_sync = self.chunk_and_send_large_entries(
            remote_repo,
            larger_entries,
            commit,
            AVG_CHUNK_SIZE,
            &bar,
        );
        let small_entries_sync = self.bundle_and_send_small_entries(
            remote_repo,
            smaller_entries,
            commit,
            AVG_CHUNK_SIZE,
            &bar,
        );

        match tokio::join!(large_entries_sync, small_entries_sync) {
            (Ok(_), Ok(_)) => {
                api::remote::commits::post_push_complete(remote_repo, &commit.id).await
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
        &self,
        remote_repo: &RemoteRepository,
        entries: Vec<CommitEntry>,
        commit: &Commit,
        chunk_size: u64,
        bar: &Arc<ProgressBar>,
    ) -> Result<(), OxenError> {
        if entries.is_empty() {
            return Ok(());
        }

        use tokio::time::{sleep, Duration};
        type PieceOfWork = (
            CommitEntry,
            LocalRepository,
            Commit,
            RemoteRepository,
            Arc<ProgressBar>,
        );
        type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
        type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

        log::debug!("Chunking and sending {} larger files", entries.len());
        let entries: Vec<PieceOfWork> = entries
            .iter()
            .map(|e| {
                (
                    e.to_owned(),
                    self.repository.to_owned(),
                    commit.to_owned(),
                    remote_repo.to_owned(),
                    bar.to_owned(),
                )
            })
            .collect();

        let queue = Arc::new(TaskQueue::new(entries.len()));
        let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
        for entry in entries.iter() {
            queue.try_push(entry.to_owned()).unwrap();
            finished_queue.try_push(false).unwrap();
        }

        let worker_count: usize = if num_cpus::get() > entries.len() {
            entries.len()
        } else {
            num_cpus::get()
        };

        log::debug!(
            "worker_count {} entries len {}",
            worker_count,
            entries.len()
        );
        for worker in 0..worker_count {
            let queue = queue.clone();
            let finished_queue = finished_queue.clone();
            tokio::spawn(async move {
                loop {
                    let (entry, repo, commit, remote_repo, bar) = queue.pop().await;
                    log::debug!("worker[{}] processing task...", worker);

                    // Open versioned file
                    let version_path = util::fs::version_path(&repo, &entry);
                    let f = std::fs::File::open(&version_path).unwrap();
                    let mut reader = BufReader::new(f);

                    // Read chunks
                    let total_size = entry.num_bytes;
                    let num_chunks = ((total_size / chunk_size) + 1) as usize;
                    let mut total_read = 0;
                    let mut chunk_size = chunk_size;

                    // TODO: We could probably upload chunks in parallel too
                    for i in 0..num_chunks {
                        // Make sure we read the last size correctly
                        if (total_read + chunk_size) > total_size {
                            chunk_size = total_size % chunk_size;
                        }

                        // Only read as much as you need to send so we don't blow up memory on large files
                        let mut buffer = vec![0u8; chunk_size as usize];
                        reader.read_exact(&mut buffer).unwrap();
                        total_read += chunk_size;

                        let size = buffer.len() as u64;
                        log::debug!("Got entry buffer of size {}", size);

                        // Send data to server
                        let is_compressed = false;
                        let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
                        let path =
                            util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();
                        let file_name = Some(String::from(path.to_str().unwrap()));

                        let params = ChunkParams {
                            chunk_num: i,
                            total_chunks: num_chunks,
                            total_size: total_size as usize,
                        };

                        match api::remote::commits::upload_data_chunk_to_server_with_retry(
                            &remote_repo,
                            &commit,
                            &buffer,
                            &entry.hash,
                            &params,
                            is_compressed,
                            &file_name,
                        )
                        .await
                        {
                            Ok(_) => {
                                bar.inc(buffer.len() as u64);
                                log::debug!("Successfully uploaded chunk {}/{}", i, num_chunks)
                            }
                            Err(err) => {
                                log::error!("Error uploading chunk: {:?}", err)
                            }
                        }
                    }

                    finished_queue.pop().await;
                }
            });
        }

        while finished_queue.len() > 0 {
            // log::debug!("Before waiting for {} workers to finish...", queue.len());
            sleep(Duration::from_secs(1)).await;
        }
        log::debug!("All large file tasks done. :-)");

        Ok(())
    }

    /// Sends entries in tarballs of size ~chunk size
    async fn bundle_and_send_small_entries(
        &self,
        remote_repo: &RemoteRepository,
        entries: Vec<CommitEntry>,
        commit: &Commit,
        avg_chunk_size: u64,
        bar: &Arc<ProgressBar>,
    ) -> Result<(), OxenError> {
        if entries.is_empty() {
            return Ok(());
        }

        // Compute size for this subset of entries
        let total_size = api::local::entries::compute_entries_size(&entries)?;
        let num_chunks = ((total_size / avg_chunk_size) + 1) as usize;

        let mut chunk_size = entries.len() / num_chunks;
        if num_chunks > entries.len() {
            chunk_size = entries.len();
        }

        // Split into chunks, zip up, and post to server
        use tokio::time::{sleep, Duration};
        type PieceOfWork = (
            Vec<CommitEntry>,
            LocalRepository,
            Commit,
            RemoteRepository,
            Arc<ProgressBar>,
        );
        type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
        type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

        log::debug!("Creating {num_chunks} chunks from {total_size} bytes with size {chunk_size}");
        let chunks: Vec<PieceOfWork> = entries
            .chunks(chunk_size)
            .map(|c| {
                (
                    c.to_owned(),
                    self.repository.to_owned(),
                    commit.to_owned(),
                    remote_repo.to_owned(),
                    bar.to_owned(),
                )
            })
            .collect();

        let worker_count: usize = num_cpus::get();
        let queue = Arc::new(TaskQueue::new(chunks.len()));
        let finished_queue = Arc::new(FinishedTaskQueue::new(entries.len()));
        for chunk in chunks {
            queue.try_push(chunk).unwrap();
            finished_queue.try_push(false).unwrap();
        }

        for worker in 0..worker_count {
            let queue = queue.clone();
            let finished_queue = finished_queue.clone();
            tokio::spawn(async move {
                loop {
                    let (chunk, repo, commit, remote_repo, bar) = queue.pop().await;
                    log::debug!("worker[{}] processing task...", worker);

                    let enc = GzEncoder::new(Vec::new(), Compression::default());
                    let mut tar = tar::Builder::new(enc);
                    log::debug!("Chunk size {}", chunk.len());
                    log::debug!("got repo {:?}", &repo.path);
                    for entry in chunk.into_iter() {
                        let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
                        let version_path = util::fs::version_path(&repo, &entry);
                        let name =
                            util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();

                        tar.append_path_with_name(version_path, name).unwrap();
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
                    match api::remote::commits::post_data_to_server(
                        &remote_repo,
                        &commit,
                        buffer,
                        is_compressed,
                        &file_name,
                        bar,
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
                    finished_queue.pop().await;
                }
            });
        }
        while finished_queue.len() > 0 {
            // log::debug!("Waiting for {} workers to finish...", queue.len());
            sleep(Duration::from_millis(1)).await;
        }
        log::debug!("All tasks done. :-)");

        Ok(())
    }

    pub async fn pull(&self, rb: &RemoteBranch) -> Result<(), OxenError> {
        println!("🐂 Oxen pull {} {}", rb.remote, rb.branch);

        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or(OxenError::remote_not_set(&rb.remote))?;

        let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
            Err(err) => return Err(err),
        };

        if let Some(commit) = self.pull_all_commit_objects(&remote_repo, rb).await? {
            self.pull_all_entries_for_commit(&remote_repo, &commit)
                .await?;
        }
        Ok(())
    }

    pub async fn pull_all_entries_for_commit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!(
            "pull_all_entries_for_commit for commit: {} -> {}",
            commit.id,
            commit.message
        );
        let limit: usize = 0; // zero means pull all
        self.pull_entries_for_commit(remote_repo, commit, limit)
            .await
    }

    pub async fn pull_all_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
    ) -> Result<Option<Commit>, OxenError> {
        let remote_branch_err = format!("Remote branch not found: {}", rb.branch);
        let remote_branch = api::remote::branches::get_by_name(remote_repo, &rb.branch)
            .await?
            .ok_or_else(|| OxenError::basic_str(&remote_branch_err))?;
        match api::remote::commits::get_by_id(remote_repo, &remote_branch.commit_id).await {
            Ok(Some(commit)) => {
                log::debug!(
                    "Oxen pull got remote commit: {} -> '{}'",
                    commit.id,
                    commit.message
                );

                // Make sure this branch points to this commit
                self.set_branch_name_for_commit(&rb.branch, &commit)?;

                println!("🐂 fetching commit objects {}", commit.id);
                // Sync the commit objects
                self.rpull_missing_commit_objects(remote_repo, &commit)
                    .await?;
                log::debug!(
                    "pull_all_commit_objects DONE {} -> '{}'",
                    commit.id,
                    commit.message
                );
                return Ok(Some(commit));
            }
            Ok(None) => {
                eprintln!("oxen pull error: remote head does not exist");
            }
            Err(err) => {
                log::debug!("oxen pull could not get remote head: {}", err);
            }
        }

        Ok(None)
    }

    fn set_branch_name_for_commit(&self, name: &str, commit: &Commit) -> Result<(), OxenError> {
        let ref_writer = RefWriter::new(&self.repository)?;
        // Make sure head is pointing to that branch
        ref_writer.set_head(name);
        ref_writer.set_branch_commit_id(name, &commit.id)
    }

    /// Just pull the commit db and history dbs that are missing (not the entries)
    async fn rpull_missing_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        remote_head_commit: &Commit,
    ) -> Result<(), OxenError> {
        // See if we have the DB pulled
        let commit_db_dir = util::fs::oxen_hidden_dir(&self.repository.path)
            .join(HISTORY_DIR)
            .join(remote_head_commit.id.clone());
        if !commit_db_dir.exists() {
            // We don't have db locally, so pull it
            log::debug!(
                "commit db for {} not found, pull from remote",
                remote_head_commit.id
            );
            self.check_parent_and_pull_commit_objects(remote_repo, remote_head_commit)
                .await?;
        } else {
            // else we are synced
            log::debug!("commit db for {} already downloaded", remote_head_commit.id);
        }

        Ok(())
    }

    #[async_recursion]
    async fn check_parent_and_pull_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        match api::remote::commits::get_remote_parent(remote_repo, &commit.id).await {
            Ok(parents) => {
                if parents.is_empty() {
                    log::debug!("no parents for commit {}", commit.id);
                } else {
                    // Recursively sync the parents
                    for parent in parents.iter() {
                        self.check_parent_and_pull_commit_objects(remote_repo, parent)
                            .await?;
                    }
                }
            }
            Err(err) => {
                log::warn!("oxen pull could not get commit parents: {}", err);
            }
        }

        // Pulls dbs and commit object
        self.pull_commit_data_objects(remote_repo, commit).await?;

        Ok(())
    }

    async fn pull_commit_data_objects(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!(
            "pull_commit_data_objects {} `{}`",
            commit.id,
            commit.message
        );

        // Download the specific commit_db that holds all the entries
        api::remote::commits::download_commit_db_to_repo(&self.repository, remote_repo, &commit.id)
            .await?;

        // Get commit and write it to local DB
        let remote_commit = api::remote::commits::get_by_id(remote_repo, &commit.id)
            .await?
            .unwrap();
        let writer = CommitWriter::new(&self.repository)?;
        writer.add_commit_to_db(&remote_commit)
    }

    // For unit testing a half synced commit
    pub async fn pull_entries_for_commit_with_limit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        self.pull_commit_data_objects(remote_repo, commit).await?;
        self.pull_entries_for_commit(remote_repo, commit, limit)
            .await
    }

    fn read_pulled_commit_entries(
        &self,
        commit: &Commit,
        mut limit: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let commit_reader = CommitEntryReader::new(&self.repository, commit)?;
        let entries = commit_reader.list_entries()?;
        log::debug!(
            "{} limit {} entries.len() {}",
            current_function!(),
            limit,
            entries.len()
        );
        if limit == 0 {
            limit = entries.len();
        }
        Ok(entries[0..limit].to_vec())
    }

    pub async fn pull_entries_for_commit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        log::debug!(
            "🐂 pull_entries_for_commit_id commit {} -> '{}'",
            commit.id,
            commit.message
        );

        if index::commit_sync_status::commit_is_synced(&self.repository, commit) {
            log::debug!(
                "🐂 commit {} -> '{}' is already synced",
                commit.id,
                commit.message
            );
            return Ok(());
        }

        let entries = self.read_pulled_commit_entries(commit, limit)?;
        log::debug!(
            "🐂 pull_entries_for_commit_id commit_id {} limit {} entries.len() {}",
            commit.id,
            limit,
            entries.len()
        );

        // Pull all the files that are missing
        puller::pull_entries(remote_repo, &entries, &self.repository.path, &|| {
            self.backup_to_versions_dir(commit, &entries).unwrap();

            if limit == 0 {
                // limit == 0 means we pulled everything, so mark it as complete
                self.pull_complete(commit).unwrap();
            }
        })
        .await?;

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(commit)?;

        Ok(())
    }

    fn backup_to_versions_dir(
        &self,
        commit: &Commit,
        entries: &Vec<CommitEntry>,
    ) -> Result<(), OxenError> {
        println!("Unpacking...");
        let bar = Arc::new(ProgressBar::new(entries.len() as u64));
        let dir_entries = api::local::entries::group_entries_to_parent_dirs(entries);

        dir_entries.par_iter().for_each(|(dir, entries)| {
            let committer = CommitDirEntryWriter::new(&self.repository, &commit.id, dir).unwrap();
            entries.par_iter().for_each(|entry| {
                let filepath = self.repository.path.join(&entry.path);
                if self.should_copy_entry(entry, &filepath) {
                    log::debug!("pull_entries_for_commit unpack {:?}", entry.path);
                    let version_path = util::fs::version_path(&self.repository, entry);
                    match util::fs::copy_mkdir(&filepath, &version_path) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!(
                                "Could not copy {:?} to {:?}: {}",
                                version_path,
                                filepath,
                                err
                            );
                        }
                    }

                    log::debug!(
                        "{} updating timestamp for {:?}",
                        current_function!(),
                        filepath
                    );

                    match fs::metadata(&filepath) {
                        Ok(metadata) => {
                            let mtime = FileTime::from_last_modification_time(&metadata);
                            committer.set_file_timestamps(entry, &mtime).unwrap();
                        }
                        Err(err) => {
                            log::error!("Could not update timestamp for {:?}: {}", filepath, err);
                        }
                    }
                }
                bar.inc(1);
            });
        });

        bar.finish();

        log::debug!("Done Unpacking.");

        Ok(())
    }

    fn pull_complete(&self, commit: &Commit) -> Result<(), OxenError> {
        // This is so that we know when we switch commits that we don't need to pull versions again
        index::commit_sync_status::mark_commit_as_synced(&self.repository, commit)?;

        // When we successfully pull the data, the repo is no longer shallow
        self.repository.write_is_shallow(false)?;

        Ok(())
    }

    fn cleanup_removed_entries(&self, commit: &Commit) -> Result<(), OxenError> {
        let repository = self.repository.clone();
        let commit = commit.clone();
        for dir_entry_result in WalkDirGeneric::<((), Option<bool>)>::new(&self.repository.path)
            .skip_hidden(true)
            .process_read_dir(move |_, parent, _, dir_entry_results| {
                let parent = util::fs::path_relative_to_dir(parent, &repository.path).unwrap();
                let commit_reader =
                    CommitDirEntryReader::new(&repository, &commit.id, &parent).unwrap();

                dir_entry_results
                    .par_iter_mut()
                    .for_each(|dir_entry_result| {
                        if let Ok(dir_entry) = dir_entry_result {
                            if !dir_entry.file_type.is_dir() {
                                let short_path = util::fs::path_relative_to_dir(
                                    dir_entry.path(),
                                    &repository.path,
                                )
                                .unwrap();
                                let path = short_path.file_name().unwrap().to_str().unwrap();
                                // If we don't have the file in the commit, remove it
                                if !commit_reader.has_file(path) {
                                    let full_path = repository.path.join(short_path);
                                    if std::fs::remove_file(full_path).is_ok() {
                                        dir_entry.client_state = Some(true);
                                    }
                                }
                            }
                        }
                    })
            })
        {
            match dir_entry_result {
                Ok(dir_entry) => {
                    if let Some(was_removed) = &dir_entry.client_state {
                        if !*was_removed {
                            log::debug!("Removed file {:?}", dir_entry)
                        }
                    }
                }
                Err(err) => {
                    log::error!("Could not remove file {}", err)
                }
            }
        }

        Ok(())
    }

    fn should_copy_entry(&self, entry: &CommitEntry, path: &Path) -> bool {
        !path.exists() || self.path_hash_is_different(entry, path)
    }

    fn path_hash_is_different(&self, entry: &CommitEntry, path: &Path) -> bool {
        if let Ok(hash) = util::hasher::hash_file_contents(path) {
            return hash != entry.hash;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::core::index::entry_indexer::UnsyncedCommitEntries;
    use crate::core::index::EntryIndexer;
    use crate::error::OxenError;
    use crate::model::RemoteBranch;
    use crate::opts::CloneOpts;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_rpush_missing_commit_objects() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            // Make a few more commits, and then make sure the total count is correct to push
            let entry_indexer = EntryIndexer::new(&repo).unwrap();

            let head_commit = api::local::commits::head_commit(&repo)?;
            let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
            entry_indexer
                .rpush_missing_commit_objects(&remote_repo, &head_commit, &mut unsynced_commits)
                .await?;

            // The initial commit and the one after
            assert_eq!(unsynced_commits.len(), 2);

            // Push to the remote
            command::push(&repo).await?;

            // There should be none unsynced
            let head_commit = api::local::commits::head_commit(&repo)?;
            let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
            entry_indexer
                .rpush_missing_commit_objects(&remote_repo, &head_commit, &mut unsynced_commits)
                .await?;

            // The initial commit and the one after
            assert_eq!(unsynced_commits.len(), 0);

            // Modify README
            let readme_path = repo.path.join("README.md");
            let readme_path = test::modify_txt_file(readme_path, "I am the readme now.")?;
            command::add(&repo, readme_path)?;

            // Commit again
            let head_commit = command::commit(&repo, "Changed the readme")?;
            let mut unsynced_commits: VecDeque<UnsyncedCommitEntries> = VecDeque::new();
            entry_indexer
                .rpush_missing_commit_objects(&remote_repo, &head_commit, &mut unsynced_commits)
                .await?;

            println!("Num unsynced {}", unsynced_commits.len());
            for commit in unsynced_commits.iter() {
                println!("FOUND UNSYNCED: {:?}", commit.commit);
            }

            // Should be one more
            assert_eq!(unsynced_commits.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_indexer_partial_pull_then_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            command::push(&repo).await?;

            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let mut opts = CloneOpts::new(remote_repo.remote.url.to_owned(), &new_repo_dir);
                opts.shallow = true;

                let cloned_repo = command::clone(&opts).await?;
                let indexer = EntryIndexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = api::local::commits::list(&repo)?;
                let latest_commit = commits.first().unwrap();
                let page_size = 2;
                let limit = page_size;
                indexer
                    .pull_entries_for_commit_with_limit(&remote_repo, latest_commit, limit)
                    .await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                // try to pull the full thing again even though we have only partially pulled some
                let rb = RemoteBranch::default();
                indexer.pull(&rb).await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(og_num_files, num_files);

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_indexer_partial_pull_multiple_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let train_dir = repo.path.join("train");
            command::add(&repo, &train_dir)?;
            // Commit the file
            command::commit(&repo, "Adding training data")?;

            let test_dir = repo.path.join("test");
            command::add(&repo, &test_dir)?;
            // Commit the file
            command::commit(&repo, "Adding testing data")?;

            // Create remote
            let remote_repo = api::remote::repositories::create(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            // Push it
            command::push(&repo).await?;

            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let mut opts = CloneOpts::new(remote_repo.remote.url.to_owned(), &new_repo_dir);
                opts.shallow = true;
                let cloned_repo = command::clone(&opts).await?;
                let indexer = EntryIndexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = api::local::commits::list(&repo)?;
                let last_commit = commits.first().unwrap();
                let limit = 7;
                indexer
                    .pull_entries_for_commit_with_limit(&remote_repo, last_commit, limit)
                    .await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }
}