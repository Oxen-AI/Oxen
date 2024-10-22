//! EntryIndexer is responsible for pushing, pulling and syncing commit entries
//!

use indicatif::ProgressBar;
use jwalk::WalkDirGeneric;
use rayon::prelude::*;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::constants::{self, DEFAULT_REMOTE_NAME, HISTORY_DIR};
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_19_0::structs::PullProgress;
use crate::core::{self, db};

use crate::core::refs::RefWriter;
use crate::core::v0_10_0::index::{puller, versioner, Merger, Stager};
use crate::core::v0_10_0::index::{CommitDirEntryReader, CommitEntryReader};

use crate::error::OxenError;
use crate::model::entry::commit_entry::{Entry, SchemaEntry};
use crate::model::entry::unsynced_commit_entry::UnsyncedCommitEntries;
use crate::model::{
    Branch, Commit, CommitEntry, LocalRepository, RemoteBranch, RemoteRepository, StagedData,
};
use crate::opts::PullOpts;
use crate::repositories;
use crate::util::progress_bar::{oxen_progress_bar, spinner_with_msg, ProgressBarType};
use crate::util::{self, concurrency};
use crate::view::repository::RepositoryDataTypesView;
use crate::{api, current_function};

use super::{pusher, CommitEntryWriter, CommitReader, SchemaReader};

pub struct EntryIndexer {
    pub repository: LocalRepository,
}

impl EntryIndexer {
    pub fn new(repository: &LocalRepository) -> Result<EntryIndexer, OxenError> {
        Ok(EntryIndexer {
            repository: repository.clone(),
        })
    }

    pub async fn push(&self, src: Branch, dst: RemoteBranch) -> Result<Branch, OxenError> {
        pusher::push(&self.repository, src, dst).await
    }

    pub async fn pull(&self, rb: &RemoteBranch, opts: PullOpts) -> Result<(), OxenError> {
        println!("🐂 oxen pull {} {}", rb.remote, rb.branch);

        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or(OxenError::remote_not_set(&rb.remote))?;

        let remote_data_view =
            match api::client::repositories::get_repo_data_by_remote(&remote).await {
                Ok(Some(repo)) => repo,
                Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
                Err(err) => return Err(err),
            };

        // > 0 is a hack because only hub returns size right now, so just don't print for pure open source
        if remote_data_view.size > 0 && remote_data_view.total_files() > 0 {
            println!(
                "{} ({}) contains {} files",
                remote_data_view.name,
                bytesize::ByteSize::b(remote_data_view.size),
                remote_data_view.total_files()
            );

            println!(
                "\n  {}\n",
                RepositoryDataTypesView::data_types_str(&remote_data_view.data_types)
            );
        }

        let remote_repo = RemoteRepository::from_data_view(&remote_data_view, &remote);
        self.pull_remote_repo(&remote_repo, rb, &opts).await
    }

    pub async fn pull_remote_repo(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
        opts: &PullOpts,
    ) -> Result<(), OxenError> {
        let mut opts = opts.clone();

        // original head commit, only applies to pulling commits after initial clone
        let maybe_head_commit = repositories::commits::head_commit(&self.repository);

        let head_commit = if let Ok(commit) = maybe_head_commit {
            Some(commit)
        } else {
            None
        };

        // For pulling after the initial clone, save status to avoid overwriting local untracked changes
        let mut status = StagedData::empty();

        // TODO: revisit after updating shallow clone
        if head_commit.is_some() && !self.repository.is_shallow_clone() {
            let stager = Stager::new(&self.repository)?;
            status = stager.status(&CommitEntryReader::new(
                &self.repository,
                &head_commit.clone().unwrap(),
            )?)?
        };

        // If our local branch is currently completely synced (from a clone or pull --all), we should
        // override the opts and pull all commits
        if let Some(ref commit) = head_commit {
            if repositories::commits::commit_history_is_complete(&self.repository, commit)? {
                opts.should_pull_all = true;
            }
        }

        let mut commit = if opts.should_pull_all {
            self.pull_all(remote_repo, rb, opts.should_update_head)
                .await?
        } else {
            self.pull_one(remote_repo, rb, opts.should_update_head)
                .await?
        };

        // TODO Do we add a flag for if this pull is a merge somehow...?
        // If the branches have diverged, we need to merge the commit into the base

        if let Some(ref head_commit) = head_commit {
            if head_commit.id != commit.id {
                let merger = Merger::new(&self.repository)?;
                if let Some(merge_commit) = merger.merge_commit_into_base(&commit, head_commit)? {
                    commit = merge_commit;
                }
            }
        }

        // Mark the new commit (merged or pulled) as synced
        core::commit_sync_status::mark_commit_as_synced(&self.repository, &commit)?;

        // Cleanup files that shouldn't be there
        // TODO: Revisit after revising shallow logic
        if !&self.repository.is_shallow_clone() {
            self.cleanup_removed_entries(&commit, status)?;
        }

        log::debug!(
            "pull complete ✅ for commit {} -> '{}'",
            commit.id,
            commit.message
        );

        Ok(())
    }

    pub async fn pull_commit(&self, commit: &Commit) -> Result<(), OxenError> {
        // Get the remote, TODO: make this configurable
        let remote = self
            .repository
            .get_remote(DEFAULT_REMOTE_NAME)
            .ok_or(OxenError::remote_not_set(DEFAULT_REMOTE_NAME))?;
        let remote_repo = match api::client::repositories::get_by_remote(&remote).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
            Err(err) => return Err(err),
        };

        self.pull_commit_entries_db(&remote_repo, commit).await?;
        let commit_vec = vec![commit.clone()];
        self.pull_tree_objects_for_commits(&remote_repo, &commit_vec)
            .await?;
        self.pull_all_entries_for_commit(&remote_repo, commit)
            .await?;

        Ok(())
    }

    async fn pull_all(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
        should_update_head: bool,
    ) -> Result<Commit, OxenError> {
        log::debug!("pulling all");
        let new_head = match self.pull_all_commit_objects(remote_repo, rb).await {
            Ok(Some(commit)) => {
                log::debug!("pull_result: {} -> {}", commit.id, commit.message);
                // Make sure this branch points to this commit
                self.set_branch_name_for_commit(&rb.branch, &commit, should_update_head)?;
                commit
            }
            Ok(None) => repositories::commits::head_commit(&self.repository)?,
            Err(err) => {
                // if no commit objects, means repo is empty, so instantiate the local repo
                log::error!("pull_all error: {}", err);
                eprintln!("warning: You appear to have cloned an empty repository. Initializing with an empty commit.");
                core::v0_10_0::commits::commit_with_no_files(
                    &self.repository,
                    constants::INITIAL_COMMIT_MSG,
                )?
            }
        };

        // Get entries between here and new head, get entries for any missing
        let commits = repositories::commits::list_from(&self.repository, &new_head.id)?;
        let commits = commits.into_iter().rev().collect::<Vec<Commit>>();

        let mut unsynced_entry_commits: Vec<Commit> = Vec::new();
        log::debug!("checking if {} commits are synced", commits.len());
        for c in &commits {
            if !core::commit_sync_status::commit_is_synced(&self.repository, c) {
                unsynced_entry_commits.push(c.clone());
            }
        }

        for commit in &unsynced_entry_commits {
            log::debug!("unsynced_entry_commits: {:#?}", commit);
        }

        self.pull_tree_objects_for_commits(remote_repo, &unsynced_entry_commits)
            .await?;

        log::debug!("about to pull entries for commits");
        // Download all files to versions dir
        self.pull_entries_for_commits(remote_repo, unsynced_entry_commits)
            .await?;

        // Mark commits as synced for future pulls
        for commit in commits {
            core::commit_sync_status::mark_commit_as_synced(&self.repository, &commit)?;
        }

        Ok(new_head)
    }

    async fn pull_one(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
        should_update_head: bool,
    ) -> Result<Commit, OxenError> {
        match self
            .pull_most_recent_commit_object(remote_repo, rb, should_update_head)
            .await
        {
            Ok(Some(commit)) => {
                log::debug!("pull_result: {} -> {}", commit.id, commit.message);
                self.pull_all_entries_for_commit(remote_repo, &commit)
                    .await?;
                // Mark commit complete
                core::commit_sync_status::mark_commit_as_synced(&self.repository, &commit)?;
                Ok(commit)
            }
            Ok(None) => repositories::commits::head_commit(&self.repository),
            Err(err) => {
                // if no commit objects, means repo is empty, so instantiate the local repo
                log::debug!("pull_one empty repo: {}", err);
                eprintln!("warning: You appear to have cloned an empty repository. Initializing with an empty commit.");
                core::v0_10_0::commits::commit_with_no_files(
                    &self.repository,
                    constants::INITIAL_COMMIT_MSG,
                )
            }
        }
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
        self.pull_entries_for_commit(remote_repo, commit.clone(), limit)
            .await?;
        log::debug!(
            "DONE! pull_all_entries_for_commit for commit: {} -> {}",
            commit.id,
            commit.message
        );
        Ok(())
    }

    pub async fn pull_most_recent_commit_object(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
        should_update_head: bool,
    ) -> Result<Option<Commit>, OxenError> {
        let remote_branch_err = format!("Remote branch not found: {}", rb.branch);
        let remote_branch = api::client::branches::get_by_name(remote_repo, &rb.branch)
            .await?
            .ok_or_else(|| OxenError::basic_str(&remote_branch_err))?;

        // Download the commits db
        println!("Fetching commits for {}", rb.branch);
        api::client::commits::download_commits_db_to_repo(&self.repository, remote_repo).await?;

        match api::client::commits::get_by_id(remote_repo, &remote_branch.commit_id).await {
            Ok(Some(commit)) => {
                log::debug!(
                    "Oxen pull got remote commit: {} -> '{}'",
                    commit.id,
                    commit.message
                );

                // Make sure this branch points to this commit
                self.set_branch_name_for_commit(&rb.branch, &commit, should_update_head)?;

                let commits_vec = vec![commit.clone()];
                self.pull_tree_objects_for_commits(remote_repo, &commits_vec)
                    .await?;

                // Sync the commit entries objects
                self.pull_commit_entries_db(remote_repo, &commit).await?;

                log::debug!(
                    "pull_commit_object DONE {} -> '{}'",
                    commit.id,
                    commit.message
                );
                return Ok(Some(commit));
            }
            Ok(None) => {
                println!("Everything up to date.");
            }
            Err(err) => {
                log::warn!(
                    "pull_most_recent_commit_object could not get remote commit: {}",
                    err
                );
            }
        }

        Ok(None)
    }

    pub async fn pull_all_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
    ) -> Result<Option<Commit>, OxenError> {
        let remote_branch_err = format!("Remote branch not found: {}", rb.branch);
        let remote_branch = api::client::branches::get_by_name(remote_repo, &rb.branch)
            .await?
            .ok_or_else(|| OxenError::basic_str(&remote_branch_err))?;

        // Download full commits db
        let spinner = spinner_with_msg("🐂 Downloading commits db from remote...");

        api::client::commits::download_commits_db_to_repo(&self.repository, remote_repo).await?;

        spinner.finish_and_clear();
        // list all the remote commits on a branch, so we know how many we have to pull
        let remote_commits =
            api::client::commits::list_commit_history(remote_repo, &remote_branch.commit_id)
                .await?;

        let mut missing_commits = Vec::new();
        for remote_commit in remote_commits {
            if !(core::v0_10_0::commits::commit_history_db_exists(
                &self.repository,
                &remote_commit,
            )?) {
                // log::debug!("Missing commit {}", remote_commit.id);
                missing_commits.push(remote_commit);
            } else {
                // log::debug!("Already have commit {}", remote_commit.id);
            }
        }

        let total_missing = missing_commits.len();
        if total_missing == 0 {
            // Nothing to do
            return Ok(None);
        }
        println!("🐂 Syncing databases for {} commits...", total_missing);

        // Download the missing commit objects
        let progress_bar = oxen_progress_bar(total_missing as u64, ProgressBarType::Counter);
        match api::client::commits::get_by_id(remote_repo, &remote_branch.commit_id).await {
            Ok(Some(commit)) => {
                log::debug!(
                    "Oxen pull got remote commit: {} -> '{}'",
                    commit.id,
                    commit.message
                );

                // Sync the commit objects
                self.pull_missing_commit_objects(remote_repo, missing_commits, &progress_bar)
                    .await?;
                log::debug!(
                    "pull_all_commit_objects DONE {} -> '{}'",
                    commit.id,
                    commit.message
                );
                return Ok(Some(commit));
            }
            Ok(None) => {
                log::debug!("pull_all_commit_objects commit does not exist");
            }
            Err(err) => {
                log::warn!(
                    "pull_all_commit_objects could not get remote commit: {}",
                    err
                );
            }
        }
        progress_bar.finish_and_clear();
        Ok(None)
    }

    fn set_branch_name_for_commit(
        &self,
        name: &str,
        commit: &Commit,
        set_head: bool,
    ) -> Result<(), OxenError> {
        let ref_writer = RefWriter::new(&self.repository)?;
        if set_head {
            // Make sure head is pointing to that branch
            ref_writer.set_head(name);
        }
        ref_writer.set_branch_commit_id(name, &commit.id)
    }

    /// Just pull the commit db and history dbs that are missing (not the entries)
    async fn pull_missing_commit_objects(
        &self,
        remote_repository: &RemoteRepository,
        commits: Vec<Commit>,
        bar: &Arc<ProgressBar>,
    ) -> Result<(), OxenError> {
        // TODO: these async task queues are gnarly...abstract away
        use tokio::time::{sleep, Duration};
        type PieceOfWork = (LocalRepository, RemoteRepository, Commit, Arc<ProgressBar>);
        type TaskQueue = deadqueue::limited::Queue<PieceOfWork>;
        type FinishedTaskQueue = deadqueue::limited::Queue<bool>;

        let total_missing = commits.len();
        log::debug!("Chunking and sending {} larger files", total_missing);
        let commits: Vec<PieceOfWork> = commits
            .iter()
            .map(|c| {
                (
                    self.repository.to_owned(),
                    remote_repository.to_owned(),
                    c.to_owned(),
                    bar.to_owned(),
                )
            })
            .collect();

        let queue = Arc::new(TaskQueue::new(total_missing));
        let finished_queue = Arc::new(FinishedTaskQueue::new(total_missing));
        for commit in commits.iter() {
            queue.try_push(commit.to_owned()).unwrap();
            finished_queue.try_push(false).unwrap();
        }

        let worker_count = concurrency::num_threads_for_items(total_missing);
        log::debug!(
            "worker_count {} total_missing {}",
            worker_count,
            total_missing
        );

        for worker in 0..worker_count {
            let queue = queue.clone();
            let finished_queue = finished_queue.clone();
            tokio::spawn(async move {
                loop {
                    let (repository, remote_repo, commit, bar) = queue.pop().await;
                    log::debug!("worker[{}] processing task...", worker);

                    // See if we have the DB pulled
                    let commit_db_dir = util::fs::oxen_hidden_dir(&repository.path)
                        .join(HISTORY_DIR)
                        .join(commit.id.clone());
                    if !commit_db_dir.exists() {
                        // We don't have db locally, so pull it
                        log::debug!("commit db for {} not found, pull from remote", commit.id);

                        // Pulls dbs and commit object
                        match api::client::commits::download_commit_entries_db_to_repo(
                            &repository,
                            &remote_repo,
                            &commit.id,
                        )
                        .await
                        {
                            Ok(_) => {
                                log::debug!("commit db for {} downloaded", commit.id);
                                bar.inc(1);
                            }
                            Err(err) => {
                                log::debug!("commit db for {} failed: {}", commit.id, err);
                            }
                        }
                    } else {
                        // else we are synced
                        log::debug!("commit db for {} already downloaded", commit.id);
                    }

                    finished_queue.pop().await;
                }
            });
        }

        while finished_queue.len() > 0 {
            // log::debug!("Before waiting for {} workers to finish...", queue.len());
            sleep(Duration::from_secs(1)).await;
        }
        log::debug!("All commit db downloads tasks done. :-)");

        Ok(())
    }

    async fn pull_commit_entries_db(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!("pull_commit_entries_db {} `{}`", commit.id, commit.message);

        // Download the specific commit_db that holds all the entries
        api::client::commits::download_commit_entries_db_to_repo(
            &self.repository,
            remote_repo,
            &commit.id,
        )
        .await?;
        Ok(())
    }

    // For unit testing a half synced commit
    pub async fn pull_entries_for_commit_with_limit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        self.pull_commit_entries_db(remote_repo, commit).await?;
        self.pull_entries_for_commit(remote_repo, commit.clone(), limit)
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

        if limit > entries.len() {
            limit = entries.len();
        }
        Ok(entries[0..limit].to_vec())
    }

    fn read_pulled_schema_entries(
        &self,
        commit: &Commit,
        mut limit: usize,
    ) -> Result<Vec<SchemaEntry>, OxenError> {
        let schema_reader = SchemaReader::new(&self.repository, &commit.id)?;
        let schemas = schema_reader.list_schema_entries()?;

        if limit == 0 {
            limit = schemas.len();
        }
        if limit > schemas.len() {
            limit = schemas.len();
        }
        Ok(schemas[0..limit].to_vec())
    }

    pub async fn pull_tree_objects_for_commits(
        &self,
        remote_repo: &RemoteRepository,
        commits: &[Commit],
    ) -> Result<(), OxenError> {
        log::debug!("🐂 pulling tree objects for {:?} commits", commits.len());
        if commits.is_empty() {
            return Ok(()); // nothing to do, pulling anyway causes objects db errors
        }
        api::client::commits::download_objects_db_to_repo(&self.repository, remote_repo).await?;
        Ok(())
    }

    pub async fn pull_entries_for_commits(
        &self,
        remote_repo: &RemoteRepository,
        commits: Vec<Commit>,
    ) -> Result<(), OxenError> {
        log::debug!("🐂 pulling entries for {:?} commits", commits.len());

        // Initialize a commitreader on the local repo
        let commit_reader = CommitReader::new(&self.repository)?;

        let mut unsynced_entries: Vec<UnsyncedCommitEntries> = Vec::new();

        log::debug!("gathering entries");
        for commit in &commits {
            for parent_id in &commit.parent_ids {
                let local_parent = commit_reader
                    .get_commit_by_id(parent_id)?
                    .ok_or_else(|| OxenError::local_parent_link_broken(&commit.id))?;

                let entries = repositories::entries::read_unsynced_entries(
                    &self.repository,
                    &local_parent,
                    commit,
                )?;

                log::debug!("about to read unsynced schemas for commit {}", commit.id);

                let schemas = repositories::entries::read_unsynced_schemas(
                    &self.repository,
                    &local_parent,
                    commit,
                )?;

                log::debug!("read unsynced schemas for commit {}", commit.id);

                // Collec these both together as Entry
                let mut entries: Vec<Entry> = entries.into_iter().map(Entry::from).collect();
                entries.extend(schemas.into_iter().map(Entry::from));

                unsynced_entries.push(UnsyncedCommitEntries {
                    commit: commit.clone(),
                    entries,
                });
            }
        }

        log::debug!("gathered entries");

        // Pull flattened entries
        // Flatten unsynced_entries
        let mut all_entries: Vec<Entry> = Vec::new();
        for commit_with_entries in &unsynced_entries {
            all_entries.extend(commit_with_entries.entries.clone());
        }

        // Only pull entries with unique hashes to save storage and data transfer for duplicate and/or moved files.
        let mut seen_entries: HashSet<String> = HashSet::new();
        all_entries.retain(|entry| {
            let key = format!("{}{}", entry.hash(), entry.extension());
            seen_entries.insert(key)
        });

        log::debug!("about to pull the entires to the versions dir");
        let progress_bar = PullProgress::new();

        puller::pull_entries_to_versions_dir(
            remote_repo,
            &all_entries,
            &self.repository.path,
            &progress_bar,
        )
        .await?;

        // Get full length of all entries arrays in unsynced_entries
        let mut entries_to_unpack: usize = 0;
        for commit_with_entries in &unsynced_entries {
            entries_to_unpack += commit_with_entries.entries.len();
        }

        let bar = oxen_progress_bar(entries_to_unpack as u64, ProgressBarType::Counter);

        println!("🐂 Unpacking files...");
        for commit_with_entries in unsynced_entries {
            self.unpack_version_files_to_working_dir(
                &commit_with_entries.commit,
                &commit_with_entries.entries,
                &bar,
            )?;
            self.pull_complete(&commit_with_entries.commit).unwrap();
        }

        Ok(())
    }

    pub async fn pull_entries_for_commit(
        &self,
        remote_repo: &RemoteRepository,
        commit: Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        log::debug!(
            "🐂 pull_entries_for_commit_id commit {} -> '{}'",
            commit.id,
            commit.message
        );

        if core::commit_sync_status::commit_is_synced(&self.repository, &commit) {
            log::debug!(
                "🐂 commit {} -> '{}' is already synced",
                commit.id,
                commit.message
            );
            return Ok(());
        }

        let entries = self.read_pulled_commit_entries(&commit, limit)?;
        log::debug!(
            "🐂 pull_entries_for_commit_id commit_id {} limit {} entries.len() {}",
            commit.id,
            limit,
            entries.len()
        );

        let schema_entries = self.read_pulled_schema_entries(&commit, limit)?;
        let mut entries: Vec<Entry> = entries.into_iter().map(Entry::from).collect();
        entries.extend(schema_entries.into_iter().map(Entry::from));

        let n_entries_to_pull = entries.len();
        log::debug!("got {} entries to pull", n_entries_to_pull);

        // Pull all the entries to the versions dir and then hydrate them into the working dir
        let progress_bar = PullProgress::new();
        puller::pull_entries_to_versions_dir(
            remote_repo,
            &entries,
            &self.repository.path,
            &progress_bar,
        )
        .await?;

        let entries_to_unpack = entries.len();

        println!("🐂 Unpacking files...");
        let bar = oxen_progress_bar(entries_to_unpack as u64, ProgressBarType::Counter);
        self.unpack_version_files_to_working_dir(&commit, &entries, &bar)?;

        if limit == 0 {
            self.pull_complete(&commit).unwrap();
        }

        Ok(())
    }

    pub fn unpack_version_files_to_working_dir(
        &self,
        _commit: &Commit,
        entries: &[Entry],
        bar: &Arc<ProgressBar>,
    ) -> Result<(), OxenError> {
        // TODO: Don't need to group anymore
        let dir_entries = repositories::entries::group_entries_to_parent_dirs(entries);
        let opts = db::key_val::opts::default();
        let files_db = CommitEntryWriter::files_db_dir(&self.repository);
        let files_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open(&opts, dunce::simplified(&files_db))?;

        dir_entries.par_iter().for_each(|(dir, entries)| {
            log::debug!(
                "unpack_version_files_to_working_dir unpacking dir {:?} with {} entries",
                dir,
                entries.len()
            );

            entries.par_iter().for_each(|entry| {
                let filepath = self.repository.path.join(entry.path());
                // log::debug!(
                //     "unpack_version_files_to_working_dir found filepath {:?}",
                //     filepath
                // );
                if versioner::should_unpack_entry(entry, &filepath) {
                    // log::debug!(
                    //     "unpack_version_files_to_working_dir unpack! {:?}",
                    //     entry.path()
                    // );
                    let version_path = util::fs::version_path_for_entry(&self.repository, entry);
                    match util::fs::copy_mkdir(version_path, &filepath) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!("pull_entries_for_commit unpack error: {}", err);
                        }
                    }
                } else {
                    log::debug!(
                        "unpack_version_files_to_working_dir do not unpack :( {:?}",
                        entry.path()
                    );
                }

                if let Entry::CommitEntry(file) = entry {
                    match CommitEntryWriter::set_file_timestamps(
                        &self.repository,
                        &file.path,
                        file,
                        &files_db,
                    ) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!(
                                "unpack_version_files_to_working_dir set_file_timestamps error: {}",
                                err
                            );
                        }
                    }
                }
                bar.inc(1);
            });
        });

        Ok(())
    }

    fn pull_complete(&self, commit: &Commit) -> Result<(), OxenError> {
        // This is so that we know when we switch commits that we don't need to pull versions again
        core::commit_sync_status::mark_commit_as_synced(&self.repository, commit)?;

        // When we successfully pull the data, the repo is no longer shallow
        self.repository.write_is_shallow(false)?;

        Ok(())
    }

    fn cleanup_removed_entries(
        &self,
        commit: &Commit,
        status: StagedData,
    ) -> Result<(), OxenError> {
        log::debug!("CLEANUP_REMOVED_ENTRIES commit {}", commit);
        let untracked_files: HashSet<PathBuf> = status.untracked_files.into_iter().collect();
        let untracked_dirs: HashSet<PathBuf> = status
            .untracked_dirs
            .iter()
            .map(|(path, _)| path.clone())
            .collect();
        log::debug!("untracked_files {:?}", untracked_files);
        log::debug!("untracked_dirs {:?}", untracked_dirs);
        // Get status to avoid removing untracked files
        let repository = self.repository.clone();
        let commit = commit.clone();
        let commit_reader = CommitEntryReader::new(&repository, &commit)?;

        let object_reader = get_object_reader(&repository, &commit.id)?;

        for dir_entry_result in WalkDirGeneric::<((), Option<bool>)>::new(&self.repository.path)
            .skip_hidden(true)
            .process_read_dir(move |_, parent, _, dir_entry_results| {
                log::debug!(
                    "cleanup_removed_entries checking parent dir {:?} for repo {:?}",
                    parent,
                    repository.path
                );

                let parent = util::fs::path_relative_to_dir(parent, &repository.path).unwrap();

                log::debug!(
                    "cleanup_removed_entries got parent dir {:?} for commit {} -> '{}'",
                    parent,
                    commit.id,
                    commit.message
                );

                let commit_entry_reader = CommitDirEntryReader::new(
                    &repository,
                    &commit.id,
                    &parent,
                    object_reader.clone(),
                )
                .unwrap();

                log::debug!(
                    "cleanup_removed_entries got commit_entry_reader for commit {} -> '{}' dir_entry_results.len() {}",
                    commit.id,
                    commit.message,
                    dir_entry_results.len()
                );

                dir_entry_results
                    .par_iter_mut()
                    .for_each(|dir_entry_result| {
                        if let Ok(dir_entry) = dir_entry_result {
                            log::debug!(
                                "{} considering entry {:?}",
                                current_function!(),
                                dir_entry
                            );

                            let short_path =
                                util::fs::path_relative_to_dir(dir_entry.path(), &repository.path)
                                    .unwrap();

                            log::debug!(
                                "{} considering short path {:?}",
                                current_function!(),
                                short_path
                            );

                            if !dir_entry.file_type.is_dir() {
                                let path = short_path.file_name().unwrap().to_str().unwrap();
                                // If we don't have the file in the commit, remove it
                                // (unless it's in untracked files or parent in untracked dirs)

                                log::debug!(
                                    "{} considering file {:?} with untracked_files.len() {} untracked_dirs.contains({:?}) '{}'",
                                    current_function!(),
                                    path,
                                    untracked_files.len(),
                                    short_path,
                                    untracked_dirs.contains(&short_path)
                                );

                                let one = commit_entry_reader.has_file(path);
                                let two = untracked_files.contains(&short_path);
                                let three = util::fs::is_any_parent_in_set(&short_path, &untracked_dirs);
                                // log::debug!("one: {one} two: {two} three {three}");

                                if !one && !two && !three
                                {
                                    log::debug!(
                                        "{} commit reader does not have file {:?}",
                                        current_function!(),
                                        path
                                    );

                                    let full_path = repository.path.join(short_path);
                                    if util::fs::remove_file(full_path).is_ok() {
                                        dir_entry.client_state = Some(true);
                                    }
                                }
                            } else {
                                // is dir
                                // make sure we have the dir in the commit and it is a subdir (!= "")
                                // unless it or a parent is in untracked dirs
                                if !commit_reader.has_dir(&short_path)
                                    && short_path != Path::new("")
                                    && !untracked_dirs.contains(&short_path)
                                    && !util::fs::is_any_parent_in_set(&short_path, &untracked_dirs)
                                {
                                    log::debug!(
                                        "{} commit reader does not have dir {:?}",
                                        current_function!(),
                                        short_path
                                    );

                                    let full_path = repository.path.join(short_path);
                                    if full_path.exists()
                                        && util::fs::remove_dir_all(full_path).is_ok()
                                    {
                                        dir_entry.client_state = Some(true);
                                    }
                                }
                            }
                        }
                    })
            })
        {
            // log::debug!("cleanup_removed_entries : {:?}", dir_entry_result);
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

        log::debug!("cleanup_removed_entries done!");

        Ok(())
    }
}
