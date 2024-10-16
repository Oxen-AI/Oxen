use crate::config::UserConfig;
use crate::constants::{COMMITS_DIR, MERGE_HEAD_FILE, ORIG_HEAD_FILE};
use crate::core::db::key_val::path_db;

use crate::core;
use crate::core::db;
use crate::core::refs::{RefReader, RefWriter};
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::{
    self, workspaces, CommitDBReader, CommitDirEntryReader, CommitEntryReader, CommitEntryWriter,
    CommitReader, EntryIndexer, ObjectDBReader,
};
use crate::error::OxenError;
use crate::model::{
    Branch, Commit, CommitEntry, NewCommit, StagedData, StagedEntry, StagedEntryStatus, Workspace,
};

use crate::repositories;
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::str;
use time::OffsetDateTime;

use crate::model::LocalRepository;

pub struct CommitWriter {
    pub commits_db: DBWithThreadMode<MultiThreaded>,
    repository: LocalRepository,
}

impl CommitWriter {
    pub fn commit_db_dir(path: &Path) -> PathBuf {
        util::fs::oxen_hidden_dir(path).join(Path::new(COMMITS_DIR))
    }

    pub fn new(repository: &LocalRepository) -> Result<CommitWriter, OxenError> {
        let db_path = CommitWriter::commit_db_dir(&repository.path);

        if !db_path.exists() {
            std::fs::create_dir_all(&db_path)?;
        }

        let opts = db::key_val::opts::default();
        Ok(CommitWriter {
            commits_db: DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?,
            repository: repository.clone(),
        })
    }

    fn create_new_commit_data(&self, message: &str) -> Result<NewCommit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();
        let ref_reader = RefReader::new(&self.repository)?;
        // Commit
        //  - parent_ids (can be empty if root)
        //  - message
        //  - date
        //  - author
        match ref_reader.head_commit_id() {
            Ok(Some(parent_id)) => {
                // We might be in a merge commit, in which case we would have multiple parents
                if self.is_merge_commit() {
                    log::debug!("Create merge commit...");
                    self.create_merge_commit(message)
                } else {
                    // We have one parent
                    log::debug!("Create commit with parent {:?}", parent_id);
                    Ok(NewCommit {
                        parent_ids: vec![parent_id],
                        message: String::from(message),
                        author: cfg.name,
                        email: cfg.email,
                        timestamp,
                    })
                }
            }
            _ => {
                // We are creating initial commit, no parents
                log::debug!("Create initial commit...");
                Ok(NewCommit {
                    parent_ids: vec![],
                    message: String::from(message),
                    author: cfg.name,
                    email: cfg.email,
                    timestamp,
                })
            }
        }
    }

    // Reads commit ids from merge commit files then removes them
    fn create_merge_commit(&self, message: &str) -> Result<NewCommit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        let orig_head_path = hidden_dir.join(ORIG_HEAD_FILE);

        // Read parent commit ids
        let merge_commit_id = util::fs::read_from_path(&merge_head_path)?;
        let head_commit_id = util::fs::read_from_path(&orig_head_path)?;

        // Cleanup
        util::fs::remove_file(merge_head_path)?;
        util::fs::remove_file(orig_head_path)?;

        Ok(NewCommit {
            parent_ids: vec![merge_commit_id, head_commit_id],
            message: String::from(message),
            author: cfg.name,
            email: cfg.email,
            timestamp,
        })
    }

    fn is_merge_commit(&self) -> bool {
        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
        let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
        merge_head_path.exists()
    }

    /// Commit the staged data with a message
    pub fn commit(&self, status: &StagedData, message: &str) -> Result<Commit, OxenError> {
        // Create a commit object, that either points to parent or not
        // must create this before anything else so that we know if it has parent or not.
        log::debug!("---COMMIT START---"); // for debug logging / timing purposes
        let new_commit = self.create_new_commit_data(message)?;
        log::debug!("Created commit obj {:?}", new_commit);
        let commit = self.commit_from_new(&new_commit, status, &self.repository.path)?;
        log::debug!("COMMIT_COMPLETE {} -> {}", commit.id, commit.message);

        // Mark as synced so we know we don't need to pull versions files again
        core::commit_sync_status::mark_commit_as_synced(&self.repository, &commit)?;

        // User output
        println!("Commit {} done.", commit.id);
        log::debug!("---COMMIT END {} -> '{}'---", commit.id, message); // for debug logging / timing purposes
        Ok(commit)
    }

    pub fn commit_from_new(
        &self,
        new_commit: &NewCommit,
        status: &StagedData,
        origin_path: &Path,
    ) -> Result<Commit, OxenError> {
        let commit = self.gen_commit(new_commit, status);
        log::debug!(
            "commit_from_new commit Id computed {} -> [{}]",
            commit.id,
            commit.message
        );

        let commit = self.add_commit_from_status(&commit, status, origin_path)?;

        Ok(commit)
    }

    // used to be commit_from_new_on_remote_branch
    /// Commit a workspace to a branch with a new commit
    pub fn commit_workspace(
        &self,
        workspace: &Workspace,
        branch: &Branch,
        new_commit: &NewCommit,
        status: &StagedData,
    ) -> Result<Commit, OxenError> {
        let Some(old_commit) =
            CommitReader::new(&self.repository)?.get_commit_by_id(&branch.commit_id)?
        else {
            return Err(OxenError::basic_str(format!(
                "Could not commit, remote staging area is out of date with commit {}",
                branch.commit_id
            )));
        };

        let entries = workspaces::stager::list_files(workspace)?;
        let object_reader = get_object_reader(&self.repository, &branch.commit_id)?;
        let commit_entry_reader = CommitEntryReader::new_from_commit_id(
            &self.repository,
            &branch.commit_id,
            object_reader,
        )?;
        // TODO: this is not the most efficient, but easiest way right now
        //       might want to make helper for dir entry reader
        let entries: Vec<CommitEntry> = entries
            .iter()
            .map(|path| commit_entry_reader.get_entry(path).unwrap().unwrap())
            .collect();

        log::debug!(
            "commit_from_new_on_remote_branch listing entries {} -> {}",
            old_commit.id,
            entries.len()
        );

        // Get the StagedData ready for commit
        let staged = self.stage_files_for_commit(workspace, &entries)?;
        // Write entries
        let new_commit = self.gen_commit(new_commit, status);
        self.write_commit_to_branch(workspace, branch, &new_commit, &staged)?;

        // Get the commit from the db post insert - it will now have the updated root hash
        let commit = CommitReader::new(&self.repository)?
            .get_commit_by_id(&new_commit.id)?
            .unwrap();

        Ok(commit)
    }

    pub fn stage_files_for_commit(
        &self,
        workspace: &Workspace,
        entries: &[CommitEntry],
    ) -> Result<StagedData, OxenError> {
        let commit = &workspace.commit;
        let workspace_dir = workspace.dir();

        log::debug!("apply_mods CommitWriter Apply {} mods", entries.len());
        for entry in entries.iter() {
            log::debug!("apply_mods entry: {:?}", entry);
            // Copy the version file to the staging dir and make the mods
            let version_path = util::fs::version_path(&self.repository, entry);
            let entry_path = workspace_dir.join(&entry.path);
            if let Some(parent) = entry_path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            log::debug!(
                "apply_mods Copy file to mod {:?} -> {:?}",
                version_path,
                entry_path
            );

            if util::fs::is_tabular(&entry_path) {
                if repositories::workspaces::data_frames::is_behind(workspace, &entry.path)? {
                    return Err(OxenError::basic_str(format!(
                        "Could not commit, remote staging area is out of date with commit {}",
                        commit.id
                    )));
                }

                log::debug!("apply_mods extracting dataset to working dir");
                workspaces::data_frames::extract_to_working_dir(workspace, entry)?;

                log::debug!("apply_mods unstaging");
                workspaces::data_frames::unstage(workspace, &entry.path)?;
            } else {
                log::debug!("apply_mods copying non-tabular file");
                // Non-tabular files are copied from their version path into the working dir
                util::fs::copy(&version_path, &entry_path)?;
            }

            workspaces::files::add(workspace, &entry_path)?;
            log::debug!("Staged file {:?}", entry_path);
        }

        // Have to recompute staged data
        log::debug!("recomputing status for workspace...");
        let root_path = PathBuf::from("");
        let staged_data = index::workspaces::stager::status(workspace, &root_path)?;

        Ok(staged_data)
    }

    fn gen_commit(&self, commit_data: &NewCommit, status: &StagedData) -> Commit {
        log::debug!("gen_commit from {} files", status.staged_files.len());
        let entries: Vec<StagedEntry> = status.staged_files.values().cloned().collect();
        let id = util::hasher::compute_commit_hash(commit_data, &entries);
        log::debug!("gen_commit id {}", id);
        Commit::from_new_and_id(commit_data, id)
    }

    // For server-generated merge commits
    pub fn commit_with_parent_ids_on_branch(
        &self,
        status: &StagedData,
        parent_ids: Vec<String>,
        message: &str,
        branch: Branch,
        cfg: UserConfig,
    ) -> Result<Commit, OxenError> {
        let timestamp = OffsetDateTime::now_utc();

        let commit = NewCommit {
            parent_ids,
            message: String::from(message),
            author: cfg.name,
            email: cfg.email,
            timestamp,
        };

        let entries: Vec<StagedEntry> = status.staged_files.values().cloned().collect();
        let id = util::hasher::compute_commit_hash(&commit, &entries);
        let commit = Commit::from_new_and_id(&commit, id);
        log::debug!("adding commit from status on local branch");
        self.add_commit_from_status_on_local_branch(
            &self.repository.path,
            &commit,
            status,
            branch,
        )?;
        Ok(commit)
    }

    pub fn commit_with_parent_ids(
        &self,
        status: &StagedData,
        parent_ids: Vec<String>,
        message: &str,
    ) -> Result<Commit, OxenError> {
        let cfg = UserConfig::get()?;
        let timestamp = OffsetDateTime::now_utc();

        let commit = NewCommit {
            parent_ids,
            message: String::from(message),
            author: cfg.name,
            email: cfg.email,
            timestamp,
        };
        let entries: Vec<StagedEntry> = status.staged_files.values().cloned().collect();
        let id = util::hasher::compute_commit_hash(&commit, &entries);
        let commit = Commit::from_new_and_id(&commit, id);
        self.add_commit_from_status(&commit, status, &self.repository.path)?;
        Ok(commit)
    }

    pub fn add_commit_from_empty_status(&self, commit: &Commit) -> Result<(), OxenError> {
        // Empty Status
        let status = StagedData::empty();
        let _commit = self.add_commit_from_status(commit, &status, &self.repository.path)?;
        Ok(())
    }

    fn add_commit_from_status_on_local_branch(
        &self,
        working_dir: impl AsRef<Path>,
        commit: &Commit,
        status: &StagedData,
        branch: Branch,
    ) -> Result<Commit, OxenError> {
        // Write entries
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;

        // Commit all staged files from db
        entry_writer.commit_staged_entries(working_dir, commit, status)?;

        // Add to commits db id -> commit_json
        log::debug!(
            "add_commit_from_status_on_local_branch add commit [{}] to db",
            commit.id
        );

        let mut commit = commit.clone();

        let dir_hashes_db =
            CommitEntryWriter::commit_dir_hash_db(&self.repository.path, &commit.id);
        let opts = db::key_val::opts::default();
        let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, dir_hashes_db, false)?;

        // Get the hash for this commit id
        let hash: String = path_db::get_entry(&dir_hashes_db, PathBuf::from(""))?.unwrap();

        commit.update_root_hash(hash.clone());

        self.add_commit_to_db(&commit)?;

        let ref_writer = RefWriter::new(&self.repository)?;
        log::debug!("setting branch commit id {} -> {}", branch.name, commit.id);
        ref_writer.set_branch_commit_id(&branch.name, &commit.id)?;

        Ok(commit)
    }

    pub fn add_commit_from_status(
        &self,
        commit: &Commit,
        status: &StagedData,
        working_dir: &Path,
    ) -> Result<Commit, OxenError> {
        // Make sure all the added and modified files exist
        log::debug!("Removing non-existent files from status");
        let mut status = status.clone();
        let status_clone = status.clone();
        for (path, entry) in status_clone.staged_files.iter() {
            match entry.status {
                StagedEntryStatus::Added => {
                    let full_path = self.repository.path.join(path);
                    if !full_path.exists() {
                        status.staged_files.remove(&path.clone());
                    }
                }
                StagedEntryStatus::Modified => {
                    let full_path = self.repository.path.join(path);
                    if !full_path.exists() {
                        status.staged_files.remove(&path.clone());
                    }
                }
                _ => continue,
            }
        }

        // Write entries
        log::debug!("init'ing CommitEntryWriter");
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;
        // Commit all staged files from db
        entry_writer.commit_staged_entries(working_dir, commit, &status)?;

        // Add to commits db id -> commit_json
        log::debug!("add_commit_from_status add commit [{}] to db", commit.id);

        let mut commit = commit.clone();

        let dir_hashes_db =
            CommitEntryWriter::commit_dir_hash_db(&self.repository.path, &commit.id);
        let opts = db::key_val::opts::default();
        let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, dir_hashes_db, false)?;

        // Get the hash for this commit id
        let hash: String = path_db::get_entry(&dir_hashes_db, PathBuf::from(""))?.unwrap();

        commit.update_root_hash(hash.clone());

        self.add_commit_to_db(&commit)?;

        let ref_writer = RefWriter::new(&self.repository)?;
        ref_writer.set_head_commit_id(&commit.id)?;

        Ok(commit)
    }

    // add_commit_from_status_on_remote_branch
    fn write_commit_to_branch(
        &self,
        workspace: &Workspace,
        branch: &Branch,
        commit: &Commit,
        status: &StagedData,
    ) -> Result<(), OxenError> {
        log::debug!(
            "add from status on remote branch has repository path {:?}",
            self.repository.path
        );
        // Write entries
        let entry_writer = CommitEntryWriter::new(&self.repository, commit)?;

        // Commit all staged files from db
        entry_writer.commit_staged_entries(&workspace.workspace_repo.path, commit, status)?;

        let mut commit = commit.clone();

        let opts = db::key_val::opts::default();
        let dir_hashes_db_dir =
            CommitEntryWriter::commit_dir_hash_db(&self.repository.path, &commit.id);
        let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, dir_hashes_db_dir, false)?;

        let hash: String = path_db::get_entry(&dir_hashes_db, "")?.unwrap();
        commit.update_root_hash(hash.clone());

        // Add to commits db id -> commit_json
        self.add_commit_to_db(&commit)?;

        let ref_writer = RefWriter::new(&self.repository)?;
        log::debug!(
            "add_commit_from_status got branch {} updating branch commit id {}",
            branch.name,
            commit.id
        );
        ref_writer.set_branch_commit_id(&branch.name, &commit.id)?;

        Ok(())
    }

    pub fn add_commit_to_db(&self, commit: &Commit) -> Result<(), OxenError> {
        // Write commit json to db
        let commit_json = serde_json::to_string(&commit)?;
        log::debug!("add_commit_to_db [{}] -> {}", commit.id, commit_json);
        self.commits_db.put(&commit.id, commit_json.as_bytes())?;
        Ok(())
    }

    // TODO: rethink this logic and make it cleaner
    pub async fn set_working_repo_to_commit(&self, commit: &Commit) -> Result<(), OxenError> {
        let head_commit = CommitDBReader::head_commit(&self.repository, &self.commits_db)?;
        if head_commit.id == commit.id {
            log::debug!(
                "set_working_repo_to_commit_id, do nothing... head commit == commit_id {}",
                commit.id
            );

            // Don't do anything if we tried to switch to same commit
            return Ok(());
        }

        // Iterate over files in that are in *current head* and make sure they should all be there
        // if they aren't in commit db we are switching to, remove them
        // Safe to unwrap because we check if it exists above
        log::debug!(
            "set_working_repo_to_commit_id: Commit: {} => '{}'",
            commit.id,
            commit.message
        );

        // Two readers, one for HEAD and one for this current commit
        let head_entry_reader = CommitEntryReader::new_from_head(&self.repository)?;
        let head_entries = head_entry_reader.list_files()?;
        log::debug!(
            "set_working_repo_to_commit_id got {} entries in commit",
            head_entries.len()
        );

        // Keep track of directories, since we do not explicitly store which ones are tracked...
        // we will remove them later if no files exist in them.
        let mut candidate_dirs_to_rm = self.cleanup_removed_files(&commit.id, &head_entries)?;

        log::debug!("Setting working directory to {}", commit.id);
        log::debug!("got {} candidate dirs", candidate_dirs_to_rm.len());

        // If we do not have the commit entry db locally, fetch it
        let path = CommitEntryReader::db_path(&self.repository.path, &commit.id);
        if !path.exists() {
            log::debug!(
                "set_working_repo_to_commit_id: Commit entry db does not exist, fetching..."
            );

            // Pull the commit entry db and the data objects
            let indexer = EntryIndexer::new(&self.repository)?;
            indexer.pull_commit(commit).await?;
        }

        // Iterate over files in current commit db, and make sure the hashes match,
        // if different, copy the correct version over
        let commit_entry_reader = CommitEntryReader::new(&self.repository, commit)?;
        let commit_entries = commit_entry_reader.list_entries()?;

        let opts = db::key_val::opts::default();
        let files_db = CommitEntryWriter::files_db_dir(&self.repository);
        let files_db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open(&opts, dunce::simplified(&files_db))?;

        self.restore_missing_files(
            &commit.id,
            &commit_entries,
            &mut candidate_dirs_to_rm,
            &files_db,
        )?;

        log::debug!("candidate_dirs_to_rm {}", candidate_dirs_to_rm.len());
        if !candidate_dirs_to_rm.is_empty() {
            println!("Cleaning up...");
        }

        // Remove un-tracked directories
        for dir in candidate_dirs_to_rm.iter() {
            let full_dir = self.repository.path.join(dir);
            log::debug!(
                "set_working_repo_to_commit_id remove dis dir {:?}",
                full_dir
            );
            if full_dir.exists() {
                util::fs::remove_dir_all(full_dir)?;
            }
        }

        Ok(())
    }

    fn restore_missing_files(
        &self,
        commit_id: &str,
        entries: &[CommitEntry],
        candidate_dirs_to_rm: &mut HashSet<PathBuf>,
        files_db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        println!("Setting working directory to {commit_id}");
        let size: u64 = unsafe { std::mem::transmute(entries.len()) };
        let bar = oxen_progress_bar(size, ProgressBarType::Counter);

        let dir_entries = self.group_entries_to_dirs(entries);

        // TODO: don't need to group to dirs anymore
        for (_dir, entries) in dir_entries.iter() {
            for entry in entries.iter() {
                bar.inc(1);
                let path = &entry.path;
                log::debug!("Checking committed entry: {:?} => {:?}", path, entry);

                let dst_path = self.repository.path.join(path);
                let version_path = util::fs::version_path(&self.repository, entry);

                // If we do not have the file, restore it from our versioned history
                if !dst_path.exists() {
                    log::debug!(
                        "set_working_repo_to_commit_id restore file [{:?}] she new 🙏 {:?} -> {:?}",
                        entry.path,
                        version_path,
                        dst_path
                    );

                    // mkdir if not exists for the parent
                    if let Some(parent) = dst_path.parent() {
                        if !parent.exists() {
                            match std::fs::create_dir_all(parent) {
                                Ok(_) => {}
                                Err(err) => {
                                    log::error!("Error creating directory: {}", err);
                                }
                            }
                        }
                    }

                    match index::restore::restore_file_with_metadata(
                        &self.repository,
                        &entry.path,
                        entry,
                        files_db,
                    ) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!("Error restoring file: {}", err);
                        }
                    }
                } else {
                    // we do have it, check if we need to update it
                    let dst_hash =
                        util::hasher::hash_file_contents(&dst_path).expect("Could not hash file");

                    // let old_contents = util::fs::read_from_path(&version_path)?;
                    // let current_contents = util::fs::read_from_path(&dst_path)?;
                    // log::debug!("old_contents {:?}\n{}", version_path, old_contents);
                    // log::debug!("current_contents {:?}\n{}", dst_path, current_contents);

                    // If the hash of the file from the commit is different than the one on disk, update it
                    if entry.hash != dst_hash {
                        // we need to update working dir
                        log::debug!(
                            "set_working_repo_to_commit_id restore file diff hash 🙏 {:?} -> {:?}",
                            version_path,
                            dst_path
                        );

                        match index::restore::restore_file_with_metadata(
                            &self.repository,
                            &entry.path,
                            entry,
                            files_db,
                        ) {
                            Ok(_) => {}
                            Err(err) => {
                                log::error!("Error restoring file: {}", err);
                            }
                        }
                    } else {
                        log::debug!(
                            "set_working_repo_to_commit_id hashes match! {:?} -> {:?}",
                            version_path,
                            dst_path
                        );
                    }
                }

                if let Some(parent) = path.parent() {
                    // Check if parent directory exists, if it does, we no longer have
                    // it as a candidate to remove
                    if candidate_dirs_to_rm.contains(parent) {
                        log::debug!("We aren't going to delete candidate {:?}", parent);
                        candidate_dirs_to_rm.remove(parent);
                    }
                }
            }
        }
        bar.finish();
        Ok(())
    }

    fn cleanup_removed_files(
        &self,
        commit_id: &str,
        paths: &[PathBuf],
    ) -> Result<HashSet<PathBuf>, OxenError> {
        println!("Checking index...");
        let size: u64 = unsafe { std::mem::transmute(paths.len()) };
        let bar = oxen_progress_bar(size, ProgressBarType::Counter);

        let mut candidate_dirs_to_rm: HashSet<PathBuf> = HashSet::new();

        let dirs_to_paths = self.group_paths_to_dirs(paths);

        let object_reader = ObjectDBReader::new(&self.repository, commit_id)?;
        for (dir, paths) in dirs_to_paths.iter() {
            let entry_reader =
                CommitDirEntryReader::new(&self.repository, commit_id, dir, object_reader.clone())?;
            for path in paths.iter() {
                let full_path = self.repository.path.join(path);
                log::debug!(
                    "set_working_repo_to_commit_id commit_entries[{:?}]",
                    full_path
                );
                if full_path.is_file() {
                    log::debug!("set_working_repo_to_commit_id path {:?}", path);

                    // TODO: Why are we doing...parent.parent here?
                    // Keep track of parents to see if we clear them
                    if let Some(parent) = path.parent() {
                        log::debug!("adding candidate dir {:?}", parent);

                        if parent.parent().is_some() {
                            // only add one directory below top level
                            // println!("set_working_repo_to_commit_id candidate dir {:?}", parent);
                            candidate_dirs_to_rm.insert(parent.to_path_buf());
                        }
                    }

                    if entry_reader.has_file(path) {
                        // We already have file ✅
                        log::debug!(
                            "set_working_repo_to_commit_id we already have file ✅ {:?}",
                            full_path
                        );
                    } else {
                        // sorry, we don't know you, bye
                        log::debug!("set_working_repo_to_commit_id see ya 💀 {:?}", full_path);
                        if full_path.exists() {
                            util::fs::remove_file(full_path)?;
                        }
                    }
                }
                bar.inc(1);
            }
        }
        bar.finish();

        Ok(candidate_dirs_to_rm)
    }

    fn group_entries_to_dirs(&self, entries: &[CommitEntry]) -> HashMap<PathBuf, Vec<CommitEntry>> {
        let mut results: HashMap<PathBuf, Vec<CommitEntry>> = HashMap::new();

        for entry in entries.iter() {
            if let Some(parent) = entry.path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push(entry.to_owned());
            }
        }

        results
    }

    fn group_paths_to_dirs(&self, files: &[PathBuf]) -> HashMap<PathBuf, Vec<PathBuf>> {
        let mut results: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();

        for path in files.iter() {
            if let Some(parent) = path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_default()
                    .push(path.to_owned());
            }
        }

        results
    }

    pub fn get_commit_by_id(&self, commit_id: &str) -> Result<Option<Commit>, OxenError> {
        // Check if the id is in the DB
        let key = commit_id.as_bytes();
        match self.commits_db.get(key) {
            Ok(Some(value)) => {
                let commit: Commit = serde_json::from_str(str::from_utf8(&value)?)?;
                Ok(Some(commit))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("Error commits_db to find commit_id {commit_id:?}\nErr: {err}");
                Err(OxenError::basic_str(err))
            }
        }
    }
}
