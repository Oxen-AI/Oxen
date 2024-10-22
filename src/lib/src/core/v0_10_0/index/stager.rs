//! # Stager
//!
//! Struct responsible for interacting with the staged data before commit
//! Adds files during `oxen add` and computes files for `oxen status`
//!

use crate::constants;
use crate::core::db;
use crate::core::db::key_val::path_db;
use crate::core::db::key_val::str_json_db;
use crate::core::df::tabular;
use crate::core::merge::entry_merge_conflict_reader::EntryMergeConflictReader;
use crate::core::oxenignore;
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::ObjectDBReader;
use crate::core::v0_10_0::index::SchemaReader;
use crate::core::v0_10_0::index::{
    CommitDirEntryReader, CommitEntryReader, CommitReader, Merger, StagedDirEntryDB,
};
use crate::error::OxenError;
use crate::model::data_frame::schema::staged_schema;
use crate::model::data_frame::schema::staged_schema::StagedSchema;
use crate::model::entry::entry_status::EntryStatus;
use crate::repositories;

use indicatif::ProgressStyle;
use jwalk::WalkDirGeneric;

use crate::opts::DFOpts;

use crate::model::data_frame::schema;
use crate::model::{
    CommitEntry, EntryMergeConflict, LocalRepository, StagedData, StagedDirStats, StagedEntry,
    StagedEntryStatus,
};
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, oxen_progress_bar_with_msg, ProgressBarType};

use filetime::FileTime;
use ignore::gitignore::Gitignore;
use indicatif::ProgressBar;
use rayon::prelude::*;
use rocksdb::SingleThreaded;
use rocksdb::ThreadMode;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::StagedDirEntryReader;

pub struct Stager {
    dir_db: DBWithThreadMode<MultiThreaded>,
    schemas_db: DBWithThreadMode<MultiThreaded>,
    pub repository: LocalRepository,
    merger: Option<Merger>,
}

impl Stager {
    pub fn dirs_db_path(path: &Path) -> Result<PathBuf, OxenError> {
        let path = util::fs::oxen_hidden_dir(path)
            .join(Path::new(constants::STAGED_DIR))
            .join(constants::DIRS_DIR);

        log::debug!("Stager new dir dir_db_path {:?}", path);
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        Ok(path)
    }

    pub fn schemas_db_path(path: &Path) -> Result<PathBuf, OxenError> {
        let path = util::fs::oxen_hidden_dir(path)
            .join(Path::new(constants::STAGED_DIR))
            .join(constants::SCHEMAS_DIR);
        log::debug!("Stager new dir schemas_db_path {:?}", path);
        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }
        Ok(path)
    }

    pub fn new(repository: &LocalRepository) -> Result<Stager, OxenError> {
        let dir_db_path = Stager::dirs_db_path(&repository.path)?;
        let schemas_db_path = Stager::schemas_db_path(&repository.path)?;

        let opts = db::key_val::opts::default();
        Ok(Stager {
            dir_db: DBWithThreadMode::open(&opts, dunce::simplified(&dir_db_path))?,
            schemas_db: DBWithThreadMode::open(&opts, dunce::simplified(&schemas_db_path))?,
            repository: repository.clone(),
            merger: None,
        })
    }

    pub fn new_with_merge(repository: &LocalRepository) -> Result<Stager, OxenError> {
        let dir_db_path = Stager::dirs_db_path(&repository.path)?;
        let schemas_db_path = Stager::schemas_db_path(&repository.path)?;

        let opts = db::key_val::opts::default();
        Ok(Stager {
            dir_db: DBWithThreadMode::open(&opts, dunce::simplified(&dir_db_path))?,
            schemas_db: DBWithThreadMode::open(&opts, dunce::simplified(&schemas_db_path))?,
            repository: repository.clone(),
            merger: Some(Merger::new(&repository.clone())?),
        })
    }

    fn should_ignore_path(&self, ignore: &Option<Gitignore>, path: &Path) -> bool {
        // If the path is the .oxen dir or is in the ignore file, ignore it
        let should_ignore = if let Some(ignore) = ignore {
            ignore.matched(path, path.is_dir()).is_ignore()
        } else {
            false
        };

        should_ignore || util::fs::is_in_oxen_hidden_dir(path)
    }

    pub fn add(
        &self,
        path: &Path,
        commit_reader: &CommitEntryReader,
        schema_reader: &SchemaReader,
        ignore: &Option<Gitignore>,
    ) -> Result<(), OxenError> {
        if self.repository.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
        }

        if self.should_ignore_path(ignore, path) {
            return Ok(());
        }

        log::debug!("stager.add({:?})", path);

        // Be able to add the current dir
        if path == Path::new(".") {
            for entry in (std::fs::read_dir(path)?).flatten() {
                let path = entry.path();
                let entry_path = self.repository.path.join(path);
                self.add(&entry_path, commit_reader, schema_reader, ignore)?;
            }
            log::debug!("ADD CURRENT DIR: {:?}", path);
            return Ok(());
        }

        if !path.exists() {
            self.process_removed_file_or_dir(path, commit_reader)?;
            return Ok(());
        }

        log::debug!("Stager.add() is_dir? {} path: {:?}", path.is_dir(), path);
        if path.is_dir() {
            match self.add_dir(path, commit_reader) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            match self.add_file(path, commit_reader, schema_reader) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    pub fn status(&self, entry_reader: &CommitEntryReader) -> Result<StagedData, OxenError> {
        log::debug!("-----status START-----");

        let result = self.compute_staged_data(&self.repository.path, entry_reader);
        log::debug!("-----status END-----");
        result
    }

    pub fn status_without_untracked(
        &self,
        entry_reader: &CommitEntryReader,
    ) -> Result<StagedData, OxenError> {
        log::debug!("-----status_without_untracked START-----");
        let result = self.staged_data_without_untracked(&self.repository.path, entry_reader);
        log::debug!("-----status_without_untracked END-----");
        result
    }

    // TODO: allow status for just certain type of files (add, mod, removed, etc) for performance gains

    // TODO: allow status for a certain directory for performance gains

    pub fn status_from_dir(
        &self,
        entry_reader: &CommitEntryReader,
        dir: impl AsRef<Path>,
    ) -> Result<StagedData, OxenError> {
        let dir = dir.as_ref();
        log::debug!("-----status_from_dir START-----");
        let result = self.compute_staged_data(dir, entry_reader);
        log::debug!("-----status_from_dir END-----");
        result
    }

    fn list_merge_conflicts(&self) -> Result<Vec<EntryMergeConflict>, OxenError> {
        let merger = EntryMergeConflictReader::new(&self.repository)?;
        merger.list_conflicts()
    }

    // This should only be used for creating commits, when we want a faster way to get
    // staged data directly from the dbs without parsing untracked files
    pub fn staged_data_without_untracked(
        &self,
        dir: &Path,
        entry_reader: &CommitEntryReader,
    ) -> Result<StagedData, OxenError> {
        let mut staged_data = StagedData::empty();
        let ignore = oxenignore::create(&self.repository);

        let mut staged_dirs = self.list_staged_dirs()?;
        if dir.is_relative() && dir != self.repository.path {
            staged_dirs.retain(|(path, _)| path.starts_with(dir))
        }

        let mut candidate_dirs: HashSet<PathBuf> = HashSet::new();
        for (dir, status) in &staged_dirs {
            let full_path = self.repository.path.join(dir);
            let stats = self.compute_staged_dir_stats(&full_path, status)?;
            staged_data.staged_dirs.add_stats(&stats);
            candidate_dirs.insert(full_path);
        }

        let object_reader = get_object_reader(&self.repository, &entry_reader.commit_id)?;
        log::debug!(
            "about to call process_dir untracked on dirs {:?}",
            candidate_dirs
        );
        for dir in candidate_dirs.iter() {
            self.process_dir_ignoring_untracked(
                dir,
                &mut staged_data,
                &ignore,
                entry_reader,
                object_reader.clone(),
            )?;
        }

        log::debug!("About to list schemas");

        let mut schemas: HashMap<PathBuf, StagedSchema> = HashMap::new();
        for (path, schema) in path_db::list_path_entries(&self.schemas_db, Path::new(""))? {
            schemas.insert(path, schema);
        }

        staged_data.staged_schemas = schemas;

        staged_data.merge_conflicts = self.list_merge_conflicts()?;

        Ok(staged_data)
    }

    fn compute_staged_data(
        &self,
        dir: &Path,
        entry_reader: &CommitEntryReader,
    ) -> Result<StagedData, OxenError> {
        log::debug!(
            "compute_staged_data listing eligible repo -> {:?} dir -> {:?}",
            self.repository.path,
            dir
        );

        if self.repository.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
        }

        // Get the time before doing this

        let mut staged_data = StagedData::empty();
        let ignore = oxenignore::create(&self.repository);

        let mut candidate_dirs: HashSet<PathBuf> = HashSet::new();
        // Start with candidate dirs from committed and added, not all the dirs
        let mut staged_dirs = self.list_staged_dirs()?;
        // If we specified a dir, only get the added dirs that are in that dir
        if dir.is_relative() && dir != self.repository.path {
            staged_dirs.retain(|(path, _)| path.starts_with(dir))
        }

        log::debug!(
            "compute_staged_data Got <added> dirs: {}",
            staged_dirs.len()
        );
        for (dir, status) in staged_dirs {
            log::debug!("compute_staged_data considering added dir {:?}", dir);
            let full_path = self.repository.path.join(&dir);
            let stats = self.compute_staged_dir_stats(&full_path, &status)?;
            staged_data.staged_dirs.add_stats(&stats);
            log::debug!("compute_staged_data got stats {:?}", stats);

            log::debug!("compute_staged_data adding <added> dir {:?}", dir);
            candidate_dirs.insert(self.repository.path.join(dir));
        }
        let mut committed_dirs = entry_reader.list_dirs()?;
        if dir.is_relative() && dir != self.repository.path {
            committed_dirs.retain(|path| path.starts_with(dir))
        }
        log::debug!(
            "compute_staged_data Got <committed> dirs: {}",
            committed_dirs.len()
        );
        for dir in committed_dirs.iter() {
            log::debug!("compute_staged_data adding <committed> dir {:?}", dir);
            if !self.should_ignore_path(&ignore, dir) {
                candidate_dirs.insert(self.repository.path.join(dir));
            }
        }

        log::debug!("compute_staged_data Considering <current> dir: {:?}", dir);
        candidate_dirs.insert(dir.to_path_buf());

        let committer = CommitReader::new(&self.repository)?;
        let commit = committer.head_commit()?;

        let object_reader = get_object_reader(&self.repository, &commit.id)?;
        let entry_reader = CommitEntryReader::new(&self.repository, &commit)?;

        let bar = oxen_progress_bar(0, ProgressBarType::Counter);

        bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} [{elapsed_precise}] [{wide_bar}] {pos}/?")
                .unwrap()
                .progress_chars("🌾🐂➖"),
        );

        for dir in candidate_dirs.iter() {
            // log::debug!("compute_staged_data CANDIDATE DIR {:?}", dir);
            log::debug!("processing dir {:?}", dir);
            self.process_dir(
                dir,
                &mut staged_data,
                &ignore,
                &entry_reader,
                object_reader.clone(),
                bar.clone(),
            )?;
        }

        // Make pairs from Added + Removed stage entries with same hash, store in staged_data.moved_entries
        self.find_moved_files(&mut staged_data)?;

        // Find merge conflicts
        staged_data.merge_conflicts = self.list_merge_conflicts()?;

        // Populate schemas from db
        let mut schemas: HashMap<PathBuf, StagedSchema> = HashMap::new();
        for (path, schema) in path_db::list_path_entries(&self.schemas_db, Path::new(""))? {
            schemas.insert(path, schema);
        }

        staged_data.staged_schemas = schemas;
        Ok(staged_data)
    }

    fn find_moved_files(&self, staged_data: &mut StagedData) -> Result<(), OxenError> {
        let files = staged_data.staged_files.clone();
        let files_vec: Vec<(&PathBuf, &StagedEntry)> = files.iter().collect();

        // Find pairs of added-removed with same hash and add them to moved.
        // We won't mutate StagedEntries here, the "moved" property is read-only
        let mut added_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();
        let mut removed_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();

        for (path, entry) in files_vec.iter() {
            match entry.status {
                StagedEntryStatus::Added => {
                    added_map.entry(entry.hash.clone()).or_default().push(path);
                }
                StagedEntryStatus::Removed => {
                    removed_map
                        .entry(entry.hash.clone())
                        .or_default()
                        .push(path);
                }
                _ => continue,
            }
        }

        for (hash, added_paths) in added_map.iter_mut() {
            if let Some(removed_paths) = removed_map.get_mut(hash) {
                while !added_paths.is_empty() && !removed_paths.is_empty() {
                    if let (Some(added_path), Some(removed_path)) =
                        (added_paths.pop(), removed_paths.pop())
                    {
                        // moved_entries.push((added_path, removed_path, hash.to_string()));
                        staged_data.moved_files.push((
                            added_path.clone(),
                            removed_path.clone(),
                            hash.to_string(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn process_dir_ignoring_untracked(
        &self,
        full_dir: &Path,
        staged_data: &mut StagedData,
        ignore: &Option<Gitignore>,
        _commit_reader: &CommitEntryReader,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<(), OxenError> {
        let committer = CommitReader::new(&self.repository)?;

        let commit = committer.head_commit()?;

        let relative_dir = util::fs::path_relative_to_dir(full_dir, &self.repository.path)?;
        let staged_dir_db: StagedDirEntryDB<MultiThreaded> =
            StagedDirEntryDB::new_read_only(&self.repository, &relative_dir)?;
        let dir_reader =
            CommitDirEntryReader::new(&self.repository, &commit.id, &relative_dir, object_reader)?;

        // List the staged entries in this dir
        let staged_entries = staged_dir_db.list_added_paths()?;

        for relative_path in &staged_entries {
            if self.should_ignore_path(ignore, relative_path) {
                continue;
            }

            let fullpath = self.repository.path.join(relative_path);

            let file_status = Stager::get_file_status(
                &self.repository.path,
                relative_path,
                &staged_dir_db,
                &dir_reader,
            );

            if fullpath.is_dir() {
                continue;
            }

            if let Some(file_type) = file_status {
                match file_type {
                    EntryStatus::Added => {
                        let file_name = relative_path.file_name().unwrap();
                        let result = staged_dir_db.get_entry(file_name);
                        if let Ok(Some(entry)) = result {
                            staged_data
                                .staged_files
                                .insert(relative_path.to_path_buf(), entry);
                        }
                    }
                    EntryStatus::Untracked => {
                        staged_data
                            .untracked_files
                            .push(relative_path.to_path_buf());
                    }
                    EntryStatus::Modified => {
                        staged_data
                            .modified_files
                            .insert(relative_path.to_path_buf());
                    }
                    EntryStatus::Removed => {
                        staged_data
                            .removed_files
                            .insert(relative_path.to_path_buf());
                    }
                }
            }
        }

        Ok(())
    }

    fn process_dir(
        &self,
        full_dir: &Path,
        staged_data: &mut StagedData,
        ignore: &Option<Gitignore>,
        commit_reader: &CommitEntryReader,
        object_reader: Arc<ObjectDBReader>,
        bar: Arc<ProgressBar>,
    ) -> Result<(), OxenError> {
        // log::debug!("process_dir {:?}", full_dir);
        log::debug!("calling process_dir on {:?}", full_dir);
        // Only check at level of this dir, no need to deep dive recursively
        let committer = CommitReader::new(&self.repository)?;
        let commit = committer.head_commit()?;
        let relative_dir = util::fs::path_relative_to_dir(full_dir, &self.repository.path)?;
        let staged_dir_db: StagedDirEntryDB<SingleThreaded> =
            StagedDirEntryDB::new(&self.repository, &relative_dir)?;

        let root_commit_dir_reader =
            CommitDirEntryReader::new(&self.repository, &commit.id, &relative_dir, object_reader)?;

        // get seconds and millis

        // Create candidate files paths to look at
        let mut candidate_files: HashSet<PathBuf> = HashSet::new();

        // Only consider working dir if it is on disk, otherwise we will grab from history
        let read_dir = std::fs::read_dir(full_dir);
        if read_dir.is_ok() {
            // Files in working directory as candidates
            for path in read_dir? {
                let path = path?.path();
                let path = util::fs::path_relative_to_dir(&path, &self.repository.path)?;
                if !self.should_ignore_path(ignore, &path) {
                    // log::debug!("adding candidate from dir {:?}", path);
                    candidate_files.insert(path);
                }
            }
        }

        // and files that were in commit as candidates
        for entry in root_commit_dir_reader.list_entries()? {
            // log::debug!("adding candidate from commit {:?}", entry.path);
            if !self.should_ignore_path(ignore, &entry.path) {
                candidate_files.insert(entry.path);
            }
        }
        log::debug!(
            "Got {} candidates in directory {:?}",
            candidate_files.len(),
            relative_dir
        );

        if let Some(combined_changes) = candidate_files
            .par_iter()
            .map(|relative| {
                let mut local_staged_data = StagedData::empty();
                log::debug!("process_dir checking relative path {:?}", relative);
                if util::fs::is_in_oxen_hidden_dir(relative) {
                    return local_staged_data;
                }

                let fullpath = self.repository.path.join(relative);

                log::debug!(
                    "process_dir checking is_dir? {} {:?}",
                    fullpath.is_dir(),
                    fullpath
                );

                if fullpath.is_dir() {
                    if !self.has_staged_dir(relative)
                        && !staged_data.staged_dirs.contains_key(relative)
                        && !commit_reader.has_dir(relative)
                    {
                        log::debug!("process_dir adding untracked dir {:?}", relative);
                        let count = util::fs::count_items_in_dir(&fullpath);
                        local_staged_data
                            .untracked_dirs
                            .push((relative.to_path_buf(), count));
                    }
                    return local_staged_data;
                } else {
                    // is file
                    let file_status = Stager::get_file_status(
                        &self.repository.path,
                        relative,
                        &staged_dir_db,
                        &root_commit_dir_reader,
                    );
                    log::debug!("process_dir got status {:?} {:?}", relative, file_status);
                    if let Some(file_type) = file_status {
                        match file_type {
                            EntryStatus::Added => {
                                let file_name = relative.file_name().unwrap();
                                let result = staged_dir_db.get_entry(file_name);
                                if let Ok(Some(entry)) = result {
                                    local_staged_data
                                        .staged_files
                                        .insert(relative.to_path_buf(), entry);
                                }
                            }
                            EntryStatus::Untracked => {
                                local_staged_data
                                    .untracked_files
                                    .push(relative.to_path_buf());
                            }
                            EntryStatus::Modified => {
                                local_staged_data
                                    .modified_files
                                    .insert(relative.to_path_buf());
                            }
                            EntryStatus::Removed => {
                                local_staged_data
                                    .removed_files
                                    .insert(relative.to_path_buf());
                            }
                        }
                    }
                    bar.inc(1);
                }
                local_staged_data
            })
            .reduce_with(|mut a, b| {
                a.untracked_dirs.extend(b.untracked_dirs);
                a.untracked_files.extend(b.untracked_files);
                a.modified_files.extend(b.modified_files);
                a.removed_files.extend(b.removed_files);
                a.staged_files.extend(b.staged_files);

                a
            })
        {
            staged_data
                .untracked_dirs
                .extend(combined_changes.untracked_dirs);
            staged_data
                .untracked_files
                .extend(combined_changes.untracked_files);
            staged_data
                .modified_files
                .extend(combined_changes.modified_files);
            staged_data
                .removed_files
                .extend(combined_changes.removed_files);
            staged_data
                .staged_files
                .extend(combined_changes.staged_files);
        }
        Ok(())
    }

    fn get_file_status<T: ThreadMode>(
        full_dir: &Path,
        path: &Path,
        staged_dir_db: &StagedDirEntryDB<T>,
        commit_dir_db: &CommitDirEntryReader,
    ) -> Option<EntryStatus> {
        let file_name = path.file_name().unwrap();
        // log::debug!("get_file_status check path in staging? {:?}", file_name);

        // Have to check the file basename not the path, because basenames are stored in each dir
        if staged_dir_db.has_entry(file_name) {
            return Some(EntryStatus::Added);
        } else {
            // Not in the staged DB
            // log::debug!("get_file_status check if commit db? {:?}", file_name);
            // check if it is in the HEAD commit to see if it is modified or removed
            if let Some(file_name) = path.file_name() {
                if let Ok(Some(commit_entry)) = commit_dir_db.get_entry(file_name) {
                    if Stager::file_is_removed(full_dir, &commit_entry) {
                        return Some(EntryStatus::Removed);
                    } else if Stager::file_is_modified(full_dir, &commit_entry) {
                        return Some(EntryStatus::Modified);
                    }
                } else {
                    return Some(EntryStatus::Untracked);
                }
            }
        }

        None
    }

    fn file_is_removed(repo_path: &Path, commit_entry: &CommitEntry) -> bool {
        let full_path = repo_path.join(&commit_entry.path);
        // log::debug!(
        //     "CHECKING REMOVED {:?} -> {:?}",
        //     repo_path,
        //     commit_entry.path
        // );
        // log::debug!("CHECKING REMOVED {:?}", full_path);
        !full_path.exists()
    }

    fn file_is_modified(repo_path: &Path, commit_entry: &CommitEntry) -> bool {
        // Get last modified time
        let full_path = repo_path.join(&commit_entry.path);
        // log::debug!(
        //     "CHECKING MODIFIED {:?} -> {:?}",
        //     repo_path,
        //     commit_entry.path
        // );
        // log::debug!("CHECKING MODIFIED {:?}", full_path);

        if !full_path.exists() {
            // might have been removed
            return false;
        }

        let metadata = fs::metadata(&full_path).unwrap();
        let mtime = FileTime::from_last_modification_time(&metadata);

        // log::debug!(
        //     "file_is_modified comparing timestamps: {} to {}",
        //     commit_entry.last_modified_nanoseconds,
        //     mtime.nanoseconds()
        // );

        if commit_entry.has_different_modification_time(&mtime) {
            // log::debug!(
            //     "file_is_modified modification times are different! {:?}",
            //     full_path
            // );

            // Then check the hashes, because the data might not be different, timestamp is just an optimization
            let hash = util::hasher::hash_file_contents(&full_path).unwrap();
            if hash != commit_entry.hash {
                return true;
            }
        }

        false
    }

    pub fn has_staged_file(&self, path: &Path) -> Result<bool, OxenError> {
        if let (Some(parent), Some(filename)) = (path.parent(), path.file_name()) {
            let reader = StagedDirEntryReader::new(&self.repository, parent)?;
            Ok(reader.has_entry(filename))
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    pub fn remove_staged_file(&self, path: &Path) -> Result<(), OxenError> {
        log::debug!("remove_staged_file {:?}", path);
        if let (Some(parent), Some(filename)) = (path.parent(), path.file_name()) {
            log::debug!(
                "remove_staged_file got filename {:?} and parent {:?}",
                filename,
                parent
            );

            let staged_dir: StagedDirEntryDB<SingleThreaded> =
                StagedDirEntryDB::new(&self.repository, parent)?;
            if util::fs::is_tabular(path) {
                log::debug!("remove_staged_file {:?} is tabular, removing schema", path);
                self.remove_staged_schema(path)?;
            }

            staged_dir.remove_path(filename)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn remove_staged_schema(&self, path: &Path) -> Result<(), OxenError> {
        log::debug!("remove_staged_schema {:?}", path);
        path_db::delete(&self.schemas_db, path)?;
        Ok(())
    }

    // TODO: can this parent-tracing logic be combined with the one in add_staged_entry_to_db?
    fn add_removed_file(
        &self,
        path: &Path,
        entry: &CommitEntry,
        staged_dir_db: &StagedDirEntryDB<MultiThreaded>,
    ) -> Result<StagedEntry, OxenError> {
        log::debug!("add_removed_file {:?}", path);

        let relative = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        if let Some(_file_name) = relative.file_name() {
            // add all parents up to root
            let mut components = path.components().collect::<Vec<_>>();
            log::debug!("add_staged_entry_to_db got components {}", components.len());
            while !components.is_empty() {
                if let Some(_component) = components.pop() {
                    let parent: PathBuf = components.iter().collect();
                    log::debug!("add_staged_entry_to_db got parent {:?}", parent);
                    log::debug!("add_staged_entry_to_db adding parent {:?}", parent);
                    path_db::put(&self.dir_db, parent, &StagedEntryStatus::Added)?;
                }
            }
        }

        if let (Some(parent), Some(filename)) = (path.parent(), path.file_name()) {
            log::debug!(
                "add_removed_file got filename {:?} and parent {:?}",
                filename,
                parent
            );

            // add parent to staged dir db
            let short_path = util::fs::path_relative_to_dir(parent, &self.repository.path)?;
            path_db::put(&self.dir_db, short_path, &StagedEntryStatus::Removed)?;
            // Also add as removed to staged schema db
            if util::fs::is_tabular(path) {
                // Get schema for this file
                let schema_reader = SchemaReader::new(&self.repository, &entry.commit_id)?;
                let schema = schema_reader.get_schema_for_file(path)?;
                if let Some(schema) = schema {
                    let staged_schema = StagedSchema {
                        status: StagedEntryStatus::Removed,
                        schema,
                    };
                    path_db::put(&self.schemas_db, path, &staged_schema)?;
                }

                log::debug!(
                    "remove_staged_file {:?} is tabular, adding StagedSchema as removed",
                    path
                );
            }

            // TODO: should we check the size of the remaining items in the dir and add it as removed if empty else modified?
            staged_dir_db.add_removed_file(filename, entry)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn process_removed_file(&self, path: &PathBuf, entry: &CommitEntry) -> Result<(), OxenError> {
        if let Some(parent) = path.parent() {
            let staged_dir: StagedDirEntryDB<MultiThreaded> =
                StagedDirEntryDB::new(&self.repository, parent)?;

            self.add_removed_file(path, entry, &staged_dir)?;
            Ok(())
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn process_removed_dir(
        &self,
        path: &Path,
        dir_entries: &HashMap<PathBuf, Vec<CommitEntry>>,
    ) -> Result<(), OxenError> {
        log::debug!(
            "Stager.add() !path.exists() {} dirs in dir {:?}",
            dir_entries.len(),
            path
        );

        // Make sure to remove the dir from the db
        for (path, entries) in dir_entries {
            log::debug!("Stager.add() rm {} files in dir {:?}", entries.len(), path);

            path_db::put(&self.dir_db, path, &StagedEntryStatus::Removed)?;

            if !entries.is_empty() {
                let staged_dir: StagedDirEntryDB<MultiThreaded> =
                    StagedDirEntryDB::new(&self.repository, path)?;

                let bar = oxen_progress_bar(entries.len() as u64, ProgressBarType::Counter);
                println!("Removing {} files", entries.len());

                entries.par_iter().for_each(|entry| {
                    match self.add_removed_file(&entry.path, entry, &staged_dir) {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!("Error adding removed file: {:?}", e);
                        }
                    }
                    bar.inc(1);
                });

                bar.finish_and_clear();
            }
        }
        Ok(())
    }

    fn process_removed_file_or_dir(
        &self,
        path: &Path,
        commit_reader: &CommitEntryReader,
    ) -> Result<(), OxenError> {
        // If it doesn't exist on disk, it might have been removed, and we can't tell if it is a file or dir
        // so we have to check if it is committed, and what the backup version is
        let relative_path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        log::debug!(
            "Stager.add() !path.exists() checking relative path: {:?}",
            relative_path
        );

        // Since entries that are committed are only files.. we will have to have different logic for dirs
        // process_removed_file and process_removed_dir deal with different logic for staged_dir_entry_db path

        // Is a file
        if let Ok(Some(value)) = commit_reader.get_entry(&relative_path) {
            self.process_removed_file(&relative_path, &value)?;
            return Ok(());
        }

        // Is a dir
        let files_in_dir = commit_reader.list_entries_per_directory(&relative_path)?;
        self.process_removed_dir(&relative_path, &files_in_dir)?;

        Ok(())
    }

    // Returns a map of directories to files to add, and a total count
    // Reads the dirs in parallel to quickly find out what needs to be added
    fn list_unstaged_files_in_dir<P: AsRef<Path>>(
        &self,
        dir: P,
    ) -> (HashMap<PathBuf, Vec<PathBuf>>, usize) {
        let mut files: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
        let mut total: usize = 0;
        let repository = self.repository.to_owned();

        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {msg}")
                .unwrap(),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));
        pb.set_message("🐂 Looking for untracked files...".to_string());

        // TODO:
        // * Fix to be more readable
        let walk_dir = WalkDirGeneric::<((), Option<bool>)>::new(&dir)
            .skip_hidden(true)
            .process_read_dir(move |_, parent, _, children| {
                let parent = util::fs::path_relative_to_dir(parent, &repository.path).unwrap();
                let reader = match StagedDirEntryReader::new(&repository, &parent) {
                    Ok(db) => db,
                    Err(err) => {
                        log::error!("Error creating staged dir db: {:?}", err);
                        return;
                    }
                };

                children.par_iter_mut().for_each(|child_result| {
                    match child_result {
                        Ok(child) => {
                            // log::debug!(
                            //     "list_unstaged_files_in_dir checking file type {:?}",
                            //     dir_entry
                            // );
                            if !child.file_type.is_dir() {
                                // Entry is file
                                let path = child.path();
                                let path =
                                    match util::fs::path_relative_to_dir(path, &repository.path) {
                                        Ok(p) => p,
                                        Err(err) => {
                                            log::error!("Error path_relative_to_dir: {:?}", err);
                                            return;
                                        }
                                    };

                                let is_added = reader.has_entry(path);
                                child.client_state = Some(is_added);
                            }
                        }
                        Err(err) => {
                            log::error!("list_unstaged_files_in_dir dir entry is err: {:?}", err);
                        }
                    }
                });
            });

        for dir_entry_result in walk_dir {
            // log::debug!(
            //     "list_unstaged_files_in_dir in for loop {:?}",
            //     dir_entry_result,
            // );
            match dir_entry_result {
                Ok(dir_entry) => {
                    // log::debug!(
                    //     "list_unstaged_files_in_dir match dir_entry_result {:?}",
                    //     &dir_entry.client_state,
                    // );
                    if let Some(is_added) = &dir_entry.client_state {
                        if !*is_added {
                            let path = util::fs::path_relative_to_dir(
                                dir_entry.path(),
                                &self.repository.path,
                            )
                            .unwrap();
                            // log::debug!(
                            //     "list_unstaged_files_in_dir got path {:?}",
                            //     path,
                            // );
                            if let Some(parent) = path.parent() {
                                // log::debug!(
                                //     "list_unstaged_files_in_dir adding {:?} -> {:?}",
                                //     parent,
                                //     path
                                // );

                                files.entry(parent.to_path_buf()).or_default().push(path);
                                total += 1;
                                pb.set_message(format!("🐂 found {} files", total));
                            }
                        }
                    }
                    // log::debug!(
                    //     "list_unstaged_files_in_dir match dir_entry_result done. {:?}",
                    //     dir_entry,
                    // );
                }
                Err(error) => {
                    log::error!("Read dir_entry error: {error}");
                }
            }
        }

        pb.finish_and_clear();
        (files, total)
    }

    pub fn add_dir(&self, dir: &Path, entry_reader: &CommitEntryReader) -> Result<(), OxenError> {
        if !dir.exists() || !dir.is_dir() {
            let err = format!("Cannot stage non-existant dir: {dir:?}");
            return Err(OxenError::basic_str(err));
        }

        // add the the directory to list of dirs we are tracking so that when we find untracked files
        // they are added to the list
        let short_path = util::fs::path_relative_to_dir(dir, &self.repository.path)?;
        path_db::put(&self.dir_db, &short_path, &StagedEntryStatus::Added)?;
        // log::debug!("Stager.add_dir added path {short_path:?}");

        // Add all untracked files and modified files
        let (dir_paths, total) = self.list_unstaged_files_in_dir(dir);
        // log::debug!("Stager.add_dir {:?} -> {}", dir, total);
        let schema_reader = SchemaReader::new(&self.repository, &entry_reader.commit_id)?;
        // println!("Adding files in directory: {short_path:?}");
        let size: u64 = unsafe { std::mem::transmute(total) };
        let msg = format!("Adding directory {short_path:?}");
        let bar = oxen_progress_bar_with_msg(size, msg);
        let object_reader = match get_object_reader(&self.repository, &entry_reader.commit_id) {
            Ok(reader) => reader,
            Err(err) => {
                log::error!("Could not create ObjectDBReader: {}", err);
                return Err(err);
            }
        };
        dir_paths.iter().for_each(|(parent, paths)| {
            let staged_db: StagedDirEntryDB<MultiThreaded> =
                StagedDirEntryDB::new(&self.repository, parent).unwrap();
            let entry_reader = match CommitDirEntryReader::new(
                &self.repository,
                &entry_reader.commit_id,
                parent,
                object_reader.clone(),
            ) {
                Ok(reader) => reader,
                Err(err) => {
                    log::error!("Could not create CommitDirEntryReader: {}", err);
                    return;
                }
            };

            // log::debug!("paths.len() {:?}", paths.len());
            paths.par_iter().for_each(|path| {
                // log::debug!("paths.par_iter().foreach {:?}", path);

                let full_path = self.repository.path.join(path);
                match self.add_staged_entry_in_dir_db(
                    &full_path,
                    &entry_reader,
                    &schema_reader,
                    &staged_db,
                ) {
                    Ok(_) => {
                        // all good
                    }
                    Err(err) => {
                        log::error!("Could not add file: {:?}\nErr: {}", path, err);
                    }
                }
                bar.inc(1);
            });
        });

        bar.finish_and_clear();

        Ok(())
    }

    pub fn has_staged_dir<P: AsRef<Path>>(&self, dir: P) -> bool {
        path_db::has_entry(&self.dir_db, dir)
    }

    pub fn has_entry<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();
        if let Ok(relative) = util::fs::path_relative_to_dir(path, &self.repository.path) {
            if let Some(parent) = relative.parent() {
                if let Ok(staged_dir) = StagedDirEntryReader::new(&self.repository, parent) {
                    let filename = relative.file_name().unwrap().to_str().unwrap();
                    return staged_dir.has_entry(filename);
                } else {
                    log::debug!(
                        "Stager.has_entry({:?}) could not find parent db {:?}",
                        path,
                        parent
                    );
                }
            }
        }
        false
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<StagedEntry>, OxenError> {
        let path = path.as_ref();
        let relative = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        if let Some(parent) = relative.parent() {
            if let Some(file_name) = relative.file_name() {
                log::debug!("get_entry got parent for path {:?} -> {:?}", path, parent);
                log::debug!("get_entry relative {:?}", file_name);

                let staged_db = StagedDirEntryReader::new(&self.repository, parent)?;
                return staged_db.get_entry(file_name);
            } else {
                log::warn!("get_entry could not get file_name: {:?}", path);
            }
        } else {
            log::warn!("get_entry no parent for path: {:?}", path);
        }
        Ok(None)
    }

    pub fn add_file(
        &self,
        path: &Path,
        entry_reader: &CommitEntryReader,
        schema_reader: &SchemaReader,
    ) -> Result<PathBuf, OxenError> {
        log::debug!("--- START OXEN ADD {:?} ---", path);
        let relative = self.add_staged_entry(path, entry_reader, schema_reader)?;

        // We should be tracking changes to this parent dir too
        let path_parent = path.parent();
        if let Some(parent) = path_parent {
            let relative_parent = util::fs::path_relative_to_dir(parent, &self.repository.path)?;
            log::debug!("add_file got parent {:?}", relative_parent);
            if !self.has_entry(&relative_parent) && relative_parent != Path::new("") {
                log::debug!("add_file({:?}) adding parent {:?}", path, relative_parent);
                path_db::put(&self.dir_db, relative_parent, &StagedEntryStatus::Added)?;
            }
        }

        log::debug!("--- END OXEN ADD ({:?}) ---", path);

        Ok(relative)
    }

    pub fn get_staged_schema(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Option<schema::Schema>, OxenError> {
        let path = path.as_ref();
        for (path_str, staged_schema) in
            str_json_db::hash_map::<MultiThreaded, StagedSchema>(&self.schemas_db)?
        {
            if staged_schema.schema.hash == path.to_string_lossy()
                || path_str == path.to_string_lossy()
            {
                return Ok(Some(staged_schema.schema));
            }
        }
        Ok(None)
    }

    pub fn get_staged_schema_by_path(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Option<schema::Schema>, OxenError> {
        let path = path.as_ref();
        let key = path.to_string_lossy();
        str_json_db::get(&self.schemas_db, key)
    }

    pub fn list_staged_schemas(&self) -> Result<HashMap<PathBuf, schema::Schema>, OxenError> {
        log::debug!("listing staged schemas");
        // Ok(
        let results =
            str_json_db::hash_map::<MultiThreaded, staged_schema::StagedSchema>(&self.schemas_db)?
                .into_iter()
                .map(|(p, v)| (PathBuf::from(p), v.schema))
                .collect::<HashMap<PathBuf, schema::Schema>>();
        log::debug!("got results {:?}", results);
        Ok(results)
        // )
    }

    fn add_staged_entry(
        &self,
        path: &Path,
        entry_reader: &CommitEntryReader,
        schema_reader: &SchemaReader,
    ) -> Result<PathBuf, OxenError> {
        log::debug!("add_staged_entry {:?}", path);
        if let Some(parent) = path.parent() {
            let relative_parent = util::fs::path_relative_to_dir(parent, &self.repository.path)?;
            let staged_db: StagedDirEntryDB<MultiThreaded> =
                StagedDirEntryDB::new(&self.repository, &relative_parent)?;
            let object_reader = get_object_reader(&self.repository, &entry_reader.commit_id)?;
            let entry_reader = CommitDirEntryReader::new(
                &self.repository,
                &entry_reader.commit_id,
                &relative_parent,
                object_reader,
            )?;

            self.add_staged_entry_in_dir_db(path, &entry_reader, schema_reader, &staged_db)
        } else {
            log::error!("add_staged_entry no parent... {:?}", path);
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn add_staged_entry_in_dir_db<T: ThreadMode>(
        &self,
        path: &Path,
        entry_reader: &CommitDirEntryReader,
        schema_reader: &SchemaReader,
        staged_db: &StagedDirEntryDB<T>,
    ) -> Result<PathBuf, OxenError> {
        // We should have normalized to path past repo at this point
        log::debug!("Add file: {:?} to {:?}", path, self.repository.path);
        if !path.exists() {
            return Err(OxenError::entry_does_not_exist(path));
        }

        // compute the hash to know if it has changed
        let hash = util::hasher::hash_file_contents(path)?;

        // Key is the filename relative to the repository
        // if repository: /Users/username/Datasets/MyRepo
        //   /Users/username/Datasets/MyRepo/train -> train
        //   /Users/username/Datasets/MyRepo/annotations/train.txt -> annotations/train.txt
        let path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        log::debug!("Add file: got relative path {:?}", path);

        let mut staged_entry = StagedEntry {
            hash: hash.to_owned(),
            status: StagedEntryStatus::Added,
        };

        // Check if it is a merge conflict, then we can add it
        if let Some(merger) = &self.merger {
            if merger.has_file(&path)? {
                log::debug!("add_staged_entry_in_dir_db merger has file! {:?}", path);
                self.add_staged_entry_to_db(&path, &staged_entry, staged_db, schema_reader)?;
                merger.remove_conflict_path(&path)?;
                return Ok(path);
            }
        }

        // Check if file has changed on disk
        // Since we are using a CommitDirEntryReader we need the base file name
        let basename = path.file_name().unwrap().to_str().unwrap();
        if let Ok(Some(entry)) = entry_reader.get_entry(basename) {
            log::debug!(
                "add_staged_entry_in_dir_db comparing hashes {:?} -> {:?}",
                staged_entry,
                entry
            );
            if entry.hash == hash {
                // file has not changed, don't add it
                log::debug!(
                    "add_staged_entry_in_dir_db do not add file, it hasn't changed: {:?}",
                    path
                );
                return Ok(path);
            } else {
                // Hash doesn't match, mark it as modified
                log::debug!(
                    "add_staged_entry_in_dir_db HASH DOESN'T MATCH {:?}",
                    entry.path
                );
                staged_entry.status = StagedEntryStatus::Modified;
            }
        }

        log::debug!("add_staged_entry_in_dir_db {:?} {:?}", path, staged_entry);
        self.add_staged_entry_to_db(&path, &staged_entry, staged_db, schema_reader)?;

        Ok(path)
    }

    fn add_staged_entry_to_db<T: ThreadMode>(
        &self,
        path: &Path,
        staged_entry: &StagedEntry,
        staged_db: &StagedDirEntryDB<T>,
        schema_reader: &SchemaReader,
    ) -> Result<(), OxenError> {
        let relative = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        if let Some(file_name) = relative.file_name() {
            // add all parents up to root
            let mut components = path.components().collect::<Vec<_>>();
            log::debug!("add_staged_entry_to_db got components {}", components.len());
            while !components.is_empty() {
                if let Some(_component) = components.pop() {
                    let parent: PathBuf = components.iter().collect();
                    log::debug!("add_staged_entry_to_db got parent {:?}", parent);
                    log::debug!("add_staged_entry_to_db adding parent {:?}", parent);
                    path_db::put(&self.dir_db, parent, &StagedEntryStatus::Added)?;
                }
            }

            // If tabular, add schema
            if util::fs::is_tabular(path) {
                log::debug!(
                    "add_staged_entry_to_db is tabular! compute schema {:?}",
                    path
                );
                let full_path = self.repository.path.join(path);

                match tabular::read_df(full_path, DFOpts::empty()) {
                    Ok(df) => {
                        self.add_schema_for_tabular(&df, path, schema_reader)?;
                    }
                    Err(err) => {
                        log::warn!("Could not compute schema for file: {}", err);
                    }
                }
            }

            staged_db.add_staged_entry_to_db(file_name, staged_entry)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    fn add_schema_for_tabular(
        &self,
        df: &polars::prelude::DataFrame,
        path: &Path,
        schema_reader: &SchemaReader,
    ) -> Result<(), OxenError> {
        let mut schema = schema::Schema::from_polars(&df.schema());
        log::debug!(
            "add_staged_entry_to_db is tabular! got schema {:?} -> {:?}",
            path,
            schema
        );

        // if we have schema overrides staged or committed, continue to apply them
        let maybe_schema: Option<schema::Schema> =
            self.maybe_get_existing_schema_from_reader(path, schema_reader)?;
        let schema = match maybe_schema {
            Some(added_schema) => {
                log::debug!(
                    "add_staged_entry_to_db is tabular! got schema overrides {:?} -> {:?}",
                    path,
                    added_schema
                );
                schema.update_metadata_from_schema(&added_schema);
                schema
            }
            None => schema,
        };

        // Need to also include all parents as staged
        let relative = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        if let Some(_file_name) = relative.file_name() {
            // add all parents up to root
            let mut components = path.components().collect::<Vec<_>>();
            log::debug!("add_staged_entry_to_db got components {}", components.len());
            while !components.is_empty() {
                if let Some(_component) = components.pop() {
                    let parent: PathBuf = components.iter().collect();
                    log::debug!("add_staged_entry_to_db got parent {:?}", parent);
                    log::debug!("add_staged_entry_to_db adding parent {:?}", parent);
                    path_db::put(&self.dir_db, parent, &StagedEntryStatus::Added)?;
                }
            }
        }

        let staged_schema = StagedSchema {
            schema,
            status: StagedEntryStatus::Added,
        };

        log::debug!(
            "add_staged_entry_to_db adding schema {:?} to db {:?}",
            staged_schema,
            self.schemas_db.path()
        );

        path_db::put(&self.schemas_db, path, &staged_schema)?;
        Ok(())
    }

    /// Update all the schema field tyfpe overrides on a staged schema
    pub fn update_schema_for_path(
        &self,
        path: impl AsRef<Path>,
        new_schema: &schema::Schema,
    ) -> Result<schema::Schema, OxenError> {
        log::debug!("in update_schema_for_path {:?}", path.as_ref());
        let path = path.as_ref();
        let maybe_schema = self.maybe_get_existing_schema(path)?;
        let relative = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        if let Some(_file_name) = relative.file_name() {
            // add all parents up to root
            let mut components = path.components().collect::<Vec<_>>();
            log::debug!("update_schema_for_path got components {}", components.len());
            while !components.is_empty() {
                if let Some(_component) = components.pop() {
                    let parent: PathBuf = components.iter().collect();
                    log::debug!("update_schema_for_path got parent {:?}", parent);
                    log::debug!("update_schema_for_path adding parent {:?}", parent);
                    path_db::put(&self.dir_db, parent, &StagedEntryStatus::Added)?;
                }
            }
        }
        if let Some(mut schema) = maybe_schema {
            log::debug!(
                "update_schema_field_dtype_overrides found schema for path {:?}\n{}",
                path,
                schema.verbose_str()
            );
            log::debug!(
                "update_schema_field_dtype_overrides new schema {:?}\n{}",
                path,
                new_schema.verbose_str()
            );

            if let Some(metadata) = &new_schema.metadata {
                schema.metadata = Some(metadata.clone());
            }

            schema.update_metadata_from_schema(new_schema);

            let staged_schema = StagedSchema {
                schema: schema.to_owned(),
                status: StagedEntryStatus::Modified,
            };
            path_db::put(&self.schemas_db, path, &staged_schema)?;
        } else {
            // If it doesn't exist, create the schema just based on the fields
            log::debug!(
                "update_schema_field_dtype_overrides could not find schema for path {:?}",
                path
            );
            log::debug!(
                "update_schema_field_dtype_overrides creating schema {:?} for path {:?}",
                new_schema,
                path
            );
            let staged_schema = StagedSchema {
                schema: new_schema.to_owned(),
                status: StagedEntryStatus::Added,
            };
            path_db::put(&self.schemas_db, path, &staged_schema)?;
        }
        let staged_schema: Option<StagedSchema> = path_db::get_entry(&self.schemas_db, path)?;

        // We just inserted so unwrap is okay
        Ok(staged_schema.unwrap().schema)
    }

    fn maybe_get_existing_schema(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Option<schema::Schema>, OxenError> {
        log::debug!("looking for staged schema for path {:?}", path.as_ref());
        let maybe_schema: Option<StagedSchema> = path_db::get_entry(&self.schemas_db, &path)?;
        if let Some(staged_schema) = maybe_schema {
            log::debug!("found staged schema for path {:?}", path.as_ref());
            return Ok(Some(staged_schema.schema));
        }
        log::debug!("couldn't find, calling get_by_path");

        let Some(commit) = repositories::commits::head_commit_maybe(&self.repository)? else {
            return Ok(None);
        };
        repositories::data_frames::schemas::get_by_path(&self.repository, &commit, path)
    }

    fn maybe_get_existing_schema_from_reader(
        &self,
        path: impl AsRef<Path>,
        schema_reader: &SchemaReader,
    ) -> Result<Option<schema::Schema>, OxenError> {
        log::debug!("looking for staged schema for path {:?}", path.as_ref());
        let maybe_schema: Option<StagedSchema> = path_db::get_entry(&self.schemas_db, &path)?;
        if let Some(staged_schema) = maybe_schema {
            log::debug!("found staged schema for path {:?}", path.as_ref());
            return Ok(Some(staged_schema.schema));
        }
        log::debug!("couldn't find, calling get_by_path");

        schema_reader.get_schema_for_file(path)
    }

    /// Remove a staged schema
    pub fn rm_schema(&self, path: impl AsRef<Path>) -> Result<(), OxenError> {
        let path = path.as_ref();
        for (path_from_db, _) in path_db::list_path_entries::<MultiThreaded, StagedSchema>(
            &self.schemas_db,
            Path::new(""),
        )? {
            if path_from_db == path {
                path_db::delete(&self.schemas_db, path)?;
            }
        }
        Ok(())
    }

    fn count_staged_files_in_dir(&self, dir: &Path) -> Result<usize, OxenError> {
        log::debug!("Counting staged files in dir {:?}", dir);
        let relative = util::fs::path_relative_to_dir(dir, &self.repository.path)?;
        let staged_dir = StagedDirEntryReader::new(&self.repository, &relative)?;
        let progress_bar = true;
        staged_dir.count_added_files(progress_bar)
    }

    pub fn list_staged_dirs(&self) -> Result<Vec<(PathBuf, StagedEntryStatus)>, OxenError> {
        path_db::list_path_entries(&self.dir_db, Path::new(""))
    }

    pub fn compute_staged_dir_stats(
        &self,
        path: &Path,
        status: &StagedEntryStatus,
    ) -> Result<StagedDirStats, OxenError> {
        let relative_path = util::fs::path_relative_to_dir(path, &self.repository.path)?;
        log::debug!("compute_staged_dir_stats {:?} -> {:?}", relative_path, path);
        let mut stats = StagedDirStats {
            path: relative_path.to_owned(),
            num_files_staged: 0,
            total_files: 0,
            status: status.to_owned(),
        };

        // Only consider directories
        if !path.is_dir() {
            log::debug!("compute_staged_dir_stats path is not dir {:?}", path);
            return Ok(stats);
        }

        // Count in db from relative path
        let num_files_staged = self.count_staged_files_in_dir(&relative_path)?;

        // Make sure we have some files added
        if num_files_staged == 0 {
            log::debug!("compute_staged_dir_stats num_files_staged == 0 {:?}", path);
            return Ok(stats);
        }

        // Count in fs from full path
        stats.total_files = util::fs::count_files_in_dir_w_progress(path);
        stats.num_files_staged = num_files_staged;

        Ok(stats)
    }

    pub fn list_removed_files(
        &self,
        entry_reader: &CommitEntryReader,
    ) -> Result<Vec<PathBuf>, OxenError> {
        // TODO: We are looping multiple times to check whether file is added,modified,or removed, etc
        //       We should do this loop once, and check each thing
        let mut paths: Vec<PathBuf> = vec![];
        for short_path in entry_reader.list_files()? {
            let path = self.repository.path.join(&short_path);
            if !path.exists() && !self.has_entry(&short_path) {
                paths.push(short_path);
            }
        }
        Ok(paths)
    }

    pub fn list_untracked_files(
        &self,
        entry_reader: &CommitEntryReader,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let dir_entries = std::fs::read_dir(&self.repository.path)?;
        // println!("Listing untracked files from {:?}", dir_entries);
        let num_in_head = entry_reader.num_entries()?;
        log::debug!(
            "stager::list_untracked_files head has {} files",
            num_in_head
        );

        let mut paths: Vec<PathBuf> = vec![];
        for entry in dir_entries {
            let local_path = entry?.path();
            if local_path.is_file() {
                // Return relative path with respect to the repo
                let relative_path =
                    util::fs::path_relative_to_dir(&local_path, &self.repository.path)?;
                log::debug!(
                    "stager::list_untracked_files considering path {:?}",
                    relative_path
                );

                // File is committed in HEAD
                if entry_reader.has_file(&relative_path) {
                    continue;
                }

                // File is staged
                if !self.has_entry(&relative_path) {
                    paths.push(relative_path);
                }
            }
        }

        Ok(paths)
    }

    pub fn remove_staged_dir<P: AsRef<Path>>(&self, short_path: P) -> Result<(), OxenError> {
        let short_path = short_path.as_ref();

        log::debug!("Remove staged dir short_path: {:?}", short_path);

        // Not most efficient to linearly scan, but we don't have pointers to parents or children
        let staged_dirs = self.list_staged_dirs()?;
        for (added_dir, _) in staged_dirs.iter() {
            if added_dir.starts_with(short_path) {
                log::debug!("Removing files from added_dir: {:?}", added_dir);

                // Remove all files within that dir
                let staged_dir: StagedDirEntryDB<MultiThreaded> =
                    StagedDirEntryDB::new(&self.repository, added_dir)?;
                staged_dir.unstage()?;

                // Remove from dir db
                path_db::delete(&self.dir_db, added_dir)?;
            }
        }

        Ok(())
    }

    pub fn unstage(&self) -> Result<(), OxenError> {
        let staged_dirs = self.list_staged_dirs()?;
        log::debug!("Unstage dirs: {}", staged_dirs.len());
        for (dir, _) in staged_dirs {
            log::debug!("Unstaging dir: {:?}", dir);
            let staged_dir: StagedDirEntryDB<MultiThreaded> =
                StagedDirEntryDB::new(&self.repository, &dir)?;
            staged_dir.unstage()?;
        }
        let staged_dir_db: StagedDirEntryDB<MultiThreaded> =
            StagedDirEntryDB::new(&self.repository, Path::new(""))?;
        staged_dir_db.unstage()?;
        path_db::clear(&self.dir_db)?;
        path_db::clear(&self.schemas_db)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::core::v0_10_0::index::{
        CommitEntryReader, CommitReader, CommitWriter, SchemaReader, Stager,
    };
    use crate::core::versions::MinOxenVersion;
    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::model::{StagedData, StagedEntryStatus};
    use crate::repositories;
    use crate::test;
    use crate::util;

    use std::path::PathBuf;

    pub fn run_empty_stager_test<T>(test: T) -> Result<(), OxenError>
    where
        T: FnOnce(Stager, LocalRepository) -> Result<(), OxenError> + std::panic::UnwindSafe,
    {
        test::init_test_env();
        let repo_dir = test::create_repo_dir(test::test_run_dir())?;
        log::debug!("BEFORE COMMAND::INIT");
        let repo = repositories::init::init_with_version(&repo_dir, MinOxenVersion::V0_10_0)?;
        log::debug!("AFTER COMMAND::INIT");
        let stager = Stager::new(&repo)?;
        log::debug!("AFTER CREATE STAGER");

        // Run test to see if it panic'd
        let result = std::panic::catch_unwind(|| match test(stager, repo) {
            Ok(_) => {}
            Err(err) => {
                panic!("Error running test. Err: {}", err);
            }
        });

        // Remove repo dir
        util::fs::remove_dir_all(&repo_dir)?;

        // Assert everything okay after we cleanup the repo dir
        assert!(result.is_ok());
        Ok(())
    }

    // This is how we initialize
    #[test]
    fn test_commit_no_files() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, repo| {
            let status = StagedData::empty();
            log::debug!("run_empty_stager_test before CommitWriter::new...");
            let commit_writer = CommitWriter::new(&repo)?;
            commit_writer.commit(&status, "Init")?;
            stager.unstage()?;

            Ok(())
        })
    }

    #[test]
    fn test_stager_unstage() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;
            let schema_reader = SchemaReader::new(&stager.repository, &commit.id)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Add a file and a directory
            stager.add_file(&hello_file, &entry_reader, &schema_reader)?;
            stager.add_dir(&sub_dir, &entry_reader)?;

            // Make sure the counts start properly
            let status = stager.status(&entry_reader)?;
            assert_eq!(status.staged_files.len(), 3);
            assert_eq!(status.staged_dirs.paths.len(), 2);

            // Unstage
            stager.unstage()?;

            // There should no longer be any added files
            let status = stager.status(&entry_reader)?;
            status.print();
            assert_eq!(status.staged_files.len(), 0);
            assert_eq!(status.staged_dirs.paths.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_twice_only_adds_once() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let head_commit = CommitReader::new(&stager.repository)?.head_commit()?;
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;
            let schema_reader = SchemaReader::new(&stager.repository, &head_commit.id)?;
            // Make sure we have a valid file
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;

            // Add it twice
            stager.add_file(&hello_file, &entry_reader, &schema_reader)?;
            stager.add_file(&hello_file, &entry_reader, &schema_reader)?;

            // Make sure we still only have it once
            let status = stager.status(&entry_reader)?;
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_non_existant_file() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;
            let schema_reader = SchemaReader::new_from_head(&stager.repository)?;
            let hello_file = PathBuf::from("non-existant.txt");
            if stager
                .add_file(&hello_file, &entry_reader, &schema_reader)
                .is_ok()
            {
                // we don't want to be able to add this file
                panic!("test_add_non_existant_file() Cannot stage non-existant file")
            }

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;
            let schema_reader = SchemaReader::new_from_head(&stager.repository)?;
            let hello_file = test::add_txt_file_to_dir(&stager.repository.path, "Hello 1")?;
            stager.add_file(&hello_file, &entry_reader, &schema_reader)?;

            let status = stager.status(&entry_reader)?;
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let hello_file = test::write_txt_file_to_path(repo.path.join("heyyo.txt"), "Hello 1")?;
            repositories::add(&repo, hello_file)?;

            // let status = repositories::status(&repo)?;

            // // print status
            // status.print();

            // assert_eq!(status.staged_files.len(), 1);

            repositories::commit(&repo, "Add Hello 1")?;

            let status = repositories::status(&repo)?;

            // print status
            status.print();

            assert_eq!(status.staged_files.len(), 0);
            assert_eq!(status.untracked_files.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_stager_single_add_file_in_dir() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;
            let schema_reader = SchemaReader::new_from_head(&stager.repository)?;
            // Write two files to directories
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data").join("deeper");
            std::fs::create_dir_all(&sub_dir)?;
            let file = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            assert!(stager
                .add_file(&file, &entry_reader, &schema_reader)
                .is_ok());

            let status = stager.status(&entry_reader)?;
            assert_eq!(status.staged_files.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_directory() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            // Write two files to directories
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_dir(&sub_dir, &entry_reader)?;

            let status = stager.status(&entry_reader)?;
            status.print();
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_stager_get_entry() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;
            let schema_reader = SchemaReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &entry_reader, &schema_reader)?;

            // we should be able to fetch this entry json
            let entry = stager.get_entry(relative_path).unwrap().unwrap();
            assert!(!entry.hash.is_empty());
            assert_eq!(entry.status, StagedEntryStatus::Added);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_files() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;
            let schema_reader = SchemaReader::new_from_head(&stager.repository)?;
            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello World")?;
            let relative_path = util::fs::path_relative_to_dir(&hello_file, repo_path)?;

            // Stage file
            stager.add_file(&hello_file, &entry_reader, &schema_reader)?;

            // List files
            let status = stager.status(&entry_reader)?;
            let files = status.staged_files;
            assert_eq!(files.len(), 1);
            assert!(files.contains_key(&relative_path));

            Ok(())
        })
    }

    #[test]
    fn test_stager_add_file_in_sub_dir() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;
            let schema_reader = SchemaReader::new_from_head(&stager.repository)?;
            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            stager.add_file(&sub_file, &entry_reader, &schema_reader)?;

            // List files
            let status = stager.status(&entry_reader)?;
            let files = status.staged_files;

            // There is one file
            assert_eq!(files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(&sub_file, repo_path)?;
            assert!(files.contains_key(&relative_path));

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_files() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, _repo| {
            // Create entry_reader with no commits
            let entry_reader = CommitEntryReader::new_from_head(&stager.repository)?;

            let repo_path = &stager.repository.path;
            let hello_file = test::add_txt_file_to_dir(repo_path, "Hello 1")?;

            // Do not add...

            // List files
            let files = stager.list_untracked_files(&entry_reader)?;
            assert_eq!(files.len(), 1);
            let relative_path = util::fs::path_relative_to_dir(hello_file, repo_path)?;
            assert_eq!(files[0], relative_path);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_untracked_dirs() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            // Must have some sort of file in the dir to add it.
            test::write_txt_file_to_path(sub_dir.join("hi.txt"), "Hi")?;

            // Do not add...

            // List files
            let dirs = stager.status(&entry_reader)?.untracked_dirs;
            assert_eq!(dirs.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_stager_list_one_untracked_directory() -> Result<(), OxenError> {
        run_empty_stager_test(|stager, repo| {
            // Create entry_reader with no commits
            let commit_reader = CommitReader::new(&repo)?;
            let commit = commit_reader.head_commit()?;
            let entry_reader = CommitEntryReader::new(&stager.repository, &commit)?;

            // Write two files to a sub directory
            let repo_path = &stager.repository.path;
            let sub_dir = repo_path.join("training_data");
            std::fs::create_dir_all(&sub_dir)?;

            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
            let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

            // Do not add...

            // List files
            let dirs = stager.status(&entry_reader)?.untracked_dirs;

            // There is one directory
            assert_eq!(dirs.len(), 1);

            Ok(())
        })
    }
}
