use ignore::gitignore::Gitignore;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rocksdb::{DBWithThreadMode, MultiThreaded, SingleThreaded};

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::command::workspace;
use crate::constants::{FILES_DIR, MODS_DIR};
use crate::core::db;
use crate::core::db::key_val::{path_db, str_json_db};
use crate::core::index::stager::FileStatus;
use crate::core::index::{
    oxenignore, CommitDirEntryReader, CommitEntryReader, CommitReader, MergeConflictReader,
    ObjectDBReader, StagedDirEntryDB, StagedDirEntryReader, Stager,
};
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::{
    LocalRepository, MergeConflict, StagedData, StagedDirStats, StagedEntry, StagedEntryStatus,
    StagedSchema,
};
use crate::util;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

fn files_db_path(workspace: &Workspace) -> PathBuf {
    workspace.dir().join(MODS_DIR).join(FILES_DIR)
}

pub fn status(workspace: &Workspace, directory: &Path) -> Result<StagedData, OxenError> {
    let repo = &workspace.base_repo;
    let workspace_repo = &workspace.workspace_repo;
    let commit = &workspace.commit;
    // Stager will be in the workspace repo
    let stager = Stager::new(workspace_repo)?;
    // But we will read from the commit in the main repo
    log::debug!(
        "list_staged_data get commit by id {} -> {} -> {:?}",
        commit.message,
        commit.id,
        directory
    );

    log::debug!("index::workspaces::stager::status for dir {:?}", directory);
    let reader = CommitEntryReader::new(repo, commit)?;
    if Path::new(".") == directory {
        let mut status = stager.status(&reader)?;
        list_staged_entries(workspace, &mut status)?;
        Ok(status)
    } else {
        let mut status = stager.status_from_dir(&reader, directory)?;
        list_staged_entries(workspace, &mut status)?;
        Ok(status)
    }
}

// Modifications to files are staged in a separate DB and applied on commit,
// so we fetch them from the mod_stager
fn list_staged_entries(workspace: &Workspace, status: &mut StagedData) -> Result<(), OxenError> {
    let mod_entries = list_files(workspace)?;

    for path in mod_entries {
        status.modified_files.push(path.to_owned());
    }

    Ok(())
}

pub fn list_files(workspace: &Workspace) -> Result<Vec<PathBuf>, OxenError> {
    let db_path = files_db_path(workspace);
    log::debug!("list_entries from files_db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<SingleThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    str_json_db::list_vals(&db)
}

pub fn add(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let db_path = files_db_path(workspace);
    log::debug!("workspaces::stager::add to db_path {db_path:?}");
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<MultiThreaded> = rocksdb::DBWithThreadMode::open(&opts, db_path)?;
    let key = path.to_string_lossy();
    str_json_db::put(&db, &key, &key)
}

pub fn rm(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let files_db_path = files_db_path(workspace);
    let files_db: DBWithThreadMode<MultiThreaded> =
        rocksdb::DBWithThreadMode::open(&opts, files_db_path)?;
    let key = path.as_ref().to_string_lossy();
    str_json_db::delete(&files_db, key)?;

    Ok(())
}

pub fn status_for_workspace(
    workspace_repository: &LocalRepository,
) -> Result<StagedData, OxenError> {
    let dir_db_path = Stager::dirs_db_path(&workspace_repository.path)?;
    let schemas_db_path = Stager::schemas_db_path(&workspace_repository.path)?;
    let opts = db::key_val::opts::default();

    log::debug!("-----status START-----");
    let dir_db =
        DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&dir_db_path), false)?;
    let schemas_db =
        DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&schemas_db_path), false)?;

    let result = compute_staged_data(workspace_repository, dir_db, schemas_db);
    log::debug!("-----status END-----");
    result
}

fn compute_staged_data(
    workspace_repository: &LocalRepository,
    dir_db: DBWithThreadMode<MultiThreaded>,
    schemas_db: DBWithThreadMode<MultiThreaded>,
) -> Result<StagedData, OxenError> {
    // if workspace_repository.is_shallow_clone() {
    //     return Err(OxenError::repo_is_shallow());
    // }

    // Get the time before doing this

    let mut staged_data = StagedData::empty();
    let ignore = oxenignore::create(workspace_repository);

    let mut candidate_dirs: HashSet<PathBuf> = HashSet::new();
    // Start with candidate dirs from committed and added, not all the dirs
    let staged_dirs = list_staged_dirs(&dir_db)?;


    log::debug!(
        "compute_staged_data Got <added> dirs: {}",
        staged_dirs.len()
    );
    for (dir, status) in staged_dirs {
        log::debug!("compute_staged_data considering added dir {:?}", dir);
        let full_path = workspace_repository.path.join(&dir);
        let stats = compute_staged_dir_stats(workspace_repository, &full_path, &status)?;
        staged_data.staged_dirs.add_stats(&stats);
        log::debug!("compute_staged_data got stats {:?}", stats);

        log::debug!("compute_staged_data adding <added> dir {:?}", dir);
        candidate_dirs.insert(workspace_repository.path.join(dir));
    }

    log::debug!(
        "compute_staged_data Considering <current> dir: {:?}",
        workspace_repository.path
    );


    let object_reader = ObjectDBReader::new(&workspace_repository)?;

    let committer = CommitReader::new(&workspace_repository)?;
    let commit = committer.head_commit()?;

    let entry_reader = CommitEntryReader::new(&workspace_repository, &commit)?;

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
        process_dir(
            &dir_db,
            &workspace_repository,
            dir,
            &mut staged_data,
            &ignore,
            &entry_reader,
            object_reader.clone(),
            bar.clone(),
        )?;
    }

    // Make pairs from Added + Removed stage entries with same hash, store in staged_data.moved_entries
    find_moved_files(&mut staged_data)?;

    // Find merge conflicts
    staged_data.merge_conflicts = list_merge_conflicts(workspace_repository)?;

    // Populate schemas from db
    let mut schemas: HashMap<PathBuf, StagedSchema> = HashMap::new();
    for (path, schema) in path_db::list_path_entries(&schemas_db, Path::new(""))? {
        schemas.insert(path, schema);
    }

    staged_data.staged_schemas = schemas;
    Ok(staged_data)
}

pub fn list_staged_dirs(
    dir_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<Vec<(PathBuf, StagedEntryStatus)>, OxenError> {
    path_db::list_path_entries(dir_db, Path::new(""))
}

pub fn compute_staged_dir_stats(
    workspace_repository: &LocalRepository,
    path: &Path,
    status: &StagedEntryStatus,
) -> Result<StagedDirStats, OxenError> {
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repository.path)?;
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
    let num_files_staged = list_staged_files_in_dir(workspace_repository, &relative_path)?.len();

    // Make sure we have some files added
    if num_files_staged == 0 {
        log::debug!("compute_staged_dir_stats num_files_staged == 0 {:?}", path);
        return Ok(stats);
    }

    // Count in fs from full path
    stats.total_files = util::fs::count_files_in_dir(path);
    stats.num_files_staged = num_files_staged;

    Ok(stats)
}

fn list_staged_files_in_dir(
    workspace_repository: &LocalRepository,
    dir: &Path,
) -> Result<Vec<PathBuf>, OxenError> {
    let relative = util::fs::path_relative_to_dir(dir, &workspace_repository.path)?;
    let staged_dir = StagedDirEntryReader::new(&workspace_repository, &relative)?;
    let paths = staged_dir.list_added_paths()?;
    Ok(paths)
}

fn process_dir(
    dir_db: &DBWithThreadMode<MultiThreaded>,
    workspace_repository: &LocalRepository,
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
    let committer = CommitReader::new(&workspace_repository)?;
    let commit = committer.head_commit()?;
    let relative_dir = util::fs::path_relative_to_dir(full_dir, &workspace_repository.path)?;
    let staged_dir_db: StagedDirEntryDB<SingleThreaded> =
        StagedDirEntryDB::new(&workspace_repository, &relative_dir)?;

    let root_commit_dir_reader = CommitDirEntryReader::new(
        &workspace_repository,
        &commit.id,
        &relative_dir,
        object_reader,
    )?;

    // get seconds and millis

    // Create candidate files paths to look at
    let mut candidate_files: HashSet<PathBuf> = HashSet::new();

    // Only consider working dir if it is on disk, otherwise we will grab from history
    let read_dir = std::fs::read_dir(full_dir);
    if read_dir.is_ok() {
        // Files in working directory as candidates
        for path in read_dir? {
            let path = path?.path();
            let path = util::fs::path_relative_to_dir(&path, &workspace_repository.path)?;
            if !should_ignore_path(ignore, &path) {
                // log::debug!("adding candidate from dir {:?}", path);
                candidate_files.insert(path);
            }
        }
    }

    // and files that were in commit as candidates
    for entry in root_commit_dir_reader.list_entries()? {
        // log::debug!("adding candidate from commit {:?}", entry.path);
        if !should_ignore_path(ignore, &entry.path) {
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

            let fullpath = workspace_repository.path.join(relative);

            log::debug!(
                "process_dir checking is_dir? {} {:?}",
                fullpath.is_dir(),
                fullpath
            );

            if fullpath.is_dir() {
                if !has_staged_dir(dir_db, relative)
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
                    &workspace_repository.path,
                    relative,
                    &staged_dir_db,
                    &root_commit_dir_reader,
                );
                log::debug!("process_dir got status {:?} {:?}", relative, file_status);
                if let Some(file_type) = file_status {
                    match file_type {
                        FileStatus::Added => {
                            let file_name = relative.file_name().unwrap();
                            let result = staged_dir_db.get_entry(file_name);
                            if let Ok(Some(entry)) = result {
                                local_staged_data
                                    .staged_files
                                    .insert(relative.to_path_buf(), entry);
                            }
                        }
                        FileStatus::Untracked => {
                            local_staged_data
                                .untracked_files
                                .push(relative.to_path_buf());
                        }
                        FileStatus::Modified => {
                            local_staged_data
                                .modified_files
                                .push(relative.to_path_buf());
                        }
                        FileStatus::Removed => {
                            local_staged_data.removed_files.push(relative.to_path_buf());
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

fn should_ignore_path(ignore: &Option<Gitignore>, path: &Path) -> bool {
    // If the path is the .oxen dir or is in the ignore file, ignore it
    let should_ignore = if let Some(ignore) = ignore {
        ignore.matched(path, path.is_dir()).is_ignore()
    } else {
        false
    };

    should_ignore || util::fs::is_in_oxen_hidden_dir(path)
}

fn find_moved_files(staged_data: &mut StagedData) -> Result<(), OxenError> {
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

pub fn has_staged_dir<P: AsRef<Path>>(dir_db: &DBWithThreadMode<MultiThreaded>, dir: P) -> bool {
    path_db::has_entry(&dir_db, dir)
}

fn list_merge_conflicts(
    workspace_repository: &LocalRepository,
) -> Result<Vec<MergeConflict>, OxenError> {
    let merger = MergeConflictReader::new(&workspace_repository)?;
    merger.list_conflicts()
}
