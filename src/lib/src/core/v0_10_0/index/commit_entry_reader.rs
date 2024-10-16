use crate::constants::{DIRS_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::v0_10_0::index::{CommitDirEntryReader, CommitReader};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry};
use crate::util;

use glob::Pattern;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::db::key_val::path_db;
use crate::model::LocalRepository;

use super::ObjectDBReader;

pub struct CommitEntryReader {
    base_path: PathBuf,
    dir_db: DBWithThreadMode<MultiThreaded>,
    object_reader: Arc<ObjectDBReader>,
    pub commit_id: String,
}

impl CommitEntryReader {
    pub fn db_path(base_path: impl AsRef<Path>, commit_id: &str) -> PathBuf {
        util::fs::oxen_hidden_dir(&base_path)
            .join(HISTORY_DIR)
            .join(commit_id)
            .join(DIRS_DIR)
    }

    pub fn get_obj_reader(&self) -> &Arc<ObjectDBReader> {
        &self.object_reader
    }

    pub fn get_dir_db(&self) -> &DBWithThreadMode<MultiThreaded> {
        &self.dir_db
    }

    pub fn new(
        repository: &LocalRepository,
        commit: &Commit,
    ) -> Result<CommitEntryReader, OxenError> {
        log::debug!("CommitEntryReader::new() commit_id: {}", commit.id);
        let object_reader = get_object_reader(repository, &commit.id)?;
        CommitEntryReader::new_from_commit_id(repository, &commit.id, object_reader)
    }

    pub fn new_from_commit_id(
        repository: &LocalRepository,
        commit_id: &str,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitEntryReader, OxenError> {
        CommitEntryReader::new_from_path(&repository.path, commit_id, object_reader)
    }

    pub fn new_from_path(
        base_path: impl AsRef<Path>,
        commit_id: &str,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitEntryReader, OxenError> {
        let path = Self::db_path(&base_path, commit_id);
        let opts = db::key_val::opts::default();
        log::debug!(
            "CommitEntryReader::new_from_path() commit_id: {} path: {:?}",
            commit_id,
            path
        );

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
            // open it then lose scope to close it
            let _db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
        }

        Ok(CommitEntryReader {
            base_path: base_path.as_ref().to_owned(),
            dir_db: DBWithThreadMode::open_for_read_only(&opts, &path, true)?,
            commit_id: commit_id.to_owned(),
            object_reader,
        })
    }

    pub fn new_from_head(repository: &LocalRepository) -> Result<CommitEntryReader, OxenError> {
        let commit_reader = CommitReader::new(repository)?;
        let commit = commit_reader.head_commit()?;
        log::debug!(
            "CommitEntryReader::new_from_head() commit_id: {}",
            commit.id
        );
        CommitEntryReader::new(repository, &commit)
    }

    pub fn get_dir_hash(&self, path: impl AsRef<Path>) -> Result<Option<String>, OxenError> {
        self.object_reader.get_dir_hash(path)
    }

    pub fn list_dirs(&self) -> Result<Vec<PathBuf>, OxenError> {
        let root = PathBuf::from("");
        let mut paths = path_db::list_paths(&self.dir_db, &root)?;
        if !paths.contains(&root) {
            paths.push(root);
        }
        paths.sort();
        Ok(paths)
    }

    pub fn list_dirs_set(&self) -> Result<HashSet<PathBuf>, OxenError> {
        let root = PathBuf::from("");
        let mut paths = path_db::list_paths(&self.dir_db, &root)?;
        if !paths.contains(&root) {
            paths.push(root);
        }
        let mut set = HashSet::new();
        for path in paths {
            set.insert(path);
        }
        Ok(set)
    }

    /// Lists all the parents of directories that are in the commit dir db
    pub fn list_dir_parents(&self, path: impl AsRef<Path>) -> Result<Vec<PathBuf>, OxenError> {
        // A little hacky, we just filter by starts_with because we aren't representing the parents in the db
        // Shouldn't be a problem unless we have repos with hundreds of thousands of directories?
        let path = path.as_ref();
        let parents = path_db::list_paths(&self.dir_db, Path::new(""))?
            .into_iter()
            .filter(|base| path.starts_with(base) && base != path)
            .collect();
        Ok(parents)
    }

    /// Lists all the child directories that are in the commit dir db
    pub fn list_dir_children(&self, path: impl AsRef<Path>) -> Result<Vec<PathBuf>, OxenError> {
        // A little hacky, we just filter by starts_with because we aren't representing the parents in the db
        // Shouldn't be a problem unless we have repos with hundreds of thousands of directories?
        let path = path.as_ref();
        let children = path_db::list_paths(&self.dir_db, Path::new(""))?
            .into_iter()
            .filter(|dir| {
                (path == Path::new("") && dir != Path::new(""))
                    || (dir.starts_with(path)
                        && dir != path
                        && dir.components().count() > path.components().count())
            })
            .collect();
        Ok(children)
    }

    /// Lists all the child directories that are in the commit dir db and returns them as a HashSet
    pub fn list_dir_children_set(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<HashSet<PathBuf>, OxenError> {
        let path = path.as_ref();
        let children = path_db::list_paths(&self.dir_db, Path::new(""))?
            .into_iter()
            .filter(|dir| {
                (path == Path::new("") && dir != Path::new(""))
                    || (dir.starts_with(path)
                        && dir != path
                        && dir.components().count() > path.components().count())
            })
            .collect::<HashSet<PathBuf>>();
        Ok(children)
    }

    pub fn has_dir<P: AsRef<Path>>(&self, path: P) -> bool {
        path_db::has_entry(&self.dir_db, path)
    }

    pub fn num_entries(&self) -> Result<usize, OxenError> {
        let mut count = 0;
        for dir in self.list_dirs()? {
            let commit_entry_dir = CommitDirEntryReader::new_from_path(
                &self.base_path,
                &self.commit_id,
                &dir,
                self.object_reader.clone(),
            )?;
            count += commit_entry_dir.num_entries();
        }
        Ok(count)
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut paths: Vec<PathBuf> = vec![];
        for dir in self.list_dirs()? {
            log::debug!("listing files for dir {:?}", dir);
            let commit_dir = CommitDirEntryReader::new_from_path(
                &self.base_path,
                &self.commit_id,
                &dir,
                self.object_reader.clone(),
            )?;
            let mut files = commit_dir.list_files()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut paths: Vec<CommitEntry> = vec![];
        for dir in self.list_dirs()? {
            // log::debug!("listing entries for dir {:?}", dir);
            let commit_dir = CommitDirEntryReader::new_from_path(
                &self.base_path,
                &self.commit_id,
                &dir,
                self.object_reader.clone(),
            )?;
            let mut files = commit_dir.list_entries()?;
            paths.append(&mut files);
        }
        Ok(paths)
    }

    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut paths: HashSet<CommitEntry> = HashSet::new();
        for dir in self.list_dirs()? {
            let commit_dir = CommitDirEntryReader::new_from_path(
                &self.base_path,
                &self.commit_id,
                &dir,
                self.object_reader.clone(),
            )?;
            let files = commit_dir.list_entries_set()?;
            paths.extend(files);
        }
        Ok(paths)
    }

    pub fn list_entry_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let mut entries = self.list_entries()?;

        // Entries not automatically path-sorted due to tree structure
        entries.sort_by(|a, b| a.path.cmp(&b.path));

        let start_page = if page == 0 { 0 } else { page - 1 };
        let start_idx = start_page * page_size;

        if (start_idx + page_size) < entries.len() {
            let subset: Vec<CommitEntry> = entries[start_idx..(start_idx + page_size)].to_vec();
            Ok(subset)
        } else if (start_idx < entries.len()) && (start_idx + page_size) >= entries.len() {
            let subset: Vec<CommitEntry> = entries[start_idx..entries.len()].to_vec();
            Ok(subset)
        } else {
            Ok(vec![])
        }
    }

    pub fn list_directory(&self, dir: &Path) -> Result<Vec<CommitEntry>, OxenError> {
        log::debug!("CommitEntryReader::list_directory() dir: {:?}", dir);
        let mut entries = vec![];
        // This lists all the committed dirs
        let mut dirs = self.list_dirs()?;
        dirs.sort();

        log::debug!("CommitEntryReader::list_directory() dirs: {}", dirs.len());

        for committed_dir in dirs {
            // Have to make sure we are in a subset of the dir (not really a tree structure)
            // log::debug!("CommitEntryReader::list_directory() checking committed_dir: {:?}", committed_dir);
            if committed_dir.starts_with(dir)
                && committed_dir.components().count() > dir.components().count()
            {
                let entry_reader = CommitDirEntryReader::new_from_path(
                    &self.base_path,
                    &self.commit_id,
                    &committed_dir,
                    self.object_reader.clone(),
                )?;
                let mut dir_entries = entry_reader.list_entries()?;
                entries.append(&mut dir_entries);

                if entries.len() % 10000 == 0 {
                    log::debug!(
                        "CommitEntryReader::list_directory() entries: {}",
                        entries.len()
                    );
                }
            }
        }
        Ok(entries)
    }

    pub fn list_directory_set(&self, dir: &Path) -> Result<HashSet<CommitEntry>, OxenError> {
        log::debug!("CommitEntryReader::list_directory_set() dir: {:?}", dir);
        let entries = self.list_directory(dir)?;
        let entries_set: HashSet<CommitEntry> = HashSet::from_iter(entries);
        Ok(entries_set)
    }

    pub fn list_entries_per_directory(
        &self,
        dir: &Path,
    ) -> Result<HashMap<PathBuf, Vec<CommitEntry>>, OxenError> {
        log::debug!("CommitEntryReader::list_directory() dir: {:?}", dir);
        let mut dir_entries: HashMap<PathBuf, Vec<CommitEntry>> = HashMap::new();
        // This lists all the committed dirs
        let dirs = self.list_dirs()?;
        for committed_dir in dirs {
            // Have to make sure we are in a subset of the dir (not really a tree structure)
            // log::debug!("CommitEntryReader::list_directory() checking committed_dir: {:?}", committed_dir);
            if committed_dir.starts_with(dir)
                && committed_dir.components().count() > dir.components().count()
            {
                let entry_reader = CommitDirEntryReader::new_from_path(
                    &self.base_path,
                    &self.commit_id,
                    &committed_dir,
                    self.object_reader.clone(),
                )?;
                let entries = entry_reader.list_entries()?;
                dir_entries.insert(committed_dir, entries);
            }
        }
        Ok(dir_entries)
    }

    pub fn has_file(&self, path: &Path) -> bool {
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            if let Ok(dir) = CommitDirEntryReader::new_from_path(
                &self.base_path,
                &self.commit_id,
                parent,
                self.object_reader.clone(),
            ) {
                return dir.has_file(file_name);
            }
        }
        false
    }

    pub fn get_entry(&self, path: &Path) -> Result<Option<CommitEntry>, OxenError> {
        if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
            let dir = CommitDirEntryReader::new_from_path(
                &self.base_path,
                &self.commit_id,
                parent,
                self.object_reader.clone(),
            )?;
            // log::debug!("CommitEntryReader::get_entry() get_entry: {:?}", path);

            // log::debug!("CommitEntryReader::get_entry() path: {:?} result: {:?}", path, result);
            dir.get_entry(file_name)
        } else {
            Err(OxenError::file_has_no_parent(path))
        }
    }

    pub fn search_entries(&self, pattern: &str) -> Result<HashSet<PathBuf>, OxenError> {
        let pattern = Pattern::new(pattern)?;
        let entries = self.list_entries()?;
        let entry_paths: Vec<PathBuf> = entries.iter().map(|entry| entry.path.to_owned()).collect();

        let mut paths = HashSet::new();
        for path in entry_paths
            .iter()
            .filter(|entry_path| pattern.matches_path(entry_path))
            .map(|entry_path| entry_path.to_owned())
        {
            paths.insert(path);
        }

        Ok(paths)
    }
}
