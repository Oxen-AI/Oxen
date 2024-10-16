//! # CommitDirEntryReader
//! Used to read the commit dir entry db

// use std::time::Instant;

use crate::constants::{self};
use crate::core::db::key_val::path_db;
use crate::core::db::key_val::tree_db::{TreeObject, TreeObjectChild};
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository};
use crate::util;

use os_path::OsPath;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use super::{CommitEntryWriter, ObjectDBReader};

/// # CommitDirEntryReader
pub struct CommitDirEntryReader {
    dir: PathBuf,
    dir_object: TreeObject,
    commit_id: String,
    object_reader: Arc<ObjectDBReader>,
}
// This was formerly for commit dir entry db, now it's just the dir hashes db
impl CommitDirEntryReader {
    pub fn dir_hash_db(base_path: &Path, commit_id: &str) -> PathBuf {
        CommitEntryWriter::commit_dir(base_path, commit_id).join(constants::DIR_HASHES_DIR)
    }

    pub fn dir_hashes_db_exists(base_path: &Path, commit_id: &str) -> bool {
        let db_path = CommitDirEntryReader::dir_hash_db(base_path, commit_id);
        db_path.join("CURRENT").exists()
    }

    // Maybe offer both options
    pub fn new(
        repository: &LocalRepository,
        commit_id: &str,
        dir: &Path,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitDirEntryReader, OxenError> {
        CommitDirEntryReader::new_from_path(&repository.path, commit_id, dir, object_reader)
    }

    pub fn new_from_hash_db(
        base_path: &Path,
        commit_id: &str,
        dir_hashes_db: &DBWithThreadMode<MultiThreaded>,
        dir: &Path,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitDirEntryReader, OxenError> {
        let dir_hash: Option<String> = path_db::get_entry(dir_hashes_db, dir)?;

        let dir_object: TreeObject = match dir_hash {
            Some(dir_hash) => match object_reader.get_dir(&dir_hash)? {
                Some(dir) => dir,
                None => {
                    log::error!(
                        "Could not get dir by hash: {} for path {:?} and commit_id {}",
                        dir_hash,
                        base_path,
                        commit_id
                    );
                    // Creating dummy dir object
                    TreeObject::Dir {
                        children: Vec::new(),
                        hash: "".to_string(),
                    }
                }
            },
            None => {
                // Creating dummy dir object
                TreeObject::Dir {
                    children: Vec::new(),
                    hash: "".to_string(),
                }
            }
        };

        Ok(CommitDirEntryReader {
            dir: dir.to_path_buf(),
            dir_object,
            commit_id: commit_id.to_string(),
            object_reader,
        })
    }

    pub fn new_from_path(
        base_path: &Path,
        commit_id: &str,
        dir: &Path,
        object_reader: Arc<ObjectDBReader>,
    ) -> Result<CommitDirEntryReader, OxenError> {
        // log::debug!(
        //     "Creating new CommitDirEntryReader for path: {:?}",
        //     base_path.join(dir)
        // );

        if object_reader.commit_id != commit_id {
            return Err(OxenError::basic_str(
                "ObjectDBReader commit_id does not match commit_id",
            ));
        }

        let dir_hash: Option<String> = object_reader.get_dir_hash(dir)?;
        // log::debug!(
        //     "CommitDirEntryReader::new_from_path dir: {:?} dir_hash: {:?}",
        //     dir,
        //     dir_hash
        // );
        let dir_object: TreeObject = match dir_hash {
            Some(dir_hash) => match object_reader.get_dir(&dir_hash)? {
                Some(dir) => dir,
                None => {
                    log::error!(
                        "Could not get dir by hash: {} for path {:?} and commit_id {}",
                        dir_hash,
                        base_path,
                        commit_id
                    );
                    // Creating dummy dir object
                    TreeObject::Dir {
                        children: Vec::new(),
                        hash: "".to_string(),
                    }
                }
            },
            None => {
                // Creating dummy dir object
                TreeObject::Dir {
                    children: Vec::new(),
                    hash: "".to_string(),
                }
            }
        };

        Ok(CommitDirEntryReader {
            dir: dir.to_path_buf(),
            dir_object,
            commit_id: commit_id.to_string(),
            object_reader,
        })
    }

    pub fn set_dir(&mut self, dir: &Path) {
        self.dir = dir.to_path_buf();
    }

    pub fn num_entries(&self) -> usize {
        let mut count = 0;
        let children = self.dir_object.children();
        // log::debug!("num_entries children: {:?}", children.len());
        for vnode_child in children {
            let vnode = self
                .object_reader
                .get_vnode(vnode_child.hash())
                .unwrap()
                .unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { .. } = entry {
                    count += 1
                }
            }
        }
        // log::debug!("num_entries in dir '{:?}' == {}", self.dir, count);
        count
    }

    pub fn has_file<P: AsRef<Path>>(&self, path: P) -> bool {
        let path = path.as_ref();
        // log::debug!("CommitDirEntryReader.has_file({:?})", path);

        let full_path = self.dir.join(path);
        // we have to make sure the full_path is `/` instead of `\\` to get the correct hashes
        let full_path_str = full_path.to_str().unwrap().replace('\\', "/");

        let path_hash_prefix = util::hasher::hash_path_name(full_path_str)[0..2].to_string();
        // log::debug!("CommitDirEntryReader.has_file({:?}) {:?} {}", path, full_path_str, path_hash_prefix);

        // Binary search for the appropriate vnode
        let vnode_child = self
            .dir_object
            .binary_search_on_path(&PathBuf::from(path_hash_prefix))
            .unwrap();

        let Some(vnode_child) = vnode_child else {
            // log::info!("could not get Some(vnode_child) for path {:?}", path);
            return false;
        };

        // Get the vnode object proper
        let Ok(maybe_vnode) = self.object_reader.get_vnode(vnode_child.hash()) else {
            // log::info!("could not get Ok(maybe_vnode) for path {:?}", path);
            return false;
        };

        let Some(vnode) = maybe_vnode else {
            // log::info!("could not get Some(vnode) for path {:?}", path);
            return false;
        };

        // Now binary search within the vnode for the appropriate file
        let full_path = self.dir.join(path);
        // log::debug!("Looking for full path: {:?}", full_path);

        let Ok(maybe_file) = vnode.binary_search_on_path(&full_path.to_path_buf()) else {
            // log::info!("could not get Ok(file) for path {:?}", path);
            return false;
        };

        let Some(file) = maybe_file else {
            // log::info!("could not get Some(file) for path {:?}", path);
            return false;
        };

        matches!(file, TreeObjectChild::File { .. })
    }

    pub fn get_entry<P: AsRef<Path>>(&self, path: P) -> Result<Option<CommitEntry>, OxenError> {
        let path = path.as_ref();
        let full_path = self.dir.join(path);
        // we have to make sure the full_path is `/` instead of `\\` to get the correct hashes
        let full_path_str = full_path.to_str().unwrap().replace('\\', "/");
        let path_hash_prefix = util::hasher::hash_path_name(full_path_str)[0..2].to_string();

        // Binary search for the appropriate vnode
        let maybe_vnode_child = self
            .dir_object
            .binary_search_on_path(&PathBuf::from(path_hash_prefix.clone()))?;

        let Some(vnode_child) = maybe_vnode_child else {
            // log::debug!("could not find vnode child for path {:?}", path);
            return Ok(None);
        };

        // Get parent vnode
        let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();

        // Now binary search within the vnode for the appropriate file
        let full_path = self.dir.join(path);
        let maybe_file = vnode.binary_search_on_path(&full_path)?;

        let Some(file) = maybe_file else {
            // log::debug!("could not find file for path {:?}", path);
            return Ok(None);
        };

        match file.clone() {
            TreeObjectChild::File { hash, .. } => {
                // Get file object by hash
                let file_object = self.object_reader.get_file(&hash)?.unwrap();
                // Get commit entry from file object
                let entry = file_object.to_commit_entry(file.path(), &self.commit_id);
                Ok(Some(entry))
            }
            _ => {
                log::debug!("wrong type of file node for path {:?}", path);
                Ok(None)
            }
        }
    }

    pub fn list_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut files = Vec::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    files.push(path.to_owned())
                }
            }
        }
        Ok(files)
    }

    pub fn list_dirs(&self) -> Result<Vec<PathBuf>, OxenError> {
        let mut dirs = Vec::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::Dir { path, hash } = entry {
                    let dir = self.object_reader.get_dir(hash)?;
                    if let Some(dir) = dir {
                        if !dir.children().is_empty() {
                            dirs.push(path.to_owned());
                        }
                    } else {
                        log::error!("Could not get dir by hash: {}", hash)
                    }
                }
            }
        }
        Ok(dirs)
    }

    pub fn list_dirs_set(&self) -> Result<HashSet<PathBuf>, OxenError> {
        let mut dirs = HashSet::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::Dir { path, hash } = entry {
                    let dir = self.object_reader.get_dir(hash)?;
                    if let Some(dir) = dir {
                        if !dir.children().is_empty() {
                            dirs.insert(path.to_owned());
                        }
                    } else {
                        log::error!("Could not get dir by hash: {}", hash)
                    }
                }
            }
        }
        Ok(dirs)
    }

    pub fn list_entries(&self) -> Result<Vec<CommitEntry>, OxenError> {
        let mut entries = Vec::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    // Get file object by hash
                    let file_object = self.object_reader.get_file(entry.hash())?.unwrap();

                    // return path with native slashes
                    let os_path = OsPath::from(path);
                    let new_path = os_path.to_pathbuf();

                    // Get commit entry from file object
                    let entry = file_object.to_commit_entry(&new_path, &self.commit_id);
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    pub fn list_entries_set(&self) -> Result<HashSet<CommitEntry>, OxenError> {
        let mut entries = HashSet::new();
        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();
            for entry in vnode.children() {
                if let TreeObjectChild::File { path, .. } = entry {
                    // Get file object by hash
                    let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                    // Get commit entry from file object
                    let entry = file_object.to_commit_entry(path, &self.commit_id);
                    entries.insert(entry);
                }
            }
        }
        Ok(entries)
    }

    pub fn list_entry_page(
        &self,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        // Don't have a skip here....
        let mut entries: Vec<CommitEntry> = Vec::new();
        let mut entry_i = 0;

        let start_page = if page == 0 { 0 } else { page - 1 };
        let start_idx = start_page * page_size;

        // For every vnode, get the vnode and add its children to a list
        // TODO: possible optimization - these will all be sorted by path coming out,
        // so if this is slow we can treat it as merging n sorted lists instead of concatenating and then sorting

        let mut file_children: Vec<TreeObjectChild> = Vec::new();

        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();

            for entry in vnode.children() {
                if let TreeObjectChild::File { .. } = entry {
                    file_children.push(entry.to_owned());
                }
            }
        }

        // Now sort these all by path
        file_children.sort_by(|a, b| {
            let a_path = a.path();
            let b_path = b.path();
            a_path.cmp(b_path)
        });

        // Apply pagination logic to the file_children list
        for entry in file_children {
            if entries.len() >= page_size {
                break;
            }

            if entry_i >= start_idx {
                // Get file object by hash
                let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                // Get commit entry from file object
                let entry = file_object.to_commit_entry(entry.path(), &self.commit_id);
                entries.push(entry);
            }
            entry_i += 1;
        }

        Ok(entries)
    }

    pub fn list_entry_page_with_offset(
        &self,
        page: usize,
        page_size: usize,
        offset: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        // Apply logic from above here
        let mut entries: Vec<CommitEntry> = Vec::new();
        let start_page = if page == 0 { 0 } else { page - 1 };
        let mut start_idx = start_page * page_size;
        let mut entry_i = 0;

        if start_idx >= offset {
            start_idx -= offset;
        }

        // Get all vnode chidlren
        let mut file_children: Vec<TreeObjectChild> = Vec::new();

        for vnode_child in self.dir_object.children() {
            let vnode = self.object_reader.get_vnode(vnode_child.hash())?.unwrap();

            for entry in vnode.children() {
                if let TreeObjectChild::File { .. } = entry {
                    file_children.push(entry.to_owned());
                }
            }
        }

        // Now sort these all by path
        file_children.sort_by(|a, b| {
            let a_path = a.path();
            let b_path = b.path();
            a_path.cmp(b_path)
        });

        // Apply pagination logic to the file_children list
        for entry in file_children {
            if entries.len() >= page_size {
                break;
            }

            if entry_i >= start_idx {
                // Get file object by hash
                let file_object = self.object_reader.get_file(entry.hash())?.unwrap();
                // Get commit entry from file object
                let entry = file_object.to_commit_entry(entry.path(), &self.commit_id);
                entries.push(entry);
            }
            entry_i += 1;
        }

        Ok(entries)
    }
}
