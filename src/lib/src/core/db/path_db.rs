use crate::error::OxenError;
use serde::{de, Serialize};

use os_path::OsPath;
use rocksdb::{DBWithThreadMode, IteratorMode, ThreadMode};
use std::collections::HashSet;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::str;

use crate::core::db::str_json_db;

/// # Checks if the file exists in this directory
/// More efficient than get_entry since it does not actual deserialize the entry
pub fn has_entry<T: ThreadMode, P: AsRef<Path>>(db: &DBWithThreadMode<T>, path: P) -> bool {
    let path = path.as_ref();

    // strip trailing / if exists for looking up directories
    let path_str = path.to_str().map(|s| s.trim_end_matches('/'));
    if let Some(key) = path_str {
        // Check if the path_str has windows \\ in it, all databases use / so we are consistent across OS's
        let key = key.replace('\\', "/");
        return str_json_db::has_key(db, key);
    }

    false
}

/// # Get the staged entry object from the file path
pub fn get_entry<T: ThreadMode, P: AsRef<Path>, D>(
    db: &DBWithThreadMode<T>,
    path: P,
) -> Result<Option<D>, OxenError>
where
    D: de::DeserializeOwned,
{
    let path = path.as_ref();
    if let Some(key) = path.to_str() {
        // de-windows-ify the path
        let key = key.replace('\\', "/");

        return str_json_db::get(db, key);
    }
    Err(OxenError::could_not_convert_path_to_str(path))
}

/// # Serializes the entry to json and writes to db
pub fn put<T: ThreadMode, P: AsRef<Path>, S>(
    db: &DBWithThreadMode<T>,
    path: P,
    entry: &S,
) -> Result<(), OxenError>
where
    S: Serialize,
{
    let path = path.as_ref();
    if let Some(key) = path.to_str() {
        // de-windows-ify the path
        let key = key.replace('\\', "/");
        str_json_db::put(db, key, entry)
    } else {
        Err(OxenError::could_not_convert_path_to_str(path))
    }
}

/// # Removes path entry from database
pub fn delete<T: ThreadMode, P: AsRef<Path>>(
    db: &DBWithThreadMode<T>,
    path: P,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    if let Some(key) = path.to_str() {
        // de-windows-ify the path
        let key = key.replace('\\', "/");
        str_json_db::delete(db, key)
    } else {
        Err(OxenError::could_not_convert_path_to_str(path))
    }
}

/// # List the file paths in the staged dir
/// More efficient than list_added_path_entries since it does not deserialize the entries
pub fn list_paths<T: ThreadMode>(
    db: &DBWithThreadMode<T>,
    base_dir: &Path,
) -> Result<Vec<PathBuf>, OxenError> {
    // log::debug!("path_db::list_paths({:?})", base_dir);
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: Vec<PathBuf> = vec![];
    for item in iter {
        match item {
            Ok((key, _value)) => {
                match str::from_utf8(&key) {
                    Ok(key) => {
                        // return path with native slashes
                        let os_path = OsPath::from(key);
                        let new_path = os_path.to_pathbuf();

                        paths.push(base_dir.join(new_path));
                    }
                    _ => {
                        log::error!("list_added_paths() Could not decode key {:?}", key)
                    }
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(paths)
}

/// # List file names and attached entries
pub fn list_path_entries<T: ThreadMode, D>(
    db: &DBWithThreadMode<T>,
    base_dir: &Path,
) -> Result<Vec<(PathBuf, D)>, OxenError>
where
    D: de::DeserializeOwned,
{
    // log::debug!("path_db::list_path_entries({:?})", db.path());
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: Vec<(PathBuf, D)> = vec![];
    for item in iter {
        match item {
            Ok((key, value)) => {
                match (str::from_utf8(&key), str::from_utf8(&value)) {
                    (Ok(key), Ok(value)) => {
                        // Full path given the dir it is in
                        let path = base_dir.join(String::from(key));
                        let entry: Result<D, serde_json::error::Error> =
                            serde_json::from_str(value);
                        if let Ok(entry) = entry {
                            paths.push((path, entry));
                        }
                    }
                    (Ok(key), _) => {
                        log::error!(
                            "list_added_path_entries() Could not values for key {}.",
                            key
                        )
                    }
                    (_, Ok(val)) => {
                        log::error!("list_added_path_entries() Could not key for value {}.", val)
                    }
                    _ => {
                        log::error!("list_added_path_entries() Could not decoded keys and values.")
                    }
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(paths)
}

/// # List entries without file names
pub fn list_entries<T: ThreadMode, D>(db: &DBWithThreadMode<T>) -> Result<Vec<D>, OxenError>
where
    D: de::DeserializeOwned,
{
    str_json_db::list_vals(db)
}

pub fn list_entries_set<T: ThreadMode, D>(db: &DBWithThreadMode<T>) -> Result<HashSet<D>, OxenError>
where
    D: de::DeserializeOwned,
    D: Hash,
    D: Eq,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut paths: HashSet<D> = HashSet::new();
    for item in iter {
        match item {
            Ok((_, value)) => {
                match str::from_utf8(&value) {
                    Ok(value) => {
                        // Full path given the dir it is in
                        let entry: Result<D, serde_json::error::Error> =
                            serde_json::from_str(value);
                        if let Ok(entry) = entry {
                            paths.insert(entry);
                        }
                    }
                    _ => {
                        log::error!("list_added_path_entries() Could not decoded keys and values.")
                    }
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(paths)
}

pub fn clear<T: ThreadMode>(db: &DBWithThreadMode<T>) -> Result<(), OxenError> {
    str_json_db::clear(db)
}

pub fn list_entry_page<T: ThreadMode, D>(
    db: &DBWithThreadMode<T>,
    page: usize,
    page_size: usize,
) -> Result<Vec<D>, OxenError>
where
    D: de::DeserializeOwned,
{
    // The iterator doesn't technically have a skip method as far as I can tell
    // so we are just going to manually do it
    let mut paths: Vec<D> = vec![];
    let iter = db.iterator(IteratorMode::Start);
    // Do not go negative, and start from 0
    let start_page = if page == 0 { 0 } else { page - 1 };
    let start_idx = start_page * page_size;
    for (entry_i, item) in iter.enumerate() {
        match item {
            Ok((_, value)) => {
                // limit to page_size
                if paths.len() >= page_size {
                    break;
                }

                // only grab values after start_idx based on page and page_size
                if entry_i >= start_idx {
                    let entry: D = serde_json::from_str(str::from_utf8(&value)?)?;
                    paths.push(entry);
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(paths)
}

/// List the entries given an offset, page, and page_size
pub fn list_entry_page_with_offset<T: ThreadMode, D>(
    db: &DBWithThreadMode<T>,
    page: usize,
    page_size: usize,
    offset: usize,
) -> Result<Vec<D>, OxenError>
where
    D: de::DeserializeOwned,
{
    // Ugh this is so hacky...should be using a real database for the entries.
    let start_page = if page == 0 { 0 } else { page - 1 };
    let mut start_idx = start_page * page_size;
    log::debug!("list_entry_page_with_offset(1) page: {page}, page_size: {page_size}, offset: {offset} start_idx: {start_idx} start_page: {start_page}");

    if start_idx >= offset {
        start_idx -= offset;
    }
    log::debug!("list_entry_page_with_offset(2) page: {page}, page_size: {page_size}, offset: {offset} start_idx: {start_idx} start_page: {start_page}");

    // The iterator doesn't technically have a skip method as far as I can tell
    // so we are just going to manually do it
    let mut paths: Vec<D> = vec![];
    let iter = db.iterator(IteratorMode::Start);
    // Do not go negative, and start from 0
    for (entry_i, item) in iter.enumerate() {
        match item {
            Ok((_, value)) => {
                // limit to page_size
                if paths.len() >= page_size {
                    break;
                }

                // only grab values after start_idx based on page and page_size
                if entry_i >= start_idx {
                    let entry: D = serde_json::from_str(str::from_utf8(&value)?)?;
                    paths.push(entry);
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(paths)
}
