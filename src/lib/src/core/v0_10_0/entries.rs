//! DEPRECIATED: Use `repositories::entries` instead.
//!

use crate::core;
use crate::core::v0_10_0::index;
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::error::OxenError;
use crate::model::merkle_tree::node::DirNode;
use crate::model::merkle_tree::node::FileNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::MerkleHash;
use crate::model::MerkleTreeNodeType;
use crate::opts::DFOpts;
use crate::opts::PaginateOpts;
use crate::view::entries::ResourceVersion;
use crate::view::DataTypeCount;
use crate::{repositories, util};

use os_path::OsPath;

use crate::core::df;
use crate::core::v0_10_0::cache::cachers;
use crate::core::v0_10_0::index::ObjectDBReader;
use crate::core::v0_10_0::index::{CommitDirEntryReader, CommitEntryReader, CommitReader};
use crate::model::{
    Commit, CommitEntry, EntryDataType, LocalRepository, MetadataEntry, ParsedResource,
};
use crate::view::PaginatedDirEntries;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::str::FromStr;

/// Legacy function to get a directory node for a commit
/// DEPRECIATED: Use `repositories::entries::get_directory` instead.
pub fn get_directory(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<DirNode>, OxenError> {
    let path = path.as_ref();
    let object_reader = get_object_reader(repo, &commit.id)?;
    let reader = CommitDirEntryReader::new(repo, &commit.id, path, object_reader.clone())?;
    let Some(entry) = reader.get_entry(path)? else {
        return Ok(None);
    };

    let node = DirNode {
        dtype: MerkleTreeNodeType::Dir,
        name: entry
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        hash: MerkleHash::from_str(&entry.hash)?,
        num_bytes: entry.num_bytes,
        last_commit_id: MerkleHash::from_str(&entry.commit_id)?,
        last_modified_seconds: entry.last_modified_seconds,
        last_modified_nanoseconds: entry.last_modified_nanoseconds,
        data_type_counts: HashMap::new(),
        data_type_sizes: HashMap::new(),
    };
    Ok(Some(node))
}

/// Legacy function to get a file node for a commit
/// DEPRECIATED: Use `repositories::entries::get_file` instead.
pub fn get_file(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    let path = path.as_ref();
    let mut entry: Option<CommitEntry> = None;
    // Try to get the parent of the file path, if it exists to get the proper CommitDirEntryReader
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
        let object_reader = get_object_reader(repo, &commit.id)?;
        let cder = CommitDirEntryReader::new(repo, &commit.id, parent, object_reader.clone())?;
        entry = cder.get_entry(file_name)?;
    }

    let entry = entry.ok_or(OxenError::path_does_not_exist(path))?;

    let node = FileNode {
        dtype: MerkleTreeNodeType::File,
        name: entry
            .path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        hash: MerkleHash::from_str(&entry.hash)?,
        num_bytes: entry.num_bytes,
        last_commit_id: MerkleHash::from_str(&entry.commit_id)?,
        last_modified_seconds: entry.last_modified_seconds,
        last_modified_nanoseconds: entry.last_modified_nanoseconds,
        ..FileNode::default()
    };
    Ok(Some(node))
}

/// List all files and directories in a directory given a specific commit
// This is wayyyy more complicated that it needs to be because we have these two separate dbs....
pub fn list_directory(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    let directory = directory.as_ref();
    let revision = revision.as_ref();
    let page = paginate_opts.page_num;
    let page_size = paginate_opts.page_size;

    let resource = Some(ResourceVersion {
        path: directory.to_str().unwrap().to_string(),
        version: revision.to_string(),
    });

    let commit = repositories::revisions::get(repo, revision)?
        .ok_or(OxenError::revision_not_found(revision.into()))?;

    // Instantiate these readers once so they can be efficiently passed down through and databases not re-opened
    let object_reader = get_object_reader(repo, &commit.id)?;
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let commit_reader = CommitReader::new(repo)?;

    // Find all the commits once, so that we can re-use to find the latest commit per entry
    let mut commits = commit_reader.history_from_commit_id(&commit.id)?;

    // Sort on timestamp oldest to newest
    commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
    for c in commits {
        let or = get_object_reader(repo, &c.id)?;
        let reader = CommitDirEntryReader::new(repo, &c.id, directory, or.clone())?;
        commit_entry_readers.push((c.clone(), reader));
    }

    // List the directories first, then the files
    let mut dir_paths: Vec<MetadataEntry> = vec![];
    for dir in entry_reader.list_dirs()? {
        if let Some(parent) = dir.parent() {
            if parent == directory || (parent == Path::new("") && directory == Path::new("")) {
                dir_paths.push(meta_entry_from_dir(
                    repo,
                    object_reader.clone(),
                    &commit,
                    &dir,
                    &commit_reader,
                    revision,
                )?);
            }
        }
    }

    let mut file_paths: Vec<MetadataEntry> = vec![];
    let dir_entry_reader =
        CommitDirEntryReader::new(repo, &commit.id, directory, object_reader.clone())?;
    let total = dir_entry_reader.num_entries() + dir_paths.len();
    let total_pages = (total as f64 / page_size as f64).ceil() as usize;

    let offset = dir_paths.len();

    for entry in dir_entry_reader.list_entry_page_with_offset(page, page_size, offset)? {
        file_paths.push(meta_entry_from_commit_entry(
            repo,
            &entry,
            &commit_entry_readers,
            revision,
        )?)
    }

    // Combine all paths, starting with dirs if there are enough, else just files
    let start_page = if page == 0 { 0 } else { page - 1 };
    let start_idx = start_page * page_size;
    let mut entries = if dir_paths.len() < start_idx {
        file_paths
    } else {
        dir_paths.append(&mut file_paths);
        dir_paths
    };

    if entries.len() > page_size {
        let mut end_idx = start_idx + page_size;
        if end_idx > entries.len() {
            end_idx = entries.len();
        }

        entries = entries[start_idx..end_idx].to_vec();
    }

    let metadata = get_dir_entry_metadata(repo, &commit, directory)?;
    let dir = meta_entry_from_dir(
        repo,
        object_reader,
        &commit,
        directory,
        &commit_reader,
        revision,
    )?;

    Ok(PaginatedDirEntries {
        dir: Some(dir),
        entries,
        resource,
        metadata: Some(metadata),
        page_size,
        page_number: page,
        total_pages,
        total_entries: total,
    })
}

pub fn get_meta_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let object_reader = get_object_reader(repo, &commit.id)?;
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let commit_reader = CommitReader::new(repo)?;
    let mut commits = commit_reader.history_from_commit_id(&commit.id)?;
    commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    // Check if the path is a dir or is the root
    if entry_reader.has_dir(path) || path == Path::new("") {
        log::debug!("get_meta_entry found dir: {:?}", path);
        meta_entry_from_dir(
            repo,
            object_reader,
            commit,
            path,
            &commit_reader,
            &commit.id,
        )
    } else {
        log::debug!("get_meta_entry has file: {:?}", path);

        let parent = path.parent().ok_or(OxenError::file_has_no_parent(path))?;
        let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
        let dir_entry_reader =
            CommitDirEntryReader::new(repo, &commit.id, parent, object_reader.clone())?;

        // load all commit entry readers once
        let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();

        for c in commits {
            let object_reader = get_object_reader(repo, &c.id)?;
            let reader = CommitDirEntryReader::new(repo, &c.id, parent, object_reader.clone())?;
            commit_entry_readers.push((c.clone(), reader));
        }

        let entry = dir_entry_reader
            .get_entry(base_name)?
            .ok_or(OxenError::entry_does_not_exist_in_commit(path, &commit.id))?;
        meta_entry_from_commit_entry(repo, &entry, &commit_entry_readers, &commit.id)
    }
}

pub fn get_dir_entry_metadata(
    repo: &LocalRepository,
    commit: &Commit,
    directory: &Path,
) -> Result<MetadataDir, OxenError> {
    let data_types_path =
        cachers::content_stats::dir_column_path(repo, commit, directory, "data_type");

    // let mime_types_path =
    //     cache::cachers::content_stats::dir_column_path(repo, commit, directory, "mime_type");

    // log::debug!(
    //     "list_directory reading data types from {}",
    //     data_types_path.display()
    // );

    if let Ok(data_type_df) = df::tabular::read_df(&data_types_path, DFOpts::empty()) {
        let dt_series: Vec<&str> = data_type_df
            .column("data_type")
            .unwrap()
            .str()
            .unwrap()
            .into_no_null_iter()
            .collect();
        let count_series: Vec<i64> = data_type_df
            .column("count")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect();

        let data_types: Vec<DataTypeCount> = dt_series
            .iter()
            .zip(count_series.iter())
            .map(|(&data_type, &count)| DataTypeCount {
                data_type: data_type.to_string(),
                count: count.try_into().unwrap(),
            })
            .collect();

        Ok(MetadataDir::new(data_types))
    } else {
        log::warn!("Unable to read {data_types_path:?}");
        Ok(MetadataDir::new(vec![]))
    }
}

fn compute_dir_size(
    repo: &LocalRepository,
    object_reader: Arc<ObjectDBReader>,
    commit: &Commit,
    path: &Path,
) -> Result<u64, OxenError> {
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let mut total_size: u64 = 0;
    // This lists all the committed dirs
    let dirs = entry_reader.list_dirs()?;
    let object_reader = get_object_reader(repo, &commit.id)?;
    for dir in dirs {
        // Have to make sure we are in a subset of the dir (not really a tree structure)
        if dir.starts_with(path) {
            let entry_reader =
                CommitDirEntryReader::new(repo, &commit.id, &dir, object_reader.clone())?;
            for entry in entry_reader.list_entries()? {
                total_size += entry.num_bytes;
            }
        }
    }
    Ok(total_size)
}

fn compute_latest_commit_for_dir(
    repo: &LocalRepository,
    object_reader: Arc<ObjectDBReader>,
    commit: &Commit,
    path: &Path,
    commit_reader: &CommitReader,
) -> Result<Option<Commit>, OxenError> {
    let entry_reader =
        CommitEntryReader::new_from_commit_id(repo, &commit.id, object_reader.clone())?;
    let commits: HashMap<String, Commit> = HashMap::new();
    let mut latest_commit = Some(commit.to_owned());
    // This lists all the committed dirs
    let dirs = entry_reader.list_dirs()?;
    for dir in dirs {
        // Have to make sure we are in a subset of the dir (not really a tree structure)
        if dir.starts_with(path) {
            let entry_reader =
                CommitDirEntryReader::new(repo, &commit.id, &dir, object_reader.clone())?;
            for entry in entry_reader.list_entries()? {
                let commit = if commits.contains_key(&entry.commit_id) {
                    Some(commits[&entry.commit_id].clone())
                } else {
                    commit_reader.get_commit_by_id(&entry.commit_id)?
                };

                if latest_commit.is_none() {
                    latest_commit.clone_from(&commit);
                }

                if latest_commit.as_ref().unwrap().timestamp < commit.as_ref().unwrap().timestamp {
                    latest_commit.clone_from(&commit);
                }
            }
        }
    }
    Ok(latest_commit)
}

pub fn get_latest_commit_for_entry(
    commits: &[(Commit, CommitDirEntryReader)],
    entry: &CommitEntry,
) -> Result<Option<Commit>, OxenError> {
    get_latest_commit_for_path(commits, &entry.path)
}

pub fn get_latest_commit_for_path(
    commits: &[(Commit, CommitDirEntryReader)],
    path: &Path,
) -> Result<Option<Commit>, OxenError> {
    let os_path = OsPath::from(path).to_pathbuf();
    let path = os_path
        .file_name()
        .ok_or(OxenError::file_has_no_name(path))?;
    log::debug!("get_latest_commit_for_path: {:?}", path);
    let mut latest_hash: Option<String> = None;
    // Store the commit from the previous iteration. Initialized as None.
    let mut previous_commit: Option<Commit> = None;

    for (commit, entry_reader) in commits.iter().rev() {
        if let Some(old_entry) = entry_reader.get_entry(path)? {
            log::debug!("Found entry! For path {:?} in commit {}", path, commit);

            if latest_hash.is_none() {
                // This is the first encountered entry, setting it as the baseline for comparison.
                latest_hash = Some(old_entry.hash.clone());
            } else if latest_hash.as_ref() != Some(&old_entry.hash) {
                // A change is detected, return the previous commit which introduced the change.
                return Ok(previous_commit);
            }
            // Update previous_commit after the check, so it holds the commit before the change was detected.
            previous_commit = Some(commit.clone());
        } else {
            log::debug!("No entry found for path {:?} in commit {}", path, commit);
        }
    }

    // If no change was detected (all entries have the same hash), or the entry was not found,
    // return None or consider returning the oldest commit if previous_commit has been set.
    Ok(previous_commit)
}

pub fn meta_entry_from_commit_entry(
    repo: &LocalRepository,
    entry: &CommitEntry,
    commit_entry_readers: &[(Commit, CommitDirEntryReader)],
    revision: &str,
) -> Result<MetadataEntry, OxenError> {
    log::debug!("meta_entry_from_commit_entry: {:?}", entry.path);
    let size = util::fs::version_file_size(repo, entry)?;
    let Some(latest_commit) = get_latest_commit_for_entry(commit_entry_readers, entry)? else {
        log::error!("No latest commit for entry: {:?}", entry.path);
        return Err(OxenError::basic_str(format!(
            "No latest commit for entry: {:?}",
            entry.path
        )));
    };

    let base_name = entry
        .path
        .file_name()
        .ok_or(OxenError::file_has_no_name(&entry.path))?;

    let version_path = util::fs::version_path(repo, entry);

    let data_type = util::fs::file_data_type(&version_path);

    let is_indexed = if data_type == EntryDataType::Tabular {
        Some(
            repositories::workspaces::data_frames::is_queryable_data_frame_indexed(
                repo,
                &entry.path,
                &latest_commit,
            )?,
        )
    } else {
        None
    };

    return Ok(MetadataEntry {
        filename: String::from(base_name.to_string_lossy()),
        hash: entry.hash.to_string(),
        is_dir: false,
        size,
        latest_commit: Some(latest_commit.clone()),
        data_type,
        mime_type: util::fs::file_mime_type(&version_path),
        extension: util::fs::file_extension(&version_path),
        resource: Some(ParsedResource {
            commit: Some(latest_commit.clone()),
            branch: None,
            version: PathBuf::from(revision),
            path: entry.path.clone(),
            resource: PathBuf::from(revision).join(entry.path.clone()),
        }),
        // Not applicable for files YET, but we will also compute this metadata
        metadata: None,
        is_queryable: is_indexed,
    });
}

/// Get a DirEntry summing up the size of all files in a directory
/// and finding the latest commit within the directory
pub fn meta_entry_from_dir(
    repo: &LocalRepository,
    object_reader: Arc<ObjectDBReader>,
    commit: &Commit,
    path: &Path,
    commit_reader: &CommitReader,
    revision: &str,
) -> Result<MetadataEntry, OxenError> {
    // We cache the latest commit and size for each file in the directory after commit
    let latest_commit_path = cachers::repo_size::dir_latest_commit_path(repo, commit, path);
    // log::debug!(
    //     "meta_entry_from_dir {:?} latest_commit_path: {:?}",
    //     path,
    //     latest_commit_path
    // );

    let latest_commit = match util::fs::read_from_path(latest_commit_path) {
        Ok(id) => {
            // log::debug!("meta_entry_from_dir found latest_commit on disk: {:?}", id);
            commit_reader.get_commit_by_id(id)?
        }
        Err(_) => {
            // log::debug!("meta_entry_from_dir computing latest_commit");
            compute_latest_commit_for_dir(repo, object_reader.clone(), commit, path, commit_reader)?
        }
    };

    let total_size_path = cachers::repo_size::dir_size_path(repo, commit, path);
    let total_size = match util::fs::read_from_path(total_size_path) {
        Ok(total_size_str) => total_size_str
            .parse::<u64>()
            .map_err(|_| OxenError::basic_str("Could not get cached total size of dir"))?,
        Err(_) => {
            // cache failed, go compute it
            compute_dir_size(repo, object_reader.clone(), commit, path)?
        }
    };

    let dir_metadata = repositories::entries::get_dir_entry_metadata(repo, commit, path)?;

    let base_name = path.file_name().unwrap_or(std::ffi::OsStr::new(""));
    return Ok(MetadataEntry {
        filename: String::from(base_name.to_string_lossy()),
        hash: commit.id.to_string(),
        is_dir: true,
        size: total_size,
        latest_commit,
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: util::fs::file_extension(path),
        resource: Some(ParsedResource {
            commit: Some(commit.clone()),
            branch: None,
            version: PathBuf::from(revision),
            path: path.to_path_buf(),
            resource: PathBuf::from(revision).join(path),
        }),
        metadata: Some(GenericMetadata::MetadataDir(dir_metadata)),
        is_queryable: None,
    });
}

pub fn get_commit_history_path(
    commits: &[(Commit, CommitDirEntryReader)],
    path: impl AsRef<Path>,
) -> Result<Vec<Commit>, OxenError> {
    let os_path = OsPath::from(path.as_ref()).to_pathbuf();
    let path = os_path
        .file_name()
        .ok_or(OxenError::file_has_no_name(&path))?;

    // log::debug!("get_commit_history_path: checking path {:?}", path);

    let mut latest_hash: Option<String> = None;
    let mut history: Vec<Commit> = Vec::new();

    for (commit, entry_reader) in commits.iter().rev() {
        if let Some(old_entry) = entry_reader.get_entry(path)? {
            if latest_hash.is_none() {
                // This is the first encountered entry; set it as the baseline for comparison.
                // log::debug!("get_commit_history_path: first entry {:?}", commit);
                latest_hash = Some(old_entry.hash.clone());
                history.push(commit.clone()); // Include the first commit as the starting point of history
            } else if latest_hash.as_ref() != Some(&old_entry.hash) {
                // A change in hash is detected, indicating an edit. Include this commit in history.
                // log::debug!("get_commit_history_path: new entry {:?}", commit);
                latest_hash = Some(old_entry.hash.clone());
                history.push(commit.clone());
            }
        }
    }

    history.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    Ok(history)
}

pub fn list_tabular_files_in_repo(
    local_repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<MetadataEntry>, OxenError> {
    let schema_reader = index::SchemaReader::new(local_repo, &commit.id)?;
    let schemas = schema_reader.list_schemas()?;

    let mut meta_entries: Vec<MetadataEntry> = vec![];
    let entry_reader = CommitEntryReader::new(local_repo, commit)?;
    let commit_reader = CommitReader::new(local_repo)?;
    let commits = commit_reader.list_all()?;

    for (path, _schema) in schemas.iter() {
        let entry = entry_reader.get_entry(path)?;

        if entry.is_some() {
            let parent = path.parent().ok_or(OxenError::file_has_no_parent(path))?;
            let mut commit_entry_readers: Vec<(Commit, CommitDirEntryReader)> = Vec::new();
            for commit in &commits {
                let object_reader = get_object_reader(local_repo, &commit.id)?;
                let reader = CommitDirEntryReader::new(
                    local_repo,
                    &commit.id,
                    parent,
                    object_reader.clone(),
                )?;
                commit_entry_readers.push((commit.clone(), reader));
            }

            let metadata = core::v0_10_0::entries::meta_entry_from_commit_entry(
                local_repo,
                &entry.unwrap(),
                &commit_entry_readers,
                &commit.id,
            )?;
            if metadata.data_type == EntryDataType::Tabular {
                meta_entries.push(metadata);
            }
        }
    }

    Ok(meta_entries)
}

pub fn count_for_commit(repo: &LocalRepository, commit: &Commit) -> Result<usize, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    reader.num_entries()
}

pub fn list_for_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    reader.list_entries()
}
