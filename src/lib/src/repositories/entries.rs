//! Entries are the files and directories that are stored in a commit.
//!

use crate::core;
use crate::core::v0_10_0::index::object_db_reader::get_object_reader;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::entry::commit_entry::{Entry, SchemaEntry};
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::metadata::MetadataDir;
use crate::opts::{DFOpts, PaginateOpts};
use crate::repositories;
use crate::view::DataTypeCount;
use rayon::prelude::*;

use crate::constants::ROOT_PATH;
use crate::core::df;
use crate::core::v0_10_0::cache::cachers;
use crate::core::v0_10_0::index::SchemaReader;
use crate::core::v0_10_0::index::{CommitDirEntryReader, CommitEntryReader};
use crate::model::{Commit, CommitEntry, LocalRepository, MetadataEntry, ParsedResource};
use crate::view::PaginatedDirEntries;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Get a directory object for a commit
pub fn get_directory(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<DirNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::entries::get_directory(repo, commit, path),
        MinOxenVersion::V0_19_0 => core::v0_19_0::entries::get_directory(repo, commit, path),
    }
}

/// Get a file node for a commit
pub fn get_file(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::entries::get_file(repo, commit, path),
        MinOxenVersion::V0_19_0 => core::v0_19_0::entries::get_file(repo, commit, path),
    }
}

/// List all the entries within a commit
pub fn list_commit_entries(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    list_directory_w_version(repo, ROOT_PATH, revision, paginate_opts, repo.min_version())
}

/// List all the entries within a directory given a specific commit
pub fn list_directory(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    list_directory_w_version(repo, directory, revision, paginate_opts, repo.min_version())
}

/// Force a version when listing a repo
pub fn list_directory_w_version(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
    version: MinOxenVersion,
) -> Result<PaginatedDirEntries, OxenError> {
    match version {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::entries::list_directory(repo, directory, revision, paginate_opts)
        }
        MinOxenVersion::V0_19_0 => {
            let revision = revision.as_ref().to_string();
            let branch = repositories::branches::get_by_name(repo, &revision)?;
            let commit = repositories::revisions::get(repo, &revision)?;
            let parsed_resource = ParsedResource {
                path: directory.as_ref().to_path_buf(),
                commit,
                branch,
                version: PathBuf::from(&revision),
                resource: PathBuf::from(&revision).join(&directory),
            };
            core::v0_19_0::entries::list_directory(repo, directory, &parsed_resource, paginate_opts)
        }
    }
}

/// Get the entry for a given path in a commit.
/// Could be a file or a directory.
pub fn get_meta_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<MetadataEntry, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::entries::get_meta_entry(repo, commit, &path),
        MinOxenVersion::V0_19_0 => {
            let path = path.as_ref();
            let parsed_resource = ParsedResource {
                path: path.to_path_buf(),
                commit: Some(commit.clone()),
                branch: None,
                version: PathBuf::from(&commit.id),
                resource: PathBuf::from(&commit.id).join(path),
            };
            core::v0_19_0::entries::get_meta_entry(repo, &parsed_resource, path)
        }
    }
}

/// List the paths of all the directories in a given commit
pub fn list_dir_paths(repo: &LocalRepository, commit: &Commit) -> Result<Vec<PathBuf>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let dir_reader = CommitEntryReader::new(repo, commit)?;
            dir_reader.list_dirs()
        }
        MinOxenVersion::V0_19_0 => {
            let tree = core::v0_19_0::index::CommitMerkleTree::from_commit(repo, commit)?;
            tree.list_dir_paths()
        }
    }
}

/// Commit entries are always files, not directories. Will return None if the path is a directory.
pub fn get_commit_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<Option<CommitEntry>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            let reader = CommitEntryReader::new(repo, commit)?;
            reader.get_entry(path)
        }
        MinOxenVersion::V0_19_0 => match core::v0_19_0::entries::get_file(repo, commit, path)? {
            None => Ok(None),
            Some(file) => {
                let entry = CommitEntry {
                    commit_id: commit.id.clone(),
                    path: path.to_path_buf(),
                    hash: file.hash.to_string(),
                    num_bytes: file.num_bytes,
                    last_modified_seconds: file.last_modified_seconds,
                    last_modified_nanoseconds: file.last_modified_nanoseconds,
                };
                Ok(Some(entry))
            }
        },
    }
}

pub fn list_for_commit(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::entries::list_for_commit(repo, commit),
        MinOxenVersion::V0_19_0 => core::v0_19_0::entries::list_for_commit(repo, commit),
    }
}

pub fn count_for_commit(repo: &LocalRepository, commit: &Commit) -> Result<usize, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::entries::count_for_commit(repo, commit),
        MinOxenVersion::V0_19_0 => core::v0_19_0::entries::count_for_commit(repo, commit),
    }
}

pub fn list_page(
    repo: &LocalRepository,
    commit: &Commit,
    page: &usize,
    page_size: &usize,
) -> Result<Vec<CommitEntry>, OxenError> {
    let reader = CommitEntryReader::new(repo, commit)?;
    reader.list_entry_page(*page, *page_size)
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

/// Given a list of entries, compute the total in bytes size of all entries.
pub fn compute_entries_size(entries: &[CommitEntry]) -> Result<u64, OxenError> {
    let total_size: u64 = entries.into_par_iter().map(|e| e.num_bytes).sum();
    Ok(total_size)
}

pub fn compute_generic_entries_size(entries: &[Entry]) -> Result<u64, OxenError> {
    let total_size: u64 = entries.into_par_iter().map(|e| e.num_bytes()).sum();
    Ok(total_size)
}

pub fn compute_schemas_size(schemas: &[SchemaEntry]) -> Result<u64, OxenError> {
    let total_size: u64 = schemas.into_par_iter().map(|e| e.num_bytes).sum();
    Ok(total_size)
}

/// Given a list of entries, group them by their parent directory.
pub fn group_commit_entries_to_parent_dirs(
    entries: &[CommitEntry],
) -> HashMap<PathBuf, Vec<CommitEntry>> {
    let mut results: HashMap<PathBuf, Vec<CommitEntry>> = HashMap::new();

    for entry in entries.iter() {
        if let Some(parent) = entry.path.parent() {
            results
                .entry(parent.to_path_buf())
                .or_default()
                .push(entry.clone());
        }
    }

    results
}

pub fn group_entries_to_parent_dirs(entries: &[Entry]) -> HashMap<PathBuf, Vec<Entry>> {
    let mut results: HashMap<PathBuf, Vec<Entry>> = HashMap::new();

    for entry in entries.iter() {
        if let Some(parent) = entry.path().parent() {
            results
                .entry(parent.to_path_buf())
                .or_default()
                .push(entry.clone());
        }
    }

    results
}

pub fn group_schemas_to_parent_dirs(
    schema_entries: &[SchemaEntry],
) -> HashMap<PathBuf, Vec<SchemaEntry>> {
    let mut results: HashMap<PathBuf, Vec<SchemaEntry>> = HashMap::new();

    for entry in schema_entries.iter() {
        if let Some(parent) = entry.path.parent() {
            results
                .entry(parent.to_path_buf())
                .or_default()
                .push(entry.clone());
        }
    }

    results
}

pub fn read_unsynced_entries(
    local_repo: &LocalRepository,
    last_commit: &Commit,
    this_commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    // Find and compare all entries between this commit and last
    let this_entry_reader = CommitEntryReader::new(local_repo, this_commit)?;

    let this_entries: Vec<CommitEntry> = this_entry_reader.list_entries()?;
    let grouped = repositories::entries::group_commit_entries_to_parent_dirs(&this_entries);
    log::debug!(
        "Checking {} entries in {} groups",
        this_entries.len(),
        grouped.len()
    );

    let object_reader = get_object_reader(local_repo, &last_commit.id)?;

    let mut entries_to_sync: Vec<CommitEntry> = vec![];
    for (dir, dir_entries) in grouped.iter() {
        // log::debug!("Checking {} entries from {:?}", dir_entries.len(), dir);

        let last_entry_reader =
            CommitDirEntryReader::new(local_repo, &last_commit.id, dir, object_reader.clone())?;
        let mut entries: Vec<CommitEntry> = dir_entries
            .into_par_iter()
            .filter(|entry| {
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

    log::debug!("Got {} entries to sync", entries_to_sync.len());

    Ok(entries_to_sync)
}

pub fn read_unsynced_schemas(
    local_repo: &LocalRepository,
    last_commit: &Commit,
    this_commit: &Commit,
) -> Result<Vec<SchemaEntry>, OxenError> {
    let this_schema_reader = SchemaReader::new(local_repo, &this_commit.id)?;
    let last_schema_reader = SchemaReader::new(local_repo, &last_commit.id)?;

    let this_schemas = this_schema_reader.list_schema_entries()?;
    let last_schemas = last_schema_reader.list_schema_entries()?;

    let mut schemas_to_sync: Vec<SchemaEntry> = vec![];

    let this_grouped = repositories::entries::group_schemas_to_parent_dirs(&this_schemas);
    let last_grouped = repositories::entries::group_schemas_to_parent_dirs(&last_schemas);

    let empty_vec = Vec::new();
    for (dir, dir_schemas) in this_grouped.iter() {
        let last_dir_schemas = last_grouped.get(dir).unwrap_or(&empty_vec);
        for schema in dir_schemas {
            let filename = schema.path.file_name().unwrap().to_str().unwrap();
            let last_schema = last_dir_schemas
                .iter()
                .find(|s| s.path.file_name().unwrap().to_str().unwrap() == filename);
            if last_schema.is_none() || last_schema.unwrap().hash != schema.hash {
                schemas_to_sync.push(schema.clone());
            }
        }
    }
    Ok(schemas_to_sync)
}

pub fn list_tabular_files_in_repo(
    local_repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<MetadataEntry>, OxenError> {
    match local_repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::entries::list_tabular_files_in_repo(local_repo, commit)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::entries::list_tabular_files_in_repo(local_repo, commit)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::error::OxenError;
    use crate::opts::PaginateOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_api_local_entries_list_all() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits("labels", |repo| {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            repositories::add(&repo, file_to_add)?;
            let commit = repositories::commit(&repo, "Adding labels file")?;

            let entries = repositories::entries::list_for_commit(&repo, &commit)?;
            assert_eq!(entries.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_one_for_commit() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits("labels", |repo| {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            repositories::add(&repo, file_to_add)?;
            let commit = repositories::commit(&repo, "Adding labels file")?;

            let count = repositories::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, 1);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_many_for_commit() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits("train", |repo| {
            // (files already created in helper)
            let dir_to_add = repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&dir_to_add);

            // Commit the dir
            repositories::add(&repo, &dir_to_add)?;
            let commit = repositories::commit(&repo, "Adding training data")?;
            let count = repositories::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
    }

    #[test]
    fn test_api_local_entries_count_many_dirs() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (files already created in helper)
            let num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Commit the dir
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all data")?;

            let count = repositories::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
    }

    #[test]
    fn test_get_meta_entry_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let path = Path::new("annotations").join("train");
            let entry = repositories::entries::get_meta_entry(&repo, commit, &path)?;

            assert!(entry.is_dir);
            assert_eq!(entry.filename, "train");
            assert_eq!(Path::new(&entry.resource.unwrap().path), path);

            Ok(())
        })
    }

    #[test]
    fn test_get_meta_entry_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let path = test::test_nlp_classification_csv();
            let entry = repositories::entries::get_meta_entry(&repo, commit, &path)?;

            assert!(!entry.is_dir);
            assert_eq!(entry.filename, "test.tsv");
            assert_eq!(
                Path::new(&entry.resource.unwrap().path),
                test::test_nlp_classification_csv()
            );

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_top_level_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: 1,
                    page_size: 10,
                },
            )?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;
            for entry in dir_entries.iter() {
                println!("{entry:?}");
            }

            assert_eq!(size, 7);
            assert_eq!(dir_entries.len(), 7);
            assert_eq!(
                dir_entries
                    .clone()
                    .into_iter()
                    .filter(|e| !e.is_dir)
                    .count(),
                2
            );
            assert_eq!(dir_entries.into_iter().filter(|e| e.is_dir).count(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new("train"),
                &commit.id,
                &PaginateOpts {
                    page_num: 1,
                    page_size: 10,
                },
            )?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;

            assert_eq!(size, 5);
            assert_eq!(dir_entries.len(), 5);

            Ok(())
        })
    }

    #[test]
    fn test_list_train_sub_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new("annotations/train"),
                &commit.id,
                &PaginateOpts {
                    page_num: 1,
                    page_size: 10,
                },
            )?;
            let dir_entries = paginated.entries;
            let size = paginated.total_entries;

            assert_eq!(size, 4);
            assert_eq!(dir_entries.len(), 4);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_subset() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new("train"),
                &commit.id,
                &PaginateOpts {
                    page_num: 2,
                    page_size: 3,
                },
            )?;

            let dir_entries = paginated.entries;
            let total_entries = paginated.total_entries;

            for entry in dir_entries.iter() {
                println!("{entry:?}");
            }

            assert_eq!(total_entries, 5);
            assert_eq!(dir_entries.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_1_exactly_ten() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 8 directories
            for n in 0..8 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 1;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;
            assert_eq!(paginated.total_entries, 10);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 10);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_all_dirs_no_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 42 directories
            for n in 0..42 {
                let dirname = format!("dir_{:0>3}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 2;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;

            for entry in paginated.entries.iter() {
                println!("{:?}", entry.filename);
            }

            assert_eq!(paginated.entries.first().unwrap().filename, "dir_010");

            println!("{paginated:?}");
            assert_eq!(paginated.total_entries, 42);
            assert_eq!(paginated.total_pages, 5);
            assert_eq!(paginated.entries.len(), 10);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_101_dirs_no_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 101 directories
            for n in 0..101 {
                let dirname = format!("dir_{:0>3}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 11;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;

            for entry in paginated.entries.iter() {
                println!("{:?}", entry.filename);
            }

            assert_eq!(paginated.entries.first().unwrap().filename, "dir_100");

            println!("{paginated:?}");
            assert_eq!(paginated.total_entries, 101);
            assert_eq!(paginated.total_pages, 11);
            assert_eq!(paginated.entries.len(), 1);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_exactly_ten_page_two() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 8 directories
            for n in 0..8 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 2;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;
            assert_eq!(paginated.total_entries, 10);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_nine_entries_page_size_ten() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 7 directories
            for n in 0..7 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 1;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;
            assert_eq!(paginated.total_entries, 9);
            assert_eq!(paginated.total_pages, 1);
            assert_eq!(paginated.entries.len(), 9);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_eleven_entries_page_size_ten() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create 9 directories
            for n in 0..9 {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 1;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;
            assert_eq!(paginated.total_entries, 11);
            assert_eq!(paginated.total_pages, 2);
            assert_eq!(paginated.entries.len(), page_size);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_many_dirs_many_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create many directories
            let num_dirs = 32;
            for n in 0..num_dirs {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Create many files
            let num_files = 45;
            for n in 0..num_files {
                let filename = format!("file_{}.txt", n);
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {}", n))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 1;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;
            assert_eq!(paginated.total_entries, num_dirs + num_files);
            assert_eq!(paginated.total_pages, 8);
            assert_eq!(paginated.entries.len(), page_size);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_one_dir_many_files_page_2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create one directory
            let dir_path = repo.path.join("lonely_dir");
            util::fs::create_dir_all(&dir_path)?;
            let filename = "data.txt";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "All the lonely directories")?;

            // Create many files
            let num_files = 45;
            for n in 0..num_files {
                let filename = format!("file_{}.txt", n);
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {}", n))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 2;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;

            assert_eq!(paginated.total_entries, num_files + 1);
            assert_eq!(paginated.total_pages, 5);
            assert_eq!(paginated.entries.len(), page_size);

            Ok(())
        })
    }

    #[test]
    fn test_list_directories_many_dir_some_files_page_2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Create many directories
            let num_dirs = 9;
            for n in 0..num_dirs {
                let dirname = format!("dir_{}", n);
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {}", n))?;
            }

            // Create many files
            let num_files = 8;
            for n in 0..num_files {
                let filename = format!("file_{}.txt", n);
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {}", n))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            let page_number = 2;
            let page_size = 10;

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                &commit.id,
                &PaginateOpts {
                    page_num: page_number,
                    page_size,
                },
            )?;

            assert_eq!(paginated.total_entries, num_files + num_dirs);
            assert_eq!(paginated.total_pages, 2);
            assert_eq!(paginated.entries.len(), 7);

            Ok(())
        })
    }

    #[test]
    fn test_file_metadata_shows_is_indexed() -> Result<(), OxenError> {
        // skip on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test(|repo| {
            // Create a deeply nested directory
            let dir_path = repo
                .path
                .join("data")
                .join("train")
                .join("images")
                .join("cats");
            util::fs::create_dir_all(&dir_path)?;

            // Add two tabular files to it
            let filename_1 = "cats.tsv";
            let filepath_1 = dir_path.join(filename_1);
            util::fs::write(filepath_1, "1\t2\t3\nhello\tworld\tsup\n")?;

            let filename_2 = "dogs.csv";
            let filepath_2 = dir_path.join(filename_2);
            util::fs::write(filepath_2, "1,2,3\nhello,world,sup\n")?;

            let path_1 = PathBuf::from("data")
                .join("train")
                .join("images")
                .join("cats")
                .join(filename_1);

            let path_2 = PathBuf::from("data")
                .join("train")
                .join("images")
                .join("cats")
                .join(filename_2);

            // And write a file in the same dir that is not tabular
            let filename = "README.md";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all
            repositories::add(&repo, &repo.path)?;
            let commit = repositories::commit(&repo, "Adding all the data")?;

            // Get the metadata entries for the two dataframes
            let meta1 = repositories::entries::get_meta_entry(&repo, &commit, &path_1)?;
            let meta2 = repositories::entries::get_meta_entry(&repo, &commit, &path_2)?;

            let entry2 = repositories::entries::get_commit_entry(&repo, &commit, &path_2)?
                .expect("Failed: could not get commit entry");

            assert_eq!(meta1.is_queryable, Some(false));
            assert_eq!(meta2.is_queryable, Some(false));

            // Now index df2
            let workspace_id = Uuid::new_v4().to_string();
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, false)?;
            repositories::workspaces::data_frames::index(&repo, &workspace, &entry2.path)?;

            // Now get the metadata entries for the two dataframes
            let meta1 = repositories::entries::get_meta_entry(&repo, &commit, &path_1)?;
            let meta2 = repositories::entries::get_meta_entry(&repo, &commit, &path_2)?;

            assert_eq!(meta1.is_queryable, Some(false));
            assert_eq!(meta2.is_queryable, Some(true));

            Ok(())
        })
    }
}
