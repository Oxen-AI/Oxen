//! Entries are the files and directories that are stored in a commit.
//!

use crate::core;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::opts::{PaginateOpts, SortOpts};
use crate::repositories;
use crate::util::concurrency;

use crate::constants::ROOT_PATH;
use crate::model::{
    Commit, CommitEntry, LocalRepository, MetadataEntry, ParsedResource, Workspace,
};
use crate::view::PaginatedDirEntries;
use futures::{StreamExt, TryStreamExt, stream};
use std::path::{Path, PathBuf};

/// Get a directory object for a commit
pub use crate::core::v_latest::entries::get_directory;

/// Get a directory object for a commit, off the async worker.
pub async fn get_directory_async(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<DirNode>, OxenError> {
    let repo = repo.clone();
    let commit = commit.clone();
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || get_directory(&repo, &commit, &path)).await?
}

/// Get a file node for a commit
pub fn get_file(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<FileNode>, OxenError> {
    core::v_latest::entries::get_file(repo, commit, path)
}

/// List all the entries within a commit
pub fn list_commit_entries(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    list_directory_w_version(repo, ROOT_PATH, revision, paginate_opts)
}

/// List all the entries within a directory given a specific commit
pub fn list_directory(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    list_directory_w_version(repo, directory, revision, paginate_opts)
}

/// List the entries within a directory given a specific revision
pub fn list_directory_w_version(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    let revision_str = revision.as_ref().to_string();
    let branch = repositories::branches::get_by_name(repo, &revision_str).ok();
    let commit = repositories::revisions::get(repo, &revision_str)?;
    let parsed_resource = ParsedResource {
        path: directory.as_ref().to_path_buf(),
        commit,
        workspace: None,
        branch,
        version: PathBuf::from(&revision_str),
        resource: PathBuf::from(&revision_str).join(directory.as_ref()),
    };
    core::v_latest::entries::list_directory(repo, directory, &parsed_resource, paginate_opts)
}

#[allow(clippy::too_many_arguments)]
pub fn list_directory_w_workspace_depth(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    workspace: Option<Workspace>,
    paginate_opts: &PaginateOpts,
    sort_opts: &SortOpts,
    depth: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let _perf = crate::perf_guard!("entries::list_directory_w_workspace");

    let _perf_setup = crate::perf_guard!("entries::list_directory_w_workspace_setup");
    let revision_str = revision.as_ref().to_string();
    let version_str = if let Some(workspace) = workspace.clone() {
        workspace.id.clone()
    } else {
        revision_str.clone()
    };

    let branch = repositories::branches::get_by_name(repo, &revision_str).ok();
    let commit = repositories::revisions::get(repo, &revision_str)?;
    let parsed_resource = ParsedResource {
        path: directory.as_ref().to_path_buf(),
        commit,
        workspace,
        branch,
        version: PathBuf::from(&version_str),
        resource: PathBuf::from(&version_str).join(directory.as_ref()),
    };
    drop(_perf_setup);

    core::v_latest::entries::list_directory_with_depth(
        repo,
        directory,
        &parsed_resource,
        paginate_opts,
        sort_opts,
        depth,
    )
}

/// List the entries within a directory to a given depth, off the async worker.
#[allow(clippy::too_many_arguments)]
pub async fn list_directory_w_workspace_depth_async(
    repo: &LocalRepository,
    directory: impl AsRef<Path>,
    revision: impl AsRef<str>,
    workspace: Option<Workspace>,
    paginate_opts: &PaginateOpts,
    sort_opts: &SortOpts,
    depth: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let repo = repo.clone();
    let directory = directory.as_ref().to_path_buf();
    let revision = revision.as_ref().to_string();
    let paginate_opts = paginate_opts.clone();
    let sort_opts = sort_opts.clone();
    tokio::task::spawn_blocking(move || {
        list_directory_w_workspace_depth(
            &repo,
            &directory,
            &revision,
            workspace,
            &paginate_opts,
            &sort_opts,
            depth,
        )
    })
    .await?
}

pub fn update_metadata(repo: &LocalRepository, revision: impl AsRef<str>) -> Result<(), OxenError> {
    core::v_latest::entries::update_metadata(repo, revision)
}

/// Get the entry for a given path in a commit.
/// Could be a file or a directory.
pub fn get_meta_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let parsed_resource = ParsedResource {
        path: path.to_path_buf(),
        commit: Some(commit.clone()),
        branch: None,
        workspace: None,
        version: PathBuf::from(&commit.id),
        resource: PathBuf::from(&commit.id).join(path),
    };
    core::v_latest::entries::get_meta_entry(repo, &parsed_resource, path)
}

/// Get the entry for a given path in a commit, off the async worker.
/// Could be a file or a directory.
pub async fn get_meta_entry_async(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<MetadataEntry, OxenError> {
    let repo = repo.clone();
    let commit = commit.clone();
    let path = path.as_ref().to_path_buf();
    tokio::task::spawn_blocking(move || get_meta_entry(&repo, &commit, &path)).await?
}

/// List the paths of all the directories in a given commit
pub fn list_dir_paths(repo: &LocalRepository, commit: &Commit) -> Result<Vec<PathBuf>, OxenError> {
    let tree = core::v_latest::index::CommitMerkleTree::from_commit(repo, commit)?;
    tree.list_dir_paths()
}

/// Commit entries are always files, not directories. Will return None if the path is a directory.
pub fn get_commit_entry(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<Option<CommitEntry>, OxenError> {
    match core::v_latest::entries::get_file(repo, commit, path)? {
        None => Ok(None),
        Some(file) => {
            let entry = CommitEntry {
                commit_id: commit.id.clone(),
                path: path.to_path_buf(),
                hash: file.hash().to_string(),
                num_bytes: file.num_bytes(),
                last_modified_seconds: file.last_modified_seconds(),
                last_modified_nanoseconds: file.last_modified_nanoseconds(),
            };
            Ok(Some(entry))
        }
    }
}

pub use crate::core::v_latest::entries::list_for_commit;

pub use crate::core::v_latest::entries::count_for_commit;

/// Given a list of entries, compute the total in bytes size of all entries.
pub fn compute_entries_size(entries: &[CommitEntry]) -> Result<u64, OxenError> {
    let total_size: u64 = entries.iter().map(|e| e.num_bytes).sum();
    Ok(total_size)
}

pub async fn list_missing_files_in_commit_range(
    repo: &LocalRepository,
    base_commit: &Option<Commit>,
    head_commit: &Commit,
) -> Result<Vec<CommitEntry>, OxenError> {
    let version_store = repo.version_store();

    match base_commit {
        Some(base_commit) => {
            let commits = repositories::commits::list_between(repo, base_commit, head_commit)?;

            let mut all_entries: Vec<CommitEntry> = Vec::new();
            for commit in commits {
                let entries = list_for_commit(repo, &commit)?;
                all_entries.extend(entries);
            }

            all_entries.sort_by(|a, b| a.path.cmp(&b.path));
            all_entries.dedup_by(|a, b| a.path == b.path);

            let worker_count = concurrency::num_threads_for_items(all_entries.len());
            let missing_files = stream::iter(all_entries)
                .map(|entry| {
                    let version_store = &version_store;
                    async move {
                        match version_store.version_exists(&entry.hash).await {
                            Ok(true) => Ok(None),
                            Ok(false) => Ok(Some(entry)),
                            Err(e) => Err(e),
                        }
                    }
                })
                .buffer_unordered(worker_count)
                .try_filter_map(|x| async move { Ok(x) })
                .try_collect::<Vec<_>>()
                .await?;

            Ok(missing_files)
        }
        None => {
            // we only receive a head commit, so we need to find all the commits between the head and the first commit
            let entries = list_for_commit(repo, head_commit)?;

            let worker_count = concurrency::num_threads_for_items(entries.len());
            let missing_files = stream::iter(entries)
                .map(|entry| {
                    let version_store = &version_store;
                    async move {
                        match version_store.version_exists(&entry.hash).await {
                            Ok(true) => Ok(None),
                            Ok(false) => Ok(Some(entry)),
                            Err(e) => Err(e),
                        }
                    }
                })
                .buffer_unordered(worker_count)
                .try_filter_map(|x| async move { Ok(x) })
                .try_collect::<Vec<_>>()
                .await?;

            Ok(missing_files)
        }
    }
}

pub use crate::core::v_latest::entries::list_tabular_files_in_repo;

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::error::OxenError;
    use crate::model::{MerkleHash, StagedEntryStatus};
    use crate::opts::{PaginateOpts, SortBy, SortOpts};
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::view::entries::EMetadataEntry;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_an_entry_naming_an_absent_commit_still_lists() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let first = repo.path.join("first.txt");
            util::fs::write_to_path(&first, "first")?;
            repositories::add(&repo, &first).await?;
            let first_commit = repositories::commit(&repo, "Add first")?;

            let second = repo.path.join("second.txt");
            util::fs::write_to_path(&second, "second")?;
            repositories::add(&repo, &second).await?;
            repositories::commit(&repo, "Add second")?;

            // first.txt still names the earlier commit, so removing that commit's node leaves the
            // tree pointing at a commit the repository no longer has.
            let absent: MerkleHash = first_commit.id.parse()?;
            repo.merkle_node_store().delete(&absent)?;
            assert!(repositories::commits::get_by_hash(&repo, &absent)?.is_none());

            let listed = repositories::entries::list_directory(
                &repo,
                Path::new(""),
                "main",
                &PaginateOpts {
                    page_num: 1,
                    page_size: 10,
                },
            )?;

            let named = |name: &str| {
                listed
                    .entries
                    .iter()
                    .find(|entry| entry.filename() == name)
                    .cloned()
            };
            let first_entry = named("first.txt").expect("first.txt is listed");
            let second_entry = named("second.txt").expect("second.txt is listed");

            assert!(
                first_entry.latest_commit().is_none(),
                "an entry whose commit is absent carries no commit"
            );
            assert!(
                second_entry.latest_commit().is_some(),
                "an entry whose commit is present still carries it"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_api_local_entries_list_all() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            repositories::add(&repo, file_to_add).await?;
            let commit = repositories::commit(&repo, "Adding labels file")?;

            let entries = repositories::entries::list_for_commit(&repo, &commit)?;
            assert_eq!(entries.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_api_local_entries_count_one_for_commit() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            // (file already created in helper)
            let file_to_add = repo.path.join("labels.txt");

            // Commit the file
            repositories::add(&repo, file_to_add).await?;
            let commit = repositories::commit(&repo, "Adding labels file")?;

            let count = repositories::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_api_local_entries_count_many_for_commit() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // (files already created in helper)
            let dir_to_add = repo.path.join("train");
            let num_files = util::fs::rcount_files_in_dir(&dir_to_add);

            // Commit the dir
            repositories::add(&repo, &dir_to_add).await?;
            let commit = repositories::commit(&repo, "Adding training data")?;
            let count = repositories::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_api_local_entries_count_many_dirs() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            test::write_txt_file_to_path(repo.path.join("README.md"), "README")?;
            test::populate_dir_with_txt_files(repo.path.join("train"), "train", 3)?;
            test::populate_dir_with_txt_files(repo.path.join("test"), "test", 2)?;
            let num_files = util::fs::rcount_files_in_dir(&repo.path);

            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Adding all data")?;

            let count = repositories::entries::count_for_commit(&repo, &commit)?;
            assert_eq!(count, num_files);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_meta_entry_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let path = Path::new("annotations").join("train");
            let entry = repositories::entries::get_meta_entry(&repo, commit, &path)?;

            assert!(entry.is_dir);
            assert_eq!(entry.filename, "train");
            assert_eq!(Path::new(&entry.resource.unwrap().path), path);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_meta_entry_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_top_level_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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

            assert_eq!(size, 9);
            assert_eq!(dir_entries.len(), 9);
            assert_eq!(
                dir_entries
                    .clone()
                    .into_iter()
                    .filter(|e| !e.is_dir())
                    .count(),
                4
            );
            assert_eq!(dir_entries.into_iter().filter(|e| e.is_dir()).count(), 5);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_directories_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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

            assert_eq!(size, 7);
            assert_eq!(dir_entries.len(), 7);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_train_sub_directory_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_subset() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            let paginated = repositories::entries::list_directory(
                &repo,
                Path::new("train"),
                &commit.id,
                &PaginateOpts {
                    page_num: 3,
                    page_size: 3,
                },
            )?;

            let dir_entries = paginated.entries;
            let total_entries = paginated.total_entries;

            for entry in dir_entries.iter() {
                println!("{entry:?}");
            }

            assert_eq!(total_entries, 7);
            assert_eq!(dir_entries.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_directories_1_exactly_ten() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 8 directories
            for n in 0..8 {
                let dirname = format!("dir_{n}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_all_dirs_no_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 42 directories
            for n in 0..42 {
                let dirname = format!("dir_{n:0>3}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
                println!("{entry:?}");
            }

            assert_eq!(paginated.entries.first().unwrap().filename(), "dir_010");

            println!("{paginated:?}");
            assert_eq!(paginated.total_entries, 42);
            assert_eq!(paginated.total_pages, 5);
            assert_eq!(paginated.entries.len(), 10);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_directories_101_dirs_no_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 101 directories
            for n in 0..101 {
                let dirname = format!("dir_{n:0>3}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
                println!("{:?}", entry.filename());
            }

            assert_eq!(paginated.entries.first().unwrap().filename(), "dir_100");

            println!("{paginated:?}");
            assert_eq!(paginated.total_entries, 101);
            assert_eq!(paginated.total_pages, 11);
            assert_eq!(paginated.entries.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_directories_exactly_ten_page_two() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 8 directories
            for n in 0..8 {
                let dirname = format!("dir_{n}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_nine_entries_page_size_ten() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 7 directories
            for n in 0..7 {
                let dirname = format!("dir_{n}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_eleven_entries_page_size_ten() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create 9 directories
            for n in 0..9 {
                let dirname = format!("dir_{n}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }
            // Create 2 files
            let filename = "labels.txt";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "hello world")?;

            let filename = "README.md";
            let filepath = repo.path.join(filename);
            util::fs::write(filepath, "readme....")?;

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_many_dirs_many_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create many directories
            let num_dirs = 32;
            for n in 0..num_dirs {
                let dirname = format!("dir_{n}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }

            // Create many files
            let num_files = 45;
            for n in 0..num_files {
                let filename = format!("file_{n}.txt");
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {n}"))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_one_dir_many_files_page_2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create one directory
            let dir_path = repo.path.join("lonely_dir");
            util::fs::create_dir_all(&dir_path)?;
            let filename = "data.txt";
            let filepath = dir_path.join(filename);
            util::fs::write(filepath, "All the lonely directories")?;

            // Create many files
            let num_files = 45;
            for n in 0..num_files {
                let filename = format!("file_{n}.txt");
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {n}"))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_list_directories_many_dir_some_files_page_2() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create many directories
            let num_dirs = 9;
            for n in 0..num_dirs {
                let dirname = format!("dir_{n}");
                let dir_path = repo.path.join(dirname);
                util::fs::create_dir_all(&dir_path)?;
                let filename = "data.txt";
                let filepath = dir_path.join(filename);
                util::fs::write(&filepath, format!("Hi {n}"))?;
            }

            // Create many files
            let num_files = 8;
            for n in 0..num_files {
                let filename = format!("file_{n}.txt");
                let filepath = repo.path.join(filename);
                util::fs::write(filepath, format!("helloooo {n}"))?;
            }

            // Add and commit all the dirs and files
            repositories::add(&repo, &repo.path).await?;
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
        .await
    }

    #[tokio::test]
    async fn test_file_metadata_shows_is_indexed() -> Result<(), OxenError> {
        // skip on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test_async(|repo| async move {
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
            repositories::add(&repo, &repo.path).await?;
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
            repositories::workspaces::data_frames::index(&repo, &workspace, &entry2.path).await?;

            // Now get the metadata entries for the two dataframes
            let meta1 = repositories::entries::get_meta_entry(&repo, &commit, &path_1)?;
            let meta2 = repositories::entries::get_meta_entry(&repo, &commit, &path_2)?;

            assert_eq!(meta1.is_queryable, Some(false));
            assert_eq!(meta2.is_queryable, Some(true));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_directory_with_depth() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create nested directory structure:
            // root/
            //   dir_a/
            //     file_a1.txt
            //     file_a2.txt
            //     subdir/
            //       file_sub.txt
            //   dir_b/
            //     file_b1.txt
            //   root_file.txt

            let dir_a = repo.path.join("dir_a");
            let dir_a_subdir = dir_a.join("subdir");
            let dir_b = repo.path.join("dir_b");

            util::fs::create_dir_all(&dir_a)?;
            util::fs::create_dir_all(&dir_a_subdir)?;
            util::fs::create_dir_all(&dir_b)?;

            util::fs::write(repo.path.join("root_file.txt"), "root content")?;
            util::fs::write(dir_a.join("file_a1.txt"), "a1 content")?;
            util::fs::write(dir_a_subdir.join("file_sub.txt"), "sub content")?;
            util::fs::write(dir_b.join("file_b1.txt"), "b1 content")?;

            repositories::add(&repo, &repo.path).await?;
            let _first_commit = repositories::commit(&repo, "Adding nested structure")?;

            sleep(std::time::Duration::from_millis(1100)).await;

            util::fs::write(dir_a.join("file_a2.txt"), "a2 content")?;
            repositories::add(&repo, dir_a.join("file_a2.txt")).await?;
            let commit = repositories::commit(&repo, "Adding newer nested file")?;

            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };

            // Test depth=0 (default) - no children populated
            let paginated = repositories::entries::list_directory_w_workspace_depth(
                &repo,
                Path::new(""),
                &commit.id,
                None,
                &paginate_opts,
                &SortOpts::default(),
                0,
            )?;

            // Should have 3 entries at root: dir_a, dir_b, root_file.txt
            assert_eq!(paginated.total_entries, 3);
            for entry in &paginated.entries {
                // With depth=0, no children should be populated
                match entry {
                    crate::view::entries::EMetadataEntry::MetadataEntry(e) => {
                        assert!(e.children.is_none());
                    }
                    crate::view::entries::EMetadataEntry::WorkspaceMetadataEntry(e) => {
                        assert!(e.children.is_none());
                    }
                }
            }

            // Test depth=1 - immediate children populated
            let paginated = repositories::entries::list_directory_w_workspace_depth(
                &repo,
                Path::new(""),
                &commit.id,
                None,
                &paginate_opts,
                &SortOpts::default(),
                1,
            )?;

            assert_eq!(paginated.total_entries, 3);
            assert_eq!(paginated.entries[0].filename(), "dir_a");
            assert_eq!(paginated.entries[1].filename(), "dir_b");
            assert_eq!(paginated.entries[2].filename(), "root_file.txt");

            // Find dir_a and check it has children
            let dir_a_entry = paginated.entries.iter().find(|e| e.filename() == "dir_a");
            assert!(dir_a_entry.is_some());

            if let Some(crate::view::entries::EMetadataEntry::MetadataEntry(e)) = dir_a_entry {
                assert!(e.children.is_some());
                let children = e.children.as_ref().unwrap();
                // dir_a should have: file_a1.txt, file_a2.txt, and subdir
                assert_eq!(children.len(), 3);
                // Default sort is name asc with directories first
                assert_eq!(children[0].filename, "subdir");
                assert_eq!(children[1].filename, "file_a1.txt");
                assert_eq!(children[2].filename, "file_a2.txt");

                // With depth=1, subdir's children should NOT be populated
                let subdir = children.iter().find(|c| c.filename == "subdir");
                assert!(subdir.is_some());
                assert!(subdir.unwrap().children.is_none());
            }

            // Test non-default sorting is applied to nested children as well
            let paginated = repositories::entries::list_directory_w_workspace_depth(
                &repo,
                Path::new(""),
                &commit.id,
                None,
                &paginate_opts,
                &SortOpts {
                    sort_by: SortBy::Date,
                    reverse: true,
                },
                1,
            )?;

            let dir_a_entry = paginated.entries.iter().find(|e| e.filename() == "dir_a");
            if let Some(crate::view::entries::EMetadataEntry::MetadataEntry(e)) = dir_a_entry {
                let children = e.children.as_ref().unwrap();
                assert_eq!(children[0].filename, "subdir");
                assert_eq!(children[1].filename, "file_a2.txt");
                assert_eq!(children[2].filename, "file_a1.txt");
            }

            // Test depth=2 - nested children populated
            let paginated = repositories::entries::list_directory_w_workspace_depth(
                &repo,
                Path::new(""),
                &commit.id,
                None,
                &paginate_opts,
                &SortOpts::default(),
                2,
            )?;

            let dir_a_entry = paginated.entries.iter().find(|e| e.filename() == "dir_a");
            if let Some(crate::view::entries::EMetadataEntry::MetadataEntry(e)) = dir_a_entry {
                let children = e.children.as_ref().unwrap();
                let subdir = children.iter().find(|c| c.filename == "subdir");
                assert!(subdir.is_some());
                // With depth=2, subdir should have its children populated
                assert!(subdir.unwrap().children.is_some());
                let sub_children = subdir.unwrap().children.as_ref().unwrap();
                assert_eq!(sub_children.len(), 1);
                assert_eq!(sub_children[0].filename, "file_sub.txt");
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_directory_async_matches_sync() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();
            let path = Path::new("annotations").join("train");

            let sync = repositories::entries::get_directory(&repo, commit, &path)?;
            let async_ = repositories::entries::get_directory_async(&repo, commit, &path).await?;
            assert_eq!(async_, sync);
            assert!(sync.is_some());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_directory_preserves_aggregates_without_children() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();
            // "" and "." both denote the repo root; a nested path resolves normally.
            let nested = Path::new("annotations").join("train");
            for path in [Path::new(""), Path::new("."), nested.as_path()] {
                // get_directory reads only the directory node.
                let dir_node = repositories::entries::get_directory(&repo, commit, path)?
                    .expect("directory should exist");

                // A full-depth read of the same directory must report identical aggregates, since
                // num_bytes and data_type_counts are stored on the directory node itself.
                let full = repositories::tree::get_node_by_path(&repo, commit, path)?
                    .expect("directory node should exist")
                    .dir()?;

                assert_eq!(dir_node.num_bytes(), full.num_bytes());
                assert_eq!(dir_node.data_type_counts(), full.data_type_counts());
                assert!(dir_node.num_bytes() > 0);
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_get_meta_entry_async_matches_sync() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();

            for path in [
                Path::new("annotations").join("train"),
                test::test_nlp_classification_csv(),
            ] {
                let sync = repositories::entries::get_meta_entry(&repo, commit, &path)?;
                let async_ =
                    repositories::entries::get_meta_entry_async(&repo, commit, &path).await?;
                assert_eq!(async_.filename, sync.filename);
                assert_eq!(async_.hash, sync.hash);
                assert_eq!(async_.is_dir, sync.is_dir);
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_directory_w_workspace_depth_async_matches_sync() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = repositories::commits::list(&repo)?;
            let commit = commits.first().unwrap();
            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };

            let sync = repositories::entries::list_directory_w_workspace_depth(
                &repo,
                Path::new(""),
                &commit.id,
                None,
                &paginate_opts,
                &SortOpts::default(),
                1,
            )?;
            let async_ = repositories::entries::list_directory_w_workspace_depth_async(
                &repo,
                Path::new(""),
                &commit.id,
                None,
                &paginate_opts,
                &SortOpts::default(),
                1,
            )
            .await?;

            assert_eq!(async_.total_entries, sync.total_entries);
            assert_eq!(async_.total_pages, sync.total_pages);
            let async_names: Vec<&str> = async_.entries.iter().map(|e| e.filename()).collect();
            let sync_names: Vec<&str> = sync.entries.iter().map(|e| e.filename()).collect();
            assert_eq!(async_names, sync_names);

            Ok(())
        })
        .await
    }

    /// Names and `changes` status for every entry in a workspace listing, keyed by filename.
    fn workspace_listing(
        paginated: &crate::view::PaginatedDirEntries,
    ) -> std::collections::HashMap<String, Option<StagedEntryStatus>> {
        paginated
            .entries
            .iter()
            .map(|entry| {
                let status = match entry {
                    EMetadataEntry::WorkspaceMetadataEntry(e) => {
                        e.changes.as_ref().map(|c| c.status.clone())
                    }
                    EMetadataEntry::MetadataEntry(_) => None,
                };
                (entry.filename().to_string(), status)
            })
            .collect()
    }

    /// Stage a tree of changes into a workspace over a committed tree that repeats the filename
    /// `shared.txt` at two depths, so cross-directory bleed is visible.
    async fn workspace_with_staged_changes(
        repo: &crate::model::LocalRepository,
    ) -> Result<crate::model::Workspace, OxenError> {
        let data_dir = repo.path.join("data");
        let nested_dir = data_dir.join("nested");
        util::fs::create_dir_all(&nested_dir)?;
        util::fs::write_to_path(repo.path.join("shared.txt"), "root shared")?;
        util::fs::write_to_path(repo.path.join("keep.txt"), "keep")?;
        util::fs::write_to_path(data_dir.join("shared.txt"), "data shared")?;
        util::fs::write_to_path(nested_dir.join("deep.txt"), "deep")?;
        repositories::add(repo, &repo.path).await?;
        let commit = repositories::commit(repo, "Adding committed tree")?;

        let workspace =
            repositories::workspaces::create(repo, &commit, Uuid::new_v4().to_string(), true)?;

        // Additions at three depths, the last of them under a directory only the workspace knows.
        for path in [
            PathBuf::from("root_new.txt"),
            Path::new("data").join("data_new.txt"),
            Path::new("data").join("nested").join("nested_new.txt"),
            Path::new("new_dir").join("inside.txt"),
        ] {
            let full_path = workspace.workspace_repo.path.join(&path);
            if let Some(parent) = full_path.parent() {
                util::fs::create_dir_all(parent)?;
            }
            util::fs::write_to_path(&full_path, "staged")?;
            repositories::workspaces::files::add(&workspace, &full_path).await?;
        }

        // Modify the deeper `shared.txt`, leaving the root one untouched.
        let modified = workspace
            .workspace_repo
            .path
            .join("data")
            .join("shared.txt");
        util::fs::write_to_path(&modified, "data shared, edited")?;
        repositories::workspaces::files::add(&workspace, &modified).await?;

        // Remove a committed file at the root.
        let err_files =
            repositories::workspaces::files::rm(&workspace, Path::new("keep.txt")).await?;
        assert!(err_files.is_empty(), "rm reported errors: {err_files:?}");

        Ok(workspace)
    }

    fn list_workspace_dir(
        repo: &crate::model::LocalRepository,
        workspace: &crate::model::Workspace,
        directory: &Path,
    ) -> Result<crate::view::PaginatedDirEntries, OxenError> {
        repositories::entries::list_directory_w_workspace_depth(
            repo,
            directory,
            &workspace.commit.id,
            Some(workspace.clone()),
            &PaginateOpts {
                page_num: 1,
                page_size: 100,
            },
            &SortOpts::default(),
            0,
        )
    }

    #[tokio::test]
    async fn test_list_workspace_root_scopes_staged_changes() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = workspace_with_staged_changes(&repo).await?;
            let listing = workspace_listing(&list_workspace_dir(&repo, &workspace, Path::new(""))?);

            // Only the root level, and staged entries are named by basename like committed ones.
            let mut names: Vec<&str> = listing.keys().map(|s| s.as_str()).collect();
            names.sort();
            assert_eq!(
                names,
                vec!["data", "keep.txt", "new_dir", "root_new.txt", "shared.txt"]
            );

            assert_eq!(listing["root_new.txt"], Some(StagedEntryStatus::Added));
            // The workspace-only directory holding `new_dir/inside.txt`.
            assert_eq!(listing["new_dir"], Some(StagedEntryStatus::Added));
            assert_eq!(listing["keep.txt"], Some(StagedEntryStatus::Removed));
            // `data/shared.txt` is the modified one, so the root's `shared.txt` is untouched.
            assert_eq!(listing["shared.txt"], None);
            // `data` came from the commit tree, so no second entry is synthesized for it.
            assert_eq!(listing["data"], None);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_nested_dir_scopes_staged_changes() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = workspace_with_staged_changes(&repo).await?;

            let listing =
                workspace_listing(&list_workspace_dir(&repo, &workspace, Path::new("data"))?);
            let mut names: Vec<&str> = listing.keys().map(|s| s.as_str()).collect();
            names.sort();
            assert_eq!(names, vec!["data_new.txt", "nested", "shared.txt"]);
            assert_eq!(listing["data_new.txt"], Some(StagedEntryStatus::Added));
            assert_eq!(listing["shared.txt"], Some(StagedEntryStatus::Modified));
            // `nested` is in the commit tree, and its staged addition does not duplicate it.
            assert_eq!(listing["nested"], None);

            let nested = Path::new("data").join("nested");
            let listing = workspace_listing(&list_workspace_dir(&repo, &workspace, &nested)?);
            let mut names: Vec<&str> = listing.keys().map(|s| s.as_str()).collect();
            names.sort();
            assert_eq!(names, vec!["deep.txt", "nested_new.txt"]);
            assert_eq!(listing["nested_new.txt"], Some(StagedEntryStatus::Added));
            assert_eq!(listing["deep.txt"], None);

            Ok(())
        })
        .await
    }

    /// A staged entry can land on a path the commit tree already fills with the other kind of
    /// entry. Committing the workspace resolves that in favor of what is staged, so the listing
    /// has to show the same thing rather than the entry being superseded.
    #[tokio::test]
    async fn test_list_workspace_directory_staged_entry_replaces_committed_entry()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // `to_dir` is committed as a file, `to_file` as a directory.
            util::fs::write_to_path(repo.path.join("to_dir"), "a file for now")?;
            let committed_dir = repo.path.join("to_file");
            util::fs::create_dir_all(&committed_dir)?;
            util::fs::write_to_path(committed_dir.join("child.txt"), "child")?;
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Adding a file and a directory")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, Uuid::new_v4().to_string(), true)?;

            // Stage beneath the committed file, turning it into a directory.
            let inside = workspace.workspace_repo.path.join("to_dir").join("in.txt");
            util::fs::create_dir_all(inside.parent().unwrap())?;
            util::fs::write_to_path(&inside, "staged")?;
            repositories::workspaces::files::add(&workspace, &inside).await?;

            // Stage the committed directory's own path as a file.
            let as_file = workspace.workspace_repo.path.join("to_file");
            util::fs::write_to_path(&as_file, "a file now")?;
            repositories::workspaces::files::add(&workspace, &as_file).await?;

            let paginated = list_workspace_dir(&repo, &workspace, Path::new(""))?;

            // One entry per name, each of the kind the commit will produce. Sorted by name here
            // because the overlay appends its own entries; ordering across the merge is its own
            // concern.
            let mut kinds: Vec<(&str, bool)> = paginated
                .entries
                .iter()
                .map(|e| (e.filename(), e.is_dir()))
                .collect();
            kinds.sort();
            assert_eq!(kinds, vec![("to_dir", true), ("to_file", false)]);

            // Both are staged, so both are annotated.
            let listing = workspace_listing(&paginated);
            assert_eq!(listing["to_dir"], Some(StagedEntryStatus::Added));
            assert_eq!(listing["to_file"], Some(StagedEntryStatus::Added));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_staged_additions_address_the_workspace() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let workspace = workspace_with_staged_changes(&repo).await?;
            let listing = list_workspace_dir(&repo, &workspace, Path::new("data"))?;

            let added = listing
                .entries
                .iter()
                .find(|e| e.filename() == "data_new.txt")
                .expect("the staged addition should be listed");
            let resource = added
                .resource()
                .expect("a staged addition needs a resource");

            // `/file/{version}/{path}` is where the staged bytes live. The base commit never held
            // them, so the version has to be the workspace.
            assert_eq!(resource.version, PathBuf::from(&workspace.id));
            assert_eq!(resource.path, Path::new("data").join("data_new.txt"));
            assert_eq!(
                resource.resource,
                Path::new(&workspace.id).join("data").join("data_new.txt")
            );
            assert_eq!(
                resource.workspace.as_ref().map(|w| w.id.as_str()),
                Some(workspace.id.as_str())
            );
            // No commit holds the staged bytes, so the entry has nothing to be dated by.
            assert!(added.latest_commit().is_none());

            // A committed entry alongside it already addressed the workspace, and still does.
            let committed = listing
                .entries
                .iter()
                .find(|e| e.filename() == "shared.txt")
                .expect("the committed entry should be listed");
            let committed_resource = committed.resource().expect("a committed entry has one");
            assert_eq!(committed_resource.version, PathBuf::from(&workspace.id));

            // The directory the workspace synthesized is addressable the same way.
            let listing = list_workspace_dir(&repo, &workspace, Path::new(""))?;
            let staged_dir = listing
                .entries
                .iter()
                .find(|e| e.filename() == "new_dir")
                .expect("the workspace-only directory should be listed");
            let resource = staged_dir.resource().expect("it needs a resource to open");
            assert_eq!(resource.version, PathBuf::from(&workspace.id));
            assert_eq!(resource.path, Path::new("new_dir"));

            Ok(())
        })
        .await
    }
}
