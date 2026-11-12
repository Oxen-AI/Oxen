//! Entries are the files and directories that are stored in a commit.
//!

use crate::core;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::opts::{PaginateOpts, SortOpts};
use crate::repositories;
use crate::tasks;
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
    tasks::spawn_blocking(move || get_directory(&repo, &commit, &path)).await?
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
    tasks::spawn_blocking(move || {
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
    tasks::spawn_blocking(move || get_meta_entry(&repo, &commit, &path)).await?
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

    use uuid::Uuid;

    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::opts::{PaginateOpts, SortBy, SortOpts};
    use crate::repositories;
    use crate::repositories::workspaces::{self, data_frames};
    use crate::repositories::{commits, tree};
    use crate::test;
    use crate::util;
    use crate::util::fs;
    use crate::util::fs::rcount_files_in_dir;

    use super::{
        count_for_commit, get_directory, get_directory_async, get_meta_entry, get_meta_entry_async,
        list_directory, list_directory_w_workspace_depth, list_directory_w_workspace_depth_async,
        list_for_commit,
    };

    #[tokio::test]
    async fn test_list_get_and_count_training_data_entries() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commit = commits::head_commit(&repo)?;

            let count = count_for_commit(&repo, &commit)?;
            assert_eq!(
                count,
                rcount_files_in_dir(&repo.path),
                "a commit of the whole tree counts every file in it"
            );
            assert_eq!(
                list_for_commit(&repo, &commit)?.len(),
                count,
                "the commit lists every entry it counts"
            );

            let page_of = |path: &str, page_num, page_size| {
                list_directory(
                    &repo,
                    Path::new(path),
                    &commit.id,
                    &PaginateOpts {
                        page_num,
                        page_size,
                    },
                )
            };
            let top = page_of("", 1, 10)?;
            assert_eq!(top.total_entries, 9);
            assert_eq!(top.entries.len(), 9);
            assert_eq!(top.entries.iter().filter(|e| e.is_dir()).count(), 5);
            assert_eq!(top.entries.iter().filter(|e| !e.is_dir()).count(), 4);

            let train = page_of("train", 1, 10)?;
            assert_eq!(train.total_entries, 7);
            assert_eq!(train.entries.len(), 7);
            let train_page_3 = page_of("train", 3, 3)?;
            assert_eq!(train_page_3.total_entries, 7);
            assert_eq!(
                train_page_3.entries.len(),
                1,
                "the third page of three holds the seventh entry"
            );

            let annotations_train = page_of("annotations/train", 1, 10)?;
            assert_eq!(annotations_train.total_entries, 4);
            assert_eq!(annotations_train.entries.len(), 4);

            let dir_path = Path::new("annotations").join("train");
            let file_path = test::test_nlp_classification_csv();
            for (path, is_dir, filename) in
                [(&dir_path, true, "train"), (&file_path, false, "test.tsv")]
            {
                let sync = get_meta_entry(&repo, &commit, path)?;
                let async_ = get_meta_entry_async(&repo, &commit, path).await?;
                assert_eq!(async_.filename, sync.filename);
                assert_eq!(async_.hash, sync.hash);
                assert_eq!(async_.is_dir, sync.is_dir);

                assert_eq!(sync.is_dir, is_dir);
                assert_eq!(sync.filename, filename);
                let resource = sync.resource.expect("a meta entry carries its resource");
                assert_eq!(Path::new(&resource.path), path.as_path());
            }

            let sync = get_directory(&repo, &commit, &dir_path)?;
            let async_ = get_directory_async(&repo, &commit, &dir_path).await?;
            assert!(sync.is_some());
            assert_eq!(async_, sync);

            // "" and "." both denote the repo root, and a nested path resolves normally.
            for path in [Path::new(""), Path::new("."), dir_path.as_path()] {
                // get_directory reads only the directory node.
                let dir_node =
                    get_directory(&repo, &commit, path)?.expect("directory should exist");

                // A full-depth read of the same directory must report identical aggregates, since
                // num_bytes and data_type_counts are stored on the directory node itself.
                let full = tree::get_node_by_path(&repo, &commit, path)?
                    .expect("directory node should exist")
                    .dir()?;

                assert_eq!(dir_node.num_bytes(), full.num_bytes());
                assert_eq!(dir_node.data_type_counts(), full.data_type_counts());
                assert!(dir_node.num_bytes() > 0);
            }

            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };
            let sync = list_directory_w_workspace_depth(
                &repo,
                Path::new(""),
                &commit.id,
                None,
                &paginate_opts,
                &SortOpts::default(),
                1,
            )?;
            let async_ = list_directory_w_workspace_depth_async(
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

    #[tokio::test]
    async fn test_list_directory_pages_dirs_before_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let num_dirs = 101;
            for n in 0..num_dirs {
                test::populate_dir_with_txt_files(
                    repo.path.join(format!("dir_{n:0>3}")),
                    "data",
                    1,
                )?;
            }
            let num_files = 45;
            test::populate_dir_with_txt_files(&repo.path, "file", num_files)?;
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Adding all the data")?;
            let total = num_dirs + num_files;

            let page = |page_num, page_size| {
                list_directory(
                    &repo,
                    Path::new(""),
                    &commit.id,
                    &PaginateOpts {
                        page_num,
                        page_size,
                    },
                )
            };

            let second = page(2, 10)?;
            assert_eq!(second.total_entries, total);
            assert_eq!(second.total_pages, 15);
            assert_eq!(second.entries.len(), 10);
            assert_eq!(
                second.entries[0].filename(),
                "dir_010",
                "directories list first, in name order"
            );

            let eleventh = page(11, 10)?;
            assert_eq!(eleventh.entries.len(), 10);
            assert_eq!(eleventh.entries[0].filename(), "dir_100");
            assert!(
                eleventh.entries[1..].iter().all(|e| !e.is_dir()),
                "files follow the last directory on the same page"
            );

            assert_eq!(
                page(15, 10)?.entries.len(),
                6,
                "the last page holds the remainder"
            );
            assert_eq!(
                page(16, 10)?.entries.len(),
                0,
                "a page past the end is empty"
            );

            // A page size at, above, and below the number of entries.
            let exact = page(1, total)?;
            assert_eq!((exact.total_pages, exact.entries.len()), (1, total));
            assert_eq!(page(2, total)?.entries.len(), 0);
            let larger = page(1, total + 1)?;
            assert_eq!((larger.total_pages, larger.entries.len()), (1, total));
            let smaller = page(1, total - 1)?;
            assert_eq!((smaller.total_pages, smaller.entries.len()), (2, total - 1));

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
            fs::write(dir_b.join("cats.tsv"), "1\t2\t3\nhello\tworld\tsup\n")?;
            fs::write(dir_b.join("dogs.csv"), "1,2,3\nhello,world,sup\n")?;

            repositories::add(&repo, &repo.path).await?;
            let first_commit = repositories::commit(&repo, "Adding nested structure")?;

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

            // Indexing one data frame in a workspace on the commit that last changed it makes only
            // that one queryable.
            if std::env::consts::OS != "windows" {
                let cats = Path::new("dir_b").join("cats.tsv");
                let dogs = Path::new("dir_b").join("dogs.csv");
                let queryable = |path: &Path| -> Result<Option<bool>, OxenError> {
                    Ok(get_meta_entry(&repo, &commit, path)?.is_queryable)
                };
                assert_eq!(queryable(&cats)?, Some(false));
                assert_eq!(queryable(&dogs)?, Some(false));

                let workspace_id = Uuid::new_v4().to_string();
                let workspace = workspaces::create(&repo, &first_commit, workspace_id, false)?;
                data_frames::index(&repo, &workspace, &dogs).await?;
                assert_eq!(queryable(&cats)?, Some(false));
                assert_eq!(queryable(&dogs)?, Some(true));
            }

            // dir_a/file_a1.txt still records the first commit, so removing that commit's node
            // leaves the tree pointing at a commit the repository no longer has.
            let absent: MerkleHash = first_commit.id.parse()?;
            repo.merkle_node_store().delete(&absent)?;
            assert!(commits::get_by_hash(&repo, &absent)?.is_none());

            let listed = list_directory(
                &repo,
                Path::new("dir_a"),
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
            let older = named("file_a1.txt").expect("file_a1.txt is listed");
            let newer = named("file_a2.txt").expect("file_a2.txt is listed");
            assert!(
                older.latest_commit().is_none(),
                "an entry whose commit is absent carries no commit"
            );
            assert!(
                newer.latest_commit().is_some(),
                "an entry whose commit is present still carries it"
            );

            Ok(())
        })
        .await
    }
}
