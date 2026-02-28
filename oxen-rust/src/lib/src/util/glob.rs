use crate::core::oxenignore;
use crate::error::OxenError;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::{Commit, LocalRepository};
use crate::opts::GlobOpts;
use crate::{repositories, util};

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use glob::{glob, Pattern};
use glob_match::glob_match;
use ignore::gitignore::Gitignore;

use walkdir::WalkDir;

// TODO: Should 'oxenignore' filter out non-glob paths too, or only dictate which paths glob patterns can expand into?

// Top level module for parsing glob paths
pub fn parse_glob_paths(
    opts: &GlobOpts,
    repo: Option<&LocalRepository>,
) -> Result<HashSet<PathBuf>, OxenError> {
    let repo_path = if let Some(repo) = repo {
        repo.path.clone()
    } else {
        PathBuf::new()
    };

    // If the repo is given, filter out paths with oxenignore
    let oxenignore = if let Some(repo) = repo {
        oxenignore::create(repo)
    } else {
        None
    };

    let paths = &opts.paths;
    log::debug!("parse_glob_paths got {:?} paths", paths.len());

    let staged_db = &opts.staged_db;
    let merkle_tree = &opts.merkle_tree;
    let working_dir = &opts.working_dir;
    let walk_dirs = &opts.walk_dirs;

    let mut expanded_paths: HashSet<PathBuf> = HashSet::new();

    for path in paths {
        log::debug!("path: {path:?}");

        // Normalize canonicalization before checking if it's a glob path
        let relative_path = util::fs::path_relative_to_dir(path, &repo_path)?;
        let glob_path = {
            let cwd = std::env::current_dir()?;
            if util::fs::is_relative_to_dir(&cwd, &repo_path) {
                let relative_cwd = util::fs::path_relative_to_dir(&cwd, &repo_path)?;
                let path_relative_to_cwd =
                    util::fs::path_relative_to_dir(&relative_path, &relative_cwd)?;

                relative_cwd.join(&path_relative_to_cwd)
            } else {
                relative_path
            }
        };

        log::debug!("glob path: {glob_path:?}");

        if util::fs::is_glob_path(&glob_path) {
            if *staged_db {
                // If staged flag set, only match against the staged db
                let Some(repo) = repo else {
                    return Err(OxenError::basic_str(
                        "Cannot parse staged_db for paths without a repo",
                    ));
                };

                let staged_paths = search_staged_db(&glob_path, repo)?;

                expanded_paths.extend(staged_paths);
                // If the merkle_tree flag is set, match against the merkle tree
            } else if *merkle_tree {
                if let Some(repo) = repo {
                    search_merkle_tree(&mut expanded_paths, repo, &glob_path)?;
                } else {
                    return Err(OxenError::basic_str(
                        "Error: Cannot parse paths from merkle tree without local repository",
                    ));
                }
            }

            // If working_dir flag set, match against the working directory
            if *working_dir {
                // If walk_dirs is set, walk directories and recursively collect their childrens' file paths
                if *walk_dirs {
                    walk_working_dir(
                        &mut expanded_paths,
                        &repo_path,
                        &glob_path,
                        oxenignore.clone(),
                    )?;
                } else {
                    // Else, collect dir and file paths in the directory itself only
                    search_working_dir(
                        &mut expanded_paths,
                        &repo_path,
                        &glob_path,
                        oxenignore.clone(),
                    )?;
                }
            }
        } else {
            // If walk_dirs flag set, walk the dir and recursively collect file paths
            let full_path = repo_path.join(&glob_path);
            if *walk_dirs && full_path.is_dir() {
                walk_working_dir(
                    &mut expanded_paths,
                    &repo_path,
                    &glob_path,
                    oxenignore.clone(),
                )?;
            } else {
                // Else, return the original path
                expanded_paths.insert(path.clone());
            }
        }
    }

    log::debug!("parse_glob_paths found paths: {:?}", expanded_paths.len());
    Ok(expanded_paths)
}

fn search_staged_db(path: &Path, repo: &LocalRepository) -> Result<HashSet<PathBuf>, OxenError> {
    let mut paths = HashSet::new();
    let path_str = path
        .to_str()
        .ok_or_else(|| OxenError::basic_str("Invalid UTF-8 in search path"))?;
    let glob_pattern = Pattern::new(path_str)?;
    let staged_data = repositories::status::status(repo)?;

    for entry in staged_data.staged_files {
        let entry_path_str = entry
            .0
            .to_str()
            .ok_or_else(|| OxenError::basic_str("Invalid UTF-8 in entry path"))?;
        if glob_pattern.matches(entry_path_str) {
            let entry_path = entry.0.to_owned();
            paths.insert(entry_path);
        }
    }

    Ok(paths)
}

// Iterate through the path, expanding glob paths and matching wildcards against the merkle tree
fn search_merkle_tree(
    paths: &mut HashSet<PathBuf>,
    repo: &LocalRepository,
    glob_path: &Path,
) -> Result<(), OxenError> {
    if let Some(head_commit) = repositories::commits::head_commit_maybe(repo)? {
        let glob_path_components: Vec<Component> = glob_path.components().collect();
        let mut search_index = 0;
        let mut search_path = PathBuf::from("");

        if !glob_path_components.is_empty() {
            r_search_merkle_tree(
                repo,
                &head_commit,
                &glob_path_components,
                paths,
                &mut search_path,
                &mut search_index,
            )?;
        }
    }

    Ok(())
}

fn r_search_merkle_tree(
    repo: &LocalRepository,
    head_commit: &Commit,
    glob_path_components: &Vec<Component>,
    paths: &mut HashSet<PathBuf>,
    search_path: &mut PathBuf,
    search_index: &mut usize,
) -> Result<(), OxenError> {
    // Advance to the next wildcard pattern
    let dir_str = glob_path_components[*search_index]
        .as_os_str()
        .to_string_lossy()
        .to_string();
    let mut dir = PathBuf::from(&dir_str);
    while *search_index < glob_path_components.len() - 1 && !util::fs::is_glob_path(&dir) {
        *search_index += 1;
        *search_path = search_path.join(&dir);

        let dir_str = glob_path_components[*search_index]
            .as_os_str()
            .to_string_lossy()
            .to_string();
        dir = PathBuf::from(&dir_str);
    }

    log::debug!("search index: {search_index:?}, search_path: {search_path:?}, dir: {dir:?}");

    if *search_index < glob_path_components.len() {
        let glob_pattern = dir.to_string_lossy().to_string();

        let is_final = *search_index == glob_path_components.len() - 1;

        // Match the current glob pattern against the Merkle Tree
        let matched_entries =
            expand_glob_pattern(repo, head_commit, &glob_pattern, search_path, &is_final)?;

        // If on the final iteration, extend paths with the matched entries
        if is_final {
            paths.extend(matched_entries);
            return Ok(());
        }

        // Else, recurse into the matching directories
        for mut entry in matched_entries {
            let mut new_index = *search_index + 1;
            r_search_merkle_tree(
                repo,
                head_commit,
                glob_path_components,
                paths,
                &mut entry,
                &mut new_index,
            )?;
        }
    }

    Ok(())
}

// Expand a glob pattern with the matching folders from the merkle tree
fn expand_glob_pattern(
    repo: &LocalRepository,
    head_commit: &Commit,
    glob_pattern: &String,
    parent_path: &PathBuf,
    is_final: &bool,
) -> Result<HashSet<PathBuf>, OxenError> {
    let mut paths = HashSet::new();

    log::debug!("Expand_glob_pattern got: pattern: {glob_pattern:?}, parent_path: {parent_path:?}, is_final: {is_final:?}");

    if let Some(dir_node) =
        repositories::tree::get_dir_with_children(repo, head_commit, parent_path, None)?
    {
        let dir_children = repositories::tree::list_files_and_folders(&dir_node)?;
        for child in dir_children {
            match &child.node {
                EMerkleTreeNode::Directory(dir_node) => {
                    let child_str = dir_node.name();
                    let child_path = parent_path.join(child_str);
                    if glob_match(glob_pattern, child_str) {
                        paths.insert(child_path);
                    }
                }
                EMerkleTreeNode::File(file_node) => {
                    let child_str = file_node.name();
                    let child_path = parent_path.join(child_str);
                    // Only include file paths on final iteration
                    if *is_final && glob_match(glob_pattern, child_str) {
                        paths.insert(child_path);
                    }
                }
                _ => {
                    return Err(OxenError::basic_str("Unexpected node type"));
                }
            }
        }
    }

    Ok(paths)
}

fn search_working_dir(
    paths: &mut HashSet<PathBuf>,
    repo_path: &Path,
    glob_path: &PathBuf,
    oxenignore: Option<Gitignore>,
) -> Result<(), OxenError> {
    let full_path = repo_path.join(glob_path);
    let path_str = full_path
        .to_str()
        .ok_or_else(|| OxenError::basic_str("Invalid UTF-8 in search path"))?;

    if let Some(oxenignore) = oxenignore {
        let oxenignore = Some(oxenignore);
        for entry in glob(path_str)? {
            let entry_path = entry?;
            let relative_path = util::fs::path_relative_to_dir(&entry_path, repo_path)?;

            if oxenignore::is_ignored(&relative_path, &oxenignore, entry_path.is_dir()) {
                continue;
            }

            paths.insert(entry_path);
        }
    } else {
        for entry in glob(path_str)? {
            let entry_path = entry?;
            paths.insert(entry_path);
        }
    }

    Ok(())
}

// Walk through dirs,
fn walk_working_dir(
    paths: &mut HashSet<PathBuf>,
    repo_path: &PathBuf,
    glob_path: &PathBuf,
    oxenignore: Option<Gitignore>,
) -> Result<(), OxenError> {
    let full_path = repo_path.join(glob_path);
    let path_str = full_path.to_str().unwrap();

    if let Some(oxenignore) = oxenignore {
        let oxenignore = Some(oxenignore);

        for entry in glob(path_str)? {
            let entry_path = entry?;
            let relative_path = util::fs::path_relative_to_dir(&entry_path, repo_path)?;

            if oxenignore::is_ignored(&relative_path, &oxenignore, entry_path.is_dir()) {
                continue;
            }

            let full_path = repo_path.join(&relative_path);
            if full_path.is_dir() {
                for entry in WalkDir::new(&full_path).into_iter().filter_map(|e| e.ok()) {
                    // Walkdir outputs full paths
                    let entry_path = entry.path().to_path_buf();
                    let relative_entry_path =
                        util::fs::path_relative_to_dir(&entry_path, repo_path)?;
                    if entry.file_type().is_file() {
                        if oxenignore::is_ignored(
                            &relative_entry_path,
                            &oxenignore,
                            entry_path.is_dir(),
                        ) {
                            continue;
                        }

                        paths.insert(entry_path);
                    }
                }
            } else {
                // Correction for remote-mode
                if entry_path.exists() {
                    paths.insert(entry_path.clone());
                } else {
                    paths.insert(full_path);
                }
            }
        }
    } else {
        for entry in glob(path_str)? {
            let entry_path = entry?;
            let relative_path = util::fs::path_relative_to_dir(&entry_path, repo_path)?;

            let full_path = repo_path.join(relative_path);
            if full_path.is_dir() {
                for entry in WalkDir::new(&full_path).into_iter().filter_map(|e| e.ok()) {
                    let entry_path = entry.path().to_path_buf();
                    if entry.file_type().is_file() {
                        paths.insert(entry_path);
                    }
                }
            // Correction for remote-mode
            } else if entry_path.exists() {
                paths.insert(entry_path.clone());
            } else {
                paths.insert(full_path);
            }
        }
    }

    Ok(())
}

// Collects the removed paths in a directory
pub fn collect_removed_paths(
    repo: &LocalRepository,
    dir_path: &PathBuf,
) -> Result<HashSet<PathBuf>, OxenError> {
    let mut removed_paths = HashSet::new();
    let repo_path = repo.path.clone();
    r_collect_removed_paths(repo, dir_path, &mut removed_paths)?;

    // Filter out existant paths
    removed_paths.retain(|path| !repo_path.join(path).exists());

    log::debug!(
        "collect_removed_paths found {:?} removed paths in dir {:?}",
        removed_paths.len(),
        dir_path
    );

    Ok(removed_paths)
}

pub fn r_collect_removed_paths(
    repo: &LocalRepository,
    dir_path: &PathBuf,
    removed_paths: &mut HashSet<PathBuf>,
) -> Result<(), OxenError> {
    let repo_path = repo.path.clone();
    if dir_path.is_dir() {
        let glob_path = util::fs::path_relative_to_dir(dir_path, &repo_path)?.join("*");

        // Search the merkle tree for all paths in the directory
        search_merkle_tree(removed_paths, repo, &glob_path)?;
        let paths = removed_paths.clone();

        // Recurse into present directories to find removed subdirs and files
        for path in paths.iter() {
            if repo_path.join(path).is_dir() {
                let dir_path = dir_path.join(path);
                r_collect_removed_paths(repo, &dir_path, removed_paths)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use crate::error::OxenError;
    use crate::opts::GlobOpts;
    use crate::repositories;
    use crate::util::glob::parse_glob_paths;

    use std::collections::HashSet;
    use std::path::PathBuf;

    #[tokio::test]
    async fn test_glob_parse_working_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let repo_path = repo.clone().path;

            // Test glob in root
            let opts = GlobOpts {
                paths: vec![PathBuf::from("*")],
                staged_db: false,
                merkle_tree: false,
                working_dir: true,
                walk_dirs: false,
            };

            let paths = parse_glob_paths(&opts, Some(&repo))?;

            let expected: HashSet<PathBuf> = vec![
                PathBuf::from("README.md"),
                PathBuf::from("LICENSE"),
                PathBuf::from("labels.txt"),
                PathBuf::from("train"),
                PathBuf::from("prompts.jsonl"),
                PathBuf::from("annotations"),
                PathBuf::from("nlp"),
                PathBuf::from("large_files"),
                PathBuf::from("test"),
            ]
            .into_iter()
            .map(|p| repo_path.join(p))
            .collect();

            assert_eq!(paths, expected);

            // Test glob in subdir
            let opts = GlobOpts {
                paths: vec![PathBuf::from("annotations/*")],
                staged_db: false,
                merkle_tree: false,
                working_dir: true,
                walk_dirs: false,
            };

            let paths = parse_glob_paths(&opts, Some(&repo))?;
            let expected: HashSet<PathBuf> = vec![
                PathBuf::from("annotations/README.md"),
                PathBuf::from("annotations/train"),
                PathBuf::from("annotations/test"),
            ]
            .into_iter()
            .map(|p| repo_path.join(p))
            .collect();

            assert_eq!(paths, expected);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_glob_parse_working_dir_walk_dirs() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let repo_path = repo.clone().path;

            // Test glob path with walk
            let opts = GlobOpts {
                paths: vec![PathBuf::from("nlp/*")],
                staged_db: false,
                merkle_tree: false,
                working_dir: true,
                walk_dirs: true,
            };

            let paths = parse_glob_paths(&opts, Some(&repo))?;

            let expected: HashSet<PathBuf> = vec![
                PathBuf::from("nlp/classification/annotations/test.tsv"),
                PathBuf::from("nlp/classification/annotations/train.tsv"),
            ]
            .into_iter()
            .map(|p| repo_path.join(p))
            .collect();

            assert_eq!(paths, expected);

            // Test non-glob path with walk
            let opts = GlobOpts {
                paths: vec![PathBuf::from("annotations")],
                staged_db: false,
                merkle_tree: false,
                working_dir: true,
                walk_dirs: true,
            };

            let paths = parse_glob_paths(&opts, Some(&repo))?;

            let expected: HashSet<PathBuf> = vec![
                PathBuf::from("annotations/README.md"),
                PathBuf::from("annotations/train/annotations.txt"),
                PathBuf::from("annotations/train/bounding_box.csv"),
                PathBuf::from("annotations/train/one_shot.csv"),
                PathBuf::from("annotations/train/two_shot.csv"),
                PathBuf::from("annotations/test/annotations.csv"),
            ]
            .into_iter()
            .map(|p| repo_path.join(p))
            .collect();

            assert_eq!(paths, expected);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_glob_parse_staged_db() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Stage some specific files and a directory
            repositories::add(&repo, "train/dog_1.jpg").await?;
            repositories::add(&repo, "annotations/train/*").await?;
            repositories::add(&repo, "README.md").await?;

            // Test glob in sub-directory
            let opts = GlobOpts {
                paths: vec![PathBuf::from("annotations/train/*")],
                staged_db: true,
                merkle_tree: false,
                working_dir: false,
                walk_dirs: false,
            };

            let paths = parse_glob_paths(&opts, Some(&repo))?;

            let expected: HashSet<PathBuf> = vec![
                PathBuf::from("annotations/train/annotations.txt"),
                PathBuf::from("annotations/train/bounding_box.csv"),
                PathBuf::from("annotations/train/one_shot.csv"),
                PathBuf::from("annotations/train/two_shot.csv"),
            ]
            .into_iter()
            .collect();

            assert_eq!(paths, expected);

            // Test glob at root
            let opts_root = GlobOpts {
                paths: vec![PathBuf::from("*")],
                staged_db: true,
                merkle_tree: false,
                working_dir: false,
                walk_dirs: false,
            };

            let paths_root = parse_glob_paths(&opts_root, Some(&repo))?;

            let expected_root: HashSet<PathBuf> = vec![
                PathBuf::from("annotations/train/annotations.txt"),
                PathBuf::from("annotations/train/bounding_box.csv"),
                PathBuf::from("annotations/train/one_shot.csv"),
                PathBuf::from("annotations/train/two_shot.csv"),
                PathBuf::from("train/dog_1.jpg"),
                PathBuf::from("README.md"),
            ]
            .into_iter()
            .collect();

            assert_eq!(paths_root, expected_root);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_glob_parse_merkle_tree() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // Test glob in sub-directory
            let opts = GlobOpts {
                paths: vec![PathBuf::from("nlp/classification/annotations/*")],
                staged_db: false,
                merkle_tree: true,
                working_dir: false,
                walk_dirs: false,
            };

            let paths = parse_glob_paths(&opts, Some(&repo))?;

            let expected: HashSet<PathBuf> = vec![
                PathBuf::from("nlp/classification/annotations/test.tsv"),
                PathBuf::from("nlp/classification/annotations/train.tsv"),
            ]
            .into_iter()
            .collect();

            assert_eq!(paths, expected);

            // Test glob at root
            let opts_root = GlobOpts {
                paths: vec![PathBuf::from("*")],
                staged_db: false,
                merkle_tree: true,
                working_dir: false,
                walk_dirs: false,
            };

            let paths_root = parse_glob_paths(&opts_root, Some(&repo))?;

            // Merkle tree search should return both files and directories at this level
            let expected_root: HashSet<PathBuf> = vec![
                PathBuf::from("README.md"),
                PathBuf::from("LICENSE"),
                PathBuf::from("labels.txt"),
                PathBuf::from("prompts.jsonl"),
                PathBuf::from("train"),
                PathBuf::from("test"),
                PathBuf::from("annotations"),
                PathBuf::from("nlp"),
                PathBuf::from("large_files"),
            ]
            .into_iter()
            .collect();

            assert_eq!(paths_root, expected_root);

            Ok(())
        })
        .await
    }
}
