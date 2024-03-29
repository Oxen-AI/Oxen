//! # oxen add
//!
//! Stage data for commit
//!

use glob::glob;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use super::helpers;
use crate::core::index::{oxenignore, CommitEntryReader, SchemaReader, Stager};
use crate::{api, error::OxenError, model::LocalRepository};
/// # Stage files into repository
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
///
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_add");
/// let repo = command::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_file)?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn add<P: AsRef<Path>>(repo: &LocalRepository, path: P) -> Result<(), OxenError> {
    let stager = Stager::new_with_merge(repo)?;
    let commit = api::local::commits::head_commit(repo)?;
    let reader = CommitEntryReader::new(repo, &commit)?;
    let schema_reader = SchemaReader::new(repo, &commit.id)?;
    let ignore = oxenignore::create(repo);
    log::debug!("---START--- oxen add: {:?}", path.as_ref());

    // Collect paths that match the glob pattern either:
    // 1. In the repo working directory (untracked or modified files)
    // 2. In the commit entry db (removed files)
    let mut paths: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.as_ref().to_str() {
        if helpers::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                paths.insert(entry?);
            }

            let pattern_entries = api::local::commits::glob_entry_paths(repo, &commit, path_str)?;
            paths.extend(pattern_entries);
        } else {
            // Non-glob path
            paths.insert(path.as_ref().to_owned());
        }
    }

    // Get all entries in the head commit
    for path in paths {
        stager.add(path.as_ref(), &reader, &schema_reader, &ignore)?;
    }

    log::debug!("---END--- oxen add: {:?}", path.as_ref());
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::api;
    use crate::command;
    use crate::error::OxenError;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_add_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            command::add(&repo, &hello_file)?;
            // Get status and make sure it is removed from the untracked, and added to the tracked
            let repo_status = command::status(&repo)?;
            assert_eq!(repo_status.staged_dirs.len(), 0);
            assert_eq!(repo_status.staged_files.len(), 1);
            assert_eq!(repo_status.untracked_files.len(), 0);
            assert_eq!(repo_status.untracked_dirs.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_command_add_modified_file_in_subdirectory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Modify and add the file deep in a sub dir
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            let file_contents = "file,label\ntrain/cat_1.jpg,0";
            test::modify_txt_file(one_shot_path, file_contents)?;
            let status = command::status(&repo)?;
            assert_eq!(status.modified_files.len(), 1);
            // Add the top level directory, and make sure the modified file gets added
            let annotation_dir_path = repo.path.join("annotations");
            command::add(&repo, annotation_dir_path)?;
            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.staged_files.len(), 1);
            command::commit(&repo, "Changing one shot")?;
            let status = command::status(&repo)?;
            assert!(status.is_clean());

            Ok(())
        })
    }

    #[test]
    fn test_command_add_removed_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // (file already created in helper)
            let file_to_remove = repo.path.join("labels.txt");

            // Commit the file
            command::add(&repo, &file_to_remove)?;
            command::commit(&repo, "Adding labels file")?;

            // Delete the file
            util::fs::remove_file(&file_to_remove)?;

            // We should recognize it as missing now
            let status = command::status(&repo)?;
            assert_eq!(status.removed_files.len(), 1);

            Ok(())
        })
    }

    // At some point we were adding rocksdb inside the working dir...def should not do that
    #[test]
    fn test_command_add_dot_should_not_add_new_files() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let num_files = util::fs::count_files_in_dir(&repo.path);

            command::add(&repo, &repo.path)?;

            // Add shouldn't add any new files in the working dir
            let num_files_after_add = util::fs::count_files_in_dir(&repo.path);

            assert_eq!(num_files, num_files_after_add);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_can_add_merge_conflict() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("labels", |repo| async move {
            let labels_path = repo.path.join("labels.txt");
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "adding initial labels file")?;

            let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

            // Add a "none" category on a branch
            let branch_name = "change-labels";
            api::local::branches::create_checkout(&repo, branch_name)?;

            test::modify_txt_file(&labels_path, "cat\ndog\nnone")?;
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "adding none category")?;

            // Add a "person" category on a the main branch
            command::checkout(&repo, og_branch.name).await?;

            test::modify_txt_file(&labels_path, "cat\ndog\nperson")?;
            command::add(&repo, &labels_path)?;
            command::commit(&repo, "adding person category")?;

            // Try to merge in the changes
            command::merge(&repo, branch_name)?;

            let status = command::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Assume that we fixed the conflict and added the file
            let path = status.merge_conflicts[0].base_entry.path.clone();
            let fullpath = repo.path.join(path);
            command::add(&repo, fullpath)?;

            // Adding should add to added files
            let status = command::status(&repo)?;

            assert_eq!(status.staged_files.len(), 1);

            // Adding should get rid of the merge conflict
            assert_eq!(status.merge_conflicts.len(), 0);

            Ok(())
        })
        .await
    }

    #[test]
    fn test_add_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let dir = Path::new("nlp");
            let repo_dir = repo.path.join(dir);
            command::add(&repo, repo_dir)?;

            let status = command::status(&repo)?;
            status.print_stdout();

            // Should add all the sub dirs
            // nlp/
            //   classification/
            //     annotations/
            assert_eq!(
                status
                    .staged_dirs
                    .paths
                    .get(Path::new("nlp"))
                    .unwrap()
                    .len(),
                3
            );
            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_command_add_stage_with_wildcard() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let _objects_dir = repo.path.join(".oxen/objects");

            // Modify and add the file deep in a sub dir
            let one_shot_path = repo.path.join("annotations/train/one_shot.csv");
            let file_contents = "file,label\ntrain/cat_1.jpg,0";
            test::modify_txt_file(one_shot_path, file_contents)?;
            let status = command::status(&repo)?;
            assert_eq!(status.modified_files.len(), 1);
            // Add the top level directory, and make sure the modified file gets added
            let annotation_dir_path = repo.path.join("annotations/*");
            command::add(&repo, annotation_dir_path)?;
            let status = command::status(&repo)?;
            status.print_stdout();
            assert_eq!(status.staged_files.len(), 1);
            command::commit(&repo, "Changing one shot")?;
            let status = command::status(&repo)?;
            assert!(status.is_clean());

            Ok(())
        })
    }

    #[test]
    fn test_wildcard_remove_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let dir = Path::new("nlp");
            let repo_dir = repo.path.join(dir);
            command::add(&repo, repo_dir)?;

            let status = command::status(&repo)?;
            status.print_stdout();

            // Should add all the sub dirs
            // nlp/
            //   classification/
            //     annotations/
            assert_eq!(
                status
                    .staged_dirs
                    .paths
                    .get(Path::new("nlp"))
                    .unwrap()
                    .len(),
                3
            );
            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            command::commit(&repo, "Adding nlp dir")?;

            // Remove the nlp dir
            let dir = Path::new("nlp");
            let repo_nlp_dir = repo.path.join(dir);
            std::fs::remove_dir_all(repo_nlp_dir)?;

            let status = command::status(&repo)?;
            assert_eq!(status.removed_files.len(), 2);
            assert_eq!(status.staged_files.len(), 0);
            // Add the removed nlp dir with a wildcard
            command::add(&repo, "nlp/*")?;

            let status = command::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
    }

    #[test]
    fn test_wildcard_add_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let dir = Path::new("nlp/*");
            let repo_dir = repo.path.join(dir);
            command::add(&repo, repo_dir)?;

            let status = command::status(&repo)?;
            status.print_stdout();

            // Should add all the sub dirs
            // nlp/
            //   classification/
            //     annotations/
            assert_eq!(
                status
                    .staged_dirs
                    .paths
                    .get(Path::new("nlp"))
                    .unwrap()
                    .len(),
                3
            );
            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
    }
}
