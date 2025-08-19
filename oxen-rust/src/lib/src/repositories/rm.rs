//! # oxen rm
//!
//! Remove files from the index and working directory
//!

use std::collections::HashSet;

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::{core, repositories};
use std::path::{Path, PathBuf};

use glob::glob;

use crate::util;

/// Removes the path from the index
pub fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    log::debug!("Rm with opts: {opts:?}");
    let path: &Path = opts.path.as_ref();

    let paths: HashSet<PathBuf> = parse_glob_path(path, repo)?;

    log::debug!("paths: {paths:?}");
    p_rm(&paths, repo, opts)?;

    Ok(())
}

fn p_rm(paths: &HashSet<PathBuf>, repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            log::debug!("Version found: V0_19_0");
            core::v_latest::rm::rm(paths, repo, opts)?;
        }
    }
    Ok(())
}

// TODO: Should removing dirs from staged require -r?
// Collect paths for removal. Returns error if dir found and -r not set
fn parse_glob_path(path: &Path, repo: &LocalRepository) -> Result<HashSet<PathBuf>, OxenError> {
    let mut paths: HashSet<PathBuf> = HashSet::new();
    log::debug!("Parsing paths: {path:?}");

    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir

            for entry in glob(path_str)? {
                paths.insert(entry?.to_path_buf());
            }

            if let Some(commit) = repositories::commits::head_commit_maybe(repo)? {
                let pattern_entries =
                    repositories::commits::search_entries(repo, &commit, path_str)?;
                log::debug!("pattern entries: {:?}", pattern_entries);
                paths.extend(pattern_entries);
            }
        } else {
            // Non-glob path
            paths.insert(path.to_path_buf());
        }
    }

    log::debug!("parse_glob_paths: {paths:?}");
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use crate::api;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::constants::OXEN_HIDDEN_DIR;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::model::StagedEntryStatus;
    use crate::opts::RestoreOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    /// Should be able to use `oxen rm -r` then restore to get files back
    ///
    /// $ oxen rm -r train/
    /// $ oxen restore --staged train/
    /// $ oxen restore train/
    #[tokio::test]
    async fn test_rm_directory_restore_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let rm_dir = PathBuf::from("train");
            let full_path = repo.path.join(&rm_dir);
            let num_files = util::fs::rcount_files_in_dir(&full_path);

            // Remove directory
            let opts = RmOpts {
                path: rm_dir.to_owned(),
                recursive: true,
                staged: false,
            };
            println!("Before rm");
            repositories::rm(&repo, &opts)?;

            // Make sure we staged these removals
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(num_files, status.staged_files.len());
            for (path, entry) in status.staged_files.iter() {
                // The root path will be added as staged
                if path != Path::new("") {
                    println!("Path is : {path:?}, entry is: {entry:?} ");
                    assert_eq!(entry.status, StagedEntryStatus::Removed);
                }
            }
            // Make sure directory is no longer on disk
            assert!(!full_path.exists());

            // Restore the content from staging area
            let opts = RestoreOpts::from_staged_path(&rm_dir);
            repositories::restore::restore(&repo, opts).await?;

            // This should have removed all the staged files, but not restored from disk yet.
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(0, status.staged_files.len());
            // One removed dir (rolled up)
            assert_eq!(1, status.removed_files.len());

            // This should restore all the files from the HEAD commit
            let opts = RestoreOpts::from_path(&rm_dir);
            repositories::restore::restore(&repo, opts).await?;

            let status = repositories::status(&repo)?;
            status.print();

            let num_restored = util::fs::rcount_files_in_dir(&full_path);
            assert_eq!(num_restored, num_files);

            Ok(())
        })
        .await
    }

    /*
    This bug occurred with a repo that looked like this:

    .
    ├── README.md
    ├── gemma-3
    │   └── chat.py
    ├── mistral-small-3-1
    │   └── chat.py
    └── phi-4
    │   └── chat.py
    └── phi-4-multimodal
        ├── chat.py
        ├── eval
        │   └── ocr-bench-v2.py

    And the command:
    $ oxen rm -r phi-4

    Then pushing to the remote.

    When loading the remote, only the README.md remained.
    */
    #[tokio::test]
    async fn test_rm_r_dir_at_root() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the directory structure
            let gemma_dir = repo.path.join("gemma-3");
            util::fs::create_dir_all(&gemma_dir)?;

            let chat_file = gemma_dir.join("chat.py");
            util::fs::write(chat_file, "print('Hello, Gemma!')")?;

            let mistral_dir = repo.path.join("mistral-small-3-1");
            util::fs::create_dir_all(&mistral_dir)?;

            let chat_file = mistral_dir.join("chat.py");
            util::fs::write(chat_file, "print('Hello, Mistral!')")?;

            let phi_dir = repo.path.join("phi-4");
            util::fs::create_dir_all(&phi_dir)?;

            let chat_file = phi_dir.join("chat.py");
            util::fs::write(chat_file, "print('Hello, Phi!')")?;

            let phi_multimodal_dir = repo.path.join("phi-4-multimodal");
            util::fs::create_dir_all(&phi_multimodal_dir)?;

            let chat_file = phi_multimodal_dir.join("chat.py");
            util::fs::write(chat_file, "print('Hello, Phi Multimodal!')")?;

            let ocr_bench_dir = phi_multimodal_dir.join("eval");
            util::fs::create_dir_all(&ocr_bench_dir)?;

            let ocr_file = ocr_bench_dir.join("ocr-bench-v2.py");
            util::fs::write(ocr_file, "print('Hello, Phi OCR Bench!')")?;

            // Write a README.md file
            let readme_file = repo.path.join("README.md");
            util::fs::write(readme_file, "Hello, world!")?;

            // Add and commit the files
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Adding initial files")?;

            // Create a remote repo
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Push it to the remote
            repositories::push(&repo).await?;

            // List the files/folders in the remote
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, Path::new(""), 1, 10)
                    .await?;
            assert_eq!(root_entries.entries.len(), 5);

            // add data via a workspace
            let workspace_id = "my_workspace";
            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);
            let file_to_post = test::test_csv_file_with_name("emojis.csv");
            let directory_name = "phi-4";
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                file_to_post,
            )
            .await;
            println!("result: {:?}", result);
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add emojis data frame".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id, &body)
                .await?;

            // List the files/folders in the remote
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, Path::new(""), 1, 10)
                    .await?;
            assert_eq!(root_entries.entries.len(), 5);

            let cloned_remote_repo = remote_repo.clone();
            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let new_repo_dir = new_repo_dir.join("new_repo");
                let cloned_repo =
                    repositories::clone_url(&cloned_remote_repo.remote.url, &new_repo_dir).await?;

                let mut rm_opts = RmOpts::from_path(Path::new("phi-4"));
                rm_opts.recursive = true;
                repositories::rm(&cloned_repo, &rm_opts)?;
                repositories::commit(&cloned_repo, "Removing phi-4")?;

                // Push it to the remote
                repositories::push(&cloned_repo).await?;

                // List the files/folders in the remote
                let root_entries =
                    api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, Path::new(""), 1, 10)
                        .await?;

                for entry in root_entries.entries.iter() {
                    println!("entry: {:?}", entry);
                }

                assert_eq!(root_entries.entries.len(), 4);

                Ok(())
            })
            .await?;

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_sub_directory() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir).await?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Create branch
            let branch_name = "remove-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            let mut rm_opts = RmOpts::from_path(Path::new("images"));
            rm_opts.recursive = true;
            repositories::rm(&repo, &rm_opts)?;
            let commit = repositories::commit(&repo, "Removing cat images")?;

            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                assert!(!repo_filepath.exists())
            }

            let tree = repositories::tree::get_root_with_children(&repo, &commit)?.unwrap();
            let (files, dirs) = repositories::tree::list_files_and_dirs(&tree)?;
            assert_eq!(files.len(), 0);
            for dir in dirs.iter() {
                println!("dir: {:?}", dir);
            }

            // Should be 0, as list_files_and_dirs explicitly excludes the root dir
            assert_eq!(dirs.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_multi_level_directory() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&images_dir)?;

            // create several levels of subdirectories
            for i in 1..=3 {
                let sub_dir = images_dir.join(format!("subdir{i}_level_1"));
                util::fs::create_dir_all(&sub_dir)?;
            }

            for i in 1..=2 {
                let sub_dir = images_dir
                    .join(format!("subdir{i}_level_1"))
                    .join(format!("subdir{i}_level_2"));
                util::fs::create_dir_all(&sub_dir)?;
            }

            // Third level
            for i in 1..=1 {
                let sub_dir = images_dir
                    .join(format!("subdir{i}_level_1"))
                    .join(format!("subdir{i}_level_2"))
                    .join(format!("subdir{i}_level_3"));
                util::fs::create_dir_all(&sub_dir)?;
            }

            // Add and commit the cats to every subdirectory
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            for j in 1..=3 {
                for i in 1..=3 {
                    let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(test_file.file_name().unwrap());
                    util::fs::copy(&test_file, &repo_filepath)?;
                }
            }

            for j in 1..=2 {
                for i in 1..=3 {
                    let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(test_file.file_name().unwrap());
                    util::fs::copy(&test_file, &repo_filepath)?;
                }
            }

            for j in 1..=1 {
                for i in 1..=3 {
                    let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(format!("subdir{j}_level_3"))
                        .join(test_file.file_name().unwrap());
                    util::fs::copy(&test_file, &repo_filepath)?;
                }
            }

            repositories::add(&repo, &images_dir).await?;

            let status = repositories::status(&repo)?;
            status.print();

            println!("status: {:?}", status);

            /*
            added: images/cats/subdir1_level_1 with 3 files
            added: images/cats/subdir2_level_1 with 3 files
            added: images/cats/subdir2_level_1/subdir2_level_2 with 3 files
            added: images/cats/subdir3_level_1 with 3 files
            added: images/cats/subdir1_level_1/subdir1_level_2/subdir1_level_3 with 3 files
            added: images/cats/subdir1_level_1/subdir1_level_2 with 3 files
            added: images/cats with 3 files
            */
            assert_eq!(status.staged_dirs.len(), 7);

            // 3 * (cats + level 1 * 3 + level 2 * 2 + level 3 * 1)
            assert_eq!(status.staged_files.len(), 21);

            repositories::commit(&repo, "Adding initial cat images")?;

            // Create branch
            let branch_name = "remove-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images and subdirectories
            let mut rm_opts = RmOpts::from_path(Path::new("images"));
            rm_opts.recursive = true;
            repositories::rm(&repo, &rm_opts)?;
            let commit = repositories::commit(&repo, "Removing cat images and sub_directories")?;

            // None of these files should exist after rm -r
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                assert!(!repo_filepath.exists())
            }

            for j in 1..=3 {
                for i in 1..=3 {
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("cat_{i}.jpg"));
                    assert!(!repo_filepath.exists())
                }
            }

            for j in 1..=2 {
                for i in 1..=3 {
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(format!("cat_{i}.jpg"));
                    assert!(!repo_filepath.exists())
                }
            }

            for j in 1..=1 {
                for i in 1..=3 {
                    let repo_filepath = images_dir
                        .join(format!("subdir{j}_level_1"))
                        .join(format!("subdir{j}_level_2"))
                        .join(format!("subdir{j}_level_3"))
                        .join(format!("cat_{i}.jpg"));
                    assert!(!repo_filepath.exists())
                }
            }

            let tree = repositories::tree::get_root_with_children(&repo, &commit)?.unwrap();
            let (files, dirs) = repositories::tree::list_files_and_dirs(&tree)?;
            assert_eq!(files.len(), 0);
            assert_eq!(dirs.len(), 0);

            let dirs = tree.list_dir_paths()?;
            println!("list_dir_paths got {} dirs", dirs.len());
            for dir in dirs.iter() {
                println!("dir: {:?}", dir);
            }

            // Should be 1, as list_dir_paths explicitly includes the root dir
            assert_eq!(dirs.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_one_file_in_dir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir).await?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir).await?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Create branch
            let branch_name = "modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Resize all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));

                let dims = 96;
                util::image::resize_and_save(&repo_filepath, &repo_filepath, dims)?;
            }

            repositories::add(&repo, &images_dir).await?;
            repositories::commit(&repo, "Resized all the cats")?;

            // Remove one of the dogs
            let repo_filepath = PathBuf::from("images").join("dog_1.jpg");

            let rm_opts = RmOpts::from_path(repo_filepath);
            repositories::rm(&repo, &rm_opts)?;
            let _commit = repositories::commit(&repo, "Removing dog")?;

            // Add dwight howard and vince carter
            let test_file = test::test_img_file_with_name("dwight_vince.jpeg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, repo_filepath)?;
            repositories::add(&repo, &images_dir).await?;
            let commit = repositories::commit(&repo, "Adding dwight and vince")?;

            // Should have 3 cats, 3 dogs, and one dwight/vince
            let tree = repositories::tree::get_root_with_children(&repo, &commit)?.unwrap();
            let (files, dirs) = repositories::tree::list_files_and_dirs(&tree)?;

            for dir in dirs.iter() {
                log::debug!("dir: {:?}", dir);
            }

            for file in files.iter() {
                log::debug!("file: {:?}", file);
            }

            assert_eq!(files.len(), 7);
            assert_eq!(dirs.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_wildcard_remove_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            let dir = Path::new("nlp");
            let repo_dir = repo.path.join(dir);
            repositories::add(&repo, repo_dir).await?;

            let status = repositories::status(&repo)?;
            status.print();

            // Should add all the sub dirs
            // nlp/classification/annotations/
            assert_eq!(status.staged_dirs.len(), 1);

            // Should add sub files
            // nlp/classification/annotations/train.tsv
            // nlp/classification/annotations/test.tsv
            assert_eq!(status.staged_files.len(), 2);

            repositories::commit(&repo, "Adding nlp dir")?;

            // Remove the nlp dir
            let dir = Path::new("nlp");
            let repo_nlp_dir = repo.path.join(dir);
            std::fs::remove_dir_all(repo_nlp_dir)?;

            let status = repositories::status(&repo)?;

            // status.removed_files currently is files and dirs,
            // we roll up the dirs into the parent dir, so len should be 1
            // TODO: https://app.asana.com/0/1204211285259102/1208493904390183/f
            assert_eq!(status.removed_files.len(), 1);
            assert_eq!(status.staged_files.len(), 0);

            println!("BEFORE ADD");
            status.print();

            // Add the removed nlp dir with a wildcard
            repositories::add(&repo, "nlp/*").await?;
            println!("AFTER ADD");
            println!("status: {:?}", status);
            status.print();

            let status = repositories::status(&repo)?;
            println!("AFTER STATUS");
            println!("status: {:?}", status);
            status.print();

            // There is one rolled up dir
            // nlp
            assert_eq!(status.staged_dirs.len(), 1);
            // 2 files
            // nlp/classification/annotations/test.tsv
            // nlp/classification/annotations/train.tsv
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_wildcard_rm_deleted_and_present() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir).await?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir).await?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Pre-remove two cats and one dog to ensure deleted images get staged as removed as well as non-deleted images
            std::fs::remove_file(repo.path.join("images").join("cat_1.jpg"))?;
            std::fs::remove_file(repo.path.join("images").join("cat_2.jpg"))?;
            std::fs::remove_file(repo.path.join("images").join("dog_1.jpg"))?;

            let status = repositories::status(&repo)?;
            println!("status: {:?}", status);
            status.print();
            assert_eq!(status.removed_files.len(), 3);
            assert_eq!(status.staged_files.len(), 0);

            // Remove with wildcard
            let rm_opts = RmOpts {
                path: PathBuf::from("images/*"),
                recursive: false,
                staged: false,
            };

            repositories::rm(&repo, &rm_opts)?;

            let status = repositories::status(&repo)?;
            println!("status: {:?}", status);
            status.print();
            // Should now have 7 staged for removal
            assert_eq!(status.staged_files.len(), 7);
            assert_eq!(status.removed_files.len(), 0);

            // Unstage the changes with staged rm
            let rm_opts = RmOpts {
                path: PathBuf::from("images/*"),
                recursive: false,
                staged: true,
            };

            repositories::rm(&repo, &rm_opts)?;
            let status = repositories::status(&repo)?;
            log::debug!("status: {:?}", status);
            status.print();

            // Files unstaged, still removed
            assert_eq!(status.staged_files.len(), 0);
            assert_eq!(status.removed_files.len(), 7);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_file() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("README", |repo| async move {
            // Stage the README.md file
            let path = Path::new("README.md");
            repositories::add(&repo, repo.path.join(path)).await?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 1);
            assert!(status.staged_files.contains_key(path));

            let opts = RmOpts::from_staged_path(path);
            repositories::rm(&repo, &opts)?;

            let status = repositories::status(&repo)?;
            log::debug!("status: {:?}", status);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_dir_without_recursive_flag_should_be_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train");
            repositories::add(&repo, repo.path.join(path)).await?;

            let status = repositories::status(&repo)?;
            status.print();
            // 2: train
            assert_eq!(status.staged_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: false, // This should be an error
            };
            let result = repositories::rm(&repo, &opts);
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_annotations_train_dir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Stage the data
            let path = Path::new("annotations").join("train");
            repositories::add(&repo, repo.path.join(&path)).await?;

            let status = repositories::status(&repo)?;
            status.print();
            // 1: annotations/train
            assert_eq!(status.staged_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: true, // make sure to pass in recursive
            };
            repositories::rm(&repo, &opts)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_dirs.len(), 0);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_train_dir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train");
            repositories::add(&repo, repo.path.join(path)).await?;

            let status = repositories::status(&repo)?;
            status.print();
            // 1: train
            assert_eq!(status.staged_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: true, // make sure to pass in recursive
            };
            repositories::rm(&repo, &opts)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_dirs.len(), 0);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_staged_dir_with_slash() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Stage the data
            let path = Path::new("train/");
            repositories::add(&repo, repo.path.join(path)).await?;

            let status = repositories::status(&repo)?;
            // 1: train dir
            assert_eq!(status.staged_dirs.len(), 1);

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: true,
                recursive: true, // make sure to pass in recursive
            };
            let result = repositories::rm(&repo, &opts);
            assert!(result.is_ok());

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_dirs.len(), 0);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_staged_rm_file() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("README", |repo| async move {
            // Remove the readme
            let path = Path::new("README.md");

            let opts = RmOpts::from_path(path);
            repositories::rm(&repo, &opts)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), 1);
            assert_eq!(
                status.staged_files.get(path).unwrap().status,
                StagedEntryStatus::Removed
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_without_recursive_flag_should_be_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train");

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: false, // This should be an error
            };

            let result = repositories::rm(&repo, &opts);
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_that_is_not_committed_should_throw_error() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("train", |repo| async move {
            // The train dir is not committed, so should get an error trying to remove
            let train_dir = Path::new("train");

            let opts = RmOpts {
                path: train_dir.to_path_buf(),
                staged: false,
                recursive: true, // Need to specify recursive
            };

            let result = repositories::rm(&repo, &opts);
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_with_modifications_should_throw_error() -> Result<(), OxenError> {
        // skip on windows, not sure why it's failing...
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let train_dir = Path::new("train");

            let opts = RmOpts {
                path: train_dir.to_path_buf(),
                staged: false,
                recursive: true, // Need to specify recursive
            };

            // copy a cat into the dog image
            util::fs::copy(
                Path::new("data")
                    .join("test")
                    .join("images")
                    .join("cat_1.jpg"),
                repo.path.join(train_dir.join("dog_1.jpg")),
            )?;

            // There should be one modified file
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(status.modified_files.len(), 1);

            let result = repositories::rm(&repo, &opts);
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_train_dir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train");

            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(path));

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: true, // Must pass in recursive = true
            };
            repositories::rm(&repo, &opts)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            // commit the removal
            let commit = repositories::commit(&repo, "removed train dir")?;

            // make sure the train dir is deleted from the commits db
            let has_dir = repositories::tree::has_dir(&repo, &commit, path)?;
            assert!(!has_dir);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dir_with_slash() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("train", |repo| async move {
            // Remove the train dir
            let path = Path::new("train/");

            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(path));

            let opts = RmOpts {
                path: path.to_path_buf(),
                staged: false,
                recursive: true, // Must pass in recursive = true
            };
            repositories::rm(&repo, &opts)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_subdir() -> Result<(), OxenError> {
        test::run_select_data_repo_test_committed_async("annotations", |repo| async move {
            // Remove the annotations/train subdir
            let path = Path::new("annotations").join("train");
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path.join(&path));

            let opts = RmOpts {
                path,
                staged: false,
                recursive: true, // Must pass in recursive = true
            };
            repositories::rm(&repo, &opts)?;

            let status = repositories::status(&repo)?;
            status.print();

            assert_eq!(status.staged_files.len(), og_num_files);
            for (_, staged_entry) in status.staged_files.iter() {
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_rm_dot_excludes_oxen_hidden_dir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|repo| async move {
            // Create some test files in the repo root
            let test_file1 = repo.path.join("test1.txt");
            let test_file2 = repo.path.join("test2.txt");
            util::fs::write_to_path(&test_file1, "test content 1")?;
            util::fs::write_to_path(&test_file2, "test content 2")?;

            // Create a test directory
            let test_dir = repo.path.join("test_dir");
            util::fs::create_dir_all(&test_dir)?;
            let test_file_in_dir = test_dir.join("file_in_dir.txt");
            util::fs::write_to_path(&test_file_in_dir, "file in directory")?;

            // Add and commit all files
            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Initial commit with test files")?;

            // Verify .oxen directory exists
            let oxen_dir = repo.path.join(OXEN_HIDDEN_DIR);
            assert!(oxen_dir.exists(), "OXEN_HIDDEN_DIR should exist");

            // Use rm with "." pattern (this should exclude .oxen directory)
            let opts = RmOpts {
                path: PathBuf::from("."),
                staged: false,
                recursive: true,
            };
            repositories::rm(&repo, &opts)?;

            // Verify that .oxen directory still exists after rm operation
            assert!(
                oxen_dir.exists(),
                "OXEN_HIDDEN_DIR should not be removed by 'oxen rm .'"
            );

            // Verify that the test files were staged for removal
            let status = repositories::status(&repo)?;
            status.print();

            // Should have staged the test files for removal but not .oxen
            assert!(
                !status.staged_files.is_empty(),
                "Some files should be staged for removal"
            );

            // Verify that none of the staged files are the .oxen directory
            for (path, staged_entry) in status.staged_files.iter() {
                assert_ne!(
                    path.to_str().unwrap_or(""),
                    OXEN_HIDDEN_DIR,
                    "OXEN_HIDDEN_DIR should not be staged for removal"
                );
                assert!(
                    !path.starts_with(OXEN_HIDDEN_DIR),
                    "No files within OXEN_HIDDEN_DIR should be staged for removal"
                );
                assert_eq!(staged_entry.status, StagedEntryStatus::Removed);
            }

            // Also test direct attempt to remove .oxen directory
            let direct_oxen_opts = RmOpts {
                path: PathBuf::from(OXEN_HIDDEN_DIR),
                staged: false,
                recursive: true,
            };
            let err = repositories::rm(&repo, &direct_oxen_opts);

            assert!(err.is_err());

            // Verify .oxen directory still exists after direct removal attempt
            assert!(
                oxen_dir.exists(),
                "OXEN_HIDDEN_DIR should not be removed by direct 'oxen rm .oxen'"
            );

            Ok(())
        })
        .await
    }
}
