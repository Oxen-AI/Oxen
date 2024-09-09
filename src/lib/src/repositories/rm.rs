//! # oxen rm
//!
//! Remove files from the index and working directory
//!

use std::collections::HashSet;
use std::path::PathBuf;

use crate::constants::OXEN_HIDDEN_DIR;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::repositories;

use glob::glob;

use crate::util;

/// Removes the path from the index
pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {

    let path: &Path = opts.path.as_ref();
    let paths: HashSet<PathBuf> = parse_glob_path(path, repo, opts);

    
    Ok(())
}

async fn p_rm(paths: &HashSet<PathBuf>, &repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 =>  {
            for path in paths {
                let opts = RmOpts::from_path_opts(path, opts);
                core::v0_10_0::index::rm(repo, opts).await;
            }
        }
        MinOxenVersion::V0_19_0 => core::v0_19_0::rm(paths, repo, opts).await,
    }
}

// Collect paths for removal. Returns error if dir found and -r not set
fn parse_glob_path(path: &Path, repo: &LocalRepository, opts: &RmOpts) -> Result<HashSet<PathBuf>, OxenError> {
    
    let mut paths: HashSet<PathBuf> = HashSet::new();
    println!("Parse glob path");

    if opts.recursive {
        if let Some(path_str) = path.to_str() {
            if util::fs::is_glob_path(path_str) {
                // Match against any untracked entries in the current dir

                for entry in glob(path_str)? {
                    let full_path = repo.path.join(entry?);
                    paths.insert(full_path);
                }
            } else {
                // Non-glob path
                let full_path = repo.path.join(path);
                paths.insert(path.to_owned());
            }
        }
    } else {
        if let Some(path_str) = path.to_str() {
            if util::fs::is_glob_path(path_str) {
                for entry in glob(path_str)? {

                    if entry.is_dir() {
                        let error = format!("`oxen rm` on directory {path:?} requires -r");
                        return Err(OxenError::basic_str(error));
                    }

                    let full_path = repo.path.join(entry?);
                    paths.insert(full_path);
                }
            } else {
                // Non-glob path

                if path.is_dir() {
                    let error = format!("`oxen rm` on directory {path:?} requires -r");
                    return Err(OxenError::basic_str(error));
                }

                let full_path = repo.path.join(path);
                paths.insert(full_path.to_owned());
            }
        }
    }

    Ok(paths)
}


#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use crate::command;
    use crate::core::v0_10_0::index::CommitEntryReader;
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::opts::RestoreOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use image::imageops;

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
                remote: false,
            };
            repositories::rm(&repo, &opts).await?;

            // Make sure we staged these removals
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(num_files, status.staged_files.len());
            for (_path, entry) in status.staged_files.iter() {
                assert_eq!(entry.status, StagedEntryStatus::Removed);
            }
            // Make sure directory is no longer on disk
            assert!(!full_path.exists());

            // Restore the content from staging area
            let opts = RestoreOpts::from_staged_path(&rm_dir);
            command::restore(&repo, opts)?;

            // This should have removed all the staged files, but not restored from disk yet.
            let status = repositories::status(&repo)?;
            status.print();
            assert_eq!(0, status.staged_files.len());
            assert_eq!(num_files, status.removed_files.len());

            // This should restore all the files from the HEAD commit
            let opts = RestoreOpts::from_path(&rm_dir);
            command::restore(&repo, opts)?;

            let status = repositories::status(&repo)?;
            status.print();

            let num_restored = util::fs::rcount_files_in_dir(&full_path);
            assert_eq!(num_restored, num_files);

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

            repositories::add(&repo, &images_dir)?;
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
            repositories::rm(&repo, &rm_opts).await?;
            let commit = repositories::commit(&repo, "Removing cat images")?;

            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                assert!(!repo_filepath.exists())
            }

            let entries = repositories::entries::list_for_commit(&repo, &commit)?;
            assert_eq!(entries.len(), 0);

            let dir_reader = CommitEntryReader::new(&repo, &commit)?;
            let dirs = dir_reader.list_dirs()?;
            for dir in dirs.iter() {
                println!("dir: {:?}", dir);
            }

            // Should just be the root dir, we removed the images and images/cat dir
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

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Create branch
            let branch_name = "modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Resize all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));

                // Open the image file.
                let img = image::open(&repo_filepath).unwrap();

                // Resize the image to the specified dimensions.
                let dims = 96;
                let new_img = imageops::resize(&img, dims, dims, imageops::Nearest);

                // Save the resized image.
                new_img.save(repo_filepath).unwrap();
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Resized all the cats")?;

            // Remove one of the dogs
            let repo_filepath = PathBuf::from("images").join("dog_1.jpg");

            let rm_opts = RmOpts::from_path(repo_filepath);
            repositories::rm(&repo, &rm_opts).await?;
            repositories::commit(&repo, "Removing dog")?;

            // Add dwight howard and vince carter
            let test_file = test::test_img_file_with_name("dwight_vince.jpeg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, repo_filepath)?;
            repositories::add(&repo, &images_dir)?;
            let commit = repositories::commit(&repo, "Adding dwight and vince")?;

            // Should have 3 cats, 3 dogs, and one dwight/vince
            let entries = repositories::entries::list_for_commit(&repo, &commit)?;

            for entry in entries.iter() {
                println!("entry: {:?}", entry.path);
            }

            assert_eq!(entries.len(), 7);

            Ok(())
        })
        .await
    }

    #[test]
    fn test_wildcard_remove_nested_nlp_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let dir = Path::new("nlp");
            let repo_dir = repo.path.join(dir);
            repositories::add(&repo, repo_dir)?;

            let status = repositories::status(&repo)?;
            status.print();

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

            repositories::commit(&repo, "Adding nlp dir")?;

            // Remove the nlp dir
            let dir = Path::new("nlp");
            let repo_nlp_dir = repo.path.join(dir);
            std::fs::remove_dir_all(repo_nlp_dir)?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.removed_files.len(), 2);
            assert_eq!(status.staged_files.len(), 0);
            // Add the removed nlp dir with a wildcard
            repositories::add(&repo, "nlp/*")?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
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

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Pre-remove two cats and one dog to ensure deleted images get staged as removed as well as non-deleted images
            std::fs::remove_file(repo.path.join("images").join("cat_1.jpg"))?;
            std::fs::remove_file(repo.path.join("images").join("cat_2.jpg"))?;
            std::fs::remove_file(repo.path.join("images").join("dog_1.jpg"))?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.removed_files.len(), 3);
            assert_eq!(status.staged_files.len(), 0);

            // Remove with wildcard
            let rm_opts = RmOpts {
                path: PathBuf::from("images/*"),
                recursive: false,
                staged: false,
                remote: false,
            };

            repositories::rm(&repo, &rm_opts).await?;

            let status = repositories::status(&repo)?;

            // Should now have 7 staged for removal
            assert_eq!(status.staged_files.len(), 7);
            assert_eq!(status.removed_files.len(), 0);

            // Unstage the changes with staged rm
            let rm_opts = RmOpts {
                path: PathBuf::from("images/*"),
                recursive: false,
                staged: true,
                remote: false,
            };

            repositories::rm(&repo, &rm_opts).await?;

            let status = repositories::status(&repo)?;

            // Files unstaged, still removed
            assert_eq!(status.staged_files.len(), 0);
            assert_eq!(status.removed_files.len(), 7);

            Ok(())
        })
        .await
    }
}