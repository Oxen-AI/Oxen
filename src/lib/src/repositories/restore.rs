//! # oxen restore
//!
//! Restore a file to a previous version
//!

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RestoreOpts;

/// # Restore a removed file that was committed
///
/// ```
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::test;
/// # use liboxen::error::OxenError;
/// # use liboxen::opts::RestoreOpts;
/// # use std::path::Path;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
///
/// // Initialize the repository
/// let base_dir = Path::new("repo_dir_commit");
/// let repo = repositories::init(base_dir)?;
///
/// // Write file to disk
/// let hello_name = "hello.txt";
/// let hello_path = base_dir.join(hello_name);
/// util::fs::write_to_path(&hello_path, "Hello World");
///
/// // Stage the file
/// repositories::add(&repo, &hello_path)?;
///
/// // Commit staged
/// let commit = repositories::commit(&repo, "My commit message")?.unwrap();
///
/// // Remove the file from disk
/// util::fs::remove_file(hello_path)?;
///
/// // Restore the file
/// repositories::restore::restore(&repo, RestoreOpts::from_path_ref(hello_name, commit.id))?;
///
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::restore::restore(repo, opts),
        MinOxenVersion::V0_19_0 => core::v0_19_0::restore::restore(repo, opts),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::path::PathBuf;

    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::opts::DFOpts;
    use crate::opts::RestoreOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::test::append_line_txt_file;
    use crate::util;

    #[test]
    fn test_command_restore_removed_file_from_head() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_filename = "hello.txt";
            let hello_file = repo.path.join(hello_filename);
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            repositories::add(&repo, &hello_file)?;
            // Commit the file
            repositories::commit(&repo, "My message")?;

            // Remove the file from disk
            util::fs::remove_file(&hello_file)?;

            // Check that it doesn't exist, then it does after we restore it
            assert!(!hello_file.exists());
            // Restore takes the filename not the full path to the test repo
            // ie: "hello.txt" instead of data/test/runs/repo_data/test/runs_fc1544ab-cd55-4344-aa13-5360dc91d0fe/hello.txt
            repositories::restore::restore(&repo, RestoreOpts::from_path(hello_filename))?;
            assert!(hello_file.exists());

            Ok(())
        })
    }

    #[test]
    fn test_command_restore_file_from_commit_id() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write to file
            let hello_filename = "hello.txt";
            let hello_file = repo.path.join(hello_filename);
            util::fs::write_to_path(&hello_file, "Hello World")?;

            // Track the file
            repositories::add(&repo, &hello_file)?;
            // Commit the file
            repositories::commit(&repo, "My message")?;

            // Modify the file once
            let first_modification = "Hola Mundo";
            let hello_file = test::modify_txt_file(hello_file, first_modification)?;
            repositories::add(&repo, &hello_file)?;
            let first_mod_commit = repositories::commit(&repo, "Changing to spanish")?;

            // Modify again
            let second_modification = "Bonjour le monde";
            let hello_file = test::modify_txt_file(hello_file, second_modification)?;
            repositories::add(&repo, &hello_file)?;
            repositories::commit(&repo, "Changing to french")?;

            // Restore from the first commit
            repositories::restore::restore(
                &repo,
                RestoreOpts::from_path_ref(hello_filename, first_mod_commit.id),
            )?;
            let content = util::fs::read_from_path(&hello_file)?;
            assert!(hello_file.exists());
            assert_eq!(content, first_modification);

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_command_restore_removed_file_from_branch_with_commits_between(
    ) -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // (file already created in helper)
            let file_to_remove = repo.path.join("labels.txt");

            // Commit the file
            repositories::add(&repo, &file_to_remove)?;
            repositories::commit(&repo, "Adding labels file")?;

            let orig_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let train_dir = repo.path.join("train");
            repositories::add(&repo, train_dir)?;
            repositories::commit(&repo, "Adding train dir")?;

            // Branch
            repositories::branches::create_checkout(&repo, "remove-labels")?;

            // Delete the file
            util::fs::remove_file(&file_to_remove)?;

            // We should recognize it as missing now
            let status = repositories::status(&repo)?;
            assert_eq!(status.removed_files.len(), 1);

            // Commit removed file
            repositories::add(&repo, &file_to_remove)?;
            repositories::commit(&repo, "Removing labels file")?;

            // Make sure file is not there
            assert!(!file_to_remove.exists());

            // Switch back to main branch
            repositories::checkout(&repo, orig_branch.name).await?;
            // Make sure we restore file
            assert!(file_to_remove.exists());

            Ok(())
        })
        .await
    }

    #[test]
    fn test_restore_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();

            let annotations_dir = Path::new("annotations");

            // Remove one file
            let bbox_file = annotations_dir.join("train").join("bounding_box.csv");
            let bbox_path = repo.path.join(bbox_file);

            let og_bbox_contents = util::fs::read_from_path(&bbox_path)?;

            util::fs::remove_file(&bbox_path)?;

            // Modify another file
            let readme_file = annotations_dir.join("README.md");
            let readme_path = repo.path.join(readme_file);
            let og_readme_contents = util::fs::read_from_path(&readme_path)?;

            let readme_path = test::append_line_txt_file(readme_path, "Adding s'more")?;

            // Restore the directory
            repositories::restore::restore(
                &repo,
                RestoreOpts::from_path_ref(annotations_dir, last_commit.id.clone()),
            )?;

            // Make sure the removed file is restored
            let restored_contents = util::fs::read_from_path(&bbox_path)?;
            assert_eq!(og_bbox_contents, restored_contents);

            // Make sure the modified file is restored
            let restored_contents = util::fs::read_from_path(readme_path)?;
            assert_eq!(og_readme_contents, restored_contents);

            Ok(())
        })
    }

    #[test]
    fn test_restore_removed_tabular_data() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();

            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            let og_contents = util::fs::read_from_path(&bbox_path)?;
            util::fs::remove_file(&bbox_path)?;

            println!("restoring {:?}", bbox_file);

            repositories::restore::restore(
                &repo,
                RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
            )?;
            let restored_contents = util::fs::read_from_path(&bbox_path)?;
            assert_eq!(og_contents, restored_contents);

            Ok(())
        })
    }

    #[test]
    fn test_restore_modified_tabular_data() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();

            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            let og_contents = util::fs::read_from_path(&bbox_path)?;

            let mut opts = DFOpts::empty();
            opts.add_row = Some("{\"file\": \"train/dog_99.jpg\", \"label\": \"dog\", \"min_x\": 101.5, \"min_y\": 32.0, \"width\": 385, \"height\": 330}".to_string());
            let mut df = tabular::read_df(&bbox_path, opts)?;
            tabular::write_df(&mut df, &bbox_path)?;

            repositories::restore::restore(
                &repo,
                RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
            )?;
            let restored_contents = util::fs::read_from_path(&bbox_path)?;
            assert_eq!(og_contents, restored_contents);

            let status = repositories::status(&repo)?;
            assert_eq!(status.modified_files.len(), 0);
            assert!(status.is_clean());

            Ok(())
        })
    }

    #[test]
    fn test_restore_modified_text_data() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let last_commit = history.first().unwrap();

            let bbox_file = Path::new("annotations")
                .join("train")
                .join("annotations.txt");
            let bbox_path = repo.path.join(&bbox_file);

            let og_contents = util::fs::read_from_path(&bbox_path)?;
            let new_contents = format!("{og_contents}\nnew 0");
            util::fs::write_to_path(&bbox_path, new_contents)?;

            repositories::restore::restore(
                &repo,
                RestoreOpts::from_path_ref(bbox_file, last_commit.id.clone()),
            )?;
            let restored_contents = util::fs::read_from_path(&bbox_path)?;
            assert_eq!(og_contents, restored_contents);

            let status = repositories::status(&repo)?;
            assert_eq!(status.modified_files.len(), 0);
            assert!(status.is_clean());

            Ok(())
        })
    }

    #[test]
    fn test_restore_staged_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            // Stage file
            repositories::add(&repo, bbox_path)?;

            // Make sure is staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 1);
            status.print();

            // Remove from staged
            repositories::restore::restore(&repo, RestoreOpts::from_staged_path(bbox_file))?;

            // Make sure is unstaged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_restore_data_frame_with_duplicates() -> Result<(), OxenError> {
        // THIS ONE FAILS BECAUSE OF THE REPOSITOROIES::COMMIT, IT DOESN'T GET TO RESTORE
        test::run_training_data_repo_test_fully_committed(|repo| {
            let ann_file = Path::new("nlp")
                .join("classification")
                .join("annotations")
                .join("train.tsv");
            let ann_path = repo.path.join(&ann_file);
            let new_line = "new_data,123,456,789";
            append_line_txt_file(&ann_path, new_line)?;
            let orig_df = tabular::read_df(&ann_path, DFOpts::empty())?;
            let og_contents = util::fs::read_from_path(&ann_path)?;

            // Commit
            repositories::add(&repo, &ann_path)?;
            let commit = repositories::commit(&repo, "adding data with duplicates")?;

            // Remove
            util::fs::remove_file(&ann_path)?;

            // Restore from commit
            repositories::restore::restore(&repo, RestoreOpts::from_path_ref(ann_file, commit.id))?;

            // Make sure is same size
            let restored_df = tabular::read_df(&ann_path, DFOpts::empty())?;
            assert_eq!(restored_df.height(), orig_df.height());
            assert_eq!(restored_df.width(), orig_df.width());

            let restored_contents = util::fs::read_from_path(&ann_path)?;
            assert_eq!(og_contents, restored_contents);

            Ok(())
        })
    }

    #[test]
    fn test_restore_bounding_box_data_frame() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let ann_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let ann_path = repo.path.join(&ann_file);

            let new_line = "new_data,123,456,789";
            append_line_txt_file(&ann_path, new_line)?;

            let orig_df = tabular::read_df(&ann_path, DFOpts::empty())?;

            let og_contents = util::fs::read_from_path(&ann_path)?;

            // Commit
            repositories::add(&repo, &ann_path)?;

            let commit = repositories::commit(&repo, "adding data with duplicates")?;

            // Remove
            util::fs::remove_file(&ann_path)?;

            // Restore from commit
            repositories::restore::restore(&repo, RestoreOpts::from_path_ref(ann_file, commit.id))?;

            // Make sure is same size
            let restored_df = tabular::read_df(&ann_path, DFOpts::empty())?;

            assert_eq!(restored_df.height(), orig_df.height());
            assert_eq!(restored_df.width(), orig_df.width());

            let restored_contents = util::fs::read_from_path(&ann_path)?;
            assert_eq!(og_contents, restored_contents);

            Ok(())
        })
    }

    #[test]
    fn test_restore_staged_directory() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let relative_path = Path::new("annotations");
            let annotations_dir = repo.path.join(relative_path);

            // Stage file
            repositories::add(&repo, annotations_dir)?;

            // Make sure is staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 3);
            assert_eq!(status.staged_files.len(), 6);
            status.print();

            // Remove from staged
            repositories::restore::restore(&repo, RestoreOpts::from_staged_path(relative_path))?;

            // Make sure is unstaged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 0);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
    }

    #[test]
    fn test_wildcard_restore_nested_nlp_dir() -> Result<(), OxenError> {
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
                1
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
            assert_eq!(status.removed_files.len(), 1);
            assert_eq!(status.staged_files.len(), 0);
            // Add the removed nlp dir with a wildcard
            repositories::add(&repo, "nlp/*")?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_dirs.len(), 1);
            assert_eq!(status.staged_files.len(), 2);

            Ok(())
        })
    }

    // FAILS BECAUSE OF search entries in commit
    #[tokio::test]
    async fn test_wildcard_restore_deleted_and_present() -> Result<(), OxenError> {
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

            // Remove all the things
            let rm_opts = RmOpts {
                path: PathBuf::from("images/*"),
                recursive: false,
                staged: false,
            };

            repositories::rm(&repo, &rm_opts)?;

            // Should now have 7 staged for removal
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 7);
            assert_eq!(status.removed_files.len(), 0);

            // Restore staged with wildcard
            let restore_opts = RestoreOpts {
                path: PathBuf::from("images/*"),
                staged: true,
                source_ref: None,
                is_remote: false,
            };

            repositories::restore::restore(&repo, restore_opts)?;

            let status = repositories::status(&repo)?;

            // Should now have unstaged the 7 ommissions, moving them to removed_files
            assert_eq!(status.removed_files.len(), 7);
            assert_eq!(status.staged_files.len(), 0);

            let restore_opts = RestoreOpts {
                path: PathBuf::from("images/*"),
                staged: false,
                source_ref: None,
                is_remote: false,
            };

            repositories::restore::restore(&repo, restore_opts)?;

            let status = repositories::status(&repo)?;

            // Should now have restored the 7 files to the working directory, no staged changes
            assert_eq!(status.removed_files.len(), 0);
            assert_eq!(status.staged_files.len(), 0);

            Ok(())
        })
        .await
    }

    // FAILS BECAUSE OF STATUS IT SEEMS LIKE
    #[tokio::test]
    async fn test_restore_staged_schemas_with_wildcard() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            // Make a new dir in the repo - new_annotations
            let new_annotations_dir = repo.path.join("new_annotations");
            // Copy over bounding_box.csv and one_shot.csv to new_annotations
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let one_shot_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("one_shot.csv");

            // Copy bbox and one_shot to new_annotations
            util::fs::create_dir_all(&new_annotations_dir)?;
            util::fs::copy(bbox_path, new_annotations_dir.join("bounding_box.csv"))?;
            util::fs::copy(one_shot_path, new_annotations_dir.join("one_shot.csv"))?;

            // Get file names for these copied files
            new_annotations_dir
                .join("bounding_box.csv")
                .file_name()
                .unwrap();
            new_annotations_dir
                .join("one_shot.csv")
                .file_name()
                .unwrap();

            // Add both files
            repositories::add(&repo, &new_annotations_dir)?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 2);
            assert_eq!(status.staged_schemas.len(), 2);

            // Restore *.csv
            let restore_opts = RestoreOpts {
                path: PathBuf::from("*.csv"),
                staged: true,
                source_ref: None,
                is_remote: false,
            };

            repositories::restore::restore(&repo, restore_opts)?;

            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_files.len(), 0);
            assert_eq!(status.staged_schemas.len(), 0);

            Ok(())
        })
    }
}
