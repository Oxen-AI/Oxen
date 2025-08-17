use crate::{api, repositories, util};

use std::path::PathBuf;
use crate::repositories::LocalRepository;
use crate::repositories::OxenError;

use glob_match::glob_match;

// TODO: Skip download on up to date files using metadata from tree
// This is particularly necessary for the glob paths -- should get the file nodes themselves, and check metadata
pub async fn restore(repo: &LocalRepository, paths: &Vec<PathBuf>, revision: &String) -> Result<(), OxenError> {
   // println!("path: {path:?}");
    let mut paths_to_download: Vec<(PathBuf, PathBuf)> = vec![];
    let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
    let head_commit = repositories::commits::head_commit_maybe(&repo)?;
    let root_path = PathBuf::from("");        
    let repo_path = repo.path.clone();
        
    for path in paths.iter() {

        let relative_path = util::fs::path_relative_to_dir(path, &repo_path)?;
        println!("rel path: {relative_path:?}");
        let full_path = repo_path.join(&relative_path);
        println!("full path: {full_path:?}");

        // If glob path, check for paths against the tree
        if util::fs::is_glob_path(&relative_path) {
            println!("s");
            let Some(ref head_commit) = head_commit else {
                // TODO: Better error message?
                return Err(OxenError::basic_str("Error: Cannot restore with glob paths in remote-mode repo without HEAD commit"));
            };

            let glob_pattern = full_path.file_name().unwrap().to_string_lossy().to_string();
            println!("glob_pattern: {glob_pattern:?}");
            let parent_path = relative_path.parent().unwrap_or(&root_path);
            println!("parent_path: {parent_path:?}");
            
            // If dir not found in tree, skip glob path
            println!("asdf");
            let Some(dir_node) = repositories::tree::get_dir_with_children(&repo, &head_commit, &parent_path)? else {
                continue;
            };
        
            let dir_paths = dir_node.list_paths()?;
            println!("paths: {dir_paths:?}");
            println!("dir_paths: {dir_paths:?}");
            for child_path in dir_paths {
                if child_path == parent_path {
                    continue;
                }
                let child_str = child_path.to_string_lossy().to_string();
                println!("child: {child_str:?}");


                if glob_match(&glob_pattern, &child_str) {
                    let full_dir_path = repo_path.join(parent_path).join(&child_path);
                    println!("full dir path: {full_dir_path:?}");
                    paths_to_download.push((full_dir_path, parent_path.join(&child_path)));
                }
            } 

        } else {
            paths_to_download.push((full_path, relative_path));
        }
    }

    api::client::entries::download_entries_to_repo(&repo, &remote_repo, &paths_to_download, &revision).await?;

    Ok(())
}




#[cfg(test)]
mod tests {


    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::opts::clone_opts::CloneOpts;
    use crate::model::staged_data::StagedDataOpts;

    use crate::{repositories, test};


    #[tokio::test]
    async fn test_remote_mode_restore_file() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[PathBuf::from(directory.clone())]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                

                let readme_path = PathBuf::from("README.md");
                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                repositories::remote_mode::restore(&cloned_repo, &vec![readme_path.clone()], &head_commit.id).await?;

                let full_path = cloned_repo.path.join(&readme_path);
                assert!(full_path.exists());

                /*let contents = std::fs::read_to_string(&full_path)?;
                assert_eq!(
                    contents,
                    "
                    # Welcome to the party

                    If you are seeing this, you are deep in the test framework, love to see it, keep testing.

                    Yes I am biased, dog is label 0, cat is label 1, not alphabetical. Interpret that as you will.

                    🐂 💨
                    "
                );*/

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

#[tokio::test]
    async fn test_remote_mode_restore_file_with_full_path() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[PathBuf::from(directory.clone())]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                
                let readme_path = PathBuf::from("README.md");
                let full_path = cloned_repo.path.join(&readme_path);
                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                repositories::remote_mode::restore(&cloned_repo, &vec![full_path.clone()], &head_commit.id).await?;

                assert!(readme_path.exists());

                /*let contents = std::fs::read_to_string(&full_path)?;
                assert_eq!(
                    contents,
                    "
                    # Welcome to the party

                    If you are seeing this, you are deep in the test framework, love to see it, keep testing.

                    Yes I am biased, dog is label 0, cat is label 1, not alphabetical. Interpret that as you will.

                    🐂 💨
                    "
                );*/

                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

    #[tokio::test]
    async fn test_remote_mode_restore_subdirectory_file() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[PathBuf::from(directory.clone())]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                

                let file_path = PathBuf::from("annotations").join("train").join("bounding_box.csv");
                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                repositories::remote_mode::restore(&cloned_repo, &vec![file_path.clone()], &head_commit.id).await?;

                let full_path = cloned_repo.path.join(&file_path);
                assert!(full_path.exists());

                /*let contents = std::fs::read_to_string(&full_path)?;
                assert_eq!(
                    contents,
                            "file,label,min_x,min_y,width,height
                    train/dog_1.jpg,dog,101.5,32.0,385,330
                    train/dog_1.jpg,dog,102.5,31.0,386,330
                    train/dog_2.jpg,dog,7.0,29.5,246,247
                    train/dog_3.jpg,dog,19.0,63.5,376,421
                    train/cat_1.jpg,cat,57.0,35.5,304,427
                    train/cat_2.jpg,cat,30.5,44.0,333,396
                    "
                );*/


                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }

     #[tokio::test]
    async fn test_remote_mode_restore_dir() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|mut _local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            
            test::run_empty_dir_test_async(|dir| async move {
                let mut opts = CloneOpts::new(&remote_repo.remote.url, dir.join("new_repo"));
                opts.is_remote = true;
                let cloned_repo = repositories::clone(&opts).await?;
                let repo_path = cloned_repo.path.clone();

                let workspace_identifier = cloned_repo.workspace_name.clone().unwrap();
                let directory = ".".to_string();
                let status_opts = StagedDataOpts::from_paths_remote_mode(&[PathBuf::from(directory.clone())]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();
                

                let annotations_path = PathBuf::from("annotations");
                let head_commit = repositories::commits::head_commit(&cloned_repo)?;
                repositories::remote_mode::restore(&cloned_repo, &vec![annotations_path.clone()], &head_commit.id).await?;

                let status_opts = StagedDataOpts::from_paths_remote_mode(&[repo_path.clone()]);
                let status = repositories::remote_mode::status(&cloned_repo, &remote_repo, &workspace_identifier, &directory, &status_opts).await?;
                status.print();

                let bounding_box_path = cloned_repo.path.join("annotations").join("train").join("bounding_box.csv");
                let two_shot_path = cloned_repo.path.join("annotations").join("train").join("two_shot.csv");
                let one_shot_path = cloned_repo.path.join("annotations").join("train").join("one_shot.csv");
                assert!(bounding_box_path.exists());
                assert!(one_shot_path.exists());
                assert!(two_shot_path.exists());

  


                Ok(())
            }).await?;

            Ok(remote_repo_copy)
        }).await
    }
}