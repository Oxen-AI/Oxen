pub use crate::core::v_latest::resource::parse_resource_from_path;

#[cfg(test)]
mod tests {
    use crate::test;
    use std::path::Path;

    use crate::error::OxenError;
    use crate::repositories;
    use crate::resource;

    #[tokio::test]
    async fn test_parse_resource_for_commit() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let history = repositories::commits::list(&repo)?;
            let commit = history.first().unwrap();
            let path_str = format!("{}/annotations/train/one_shot.csv", commit.id);
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    assert_eq!(commit.id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a commit");
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_for_branch() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "my-branch";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/annotations/train/one_shot.csv");
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_for_long_branch_name() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "my/crazy/branch/name";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/annotations/train/one_shot.csv");
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_for_branch_base_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "my_branch";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = branch_name.to_string();
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new(""));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_from_path_root_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "main";
            // let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/");
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    assert_eq!(resource.path, Path::new(""))
                }
                _ => {
                    panic!("Should return a parsed resource");
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_from_path_root_dir_complicated_branch() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "super/complex/branch-name/slashes";
            let _branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/");
            let path = Path::new(&path_str);

            if !cfg!(windows) {
                // skip on windows, running on linux
                match resource::parse_resource_from_path(&repo, path) {
                    Ok(Some(resource)) => {
                        assert_eq!(resource.path, Path::new(""))
                    }
                    _ => {
                        panic!("Should return a parsed resource");
                    }
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_from_path_nonroot_complicated_branch() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "super/complex/branch-name/slashes";
            let _branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path = Path::new(branch_name).join("folder-new");

            // should be running server on linux so skip windows
            if !cfg!(windows) {
                match resource::parse_resource_from_path(&repo, &path) {
                    Ok(Some(resource)) => {
                        assert_eq!(resource.path, Path::new("folder-new"))
                    }
                    _ => {
                        panic!("Should return a parsed resource");
                    }
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_from_path_with_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "super/complex/branch-name/slashes";
            let _branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/folder/item.txt");
            let path = Path::new(&path_str);

            if !cfg!(windows) {
                match resource::parse_resource_from_path(&repo, path) {
                    Ok(Some(resource)) => {
                        assert_eq!(resource.path, Path::new("folder/item.txt"))
                    }
                    _ => {
                        panic!("Should return a parsed resource");
                    }
                }
            }

            Ok(())
        })
        .await
    }

    // -----------------------------------------------------------------------
    // Precedence tests — lock in the commit > branch > workspace ordering
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_parse_resource_commit_wins_over_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;

            // Create a branch whose name is exactly the commit id.
            repositories::branches::create_from_head(&repo, &commit.id)?;

            let path_str = format!("{}/some/file.txt", commit.id);
            let path = Path::new(&path_str);
            let resource =
                resource::parse_resource_from_path(&repo, path)?.expect("should resolve");

            // Must resolve as a commit, not the branch.
            assert!(resource.commit.is_some(), "expected commit");
            assert!(resource.branch.is_none(), "should not resolve as branch");
            assert_eq!(resource.commit.unwrap().id, commit.id);
            assert_eq!(resource.path, Path::new("some/file.txt"));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_branch_wins_over_workspace() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;

            // Create a branch and a workspace with the same name.
            let shared_name = "shared-name";
            let branch = repositories::branches::create_from_head(&repo, shared_name)?;
            let _workspace = repositories::workspaces::create(&repo, &commit, shared_name, false)?;

            let path_str = format!("{shared_name}/data.csv");
            let path = Path::new(&path_str);
            let resource =
                resource::parse_resource_from_path(&repo, path)?.expect("should resolve");

            // Must resolve as a branch, not the workspace.
            assert!(resource.branch.is_some(), "expected branch");
            assert!(
                resource.workspace.is_none(),
                "should not resolve as workspace"
            );
            assert_eq!(resource.branch.unwrap().name, branch.name);
            assert_eq!(resource.path, Path::new("data.csv"));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_workspace_resolves_when_no_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;

            let workspace = repositories::workspaces::create(&repo, &commit, "my-workspace", true)?;

            let path_str = format!("{}/data.csv", workspace.id);
            let path = Path::new(&path_str);
            let resource =
                resource::parse_resource_from_path(&repo, path)?.expect("should resolve");

            assert!(resource.workspace.is_some(), "expected workspace");
            assert!(resource.branch.is_none());
            assert!(resource.commit.is_none());
            assert_eq!(resource.workspace.unwrap().id, workspace.id);
            assert_eq!(resource.path, Path::new("data.csv"));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_full_branch_path_match() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Branch name spans the entire path — no file portion.
            let branch_name = "feature/v2/experiment";
            let branch = repositories::branches::create_from_head(&repo, branch_name)?;

            let path = Path::new(branch_name);
            let resource =
                resource::parse_resource_from_path(&repo, path)?.expect("should resolve");

            assert!(resource.branch.is_some());
            assert_eq!(resource.branch.unwrap().name, branch.name);
            assert_eq!(resource.path, Path::new(""));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_parse_resource_returns_none_for_unknown() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let path = Path::new("nonexistent-thing/file.txt");
            let result = resource::parse_resource_from_path(&repo, path)?;
            assert!(result.is_none(), "unknown identifier should return None");

            Ok(())
        })
        .await
    }
}
