use crate::core::refs::with_ref_manager;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, ParsedResource};
use crate::repositories;

use std::path::Path;

pub fn parse_resource_from_path(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::resource::parse_resource_from_path(repo, path),
    }
}

/// Pass in a branch name and maybe get a commit id back
pub fn maybe_get_commit_id_from_branch_name<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: S,
) -> Result<Option<String>, OxenError> {
    with_ref_manager(repo, |manager| {
        manager.get_commit_id_for_branch(commit_id_or_branch_name.as_ref())
    })
}

/// Pass in a commit id or a branch name and resolve it to a
pub fn maybe_get_commit<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: S,
) -> Result<Option<Commit>, OxenError> {
    if let Some(commit) = repositories::commits::get_by_id(repo, &commit_id_or_branch_name)? {
        return Ok(Some(commit));
    }

    match maybe_get_commit_id_from_branch_name(repo, &commit_id_or_branch_name) {
        Ok(Some(commit_id)) => repositories::commits::get_by_id(repo, &commit_id),
        Ok(None) => Err(OxenError::local_revision_not_found(
            commit_id_or_branch_name.as_ref(),
        )),
        Err(err) => Err(err),
    }
}

pub fn get_commit_or_head<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: Option<S>,
) -> Result<Commit, OxenError> {
    if commit_id_or_branch_name.is_none() {
        return repositories::commits::head_commit(repo);
    }

    match maybe_get_commit(repo, commit_id_or_branch_name.unwrap().as_ref()) {
        Ok(Some(commit)) => Ok(commit),
        _ => repositories::commits::head_commit(repo),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::error::OxenError;
    use crate::repositories;
    use crate::resource;

    #[tokio::test]
    async fn test_parse_resource_for_commit() -> Result<(), OxenError> {
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
        oxen_test::run_training_data_repo_test_fully_committed_async(|repo| async move {
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
}
