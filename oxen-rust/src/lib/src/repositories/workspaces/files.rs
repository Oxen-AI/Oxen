use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::file::TempFilePathNew;
use crate::model::Commit;
use crate::model::Workspace;
use crate::model::{Branch, User};
use crate::view::ErrorFileInfo;

use std::path::{Path, PathBuf};

pub fn exists(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::exists(workspace, path),
    }
}

pub async fn add(workspace: &Workspace, path: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::add(workspace, path).await,
    }
}

pub async fn rm(
    workspace: &Workspace,
    path: impl AsRef<Path>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::rm(workspace, path).await,
    }
}

pub fn delete(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::delete(workspace, path),
    }
}

pub async fn import(
    url: &str,
    auth: &str,
    directory: PathBuf,
    filename: String,
    workspace: &Workspace,
) -> Result<(), OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::workspaces::files::import(url, auth, directory, filename, workspace)
                .await?;
            Ok(())
        }
    }
}

pub async fn upload_zip(
    commit_message: &str,
    user: &User,
    temp_files: Vec<TempFilePathNew>,
    workspace: &Workspace,
    branch: &Branch,
) -> Result<Commit, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::workspaces::files::upload_zip(
                commit_message,
                user,
                temp_files,
                workspace,
                branch,
            )
            .await
        }
    }
}

pub fn mv(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::mv(workspace, path, new_path),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::config::UserConfig;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::repositories::{self, workspaces};
    use crate::test;

    #[tokio::test]
    async fn test_mv_file_in_workspace() -> Result<(), OxenError> {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch_name = "test-mv";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace_id = UserConfig::identifier()?;
            let workspace = repositories::workspaces::create(&repo, &commit, workspace_id, true)?;

            // Original file path that exists in the repo
            let original_path = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let new_path = Path::new("renamed").join("data").join("bbox_renamed.csv");

            // Move the file
            let result = workspaces::files::mv(&workspace, &original_path, &new_path)?;
            assert_eq!(result, new_path);

            // Check status - should show the original as removed and new as added
            let status = workspaces::status::status(&workspace)?;
            println!("Status after mv: {:?}", status);

            // The original path should be staged as removed in staged_files
            let removed_entry = status.staged_files.get(&original_path);
            assert!(
                removed_entry.is_some(),
                "Original path should be in staged_files"
            );
            assert_eq!(
                removed_entry.unwrap().status,
                crate::model::StagedEntryStatus::Removed,
                "Original path should have Removed status"
            );

            // The new path should be in staged_files with Added status
            let added_entry = status.staged_files.get(&new_path);
            assert!(added_entry.is_some(), "New path should be staged");
            assert_eq!(
                added_entry.unwrap().status,
                crate::model::StagedEntryStatus::Added,
                "New path should have Added status"
            );

            // The move should be detected
            assert!(
                !status.moved_files.is_empty(),
                "Move should be detected in moved_files"
            );

            // Commit the workspace and verify the file is at the new location
            let user = UserConfig::get()?.to_user();
            let new_commit = NewCommitBody {
                author: user.name.clone(),
                email: user.email.clone(),
                message: "Moved file to new location".to_string(),
            };
            let commit =
                workspaces::commit(&workspace, &new_commit, branch_name.to_string()).await?;

            // Verify the file exists at the new path in the commit
            let new_file = repositories::tree::get_file_by_path(&repo, &commit, &new_path)?;
            assert!(
                new_file.is_some(),
                "File should exist at new path after commit"
            );

            // Verify the file no longer exists at the original path
            let old_file = repositories::tree::get_file_by_path(&repo, &commit, &original_path)?;
            assert!(
                old_file.is_none(),
                "File should not exist at original path after commit"
            );

            Ok(())
        })
        .await
    }

    /// Workspace created with no files staged; exists() should return Ok(false).
    #[tokio::test]
    async fn test_exists_returns_false_when_no_files_staged() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-workspace", false)?;

            let result = workspaces::files::exists(&workspace, std::path::Path::new("hello.txt"))?;
            assert!(!result);

            Ok(())
        })
        .await
    }

    /// exists() returns Ok(false) for an unstaged path and Ok(true) after staging it.
    #[tokio::test]
    async fn test_exists_false_before_staging_true_after() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-workspace", false)?;

            // Write modified content so the file is detected as changed
            let workspace_file = workspace.workspace_repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&workspace_file, "hello world")?;

            let hello = Path::new("hello.txt");
            let nonexistent = Path::new("does_not_exist.txt");

            // Before staging: both should be false
            assert!(!workspaces::files::exists(&workspace, hello)?);
            assert!(!workspaces::files::exists(&workspace, nonexistent)?);

            // Stage the file in the workspace
            workspaces::files::add(&workspace, &workspace_file).await?;

            // After staging: staged file is true, non-existent file is still false
            assert!(workspaces::files::exists(&workspace, hello)?);
            assert!(!workspaces::files::exists(&workspace, nonexistent)?);

            Ok(())
        })
        .await
    }
}
