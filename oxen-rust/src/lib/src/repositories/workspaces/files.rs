use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::file::TempFilePathNew;
use crate::model::Commit;
use crate::model::Workspace;
use crate::model::{Branch, User};
use crate::opts::PaginateOpts;
use crate::view::ErrorFileInfo;
use crate::view::PaginatedDirEntries;

use std::path::{Path, PathBuf};

pub fn list(
    workspace: &Workspace,
    directory: impl AsRef<Path>,
    paginate_opts: &PaginateOpts,
) -> Result<PaginatedDirEntries, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::files::list(workspace, directory, paginate_opts),
    }
}

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
    use crate::opts::PaginateOpts;
    use crate::repositories::{self, workspaces};
    use crate::test;
    use crate::view::entries::EMetadataEntry;

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

    #[tokio::test]
    async fn test_list_workspace_files_no_changes() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::current_branch(&repo)?.unwrap();
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-list-no-changes", true)?;

            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };
            let result = workspaces::files::list(&workspace, Path::new(""), &paginate_opts)?;

            // Should have entries from the commit tree
            assert!(
                !result.entries.is_empty(),
                "Should return commit tree entries"
            );

            // All entries should be WorkspaceMetadataEntry with no changes
            for entry in &result.entries {
                if let EMetadataEntry::WorkspaceMetadataEntry(ws_entry) = entry {
                    assert!(
                        ws_entry.changes.is_none(),
                        "No workspace changes should be present"
                    );
                } else {
                    panic!("Expected WorkspaceMetadataEntry");
                }
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_files_with_added_file() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test_async(|repo| async move {
            // Create a file and commit
            let file = repo.path.join("original.txt");
            crate::util::fs::write_to_path(&file, "original content")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add original.txt")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-list-added", true)?;

            // Add a new file to the workspace
            let new_file = workspace.dir().join("added.txt");
            crate::util::fs::write_to_path(&new_file, "new content")?;
            workspaces::files::add(&workspace, &new_file).await?;

            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };
            let result = workspaces::files::list(&workspace, Path::new(""), &paginate_opts)?;

            // Should have 2 entries: original.txt + added.txt
            assert_eq!(result.entries.len(), 2, "Should have original + added file");
            assert_eq!(result.total_entries, 2);

            // Find the added file
            let added = result
                .entries
                .iter()
                .find(|e| e.filename() == "added.txt")
                .expect("Should find added.txt");
            if let EMetadataEntry::WorkspaceMetadataEntry(ws_entry) = added {
                assert!(ws_entry.changes.is_some(), "Added file should have changes");
                assert_eq!(
                    ws_entry.changes.as_ref().unwrap().status,
                    crate::model::StagedEntryStatus::Added
                );
            } else {
                panic!("Expected WorkspaceMetadataEntry");
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_files_with_removed_file() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test_async(|repo| async move {
            // Create two files and commit
            let file1 = repo.path.join("keep.txt");
            let file2 = repo.path.join("remove_me.txt");
            crate::util::fs::write_to_path(&file1, "keep")?;
            crate::util::fs::write_to_path(&file2, "remove")?;
            repositories::add(&repo, &file1).await?;
            repositories::add(&repo, &file2).await?;
            let commit = repositories::commit(&repo, "Add two files")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-list-removed", true)?;

            // Remove the file from the workspace (path relative to repo root)
            let rm_path = Path::new("remove_me.txt");
            workspaces::files::rm(&workspace, rm_path).await?;

            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };
            let result = workspaces::files::list(&workspace, Path::new(""), &paginate_opts)?;

            // Should have only 1 entry — the removed file is filtered out
            assert_eq!(
                result.entries.len(),
                1,
                "Removed file should be filtered out"
            );
            assert_eq!(result.total_entries, 1);
            assert_eq!(result.entries[0].filename(), "keep.txt");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_files_with_modified_file() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("data.txt");
            crate::util::fs::write_to_path(&file, "original")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add data.txt")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-list-modified", true)?;

            // Modify the file in the workspace
            let workspace_file = workspace.dir().join("data.txt");
            crate::util::fs::write_to_path(&workspace_file, "modified content")?;
            workspaces::files::add(&workspace, &workspace_file).await?;

            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };
            let result = workspaces::files::list(&workspace, Path::new(""), &paginate_opts)?;

            assert_eq!(result.entries.len(), 1);
            let entry = &result.entries[0];
            if let EMetadataEntry::WorkspaceMetadataEntry(ws_entry) = entry {
                assert!(
                    ws_entry.changes.is_some(),
                    "Modified file should have changes"
                );
                assert_eq!(
                    ws_entry.changes.as_ref().unwrap().status,
                    crate::model::StagedEntryStatus::Modified
                );
            } else {
                panic!("Expected WorkspaceMetadataEntry");
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_files_subdirectory() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::current_branch(&repo)?.unwrap();
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-list-subdir", true)?;

            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };
            let result =
                workspaces::files::list(&workspace, Path::new("annotations"), &paginate_opts)?;

            // Should return entries inside "annotations" directory
            assert!(
                !result.entries.is_empty(),
                "Should have entries in annotations directory"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_files_added_file_new_dir() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("root.txt");
            crate::util::fs::write_to_path(&file, "root file")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add root.txt")?;

            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-list-new-dir", true)?;

            // Add a file to a new directory that doesn't exist in the commit
            let new_dir = workspace.dir().join("new_dir");
            std::fs::create_dir_all(&new_dir)?;
            let new_file = new_dir.join("new_file.txt");
            crate::util::fs::write_to_path(&new_file, "new file in new dir")?;
            workspaces::files::add(&workspace, &new_file).await?;

            // List at root — should show root.txt and the new directory entry
            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 100,
            };
            let result = workspaces::files::list(&workspace, Path::new(""), &paginate_opts)?;

            // Should have root.txt + new_dir (directory entry from addition)
            let filenames: Vec<&str> = result.entries.iter().map(|e| e.filename()).collect();
            assert!(filenames.contains(&"root.txt"), "Should contain root.txt");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspace_files_pagination() -> Result<(), OxenError> {
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let branch = repositories::branches::current_branch(&repo)?.unwrap();
            let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();
            let workspace =
                repositories::workspaces::create(&repo, &commit, "test-list-paginate", true)?;

            // Use a small page size to test pagination
            let paginate_opts = PaginateOpts {
                page_num: 1,
                page_size: 2,
            };
            let result = workspaces::files::list(&workspace, Path::new(""), &paginate_opts)?;

            assert_eq!(result.entries.len(), 2, "Should respect page_size");
            assert_eq!(result.page_size, 2);
            assert_eq!(result.page_number, 1);
            assert!(
                result.total_entries > 2,
                "Training data repo should have more than 2 entries at root"
            );
            assert!(result.total_pages > 1, "Should have multiple pages");

            // Fetch page 2
            let paginate_opts_p2 = PaginateOpts {
                page_num: 2,
                page_size: 2,
            };
            let result_p2 = workspaces::files::list(&workspace, Path::new(""), &paginate_opts_p2)?;
            assert_eq!(result_p2.page_number, 2);
            assert!(!result_p2.entries.is_empty(), "Page 2 should have entries");

            // Entries on different pages should be different
            assert_ne!(
                result.entries[0].filename(),
                result_p2.entries[0].filename(),
                "Different pages should have different entries"
            );

            Ok(())
        })
        .await
    }
}
