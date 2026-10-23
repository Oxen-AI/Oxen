use crate::core;
use crate::error::OxenError;
use crate::model::Workspace;

use std::path::PathBuf;

pub use crate::core::v_latest::workspaces::files::exists;

pub use crate::core::v_latest::workspaces::files::is_dir;

pub use crate::core::v_latest::workspaces::files::add;

pub use crate::core::v_latest::workspaces::files::rm;

pub use crate::core::v_latest::workspaces::files::unstage;

pub async fn import(
    url: &str,
    auth: &str,
    directory: PathBuf,
    filename: Option<String>,
    workspace: &Workspace,
    allow_loopback: bool,
) -> Result<(), OxenError> {
    core::v_latest::workspaces::files::import(
        url,
        auth,
        directory,
        filename,
        workspace,
        allow_loopback,
    )
    .await?;
    Ok(())
}

pub use crate::core::v_latest::workspaces::files::upload_zip;

pub use crate::core::v_latest::workspaces::files::mv;

pub use crate::core::v_latest::workspaces::files::copy;

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use crate::config::UserConfig;
    use crate::core::staged::staged_db_manager::get_staged_db_manager;
    use crate::error::OxenError;
    use crate::model::{Commit, LocalRepository, NewCommitBody, StagedEntryStatus, Workspace};
    use crate::repositories::size;
    use crate::repositories::{self, workspaces};
    use crate::test;
    use crate::util::fs;

    #[tokio::test]
    async fn test_mv_file_in_workspace() -> Result<(), OxenError> {
        // Skip workspace ops on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_bounding_box_csv_repo_test_fully_committed_async(|repo| async move {
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
            workspaces::files::mv(&workspace, &original_path, &new_path).await?;

            // Check status - should show the original as removed and new as added
            let status = workspaces::status::status(&workspace)?;
            println!("Status after mv: {status:?}");

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

            assert!(
                !size::repo_size_path(&repo).exists(),
                "committing locally records no size, so there is none to start from"
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

            assert_eq!(
                size::wait_for_recorded_size(&repo)?,
                repo.version_bytes()?,
                "the figure recorded is the bytes the tree references"
            );
            assert_eq!(
                fs::read_from_path(size::repo_size_path(&repo))?.trim(),
                repo.version_bytes()?.to_string(),
                "the figure is recorded on its own, with no status beside it"
            );

            // A record claiming a calculation is under way, with nothing running to finish it.
            fs::write_to_path(
                size::repo_size_path(&repo),
                r#"{"status":"pending","size":7}"#,
            )?;
            assert_eq!(
                size::wait_for_recorded_size(&repo)?,
                repo.version_bytes()?,
                "a record no pass stands behind is recalculated rather than believed"
            );

            Ok(())
        })
        .await
    }

    /// An editable workspace called `name` on `commit`, and a branch called `name` at `commit` for
    /// it to commit to.
    fn committing_workspace(
        repo: &LocalRepository,
        commit: &Commit,
        name: &str,
    ) -> Result<Workspace, OxenError> {
        repositories::branches::create(repo, name, &commit.id)?;
        repositories::workspaces::create(repo, commit, name, true)
    }

    async fn stage_new_file(workspace: &Workspace, path: &str) -> Result<(), OxenError> {
        let path = workspace.workspace_repo.path.join(path);
        fs::write_to_path(&path, "new")?;
        workspaces::files::add(workspace, path).await?;
        Ok(())
    }

    fn staged_status(workspace: &Workspace, path: &str) -> Option<StagedEntryStatus> {
        workspaces::status::status(workspace)
            .unwrap()
            .staged_files
            .get(Path::new(path))
            .map(|entry| entry.status.clone())
    }

    fn assert_files(repo: &LocalRepository, commit: &Commit, present: &[&str], absent: &[&str]) {
        for path in present {
            assert!(
                repositories::tree::get_file_by_path(repo, commit, path)
                    .unwrap()
                    .is_some(),
                "{path} should be in the commit"
            );
        }
        for path in absent {
            assert!(
                repositories::tree::get_node_by_path(repo, commit, path)
                    .unwrap()
                    .is_none(),
                "{path} should not be in the commit"
            );
            assert!(
                !repositories::tree::has_dir(repo, commit, path).unwrap(),
                "{path} should not be a directory in the commit"
            );
        }
    }

    /// Each case works in its own workspace on one commit of data/a.txt, data/sub/b.txt, other.txt
    /// and table.csv. A case that commits does so to its own branch, with its own message, since
    /// two commits with the same parent, message, and author in the same second get the same id.
    #[tokio::test]
    async fn test_move_and_remove_workspace_files() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            for (path, content) in [
                ("data/a.txt", "A"),
                ("data/sub/b.txt", "B"),
                ("other.txt", "Other"),
                ("table.csv", "x,y\n1,2\n"),
            ] {
                fs::write_to_path(repo.path.join(path), content)?;
            }
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Add data")?;
            let user = UserConfig::get()?;
            let other = Path::new("other.txt");

            // exists() is false for a path the workspace has not staged, and true once it has.
            let workspace = repositories::workspaces::create(&repo, &commit, "exists", false)?;
            let nonexistent = Path::new("does_not_exist.txt");
            assert!(
                !workspaces::files::exists(&workspace, other)?,
                "a committed path the workspace has not staged"
            );
            let workspace_file = workspace.workspace_repo.path.join(other);
            fs::write_to_path(&workspace_file, "Other, changed")?;
            assert!(
                !workspaces::files::exists(&workspace, other)?,
                "a changed working copy is not staged until it is added"
            );
            assert!(!workspaces::files::exists(&workspace, nonexistent)?);
            workspaces::files::add(&workspace, &workspace_file).await?;
            assert!(workspaces::files::exists(&workspace, other)?);
            assert!(!workspaces::files::exists(&workspace, nonexistent)?);

            // Import refuses every URL that does not reach a public HTTP host.
            let workspace = repositories::workspaces::create_temporary(&repo, &commit).await?;
            let private = "private/reserved IP address";
            let cases = [
                ("http://127.0.0.1/secret", "loopback address", private),
                ("http://10.0.0.1/internal", "private address", private),
                (
                    "http://169.254.169.254/latest/meta-data/",
                    "link-local/metadata address",
                    private,
                ),
                (
                    "http://[::ffff:169.254.169.254]/latest/meta-data/",
                    "IPv4-mapped metadata address",
                    private,
                ),
                ("http://[::1]/secret", "IPv6 loopback", private),
                // The import client's resolver checks a host name as it connects.
                (
                    "http://localhost/secret",
                    "name resolving to loopback",
                    private,
                ),
                (
                    "file:///etc/passwd",
                    "non-HTTP scheme",
                    "Only http and https",
                ),
            ];
            for (url, label, expected) in cases {
                // allow_loopback is false, so loopback targets are rejected like other private IPs.
                let result = workspaces::files::import(
                    url,
                    "",
                    PathBuf::from("data"),
                    None,
                    &workspace,
                    false,
                )
                .await;
                let err = result.expect_err(label).to_string();
                assert!(err.contains(expected), "{label}: got {err}");
            }

            // A redirect to a private IP literal is refused, even from an allowed host.
            let mut server = mockito::Server::new_async().await;
            let _redirect = server
                .mock("GET", "/moved")
                .with_status(302)
                .with_header("location", "http://10.0.0.1/internal")
                .create_async()
                .await;
            let err = workspaces::files::import(
                &format!("{}/moved", server.url()),
                "",
                PathBuf::from("data"),
                None,
                &workspace,
                true,
            )
            .await
            .expect_err("redirect to a private address")
            .to_string();
            assert!(err.contains(private), "redirect: got {err}");
            drop(workspace);

            // Skip workspace moves and removals on windows
            if std::env::consts::OS == "windows" {
                return Ok(());
            }

            // A copy commits beside its source with the source's content, and is refused where the
            // source has staged changes or something is already at the destination.
            let workspace = committing_workspace(&repo, &commit, "copy")?;
            let table_copy = "out/table_copy.csv";
            workspaces::files::copy(&workspace, Path::new("table.csv"), Path::new(table_copy))
                .await?;
            workspaces::files::copy(&workspace, other, Path::new("other_copy.txt")).await?;
            assert_eq!(
                staged_status(&workspace, table_copy),
                Some(StagedEntryStatus::Added)
            );
            assert_eq!(
                staged_status(&workspace, "table.csv"),
                None,
                "the source stays as committed"
            );
            stage_new_file(&workspace, "data/a.txt").await?;
            workspaces::files::rm(&workspace, &[PathBuf::from("data/sub/b.txt")]).await?;
            for (source, dest, refusal) in [
                ("data/a.txt", "a_copy.txt", "SourceHasStagedChanges"),
                ("data/sub/b.txt", "b_copy.txt", "PathStagedForRemoval"),
                ("data", "data_copy", "NotAFile"),
                ("missing.txt", "missing_copy.txt", "PathDoesNotExist"),
                ("table.csv", "other.txt", "DestinationExists"),
                ("table.csv", "other_copy.txt", "DestinationAlreadyStaged"),
                ("table.csv", "other.txt/table.csv", "NotADirectory"),
            ] {
                let result =
                    workspaces::files::copy(&workspace, Path::new(source), Path::new(dest)).await;
                assert!(
                    result
                        .as_ref()
                        .is_err_and(|err| format!("{err:?}").starts_with(refusal)),
                    "copying {source} to {dest} should be refused with {refusal}, got {result:?}"
                );
            }
            let committed = workspaces::commit(
                &workspace,
                &NewCommitBody::from_config(&user, "Copy files"),
                "copy",
            )
            .await?;
            for (source, copy) in [("table.csv", table_copy), ("other.txt", "other_copy.txt")] {
                let hash = |path: &str| {
                    *repositories::tree::get_file_by_path(&repo, &committed, path)
                        .unwrap()
                        .unwrap_or_else(|| panic!("{path} should be in the commit"))
                        .hash()
                };
                assert_eq!(hash(copy), hash(source), "{copy} has {source}'s content");
            }

            // A committed directory moves with its nested subdirectory, and retrying the move
            // finds its source staged for removal.
            let workspace = committing_workspace(&repo, &commit, "nested")?;
            workspaces::files::mv(&workspace, Path::new("data"), Path::new("moved")).await?;
            for (path, status) in [
                ("data/a.txt", StagedEntryStatus::Removed),
                ("data/sub/b.txt", StagedEntryStatus::Removed),
                ("moved/a.txt", StagedEntryStatus::Added),
                ("moved/sub/b.txt", StagedEntryStatus::Added),
            ] {
                assert_eq!(staged_status(&workspace, path), Some(status), "{path}");
            }
            let retry =
                workspaces::files::mv(&workspace, Path::new("data"), Path::new("moved")).await;
            assert!(
                matches!(retry, Err(OxenError::PathStagedForRemoval(_))),
                "retrying the move finds its source staged for removal, got {retry:?}"
            );
            let committed = workspaces::commit(
                &workspace,
                &NewCommitBody::from_config(&user, "Move data"),
                "nested",
            )
            .await?;
            assert_files(
                &repo,
                &committed,
                &["moved/a.txt", "moved/sub/b.txt", "other.txt"],
                &["data", "data/sub", "data/a.txt", "data/sub/b.txt"],
            );
            assert!(repositories::tree::has_dir(&repo, &committed, "moved/sub")?);

            // A later commit inside the moved directory reads it as that commit left it, and
            // the one after builds on that rather than on the directory as the move left it.
            let workspace = repositories::workspaces::create(&repo, &committed, "nested-2", true)?;
            workspaces::files::mv(
                &workspace,
                Path::new("moved/a.txt"),
                Path::new("moved/c.txt"),
            )
            .await?;
            let renamed = workspaces::commit(
                &workspace,
                &NewCommitBody::from_config(&user, "Rename a in moved"),
                "nested",
            )
            .await?;
            assert_files(
                &repo,
                &renamed,
                &["moved/c.txt", "moved/sub/b.txt"],
                &["moved/a.txt"],
            );
            let workspace = repositories::workspaces::create(&repo, &renamed, "nested-3", true)?;
            stage_new_file(&workspace, "moved/d.txt").await?;
            let added = workspaces::commit(
                &workspace,
                &NewCommitBody::from_config(&user, "Add d in moved"),
                "nested",
            )
            .await?;
            assert_files(
                &repo,
                &added,
                &["moved/c.txt", "moved/d.txt", "moved/sub/b.txt"],
                &["moved/a.txt"],
            );

            // Files added in the workspace move with the directory, and a file staged for removal
            // stays removed rather than reappearing under the new path.
            let workspace = committing_workspace(&repo, &commit, "mixed")?;
            stage_new_file(&workspace, "data/new.txt").await?;
            stage_new_file(&workspace, "data/fresh/deep.txt").await?;
            workspaces::files::rm(&workspace, &[PathBuf::from("data/a.txt")]).await?;
            workspaces::files::mv(&workspace, Path::new("data"), Path::new("moved")).await?;
            assert_eq!(staged_status(&workspace, "data/new.txt"), None);
            assert_eq!(staged_status(&workspace, "data/fresh/deep.txt"), None);
            assert_eq!(staged_status(&workspace, "moved/a.txt"), None);
            assert_eq!(
                staged_status(&workspace, "moved/new.txt"),
                Some(StagedEntryStatus::Added)
            );
            assert_eq!(
                staged_status(&workspace, "moved/fresh/deep.txt"),
                Some(StagedEntryStatus::Added)
            );
            let committed = workspaces::commit(
                &workspace,
                &NewCommitBody::from_config(&user, "Move data with added files"),
                "mixed",
            )
            .await?;
            assert_files(
                &repo,
                &committed,
                &["moved/new.txt", "moved/fresh/deep.txt", "moved/sub/b.txt"],
                &["data", "data/fresh", "moved/a.txt"],
            );

            // A directory that exists only in the workspace moves, and its emptied parent goes.
            let workspace = committing_workspace(&repo, &commit, "staged-only")?;
            stage_new_file(&workspace, "outer/fresh/x.txt").await?;
            stage_new_file(&workspace, "outer/fresh/deep/y.txt").await?;
            workspaces::files::mv(&workspace, Path::new("outer/fresh"), Path::new("renamed"))
                .await?;
            let committed = workspaces::commit(
                &workspace,
                &NewCommitBody::from_config(&user, "Move fresh"),
                "staged-only",
            )
            .await?;
            assert_files(
                &repo,
                &committed,
                &["renamed/x.txt", "renamed/deep/y.txt", "data/a.txt"],
                &["outer", "outer/fresh"],
            );

            // Removing a directory also unstages the files added under it in the workspace, so
            // the commit does not bring the directory back.
            let workspace = committing_workspace(&repo, &commit, "rm-dir")?;
            stage_new_file(&workspace, "data/new.txt").await?;
            stage_new_file(&workspace, "data/fresh/deep.txt").await?;
            let err_files = workspaces::files::rm(&workspace, &[PathBuf::from("data")]).await?;
            assert!(err_files.is_empty(), "{err_files:?}");
            assert_eq!(staged_status(&workspace, "data/new.txt"), None);
            assert_eq!(staged_status(&workspace, "data/fresh/deep.txt"), None);
            let committed = workspaces::commit(
                &workspace,
                &NewCommitBody::from_config(&user, "Remove data"),
                "rm-dir",
            )
            .await?;
            assert_files(
                &repo,
                &committed,
                &["other.txt"],
                &["data", "data/new.txt", "data/fresh"],
            );

            // A directory move onto an existing or staged path, into itself, or from a missing
            // source is refused and stages nothing.
            let workspace = repositories::workspaces::create(&repo, &commit, "refused", true)?;
            stage_new_file(&workspace, "staged/z.txt").await?;
            for dest in ["other.txt", "staged"] {
                let result =
                    workspaces::files::mv(&workspace, Path::new("data"), Path::new(dest)).await;
                assert!(
                    matches!(result, Err(OxenError::DestinationAlreadyStaged(_))),
                    "moving onto {dest} should be refused, got {result:?}"
                );
            }
            for dest in ["data", "data/sub/inner"] {
                let result =
                    workspaces::files::mv(&workspace, Path::new("data"), Path::new(dest)).await;
                assert!(
                    matches!(result, Err(OxenError::MoveIntoItself(_))),
                    "moving into {dest} should be refused, got {result:?}"
                );
            }
            let result =
                workspaces::files::mv(&workspace, Path::new("nope"), Path::new("elsewhere")).await;
            assert!(
                matches!(result, Err(OxenError::PathDoesNotExist(_))),
                "a missing source should be refused, got {result:?}"
            );
            // Beneath a committed or a staged file, a move would turn that file into a directory.
            for (source, dest) in [
                ("data", "other.txt/inner"),
                ("data/a.txt", "other.txt/a.txt"),
                ("data", "staged/z.txt/deep/inner"),
            ] {
                let result =
                    workspaces::files::mv(&workspace, Path::new(source), Path::new(dest)).await;
                assert!(
                    matches!(result, Err(OxenError::NotADirectory(_))),
                    "moving {source} to {dest} should be refused, got {result:?}"
                );
            }
            let result = repositories::workspaces::data_frames::rename(
                &workspace,
                "table.csv",
                "other.txt/table.csv",
            )
            .await;
            assert!(
                matches!(result, Err(OxenError::NotADirectory(_))),
                "renaming a data frame beneath a file should be refused, got {result:?}"
            );
            assert_eq!(staged_status(&workspace, "data/a.txt"), None);
            assert_eq!(staged_status(&workspace, "other.txt"), None);
            assert_eq!(
                staged_status(&workspace, "staged/z.txt"),
                Some(StagedEntryStatus::Added)
            );
            // A data frame the workspace never indexed renames too.
            repositories::workspaces::data_frames::rename(&workspace, "table.csv", "renamed.csv")
                .await?;
            assert_eq!(
                staged_status(&workspace, "renamed.csv"),
                Some(StagedEntryStatus::Added)
            );

            // Moving or renaming a file the workspace removed would restore its content at the new
            // path. Tabular files move through the data frame rename, which refuses it too.
            let workspace = repositories::workspaces::create(&repo, &commit, "removed", true)?;
            workspaces::files::rm(&workspace, &[PathBuf::from("other.txt")]).await?;
            let result =
                workspaces::files::mv(&workspace, Path::new("other.txt"), Path::new("moved.txt"))
                    .await;
            assert!(
                matches!(result, Err(OxenError::PathStagedForRemoval(_))),
                "{result:?}"
            );
            assert_eq!(staged_status(&workspace, "moved.txt"), None);
            assert_eq!(
                staged_status(&workspace, "other.txt"),
                Some(StagedEntryStatus::Removed)
            );
            workspaces::files::rm(&workspace, &[PathBuf::from("table.csv")]).await?;
            let result =
                repositories::workspaces::data_frames::rename(&workspace, "table.csv", "moved.csv")
                    .await;
            assert!(
                matches!(result, Err(OxenError::PathStagedForRemoval(_))),
                "{result:?}"
            );
            assert_eq!(staged_status(&workspace, "moved.csv"), None);
            assert_eq!(
                staged_status(&workspace, "table.csv"),
                Some(StagedEntryStatus::Removed)
            );

            // A chained rename: the second move's source is staged but absent from the base
            // commit, so it is unstaged outright rather than staged for removal, and only the
            // original committed path carries the `Removed` marker.
            let workspace = repositories::workspaces::create(&repo, &commit, "mv-twice", true)?;
            let second = Path::new("second.txt");
            let third = Path::new("third.txt");
            super::mv(&workspace, other, second).await?;
            workspaces::files::mv(&workspace, second, third).await?;
            let status = workspaces::status::status(&workspace)?;
            assert_eq!(
                status.staged_files.get(other).map(|entry| &entry.status),
                Some(&StagedEntryStatus::Removed),
                "the committed source keeps its Removed marker across both moves"
            );
            assert_eq!(
                status.staged_files.get(third).map(|entry| &entry.status),
                Some(&StagedEntryStatus::Added),
                "the final destination is staged as Added"
            );
            assert!(
                !status.staged_files.contains_key(second),
                "the intermediate path was never committed, so it is unstaged rather than \
                 staged for removal"
            );

            // Renaming a data frame onto one staged in the workspace is refused, so the staged one
            // keeps its rows.
            let workspace = repositories::workspaces::create(&repo, &commit, "df-onto", true)?;
            let staged_csv = workspace.workspace_repo.path.join("staged.csv");
            fs::write_to_path(&staged_csv, "x,y\n3,4\n")?;
            workspaces::files::add(&workspace, &staged_csv).await?;
            repositories::workspaces::data_frames::index(&repo, &workspace, "table.csv").await?;
            let result = repositories::workspaces::data_frames::rename(
                &workspace,
                "table.csv",
                "staged.csv",
            )
            .await;
            assert!(
                matches!(result, Err(OxenError::DestinationAlreadyStaged(_))),
                "{result:?}"
            );
            assert_eq!(
                staged_status(&workspace, "staged.csv"),
                Some(StagedEntryStatus::Added)
            );
            assert_eq!(staged_status(&workspace, "table.csv"), None);

            // A path that exists only in the workspace is unstaged, and only a path with nothing
            // at it fails.
            let workspace = repositories::workspaces::create(&repo, &commit, "rm-staged", true)?;
            stage_new_file(&workspace, "outer/fresh/x.txt").await?;
            stage_new_file(&workspace, "outer/fresh/deep/y.txt").await?;
            stage_new_file(&workspace, "lone.txt").await?;
            stage_new_file(&workspace, "data/new.txt").await?;
            let err_files = workspaces::files::rm(
                &workspace,
                &[
                    PathBuf::from("outer/fresh"),
                    PathBuf::from("lone.txt"),
                    PathBuf::from("data/new.txt"),
                ],
            )
            .await?;
            assert!(err_files.is_empty(), "{err_files:?}");
            // Markers on committed directories may stay; nothing else should.
            let left: Vec<PathBuf> = get_staged_db_manager(&workspace.workspace_repo)?
                .read_entries_under(Path::new(""))?
                .into_iter()
                .map(|(path, _)| path)
                .filter(|path| path != Path::new("") && path != Path::new("data"))
                .collect();
            assert!(left.is_empty(), "nothing should be left staged: {left:?}");
            let err_files = workspaces::files::rm(&workspace, &[PathBuf::from("nope")]).await?;
            assert_eq!(err_files.len(), 1);

            Ok(())
        })
        .await
    }
}
