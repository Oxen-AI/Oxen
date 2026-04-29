//!
//! Get the status of a workspace
//!
//! What files are staged for commit within a directory
//!

use std::path::Path;

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{StagedData, Workspace};

pub fn status(workspace: &Workspace) -> Result<StagedData, OxenError> {
    status_from_dir(workspace, Path::new(""))
}

pub fn status_from_dir(
    workspace: &Workspace,
    directory: impl AsRef<Path>,
) -> Result<StagedData, OxenError> {
    match workspace.base_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::status::status(workspace, directory),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    /// A clean workspace (named or otherwise) must yield an empty StagedData rather than
    /// an error. The HTTP layer relies on this to return 200 instead of 404.
    #[tokio::test]
    async fn test_status_clean_workspace_returns_empty() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            let workspace = repositories::workspaces::create_with_name(
                &repo,
                &commit,
                "wid-clean",
                Some("named-clean".to_string()),
                true,
            )
            .await?;

            let staged =
                repositories::workspaces::status::status_from_dir(&workspace, Path::new(""))?;
            assert!(staged.staged_files.is_empty());
            assert!(staged.modified_files.is_empty());
            assert!(staged.removed_files.is_empty());

            Ok(())
        })
        .await
    }

    /// A workspace whose staged db directory exists but is missing critical RocksDB
    /// internals (e.g. `CURRENT`) — a never-fully-initialized or mid-init state — must
    /// still return an empty `StagedData` rather than a corruption error. Otherwise the
    /// HTTP layer surfaces 500 for a benign clean-workspace race.
    #[tokio::test]
    async fn test_status_partial_staged_db_returns_empty() -> Result<(), OxenError> {
        use crate::constants::STAGED_DIR;
        use crate::core::staged::staged_db_manager;
        use crate::util;
        use rocksdb::{DB, Options};

        test::run_empty_local_repo_test_async(|repo| async move {
            let file = repo.path.join("hello.txt");
            crate::util::fs::write_to_path(&file, "hello")?;
            repositories::add(&repo, &file).await?;
            let commit = repositories::commit(&repo, "Add hello.txt")?;

            let workspace = repositories::workspaces::create_with_name(
                &repo,
                &commit,
                "wid-partial",
                Some("named-partial".to_string()),
                true,
            )
            .await?;

            let workspace_repo_path = workspace.workspace_repo.path.clone();
            let staged_db_dir = util::fs::oxen_hidden_dir(&workspace_repo_path).join(STAGED_DIR);
            std::fs::create_dir_all(&staged_db_dir)?;

            // Materialize a real RocksDB on disk, then drop it so we can perturb the files.
            {
                let mut opts = Options::default();
                opts.create_if_missing(true);
                let db = DB::open(&opts, &staged_db_dir)
                    .map_err(|e| OxenError::basic_str(format!("test setup: open db: {e}")))?;
                drop(db);
            }

            // Drop any cached handle so the next open re-reads from disk.
            staged_db_manager::remove_from_cache_with_children(&workspace_repo_path)?;

            // Simulate the race/corruption: directory exists, CURRENT does not.
            let current_path = staged_db_dir.join("CURRENT");
            assert!(
                current_path.exists(),
                "test setup expected CURRENT to exist"
            );
            std::fs::remove_file(&current_path)?;

            let staged =
                repositories::workspaces::status::status_from_dir(&workspace, Path::new(""))?;
            assert!(staged.staged_files.is_empty());
            assert!(staged.modified_files.is_empty());
            assert!(staged.removed_files.is_empty());

            Ok(())
        })
        .await
    }
}
