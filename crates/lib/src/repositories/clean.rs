//! # oxen clean
//!
//! Remove untracked files and directories from the working tree. Analogous to `git clean -fd`.
//!
//! Without `opts.force` the command is a dry run: it reports what *would* be removed but makes
//! no filesystem changes. Pass `force = true` to actually delete.
//!
//! Invariants:
//!
//! - Never touches `.oxen/`.
//! - Never touches tracked files (candidates come only from `StagedData.untracked_*`).
//! - Never touches files matched by `.oxenignore` (inherited via `oxen status`'s existing
//!   filter; `clean` does not re-filter).
//! - An untracked directory is only removed whole when its entire subtree is untracked —
//!   `oxen status` already classifies partially-tracked directories differently.

use crate::core;
use crate::core::v_latest::clean::CleanResult;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::CleanOpts;

/// Remove untracked files and directories from `repo`'s working tree.
///
/// See the module-level docs for the full behavior contract.
pub async fn clean(repo: &LocalRepository, opts: &CleanOpts) -> Result<CleanResult, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::clean::clean(repo, opts).await,
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use crate::error::OxenError;
    use crate::opts::CleanOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    fn opts_force() -> CleanOpts {
        CleanOpts {
            paths: vec![],
            force: true,
        }
    }

    fn opts_dry_run() -> CleanOpts {
        CleanOpts {
            paths: vec![],
            force: false,
        }
    }

    #[tokio::test]
    async fn test_clean_force_removes_untracked_file() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let scratch = repo.path.join("scratch.txt");
            util::fs::write_to_path(&scratch, "hello")?;

            let result = repositories::clean::clean(&repo, &opts_force()).await?;

            assert!(result.applied);
            assert!(!scratch.exists());
            assert!(result.files.contains(&PathBuf::from("scratch.txt")));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_default_is_dry_run() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let scratch = repo.path.join("scratch.txt");
            util::fs::write_to_path(&scratch, "hello")?;

            let result = repositories::clean::clean(&repo, &opts_dry_run()).await?;

            assert!(!result.applied);
            assert!(scratch.exists(), "dry-run must not delete anything");
            assert!(result.files.contains(&PathBuf::from("scratch.txt")));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_force_removes_untracked_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let junk_dir = repo.path.join("junk");
            util::fs::create_dir_all(&junk_dir)?;
            util::fs::write_to_path(junk_dir.join("a.txt"), "a")?;
            util::fs::write_to_path(junk_dir.join("b.txt"), "b")?;

            let result = repositories::clean::clean(&repo, &opts_force()).await?;

            assert!(result.applied);
            assert!(!junk_dir.exists(), "untracked dir should be removed");
            assert!(result.dirs.contains(&PathBuf::from("junk")));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_preserves_tracked_files() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // labels.txt is tracked by the training_data helper.
            let tracked = repo.path.join("labels.txt");
            assert!(tracked.exists(), "sanity: tracked file should be present");

            let untracked = repo.path.join("scratch.txt");
            util::fs::write_to_path(&untracked, "hello")?;

            repositories::clean::clean(&repo, &opts_force()).await?;

            assert!(tracked.exists(), "tracked files must survive clean");
            assert!(!untracked.exists(), "untracked file must be removed");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_preserves_oxen_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let oxen_dir = repo.path.join(".oxen");
            assert!(oxen_dir.exists(), "sanity: .oxen/ should exist");

            let untracked = repo.path.join("scratch.txt");
            util::fs::write_to_path(&untracked, "hello")?;

            repositories::clean::clean(&repo, &opts_force()).await?;

            assert!(oxen_dir.exists(), ".oxen/ must never be removed");
            assert!(oxen_dir.join("HEAD").exists() || oxen_dir.join("config.toml").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_respects_oxenignore() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let ignored_name = "ignored_scratch.txt";
            util::fs::write_to_path(repo.path.join(ignored_name), "should stay")?;
            util::fs::write_to_path(repo.path.join(".oxenignore"), ignored_name)?;

            let result = repositories::clean::clean(&repo, &opts_force()).await?;

            assert!(
                repo.path.join(ignored_name).exists(),
                ".oxenignore-matched file must not be removed"
            );
            assert!(
                !result.files.iter().any(|p| p == Path::new(ignored_name)),
                "CleanResult must not list ignored files"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_path_scoping() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            util::fs::create_dir_all(repo.path.join("subtree_a"))?;
            util::fs::create_dir_all(repo.path.join("subtree_b"))?;
            util::fs::write_to_path(repo.path.join("subtree_a").join("x.txt"), "x")?;
            util::fs::write_to_path(repo.path.join("subtree_b").join("y.txt"), "y")?;

            let opts = CleanOpts {
                paths: vec![repo.path.join("subtree_a"), repo.path.join(".oxen")],
                force: true,
            };
            repositories::clean::clean(&repo, &opts).await?;

            assert!(
                !repo.path.join("subtree_a").exists(),
                "subtree_a should be removed (in scope)"
            );
            assert!(
                repo.path.join("subtree_b").join("y.txt").exists(),
                "subtree_b should survive (out of scope)"
            );
            assert!(
                repo.path.join(".oxen").exists(),
                ".oxen should always survive, even if the user tries to clean it"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_partially_untracked_dir_not_removed_wholesale() -> Result<(), OxenError> {
        // `annotations/train/` is tracked; adding an untracked sibling file inside it
        // should leave the tracked content alone and only remove the untracked file.
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let train_dir = repo.path.join("annotations").join("train");
            let tracked = train_dir.join("bounding_box.csv");
            assert!(tracked.exists(), "sanity: tracked file must exist");

            let untracked = train_dir.join("scratch.txt");
            util::fs::write_to_path(&untracked, "temp")?;

            repositories::clean::clean(&repo, &opts_force()).await?;

            assert!(train_dir.exists(), "partially-tracked dir must survive");
            assert!(tracked.exists(), "tracked file must survive");
            assert!(!untracked.exists(), "untracked file must be removed");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_result_counts() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            util::fs::write_to_path(repo.path.join("a.txt"), "12345")?; // 5 bytes
            util::fs::write_to_path(repo.path.join("b.txt"), "hi")?; // 2 bytes

            let junk_dir = repo.path.join("junk");
            util::fs::create_dir_all(&junk_dir)?;
            util::fs::write_to_path(junk_dir.join("c.txt"), "xyz")?; // 3 bytes

            let result = repositories::clean::clean(&repo, &opts_dry_run()).await?;

            assert_eq!(result.files.len(), 2, "two top-level untracked files");
            assert_eq!(result.dirs.len(), 1, "one untracked directory");
            assert_eq!(result.total_bytes, 10, "5 + 2 + 3 bytes across candidates");

            // Dry-run: nothing removed.
            assert!(repo.path.join("a.txt").exists());
            assert!(repo.path.join("b.txt").exists());
            assert!(junk_dir.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_clean_recovers_from_partial_pull_state() -> Result<(), OxenError> {
        // Integration test mirroring the ENG-888 recovery flow: an interrupted pull leaves
        // behind untracked files/dirs (from the target commit) plus modified tracked files.
        // `oxen restore` handles the modifications, then `oxen clean -f` removes the
        // untracked leftovers, leaving `oxen status` clean.
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            // Simulate modified tracked file left over from a partial pull.
            let tracked = repo.path.join("labels.txt");
            let original = util::fs::read_from_path(&tracked)?;
            util::fs::write_to_path(&tracked, "partial write from interrupted pull\n")?;

            // Simulate untracked leftovers: both a loose file and a new directory.
            util::fs::write_to_path(repo.path.join("leftover.txt"), "stale")?;
            util::fs::create_dir_all(repo.path.join("leftover_dir"))?;
            util::fs::write_to_path(repo.path.join("leftover_dir").join("a.txt"), "a")?;

            // Step 1: restore the modified tracked file.
            repositories::restore::restore(
                &repo,
                crate::opts::RestoreOpts::from_path("labels.txt"),
            )
            .await?;
            assert_eq!(util::fs::read_from_path(&tracked)?, original);

            // Step 2: clean the untracked leftovers.
            repositories::clean::clean(&repo, &opts_force()).await?;

            // Working tree should now be clean.
            let status = repositories::status::status(&repo)?;
            assert!(
                status.is_clean(),
                "working tree should be clean after restore + clean; got {status:?}"
            );

            Ok(())
        })
        .await
    }
}
