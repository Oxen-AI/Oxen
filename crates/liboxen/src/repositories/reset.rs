//! # oxen reset
//!
//! Move the checked out branch to another commit. Analogous to `git reset`.
//!
//! `ResetMode::Mixed` moves the branch pointer and leaves the working tree untouched, so the
//! files added by the commits that were dropped stay on disk and reappear as untracked or
//! modified. Staging and committing them again recreates the history.
//!
//! `ResetMode::Hard` additionally restores the working tree to the target commit, discarding
//! whatever disagreed with it.
//!
//! Resetting only rewrites the local branch. A branch that has already been pushed needs
//! `oxen push --force` afterwards for the remote to follow.

use crate::core::v_latest::branches::OnConflict;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResetMode {
    /// Move the branch pointer and leave the working tree as it is.
    Mixed,
    /// Move the branch pointer and restore the working tree to the target commit, discarding
    /// any working-tree state that disagrees with it.
    Hard,
}

/// Point the checked out branch at `revision`, which may be a commit id or another branch name.
///
/// Returns the commit the branch now points to. See the module docs for what each mode does to
/// the working tree.
pub async fn reset(
    repo: &LocalRepository,
    revision: &str,
    mode: ResetMode,
) -> Result<Commit, OxenError> {
    let branch = repositories::branches::current_branch(repo)?
        .ok_or_else(|| OxenError::basic_str("Cannot reset without a branch checked out"))?;
    let commit = repositories::revisions::get(repo, revision)?
        .ok_or_else(|| OxenError::RevisionNotFound(revision.into()))?;

    if mode == ResetMode::Hard {
        // Two checkouts, because neither `from_commit` argument alone gives the working tree the
        // target commit's exact contents.
        //
        // With the current HEAD as the hint, files the two commits share are skipped, so a
        // locally modified copy of one survives; and when HEAD already is the target the whole
        // call short-circuits. But the hint is also what drives removal of files the target
        // commit does not have.
        //
        // So: first pass with the hint to remove those files, second pass without it to restore
        // every file in the target, skipping nothing.
        let from_commit = repositories::commits::head_commit_maybe(repo)?;
        repositories::branches::checkout_commit_from_commit(
            repo,
            &commit,
            &from_commit,
            OnConflict::Overwrite,
        )
        .await?;
        repositories::branches::checkout_commit_from_commit(
            repo,
            &commit,
            &None,
            OnConflict::Overwrite,
        )
        .await?;
    }

    repositories::branches::update(repo, &branch.name, &commit.id)?;

    Ok(commit)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::error::OxenError;
    use crate::repositories;
    use crate::repositories::ResetMode;
    use crate::test;
    use crate::util;

    /// Two commits: `first.txt` then `second.txt`. Returns the first commit's id.
    async fn two_commits(repo: &crate::model::LocalRepository) -> Result<String, OxenError> {
        let first_path = repo.path.join("first.txt");
        util::fs::write_to_path(&first_path, "first")?;
        repositories::add(repo, &first_path).await?;
        let first = repositories::commit(repo, "first")?;

        let second_path = repo.path.join("second.txt");
        util::fs::write_to_path(&second_path, "second")?;
        repositories::add(repo, &second_path).await?;
        repositories::commit(repo, "second")?;

        Ok(first.id)
    }

    #[tokio::test]
    async fn test_reset_mixed_moves_branch_and_keeps_working_tree() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let first_id = two_commits(&repo).await?;

            let commit = repositories::reset(&repo, &first_id, ResetMode::Mixed).await?;

            assert_eq!(commit.id, first_id);
            assert_eq!(repositories::commits::head_commit(&repo)?.id, first_id);
            assert!(
                repo.path.join("second.txt").exists(),
                "mixed reset must leave the working tree alone"
            );

            let status = repositories::status(&repo).await?;
            assert!(
                status
                    .untracked_files
                    .contains(&PathBuf::from("second.txt"))
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_reset_mixed_lets_the_commit_be_recreated() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let first_id = two_commits(&repo).await?;
            repositories::reset(&repo, &first_id, ResetMode::Mixed).await?;

            let second_path = repo.path.join("second.txt");
            repositories::add(&repo, &second_path).await?;
            let recreated = repositories::commit(&repo, "second, again")?;

            assert_eq!(repositories::commits::head_commit(&repo)?.id, recreated.id);
            assert!(second_path.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_reset_hard_discards_files_from_dropped_commits() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let first_id = two_commits(&repo).await?;

            repositories::reset(&repo, &first_id, ResetMode::Hard).await?;

            assert_eq!(repositories::commits::head_commit(&repo)?.id, first_id);
            assert!(repo.path.join("first.txt").exists());
            assert!(
                !repo.path.join("second.txt").exists(),
                "hard reset must discard files the target commit does not have"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_reset_hard_discards_local_modifications() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("first.txt");
            util::fs::write_to_path(&path, "first")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "first")?;

            util::fs::write_to_path(&path, "scribbled over")?;
            repositories::reset(&repo, &commit.id, ResetMode::Hard).await?;

            assert_eq!(util::fs::read_from_path(&path)?, "first");

            Ok(())
        })
        .await
    }

    /// The case that needs both checkout passes: one reset has to drop `second.txt` *and*
    /// restore `first.txt`, which both commits share and which was edited locally.
    #[tokio::test]
    async fn test_reset_hard_both_removes_and_restores() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let first_id = two_commits(&repo).await?;

            let shared = repo.path.join("first.txt");
            util::fs::write_to_path(&shared, "scribbled over")?;

            repositories::reset(&repo, &first_id, ResetMode::Hard).await?;

            assert_eq!(util::fs::read_from_path(&shared)?, "first");
            assert!(!repo.path.join("second.txt").exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_reset_accepts_a_branch_name() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let first_id = two_commits(&repo).await?;
            repositories::branches::create(&repo, "at-first", &first_id)?;

            let commit = repositories::reset(&repo, "at-first", ResetMode::Mixed).await?;

            assert_eq!(commit.id, first_id);
            assert_eq!(repositories::commits::head_commit(&repo)?.id, first_id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_reset_rejects_an_unknown_revision() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            two_commits(&repo).await?;
            let head_before = repositories::commits::head_commit(&repo)?.id;

            let result = repositories::reset(&repo, "not-a-revision", ResetMode::Mixed).await;

            assert!(result.is_err());
            assert_eq!(repositories::commits::head_commit(&repo)?.id, head_before);

            Ok(())
        })
        .await
    }
}
