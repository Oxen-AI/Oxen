//! Give workspaces that predate recorded creation times an approximate one, and move their config
//! onto its current filename.
//!
//! A workspace config records `created_at` only from the release that introduced it onward, so
//! every workspace already on disk reads back as `None` — which is exactly the abandoned population
//! an age-based reaper has to act on. This pass stamps each one with its directory's mtime, close
//! enough for "roughly when this appeared".
//!
//! Re-running is safe: the config sits two levels below the directory whose mtime is read, so
//! writing it leaves that mtime alone and a second pass reads the same source value as the first.
//!
//! A workspace whose config cannot be read or parsed is logged and skipped rather than failing the
//! pass, so one unreadable config does not strand every other workspace in the repository.

use std::path::Path;

use time::OffsetDateTime;

use super::{Direction, Migrate};
use crate::error::OxenError;
use crate::model::workspace::WorkspaceConfig;
use crate::model::{LocalRepository, Workspace};
use crate::repositories::workspaces;
use crate::util;

pub struct BackfillWorkspaceCreatedAtMigration;

impl Migrate for BackfillWorkspaceCreatedAtMigration {
    fn name(&self) -> &'static str {
        "backfill_workspace_created_at"
    }

    fn description(&self) -> &'static str {
        "Records an approximate creation time on workspaces that lack one, from directory mtime"
    }

    fn is_needed(&self, _repo: &LocalRepository) -> Result<bool, OxenError> {
        // Optional: a workspace with no creation time still loads and serves requests, so nothing
        // is blocked on this having run. Reaping is what wants the timestamp.
        Ok(false)
    }

    fn is_applicable(
        &self,
        direction: Direction,
        repo: &LocalRepository,
    ) -> Result<bool, OxenError> {
        match direction {
            Direction::Up => Ok(workspaces::list_dirs(repo)?
                .iter()
                .any(|workspace_dir| pending_config(workspace_dir).is_some())),
            // Recorded creation times are the only record of when these workspaces appeared.
            Direction::Down => Ok(false),
        }
    }

    fn up(&self, repo: LocalRepository) -> Result<(), OxenError> {
        let mut backfilled = 0;
        for workspace_dir in workspaces::list_dirs(&repo)? {
            let Some(mut config) = pending_config(&workspace_dir) else {
                continue;
            };

            if config.created_at.is_none() {
                match directory_mtime(&workspace_dir) {
                    Ok(mtime) => config.created_at = Some(mtime),
                    Err(e) => {
                        log::warn!(
                            "[Skip workspace] could not read mtime of {workspace_dir:?}: {e}"
                        );
                        continue;
                    }
                }
            }

            workspaces::write_config(&workspace_dir, &config)?;
            backfilled += 1;
        }

        log::info!(
            "Backfilled {backfilled} workspace configs in repo: {:?}",
            repo.path
        );
        Ok(())
    }

    fn down(&self, _repo: LocalRepository) -> Result<(), OxenError> {
        Err(OxenError::internal_error(
            "backfill_workspace_created_at cannot be reversed: the recorded creation times are the \
             only record of when these workspaces appeared",
        ))
    }
}

/// The config at `workspace_dir` when it still lacks a creation time or a file under the former
/// name is still present. `None` when the workspace is already migrated, holds no config, or holds
/// one that cannot be read.
fn pending_config(workspace_dir: &Path) -> Option<WorkspaceConfig> {
    let config_path = Workspace::existing_config_path_from_dir(workspace_dir)?;

    let contents = util::fs::read_from_path(&config_path)
        .inspect_err(|e| {
            log::warn!("[Skip workspace] could not read workspace config at {config_path:?}: {e}")
        })
        .ok()?;
    let config: WorkspaceConfig = toml::from_str(&contents)
        .inspect_err(|e| {
            log::warn!("[Skip workspace] could not parse workspace config at {config_path:?}: {e}")
        })
        .ok()?;

    // Keyed on the former name being present rather than on which config was read: a workspace
    // holding both is mid-rename, and the leftover still needs clearing even though the config
    // that was read is complete.
    let legacy_present = Workspace::legacy_config_path_from_dir(workspace_dir).exists();
    (config.created_at.is_none() || legacy_present).then_some(config)
}

/// When `workspace_dir` was last written, which for a workspace directory approximates when it
/// was created.
fn directory_mtime(workspace_dir: &Path) -> Result<OffsetDateTime, OxenError> {
    let modified = util::fs::metadata(workspace_dir)?.modified()?;
    Ok(OffsetDateTime::from(modified))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::repositories;
    use crate::test;
    use crate::util::fs::AtomicFile;

    /// Rewrites a workspace's config the way one written before this migration looks: no
    /// `created_at` key, stored under the former filename.
    fn make_legacy(workspace: &Workspace) -> Result<PathBuf, OxenError> {
        let current_path = workspace.config_path();
        let contents = util::fs::read_from_path(&current_path)?;
        let without_created_at: String = contents
            .lines()
            .filter(|line| !line.starts_with("created_at"))
            .map(|line| format!("{line}\n"))
            .collect();

        let legacy_path = Workspace::legacy_config_path_from_dir(workspace.dir());
        AtomicFile::new(&legacy_path).write(without_created_at.as_bytes())?;
        util::fs::remove_file(&current_path)?;
        Ok(legacy_path)
    }

    /// A workspace config from before this migration gets a creation time from its directory's
    /// mtime and moves onto the current filename, and the workspace still loads afterward.
    #[tokio::test]
    async fn test_up_backfills_created_at_and_renames_the_config() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let workspace = repositories::workspaces::create(&repo, &commit, "ws-legacy", true)?;
            let workspace_dir = workspace.dir();
            let legacy_path = make_legacy(&workspace)?;

            let migration = BackfillWorkspaceCreatedAtMigration;
            assert!(migration.is_applicable(Direction::Up, &repo)?);
            assert!(
                !migration.is_needed(&repo)?,
                "the migration is optional: nothing is blocked on it having run"
            );

            migration.up(repo.clone())?;

            assert!(!legacy_path.exists(), "the former config name is cleared");
            assert!(Workspace::config_path_from_dir(&workspace_dir).exists());

            let reloaded = repositories::workspaces::get(&repo, "ws-legacy")?
                .expect("a migrated workspace should still load");
            assert_eq!(
                reloaded.created_at,
                Some(directory_mtime(&workspace_dir)?),
                "created_at comes from the workspace directory's mtime"
            );

            assert!(
                !migration.is_applicable(Direction::Up, &repo)?,
                "a migrated repo has nothing left to migrate"
            );

            Ok(())
        })
        .await
    }

    /// Re-running reads the same directory mtime it read the first time, so a workspace's recorded
    /// creation time does not drift with every pass.
    #[tokio::test]
    async fn test_up_is_idempotent() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let workspace = repositories::workspaces::create(&repo, &commit, "ws-legacy", true)?;
            make_legacy(&workspace)?;

            let migration = BackfillWorkspaceCreatedAtMigration;
            migration.up(repo.clone())?;
            let after_first = repositories::workspaces::get(&repo, "ws-legacy")?
                .expect("workspace should load after the first pass")
                .created_at;
            assert!(after_first.is_some());

            migration.up(repo.clone())?;
            let after_second = repositories::workspaces::get(&repo, "ws-legacy")?
                .expect("workspace should load after the second pass")
                .created_at;

            assert_eq!(after_first, after_second);
            Ok(())
        })
        .await
    }

    /// An interrupted rename leaves a config under both names. The one under the current name is
    /// authoritative and complete, so the pass keeps it and clears the leftover.
    #[tokio::test]
    async fn test_up_clears_a_leftover_config_from_an_interrupted_rename() -> Result<(), OxenError>
    {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let workspace = repositories::workspaces::create(&repo, &commit, "ws-both", true)?;
            let workspace_dir = workspace.dir();

            // Both names present, the current one already carrying a creation time.
            let current_path = Workspace::config_path_from_dir(&workspace_dir);
            let legacy_path = Workspace::legacy_config_path_from_dir(&workspace_dir);
            let contents = util::fs::read_from_path(&current_path)?;
            AtomicFile::new(&legacy_path).write(contents.as_bytes())?;

            let migration = BackfillWorkspaceCreatedAtMigration;
            assert!(
                migration.is_applicable(Direction::Up, &repo)?,
                "a leftover under the former name is still work to do"
            );

            migration.up(repo.clone())?;

            assert!(!legacy_path.exists(), "the leftover is cleared");
            assert!(current_path.exists());

            let reloaded = repositories::workspaces::get(&repo, "ws-both")?
                .expect("the workspace should still load");
            assert_eq!(
                reloaded.created_at, workspace.created_at,
                "the config under the current name is kept as-is"
            );

            Ok(())
        })
        .await
    }

    /// A workspace created with a creation time already recorded is left exactly as it was.
    #[tokio::test]
    async fn test_up_leaves_an_already_recorded_workspace_alone() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let workspace = repositories::workspaces::create(&repo, &commit, "ws-current", true)?;

            let migration = BackfillWorkspaceCreatedAtMigration;
            assert!(!migration.is_applicable(Direction::Up, &repo)?);

            migration.up(repo.clone())?;

            let reloaded = repositories::workspaces::get(&repo, "ws-current")?
                .expect("workspace should still load");
            assert_eq!(reloaded.created_at, workspace.created_at);
            Ok(())
        })
        .await
    }

    /// One unreadable config does not strand the other workspaces in the repository.
    #[tokio::test]
    async fn test_up_skips_an_unparseable_config() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let good = repositories::workspaces::create(&repo, &commit, "ws-good", true)?;
            let broken = repositories::workspaces::create(&repo, &commit, "ws-broken", true)?;
            make_legacy(&good)?;
            AtomicFile::new(broken.config_path()).write(b"this is not toml {{{")?;

            let migration = BackfillWorkspaceCreatedAtMigration;
            migration.up(repo.clone())?;

            let reloaded = repositories::workspaces::get(&repo, "ws-good")?
                .expect("the readable workspace should still load");
            assert!(
                reloaded.created_at.is_some(),
                "a readable workspace is backfilled even when a sibling config is unreadable"
            );
            Ok(())
        })
        .await
    }

    /// The migration only runs forward: reversing it would discard the only record of when these
    /// workspaces appeared.
    #[tokio::test]
    async fn test_down_is_refused() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let migration = BackfillWorkspaceCreatedAtMigration;
            assert!(!migration.is_applicable(Direction::Down, &repo)?);
            assert!(migration.down(repo).is_err());
            Ok(())
        })
        .await
    }
}
