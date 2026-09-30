//! Moving a repository from the legacy `{namespace}/{name}` layout to the directory its UUID places
//! it in, where the server creates every repository with a UUID.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::{Duration, Instant};

use uuid::Uuid;

use crate::config::RepositoryConfig;
use crate::core::db::data_frames::df_db::remove_df_db_from_cache_with_children;
use crate::core::refs::ref_manager;
use crate::core::staged;
use crate::core::v_latest::commits::remove_commit_count_db_from_cache_with_children;
use crate::core::workspaces::workspace_name_index;
use crate::error::OxenError;
use crate::lmdb::env_registry::shared_env_is_live_under;
use crate::repositories::name_table::NameTable;
use crate::sync_dir::{namespace_dirs, prepare_placed_repo_dir, repo_dirs};
use crate::util;

/// How long a move waits for the LMDB envs open under a repository to close.
pub(crate) const ENV_CLOSE_WAIT: Duration = Duration::from_secs(10);

/// What a walk of the legacy layout did.
#[derive(Debug)]
pub struct Placed {
    /// Repositories moved to the directory their UUID places them in.
    pub moved: usize,
    /// Repositories left where they are, and namespace directories that could not be listed, each
    /// with the reason.
    pub refused: Vec<(PathBuf, OxenError)>,
}

/// Move every repository in the legacy layout under `sync_dir` to the directory its UUID places it
/// in, leaving any it cannot move where it is. Run with nothing else using `sync_dir`.
pub fn place_all_by_uuid(sync_dir: &Path) -> Result<Placed, OxenError> {
    let mut placed = Placed {
        moved: 0,
        refused: vec![],
    };
    for namespace_dir in namespace_dirs(sync_dir)? {
        let in_namespace = match repo_dirs(&namespace_dir) {
            Ok(in_namespace) => in_namespace,
            Err(err) => {
                let err = OxenError::file_error(&namespace_dir, err);
                placed.refused.push((namespace_dir, err));
                continue;
            }
        };
        for repo_dir in in_namespace {
            match place_by_uuid(sync_dir, &repo_dir, ENV_CLOSE_WAIT) {
                Ok(_) => placed.moved += 1,
                Err(err) => placed.refused.push((repo_dir, err)),
            }
        }
    }
    Ok(placed)
}

/// Move the repository at `repo_dir`, in the legacy layout under `sync_dir`, to the directory the
/// UUID its config records places it in, returning that directory. The repository still resolves
/// at the namespace and name it resolved at before, and a namespace directory it was the last
/// repository in is removed. Run with no write in progress on the repository: the move waits up to
/// `env_wait` for readers to close its LMDB envs, and drops the cached database handles under its
/// old directory.
///
/// # Errors
/// [`OxenError::RepoUuidTaken`] when a repository is already placed by the UUID.
/// [`OxenError::LockTimeout`] when an LMDB env under the directory is still open after `env_wait`.
/// An internal error when the config records no UUID, when the directory is named for a different
/// UUID, when the config keeps version files at an absolute path inside the directory, or when
/// nothing would resolve the repository's namespace and name to it once it has moved.
pub(crate) fn place_by_uuid(
    sync_dir: &Path,
    repo_dir: &Path,
    env_wait: Duration,
) -> Result<PathBuf, OxenError> {
    let (Some(namespace), Some(name)) = (
        repo_dir
            .parent()
            .and_then(Path::file_name)
            .and_then(OsStr::to_str),
        repo_dir.file_name().and_then(OsStr::to_str),
    ) else {
        return Err(OxenError::internal_error(format!(
            "{repo_dir:?} has a namespace or name that is not valid UTF-8"
        )));
    };
    let config = RepositoryConfig::from_file(util::fs::config_filepath(repo_dir))?;
    let Some(repo_uuid) = config.identity.map(|identity| identity.repo_uuid) else {
        return Err(OxenError::internal_error(
            "Its config records no repository UUID. Record one with the backfill_repo_identity \
             migration in docs/migrations.md, then run this again",
        ));
    };

    let named_for = Uuid::try_parse(name).ok();
    if let Some(named_for) = named_for
        && named_for != repo_uuid
    {
        return Err(OxenError::internal_error(format!(
            "Its directory is named for {named_for}, but its config records {repo_uuid}"
        )));
    }
    if let Some(versions_path) = config.storage.and_then(|storage| storage.versions_path)
        && versions_path.is_absolute()
        && (versions_path.starts_with(repo_dir)
            || versions_path.starts_with(util::fs::canonicalize(repo_dir)?))
    {
        return Err(OxenError::internal_error(format!(
            "Its config keeps version files at {versions_path:?}, inside the directory being \
             moved. Make the path relative, starting with .oxen, then run this again"
        )));
    }
    // After the move only the name table, or a UUID in the name position, leads here.
    if named_for.is_none() && NameTable::new(sync_dir).get(namespace, name)? != Some(repo_uuid) {
        return Err(OxenError::internal_error(format!(
            "The name table does not record {namespace}/{name} for {repo_uuid}, so the repository \
             could no longer be found there. Run oxen-server seed-name-table, then run this again"
        )));
    }

    let placed_dir = prepare_placed_repo_dir(sync_dir, repo_uuid)?;
    if placed_dir.symlink_metadata().is_ok() {
        return Err(OxenError::RepoUuidTaken(repo_uuid));
    }
    forget_cached_handles(repo_dir)?;
    let deadline = Instant::now() + env_wait;
    while shared_env_is_live_under(repo_dir) {
        if Instant::now() >= deadline {
            return Err(OxenError::LockTimeout(
                "The repository is in use and cannot move yet. Try again later.".into(),
            ));
        }
        sleep(Duration::from_millis(2));
    }
    util::fs::rename(repo_dir, &placed_dir)?;
    // Again, so no handle opened while the move ran answers for the old directory.
    forget_cached_handles(repo_dir)?;
    if let Some(namespace_dir) = repo_dir.parent() {
        remove_if_empty(namespace_dir);
    }
    Ok(placed_dir)
}

/// Drop the cached database handles under `repo_dir`, so the next open finds the repository
/// wherever it then is. A handle a caller still holds closes on its last drop.
fn forget_cached_handles(repo_dir: &Path) -> Result<(), OxenError> {
    staged::remove_from_cache_with_children(repo_dir)?;
    ref_manager::remove_from_cache_with_children(repo_dir)?;
    workspace_name_index::remove_from_cache_with_children(repo_dir);
    remove_commit_count_db_from_cache_with_children(repo_dir);
    remove_df_db_from_cache_with_children(repo_dir)
}

/// Remove `namespace_dir` when nothing is left in it.
fn remove_if_empty(namespace_dir: &Path) {
    let empty = std::fs::read_dir(namespace_dir).is_ok_and(|mut entries| entries.next().is_none());
    if empty && let Err(err) = std::fs::remove_dir(namespace_dir) {
        tracing::warn!(?namespace_dir, %err, "Could not remove an emptied namespace directory");
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use uuid::Uuid;

    use super::{place_all_by_uuid, place_by_uuid};
    use crate::command::migrate::{Direction, Migrate, PlaceRepositoryByUuidMigration};
    use crate::config::RepositoryConfig;
    use crate::core::repo_locks::begin_write;
    use crate::error::OxenError;
    use crate::model::{MerkleHash, RepoIdentity};
    use crate::repositories;
    use crate::storage::{StorageConfig, StorageKind};
    use crate::sync_dir::placed_repo_dir;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_place_all_by_uuid_moves_what_still_resolves_and_reports_the_rest()
    -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            // Addressed by name, the way a self-hosted server addresses its repositories.
            let cats = RepoIdentity::minted("ox", "cats");
            let stale_cats =
                test::create_legacy_repo_with_identity(&sync_dir, "ox", "cats", cats.clone())?;
            // Addressed by UUID in both positions, the way a control plane addresses them.
            let hosted = RepoIdentity::hintless(Uuid::new_v4());
            let (owner, hosted_name) = (Uuid::new_v4().to_string(), hosted.repo_uuid.to_string());
            drop(test::create_legacy_repo_with_identity(
                &sync_dir,
                &owner,
                &hosted_name,
                hosted.clone(),
            )?);

            // Each left where it is, for the reason its name gives.
            drop(test::create_legacy_repo(&sync_dir, "zoo", "no-uuid")?);
            let unrecorded = RepoIdentity::minted("zoo", "unrecorded");
            let config_path = util::fs::config_filepath(
                &test::create_legacy_repo(&sync_dir, "zoo", "unrecorded")?.path,
            );
            let mut config = RepositoryConfig::from_file(&config_path)?;
            config.identity = Some(unrecorded);
            config.save(&config_path)?;
            let mismatched = Uuid::new_v4().to_string();
            drop(test::create_legacy_repo_with_identity(
                &sync_dir,
                "zoo",
                &mismatched,
                RepoIdentity::hintless(Uuid::new_v4()),
            )?);
            let taken = RepoIdentity::minted("zoo", "taken");
            drop(test::create_legacy_repo_with_identity(
                &sync_dir,
                "zoo",
                "taken",
                taken.clone(),
            )?);
            util::fs::create_dir_all(placed_repo_dir(&sync_dir, taken.repo_uuid))?;
            let inside = RepoIdentity::minted("zoo", "inside");
            let inside_dir =
                test::create_legacy_repo_with_identity(&sync_dir, "zoo", "inside", inside)?.path;
            let config_path = util::fs::config_filepath(&inside_dir);
            let mut config = RepositoryConfig::from_file(&config_path)?;
            config.storage = Some(StorageConfig {
                kind: StorageKind::Local,
                versions_path: Some(util::fs::canonicalize(&inside_dir)?.join(".oxen/versions")),
            });
            config.save(&config_path)?;

            let placed = place_all_by_uuid(&sync_dir)?;
            assert_eq!(placed.moved, 2, "refused: {:?}", placed.refused);
            let mut refused: Vec<String> = placed
                .refused
                .iter()
                .map(|(dir, _)| {
                    dir.file_name()
                        .expect("a repository directory")
                        .to_string_lossy()
                })
                .map(String::from)
                .collect();
            refused.sort();
            let mut expected = vec![
                "inside",
                "no-uuid",
                "taken",
                "unrecorded",
                mismatched.as_str(),
            ];
            expected.sort();
            assert_eq!(
                refused, expected,
                "each refusal is reported: {:?}",
                placed.refused
            );
            assert!(
                matches!(
                    placed.refused.iter().find(|(dir, _)| dir.ends_with("taken")),
                    Some((_, OxenError::RepoUuidTaken(repo_uuid))) if *repo_uuid == taken.repo_uuid
                ),
                "a UUID another repository is placed by is refused: {:?}",
                placed.refused
            );

            for (namespace, name, identity) in
                [("ox", "cats", &cats), (&*owner, &*hosted_name, &hosted)]
            {
                let placed_dir = placed_repo_dir(&sync_dir, identity.repo_uuid);
                assert_eq!(
                    repositories::resolve_repo_dir(&sync_dir, namespace, name)?,
                    Some(placed_dir.clone()),
                    "{namespace}/{name} resolves where it moved to"
                );
                assert_eq!(
                    RepositoryConfig::from_file(util::fs::config_filepath(&placed_dir))?.identity,
                    Some(identity.clone()),
                    "the moved repository is the one that recorded the identity"
                );
            }
            assert!(
                !sync_dir.join("ox").exists() && !sync_dir.join(&owner).exists(),
                "a namespace directory its last repository moved out of is removed"
            );
            assert!(
                sync_dir.join("zoo").join("no-uuid").is_dir(),
                "a refused repository stays where it is"
            );

            let again = place_all_by_uuid(&sync_dir)?;
            assert_eq!(
                (again.moved, again.refused.len()),
                (0, placed.refused.len()),
                "a second run moves nothing more and refuses the same repositories"
            );
            assert!(
                matches!(begin_write(&stale_cats), Err(OxenError::LockTimeout(_))),
                "a write that looked up the old directory is sent back to look again"
            );

            let working_copy =
                test::create_legacy_repo(&sync_dir.join("elsewhere"), "ox", "birds")?;
            assert!(
                !PlaceRepositoryByUuidMigration.is_applicable(Direction::Up, &working_copy)?,
                "a directory with no name table beside its namespace is not a server's"
            );

            let busy_identity = RepoIdentity::minted("ox", "busy");
            let busy = test::create_legacy_repo_with_identity(
                &sync_dir,
                "ox",
                "busy",
                busy_identity.clone(),
            )?;
            busy.merkle_node_store()
                .exists(&MerkleHash::new(0))
                .expect("the repository's LMDB env opens");
            assert!(
                matches!(
                    place_by_uuid(&sync_dir, &busy.path, Duration::ZERO),
                    Err(OxenError::LockTimeout(_))
                ),
                "a repository whose LMDB env is open is not moved"
            );
            assert!(
                busy.path.is_dir(),
                "the refused move leaves it where it was"
            );
            assert!(PlaceRepositoryByUuidMigration.is_applicable(Direction::Up, &busy)?);
            PlaceRepositoryByUuidMigration.up(busy)?;
            assert_eq!(
                repositories::resolve_repo_dir(&sync_dir, "ox", "busy")?,
                Some(placed_repo_dir(&sync_dir, busy_identity.repo_uuid)),
                "the migration moves it once the only open env was its own"
            );

            Ok(())
        })
        .await
    }
}
