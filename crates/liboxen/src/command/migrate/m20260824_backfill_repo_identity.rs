use std::path::Path;

use uuid::Uuid;

use super::{Direction, Migrate};
use crate::config::RepositoryConfig;
use crate::error::OxenError;
use crate::model::{LocalRepository, RepoIdentity};
use crate::util;

pub struct BackfillRepoIdentityMigration;

impl Migrate for BackfillRepoIdentityMigration {
    fn name(&self) -> &'static str {
        "backfill_repo_identity"
    }

    fn description(&self) -> &'static str {
        "Records who a repository is in its config, derived from where it sits on disk"
    }

    /// Writes the config directly rather than through `repositories::record_name_hints`: this runs
    /// under the repository's exclusive lock, which a write reservation cannot be taken against.
    fn up(&self, repo: LocalRepository) -> Result<(), OxenError> {
        // Whether identity is already recorded comes from the config rather than `repo`, which the
        // caller opened before taking the exclusive lock: re-minting would change the UUID the
        // repository's version files are addressed by.
        let path = util::fs::config_filepath(&repo.path);
        let mut config = RepositoryConfig::from_file(&path)?;
        if config.identity.is_some() {
            return Ok(());
        }

        let identity = identity_from_path(&repo.path)?;
        log::info!(
            "Recording identity {} for repo: {:?}",
            identity.repo_uuid,
            repo.path
        );

        config.identity = Some(identity);
        config.save(&path)?;
        Ok(())
    }

    fn down(&self, _repo: LocalRepository) -> Result<(), OxenError> {
        Err(OxenError::internal_error(
            "backfill_repo_identity is up-only: a repository's UUID is what its storage is \
             addressed by, so clearing it strands the version files it names",
        ))
    }

    /// Always false: a repository without recorded identity works exactly as it always has, and
    /// every client-side repository is in that state permanently. Blocking on it would refuse
    /// every command on every local repo.
    fn is_needed(&self, _repo: &LocalRepository) -> Result<bool, OxenError> {
        Ok(false)
    }

    /// Optional and up-only, so it runs when an operator asks for it and never on its own.
    fn is_applicable(
        &self,
        direction: Direction,
        repo: &LocalRepository,
    ) -> Result<bool, OxenError> {
        match direction {
            Direction::Up => Ok(repo.identity.is_none()),
            Direction::Down => Ok(false),
        }
    }
}

/// Who the repository at `repo_path` is, according to where it sits.
///
/// A UUID-named directory was placed by a control plane that addresses repositories by UUID in
/// both name positions, so it already holds the UUID it was assigned and keeps it, with the name
/// hints left for whoever knows the real names. Any other directory is addressed by name, so the
/// server owns the identity and records both names alongside a minted UUID.
fn identity_from_path(repo_path: &Path) -> Result<RepoIdentity, OxenError> {
    let name = final_segment(repo_path)?;
    let namespace_dir = repo_path.parent().ok_or_else(|| {
        OxenError::internal_error(format!("{repo_path:?} sits in no namespace directory"))
    })?;
    let namespace = final_segment(namespace_dir)?;

    Ok(match Uuid::parse_str(name) {
        Ok(repo_uuid) => RepoIdentity::hintless(repo_uuid),
        Err(_) => RepoIdentity::minted(namespace, name),
    })
}

fn final_segment(path: &Path) -> Result<&str, OxenError> {
    path.file_name()
        .and_then(|segment| segment.to_str())
        .ok_or_else(|| OxenError::internal_error(format!("{path:?} has no usable final segment")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::requests::RepoNew;
    use crate::repositories;
    use crate::test;

    /// Create a repository at `namespace/name` carrying no identity, the state every repository
    /// predating identity is in.
    async fn without_identity(
        sync_dir: &Path,
        namespace: &str,
        name: &str,
    ) -> Result<LocalRepository, OxenError> {
        let repo_new = RepoNew::from_namespace_name(namespace, name, None);
        repositories::create(sync_dir, repo_new, None, None).await
    }

    fn recorded(repo: &LocalRepository) -> Result<RepoIdentity, OxenError> {
        Ok(
            RepositoryConfig::from_file(util::fs::config_filepath(&repo.path))?
                .identity
                .expect("the migration recorded an identity"),
        )
    }

    /// A control plane put the UUID it assigned in the name position, so the repository keeps it
    /// rather than being handed a second one that disagrees with its own directory.
    #[tokio::test]
    async fn test_a_uuid_named_repo_keeps_the_uuid_it_was_placed_under() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo_uuid = Uuid::new_v4();
            let repo = without_identity(
                &sync_dir,
                &Uuid::new_v4().to_string(),
                &repo_uuid.to_string(),
            )
            .await?;

            BackfillRepoIdentityMigration.up(repo.clone())?;

            let identity = recorded(&repo)?;
            assert_eq!(identity.repo_uuid, repo_uuid);
            assert_eq!(identity.namespace, None, "the namespace position is a UUID");
            assert_eq!(identity.name, None, "the name position is a UUID");

            Ok(())
        })
        .await
    }

    /// The segments are real names where the server owns its own namespaces, so it records them
    /// and mints the UUID nothing on disk carries.
    #[tokio::test]
    async fn test_a_name_addressed_repo_records_both_names_and_mints() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo = without_identity(&sync_dir, "ox", "cats").await?;

            BackfillRepoIdentityMigration.up(repo.clone())?;

            let identity = recorded(&repo)?;
            assert_eq!(identity.namespace.as_deref(), Some("ox"));
            assert_eq!(identity.name.as_deref(), Some("cats"));

            Ok(())
        })
        .await
    }

    /// A repository that already carries identity has no work left, so an operator asking for the
    /// migration again is told there is nothing to do.
    #[tokio::test]
    async fn test_a_recorded_identity_makes_the_migration_not_applicable() -> Result<(), OxenError>
    {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo = without_identity(&sync_dir, "ox", "cats").await?;
            BackfillRepoIdentityMigration.up(repo.clone())?;

            let reloaded = LocalRepository::from_dir(&repo.path)?;
            assert!(!BackfillRepoIdentityMigration.is_applicable(Direction::Up, &reloaded)?);

            Ok(())
        })
        .await
    }

    /// Running twice must not hand a repository a second UUID, since the first one is what its
    /// storage is addressed by. The caller opens the repository before taking the exclusive lock,
    /// so a second request holds a snapshot from before the first one recorded anything.
    #[tokio::test]
    async fn test_a_stale_snapshot_does_not_re_mint() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo = without_identity(&sync_dir, "ox", "cats").await?;

            BackfillRepoIdentityMigration.up(repo.clone())?;
            let first = recorded(&repo)?;

            BackfillRepoIdentityMigration.up(repo.clone())?;

            assert_eq!(recorded(&repo)?, first);

            Ok(())
        })
        .await
    }

    /// Reversing would strip the UUID a repository's version files are addressed by.
    #[tokio::test]
    async fn test_the_migration_is_up_only() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo = without_identity(&sync_dir, "ox", "cats").await?;

            assert!(!BackfillRepoIdentityMigration.is_applicable(Direction::Down, &repo)?);
            assert!(BackfillRepoIdentityMigration.down(repo).is_err());

            Ok(())
        })
        .await
    }

    /// Every client-side repository carries no identity and always will, so blocking on this
    /// migration would refuse every command on every local repo.
    #[tokio::test]
    async fn test_a_repo_without_identity_is_never_blocked() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|sync_dir| async move {
            let repo = without_identity(&sync_dir, "ox", "cats").await?;

            assert_eq!(repo.identity, None);
            assert!(!BackfillRepoIdentityMigration.is_needed(&repo)?);
            assert!(
                BackfillRepoIdentityMigration.is_applicable(Direction::Up, &repo)?,
                "there is still work to do when an operator asks"
            );

            Ok(())
        })
        .await
    }
}
