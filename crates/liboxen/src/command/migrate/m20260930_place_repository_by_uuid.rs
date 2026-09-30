use std::path::Path;

use super::{Direction, Migrate};
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::repositories::placement::{ENV_CLOSE_WAIT, place_by_uuid};
use crate::sync_dir::NAME_TABLE_DIR;

pub struct PlaceRepositoryByUuidMigration;

impl Migrate for PlaceRepositoryByUuidMigration {
    fn name(&self) -> &'static str {
        "place_repository_by_uuid"
    }

    fn description(&self) -> &'static str {
        "Moves a server's repository to the directory its UUID places it in"
    }

    /// Moves the repository's directory, so `repo` names a directory that is gone once this
    /// returns. Run under the repository's exclusive lock.
    fn up(&self, repo: LocalRepository) -> Result<(), OxenError> {
        let repo_dir = repo.path.clone();
        // Dropped first, so its own LMDB env cannot hold off the move.
        drop(repo);
        let Some(sync_dir) = server_sync_dir(&repo_dir) else {
            return Err(OxenError::internal_error(format!(
                "{repo_dir:?} is not in a server's sync directory"
            )));
        };
        let placed_dir = place_by_uuid(sync_dir, &repo_dir, ENV_CLOSE_WAIT)?;
        log::info!("Moved repository {repo_dir:?} to {placed_dir:?}");
        Ok(())
    }

    fn down(&self, _repo: LocalRepository) -> Result<(), OxenError> {
        Err(OxenError::internal_error(
            "place_repository_by_uuid is up-only: the server creates every new repository where \
             its UUID places it, so there is nothing to move back to",
        ))
    }

    /// Always false: the server serves a repository from either layout, and a client-side
    /// repository is in neither.
    fn is_needed(&self, _repo: &LocalRepository) -> Result<bool, OxenError> {
        Ok(false)
    }

    /// Optional and up-only, for a repository at `{namespace}/{name}` in a server's sync directory.
    fn is_applicable(
        &self,
        direction: Direction,
        repo: &LocalRepository,
    ) -> Result<bool, OxenError> {
        match direction {
            Direction::Up => Ok(server_sync_dir(&repo.path).is_some()),
            Direction::Down => Ok(false),
        }
    }
}

/// The sync directory `repo_dir` sits at `{namespace}/{name}` in, recognized by the server's name
/// table beside the namespaces.
fn server_sync_dir(repo_dir: &Path) -> Option<&Path> {
    let sync_dir = repo_dir.parent()?.parent()?;
    sync_dir.join(NAME_TABLE_DIR).is_dir().then_some(sync_dir)
}
