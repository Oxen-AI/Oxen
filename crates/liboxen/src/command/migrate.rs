use crate::{error::OxenError, model::LocalRepository};

use colored::Colorize;

pub mod m20260408_add_workspace_name_index;
pub use m20260408_add_workspace_name_index::AddWorkspaceNameIndexMigration;

pub mod m20260626_migrate_merkle_nodes_to_lmdb;
pub use m20260626_migrate_merkle_nodes_to_lmdb::MerkleNodesToLmdbMigration;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr, VariantNames};

/// Migration direction. Passed to [`Migrate::is_applicable`] so reversible migrations
/// (like the file ↔ LMDB merkle store transcode) can report applicability separately
/// for each direction.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    EnumString,
    VariantNames,
    Display,
    IntoStaticStr,
    Serialize,
    Deserialize,
    utoipa::ToSchema,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")] // WARNING!! must mirror serde's `rename_all`
pub enum Direction {
    Up,
    Down,
}

/// The default direction for a migration is up.
impl Default for Direction {
    fn default() -> Self {
        Direction::Up
    }
}

pub trait Migrate {
    /// Apply the migration.
    fn up(&self, repo: LocalRepository) -> Result<(), OxenError>;

    /// Undo the migration.
    fn down(&self, repo: LocalRepository) -> Result<(), OxenError>;

    /// If true, then this migraiton must be applied to the repository before it can be used.
    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError>;

    /// Whether this migration could meaningfully run on `repo` in the given direction.
    ///
    /// Used by the `--run-optional` CLI branch to gate explicit invocation of optional
    /// migrations whose `is_needed` returns `false`.
    ///
    /// Defaults to delegating to [`Migrate::is_needed`] for Up migrations and `Ok(true)`
    /// for a Down migration. By default, Up migrations are only run if needed
    /// and Down migrations are unconditionally run. Migrations that are either optional
    /// or need different logic for determining whether or not they can run on a repository
    /// must override this method.
    fn is_applicable(
        &self,
        direction: Direction,
        repo: &LocalRepository,
    ) -> Result<bool, OxenError> {
        match direction {
            Direction::Up => self.is_needed(repo),
            Direction::Down => Ok(true),
        }
    }

    /// The name of the migration as referenced from the CLI.
    fn name(&self) -> &'static str;

    /// A human-readable description of what the migration does.
    fn description(&self) -> &'static str;
}

/// Every registered migration is identified by its `name()`.
///
/// Used by both the CLI (`oxen migrate up/down`) and the server
/// (`POST /api/repos/:ns/:name/migrations/:migration_name`) to look up a
/// migration by name at runtime. New migrations **MUST** be listed here
/// for the [`all_migrations`] function to work properly.
pub const ALL_MIGRATIONS: [&dyn Migrate; 2] =
    [&AddWorkspaceNameIndexMigration, &MerkleNodesToLmdbMigration];

/// Maps a registered migration's name to its implementation.
/// The name is exactly the same value returned by `<dyn Migrate>::name()`.
pub fn all_migrations(migration_name: &str) -> Option<&dyn Migrate> {
    for migration in ALL_MIGRATIONS.iter() {
        if migration_name == migration.name() {
            // let m = &**migration;
            // return Some(m);
            return Some(*migration);
        }
    }
    None
}

#[derive(Debug, thiserror::Error)]
pub enum MigrationResult {
    #[error("Nothing to do: '{migration_name}' ({direction}) is applicable, but not needed.")]
    IsApplicableButOptional {
        direction: Direction,
        migration_name: &'static str,
    },

    #[error("Nothing to do: '{migration_name}' ({direction}) is not needed nor is it applicable.")]
    NotNeededNorApplicable {
        direction: Direction,
        migration_name: &'static str,
    },

    #[error("Successfully applied migration {direction} {migration_name}")]
    Success {
        direction: Direction,
        migration_name: &'static str,
    },
}

impl MigrationResult {
    pub fn did_run(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    pub fn as_hint(&self, is_server: bool) -> String {
        let msg = self.to_string();
        match self {
            Self::IsApplicableButOptional { .. } => format!(
                "{msg} You must run with {} to apply it.",
                (if is_server {
                    "run_optional=true"
                } else {
                    "--run-optional"
                })
                .yellow()
            ),
            _ => msg,
        }
    }
}

/// Attempt to apply the specified migration to the repository in the options.
///
/// Returns:
///     - Ok(Success): Repository is successfully migrated to new state.
///     - Ok(...): Not applied and no error was encountered.
///     - Err(...): Not applied and an error was encountered that prevented it from being applied.
///
/// For `Ok`, use `.did_run()` to recover a `bool` for whether or not the migration was performed.
///
/// Note that this consumes the [`LocalRepository`]. Post migration, the repository should be
/// reloaded from its repository path.
pub fn try_apply_migration(
    migration: &dyn Migrate,
    direction: Direction,
    run_optional: bool,
    repo: LocalRepository,
) -> Result<MigrationResult, OxenError> {
    let migration_name = migration.name();

    // is_needed is only valid in the up direction
    if matches!(direction, Direction::Up) && migration.is_needed(&repo)? {
        migration.up(repo)?;
        return Ok(MigrationResult::Success {
            direction,
            migration_name,
        });
    }

    // otherwise, we apply the migration if it is applicable
    if migration.is_applicable(direction, &repo)? {
        match direction {
            // always run applicable down migrations
            Direction::Down => {
                migration.down(repo)?;
                Ok(MigrationResult::Success {
                    direction,
                    migration_name,
                })
            }
            // only run up migrations if we've requested optional migrations
            Direction::Up if run_optional => {
                migration.up(repo)?;
                Ok(MigrationResult::Success {
                    direction,
                    migration_name,
                })
            }
            _ => Ok(MigrationResult::IsApplicableButOptional {
                direction,
                migration_name,
            }),
        }
    } else {
        Ok(MigrationResult::NotNeededNorApplicable {
            direction,
            migration_name,
        })
    }
}
