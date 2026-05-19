use std::collections::HashMap;
use std::path::Path;

use crate::{error::OxenError, model::LocalRepository};

pub mod m20250111083535_add_child_counts_to_nodes;
pub use m20250111083535_add_child_counts_to_nodes::AddChildCountsToNodesMigration;

pub mod m20260408_add_workspace_name_index;
pub use m20260408_add_workspace_name_index::AddWorkspaceNameIndexMigration;
use strum::{Display, EnumString, IntoStaticStr, VariantNames};

pub mod m20260512000000_switch_merkle_store_to_lmdb;
pub use m20260512000000_switch_merkle_store_to_lmdb::SwitchMerkleStoreToLmdbMigration;

/// Migration direction. Passed to [`Migrate::is_applicable`] so reversible migrations
/// (like the file ↔ LMDB merkle store transcode) can report applicability separately
/// for each direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, VariantNames, Display, IntoStaticStr)]
#[strum(serialize_all = "lowercase")]
pub enum Direction {
    Up,
    Down,
}

pub trait Migrate {
    /// Apply the migration.
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError>;

    /// Undo the migration.
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError>;

    /// If true, then this migraiton must be applied to the repository before it can be used.
    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError>;

    /// Whether this migration could meaningfully run on `repo` in the given direction.
    ///
    /// Used by the `--run-optional` CLI branch to gate explicit invocation of optional
    /// migrations whose `is_needed` returns `false`. Defaults to delegating to
    /// [`Migrate::is_needed`]: required migrations are always applicable.
    fn is_applicable(
        &self,
        direction: Direction,
        repo: &LocalRepository,
    ) -> Result<bool, OxenError> {
        let _ = direction;
        self.is_needed(repo)
    }

    /// The name of the migration as referenced from the CLI.
    fn name(&self) -> &'static str;

    /// A human-readable description of what the migration does.
    fn description(&self) -> &'static str;
}

/// Returns every registered migration keyed by its `name()`.
///
/// Used by both the CLI (`oxen migrate up/down`) and the server
/// (`POST /api/repos/:ns/:name/migrations/:migration_name`) to look up a
/// migration by name at runtime. New migrations must be appended here.
pub fn all_migrations() -> HashMap<String, Box<dyn Migrate>> {
    let mut map: HashMap<String, Box<dyn Migrate>> = HashMap::new();
    map.insert(
        AddChildCountsToNodesMigration.name().to_string(),
        Box::new(AddChildCountsToNodesMigration),
    );
    map.insert(
        AddWorkspaceNameIndexMigration.name().to_string(),
        Box::new(AddWorkspaceNameIndexMigration),
    );
    map.insert(
        SwitchMerkleStoreToLmdbMigration.name().to_string(),
        Box::new(SwitchMerkleStoreToLmdbMigration),
    );
    map
}
