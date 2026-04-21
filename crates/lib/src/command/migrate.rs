use std::collections::HashMap;
use std::path::Path;

use crate::{error::OxenError, model::LocalRepository};

pub mod m20250111083535_add_child_counts_to_nodes;
pub use m20250111083535_add_child_counts_to_nodes::AddChildCountsToNodesMigration;

pub mod m20260408_add_workspace_name_index;
pub use m20260408_add_workspace_name_index::AddWorkspaceNameIndexMigration;

pub trait Migrate {
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError>;
    fn name(&self) -> &'static str;
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
    map
}
