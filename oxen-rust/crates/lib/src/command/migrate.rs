use std::path::Path;

use crate::{error::OxenError, model::LocalRepository};

pub mod m20250111083535_add_child_counts_to_nodes;
pub use m20250111083535_add_child_counts_to_nodes::AddChildCountsToNodesMigration;

pub trait Migrate {
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError>;
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
}
