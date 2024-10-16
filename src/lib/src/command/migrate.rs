use std::path::Path;

use crate::{error::OxenError, model::LocalRepository};

pub mod m00_update_version_files;
pub use m00_update_version_files::UpdateVersionFilesMigration;

pub mod m01_cache_dataframe_size;
pub use m01_cache_dataframe_size::CacheDataFrameSizeMigration;

pub mod m02_propagate_schemas;
pub use m02_propagate_schemas::PropagateSchemasMigration;

pub mod m03_add_directories_to_cache;
pub use m03_add_directories_to_cache::AddDirectoriesToCacheMigration;

pub mod m04_create_merkle_trees;
pub use m04_create_merkle_trees::CreateMerkleTreesMigration;

pub mod m05_optimize_merkle_tree;
pub use m05_optimize_merkle_tree::OptimizeMerkleTreesMigration;

pub trait Migrate {
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError>;
    fn name(&self) -> &'static str;
    fn description(&self) -> &'static str;
}
