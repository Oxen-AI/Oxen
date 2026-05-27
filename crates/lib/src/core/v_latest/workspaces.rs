use crate::config::RepositoryConfig;
use crate::constants;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util;
use std::path::Path;

pub mod commit;
pub mod data_frames;
pub mod diff;
pub mod files;
pub mod status;

pub fn init_workspace_repo(
    repo: &LocalRepository,
    workspace_dir: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let workspace_dir = workspace_dir.as_ref();
    let oxen_hidden_dir = repo.path.join(constants::OXEN_HIDDEN_DIR);
    let workspace_hidden_dir = workspace_dir.join(constants::OXEN_HIDDEN_DIR);
    log::debug!("init_workspace_repo {workspace_hidden_dir:?}");
    util::fs::create_dir_all(&workspace_hidden_dir)?;

    // Copy the config file
    let config_file = oxen_hidden_dir.join(constants::REPO_CONFIG_FILENAME);
    let target_config_file = workspace_hidden_dir.join(constants::REPO_CONFIG_FILENAME);
    util::fs::copy(config_file, &target_config_file)?;

    // Workspace inherits the parent repo's full config — min_version, merkle_store_kind, vfs,
    // and storage — so a workspace built from an LMDB/VFS/S3 repo lands on the same backends.
    let config = RepositoryConfig::from_file(&target_config_file)?;
    LocalRepository::new(workspace_dir, config)
}
