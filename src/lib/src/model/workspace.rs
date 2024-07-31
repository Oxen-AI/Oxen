use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::api;
use crate::constants;
use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACES_DIR, WORKSPACE_CONFIG};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;
use toml;

fn workspace_dir(repo: &LocalRepository, workspace_id_hash: &str) -> PathBuf {
    repo.path
        .join(OXEN_HIDDEN_DIR)
        .join(WORKSPACES_DIR)
        .join(workspace_id_hash)
}

// Define a struct for the workspace config to make it easier to serialize
#[derive(Serialize, Deserialize)]
struct WorkspaceConfig {
    workspace_commit_id: String,
    is_editable: bool,
    workspace_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Workspace {
    pub id: String,
    // Workspaces have a base repository that they are created in .oxen/
    pub base_repo: LocalRepository,
    // And a sub repository that is just to make changes in
    // .oxen/workspaces/<workspace_id>/.oxen/
    pub workspace_repo: LocalRepository,
    // .oxen/workspaces/<workspace_ id>/.oxen/WORKSPACE_CONFIG
    pub is_editable: bool,
    pub commit: Commit,
}

impl Workspace {
    /// Loads a workspace from the filesystem. Must call create() first to create the workspace.
    ///
    /// Returns an error if the workspace does not exist
    pub fn new(repo: &LocalRepository, workspace_id: impl AsRef<str>) -> Result<Self, OxenError> {
        let workspace_id = workspace_id.as_ref();
        let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
        log::debug!(
            "workspace::new got workspace_id: {workspace_id:?} hash: {workspace_id_hash:?}"
        );

        let workspace_dir = workspace_dir(repo, &workspace_id_hash);
        let config_path = workspace_dir.join(OXEN_HIDDEN_DIR).join(WORKSPACE_CONFIG);

        if !config_path.exists() {
            return Err(OxenError::workspace_not_found(workspace_id.into()));
        }

        let config_contents = util::fs::read_from_path(&config_path)?;
        let config: WorkspaceConfig = toml::from_str(&config_contents).map_err(|e| {
            OxenError::basic_str(format!("Failed to parse workspace config: {}", e))
        })?;

        let Some(commit) = api::local::commits::get_by_id(repo, &config.workspace_commit_id)?
        else {
            return Err(OxenError::basic_str(format!(
                "Workspace {} has invalid commit_id {}",
                workspace_id, config.workspace_commit_id
            )));
        };

        Ok(Workspace {
            id: workspace_id.to_owned(),
            base_repo: repo.clone(),
            workspace_repo: LocalRepository::new(&workspace_dir)?,
            commit,
            is_editable: config.is_editable,
        })
    }

    /// Creates a new workspace and saves it to the filesystem
    pub fn create(
        base_repo: &LocalRepository,
        commit: &Commit,
        workspace_id: impl AsRef<str>,
        is_editable: bool,
    ) -> Result<Self, OxenError> {
        let workspace_id = workspace_id.as_ref();
        let workspace_name = workspace_id.to_owned();
        let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
        let workspace_dir = workspace_dir(base_repo, &workspace_id_hash);
        let oxen_dir = workspace_dir.join(OXEN_HIDDEN_DIR);

        log::debug!("index::workspaces::create called! {:?}", oxen_dir);

        if oxen_dir.exists() {
            log::debug!(
                "index::workspaces::create already have oxen repo directory {:?}",
                oxen_dir
            );
            return Err(OxenError::basic_str(format!(
                "Workspace {} already exists",
                workspace_id
            )));
        }

        // Check for existing non-editable workspaces on the same commit
        if !is_editable {
            let workspaces = Workspace::list(base_repo)?;
            for workspace in workspaces {
                if workspace.commit.id == commit.id && !workspace.is_editable {
                    // Found another non-editable workspace with the same commit
                    return Err(OxenError::basic_str(format!(
                        "A non-editable workspace already exists for commit {}",
                        commit.id
                    )));
                }
            }
        }

        log::debug!("index::workspaces::create Initializing oxen repo! 🐂");

        let workspace_repo = Self::init_workspace_repo(base_repo, &workspace_dir, commit)?;

        // Serialize the workspace config to TOML
        let workspace_config = WorkspaceConfig {
            workspace_commit_id: commit.id.clone(),
            is_editable,
            workspace_name: workspace_name.clone(),
        };

        let toml_string = match toml::to_string(&workspace_config) {
            Ok(s) => s,
            Err(e) => {
                return Err(OxenError::basic_str(format!(
                    "Failed to serialize workspace config to TOML: {}",
                    e
                )));
            }
        };

        // Write the TOML string to WORKSPACE_CONFIG
        let commit_id_path = workspace_repo
            .path
            .join(OXEN_HIDDEN_DIR)
            .join(WORKSPACE_CONFIG);
        log::debug!(
            "index::workspaces::create writing workspace config to: {:?}",
            commit_id_path
        );
        util::fs::write_to_path(&commit_id_path, toml_string)?;

        Ok(Workspace {
            id: workspace_id.to_owned(),
            base_repo: base_repo.clone(),
            workspace_repo,
            commit: commit.clone(),
            is_editable,
        })
    }

    pub fn list(repo: &LocalRepository) -> Result<Vec<Self>, OxenError> {
        let workspaces_dir = repo.path.join(OXEN_HIDDEN_DIR).join(WORKSPACES_DIR);
        log::debug!("workspace::list got workspaces_dir: {:?}", workspaces_dir);
        if !workspaces_dir.exists() {
            // Return early if the workspaces directory does not exist
            return Ok(vec![]);
        }

        let workspaces_hashes = util::fs::list_dirs_in_dir(&workspaces_dir).map_err(|e| {
            OxenError::basic_str(format!("Error listing workspace directories: {}", e))
        })?;
        log::debug!(
            "workspace::list got workspaces_hashes: {:?}",
            workspaces_hashes
        );

        let mut workspaces = Vec::new();
        for workspace_hash in workspaces_hashes {
            let workspace_config_path = workspace_hash.join(OXEN_HIDDEN_DIR).join(WORKSPACE_CONFIG);

            if !workspace_config_path.exists() {
                log::warn!("Workspace config not found at: {:?}", workspace_config_path);
                continue;
            }

            // Read the workspace config file
            let config_toml = match util::fs::read_from_path(&workspace_config_path) {
                Ok(content) => content,
                Err(e) => {
                    log::error!("Failed to read workspace config: {}", e);
                    continue;
                }
            };

            // Deserialize the TOML content
            let workspace_config: WorkspaceConfig = match toml::from_str(&config_toml) {
                Ok(config) => config,
                Err(e) => {
                    log::error!("Failed to deserialize workspace config: {}", e);
                    continue;
                }
            };

            // Construct the Workspace and add it to the list
            match Workspace::new(repo, workspace_config.workspace_name) {
                Ok(workspace) => workspaces.push(workspace),
                Err(e) => {
                    log::error!("Failed to create workspace: {}", e);
                    continue;
                }
            }
        }

        Ok(workspaces)
    }

    fn init_workspace_repo(
        repo: &LocalRepository,
        workspace_dir: &Path,
        commit: &Commit,
    ) -> Result<LocalRepository, OxenError> {
        let oxen_hidden_dir = repo.path.join(OXEN_HIDDEN_DIR);
        let workspace_hidden_dir = workspace_dir.join(OXEN_HIDDEN_DIR);
        log::debug!("init_workspace_repo {workspace_hidden_dir:?}");
        util::fs::create_dir_all(&workspace_hidden_dir)?;

        let dir_hashes_dir = format!(
            "{}/{}/{}",
            constants::HISTORY_DIR,
            commit.id,
            constants::DIR_HASHES_DIR
        );
        let dirs_dir = format!(
            "{}/{}/{}",
            constants::HISTORY_DIR,
            commit.id,
            constants::DIRS_DIR
        );

        let dirs_to_copy = vec![
            constants::COMMITS_DIR,
            &dir_hashes_dir,
            &dirs_dir,
            constants::REFS_DIR,
            constants::HEAD_FILE,
            constants::OBJECTS_DIR,
        ];

        for dir in dirs_to_copy {
            let oxen_dir = oxen_hidden_dir.join(dir);
            let target_dir = workspace_hidden_dir.join(dir);

            log::debug!("init_workspace_repo copying {dir} dir {oxen_dir:?} -> {target_dir:?}");
            if oxen_dir.is_dir() {
                util::fs::copy_dir_all(oxen_dir, target_dir)?;
            } else {
                util::fs::copy(oxen_dir, target_dir)?;
            }
        }

        println!("==================================");
        println!("debug point 1");
        println!("==================================");

        LocalRepository::new(workspace_dir)
    }

    /// Returns the path to the workspace directory
    pub fn dir(&self) -> PathBuf {
        let workspace_id_hash = util::hasher::hash_str_sha256(&self.id);
        workspace_dir(&self.base_repo, &workspace_id_hash)
    }
}
