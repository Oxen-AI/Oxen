use crate::constants::{OXEN_HIDDEN_DIR, WORKSPACE_CONFIG};
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{MetadataEntry, ParsedResource, StagedData, StagedEntryStatus};
use crate::repositories;
use crate::util;

use crate::model::{workspace::WorkspaceConfig, Commit, LocalRepository, NewCommitBody, Workspace};

pub mod data_frames;
pub mod df;
pub mod diff;
pub mod files;
pub mod status;
pub mod upload;

pub use df::df;
pub use diff::diff;
pub use upload::upload;

use std::collections::HashMap;
use std::path::Path;

/// Loads a workspace from the filesystem. Must call create() first to create the workspace.
///
/// Returns an error if the workspace does not exist
pub fn get(repo: &LocalRepository, workspace_id: impl AsRef<str>) -> Result<Workspace, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
    log::debug!("workspace::get workspace_id: {workspace_id:?} hash: {workspace_id_hash:?}");

    let workspace_dir = Workspace::workspace_dir(repo, &workspace_id_hash);
    let config_path = workspace_dir.join(OXEN_HIDDEN_DIR).join(WORKSPACE_CONFIG);

    if config_path.exists() {
        get_by_dir(repo, workspace_dir)
    } else {
        let workspace = get_by_name(repo, workspace_id)?;
        let workspace_id = util::hasher::hash_str_sha256(&workspace.id);
        let workspace_dir = Workspace::workspace_dir(repo, &workspace_id);
        get_by_dir(repo, workspace_dir)
    }
}

pub fn get_by_dir(
    repo: &LocalRepository,
    workspace_dir: impl AsRef<Path>,
) -> Result<Workspace, OxenError> {
    let workspace_dir = workspace_dir.as_ref();
    let workspace_id = workspace_dir.file_name().unwrap().to_str().unwrap();
    let config_path = workspace_dir.join(OXEN_HIDDEN_DIR).join(WORKSPACE_CONFIG);

    if !config_path.exists() {
        log::debug!("workspace::get workspace not found: {:?}", workspace_dir);
        return Err(OxenError::workspace_not_found(workspace_id.into()));
    }

    let config_contents = util::fs::read_from_path(&config_path)?;
    let config: WorkspaceConfig = toml::from_str(&config_contents)
        .map_err(|e| OxenError::basic_str(format!("Failed to parse workspace config: {}", e)))?;

    let Some(commit) = repositories::commits::get_by_id(repo, &config.workspace_commit_id)? else {
        return Err(OxenError::basic_str(format!(
            "Workspace {} has invalid commit_id {}",
            workspace_id, config.workspace_commit_id
        )));
    };

    Ok(Workspace {
        id: config.workspace_id.unwrap_or(workspace_id.to_owned()),
        name: config.workspace_name,
        base_repo: repo.clone(),
        workspace_repo: LocalRepository::new(workspace_dir)?,
        commit,
        is_editable: config.is_editable,
    })
}

pub fn get_by_name(
    repo: &LocalRepository,
    workspace_name: impl AsRef<str>,
) -> Result<Workspace, OxenError> {
    let workspace_name = workspace_name.as_ref();
    let workspaces = list(repo)?;
    for workspace in workspaces {
        if workspace.name == Some(workspace_name.to_string()) {
            return Ok(workspace);
        }
    }
    Err(OxenError::basic_str(format!(
        "Workspace {} not found",
        workspace_name
    )))
}
/// Creates a new workspace and saves it to the filesystem
pub fn create(
    base_repo: &LocalRepository,
    commit: &Commit,
    workspace_id: impl AsRef<str>,
    is_editable: bool,
) -> Result<Workspace, OxenError> {
    create_with_name(base_repo, commit, workspace_id, None, is_editable)
}

pub fn create_with_name(
    base_repo: &LocalRepository,
    commit: &Commit,
    workspace_id: impl AsRef<str>,
    workspace_name: Option<String>,
    is_editable: bool,
) -> Result<Workspace, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
    let workspace_dir = Workspace::workspace_dir(base_repo, &workspace_id_hash);
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
    let workspaces = list(base_repo)?;

    // Check for existing non-editable workspaces on the same commit
    for workspace in workspaces {
        if !is_editable {
            check_non_editable_workspace(&workspace, commit)?;
        }
        if let Some(workspace_name) = workspace_name.clone() {
            check_existing_workspace_name(&workspace, &workspace_name)?;
        }
    }

    log::debug!("index::workspaces::create Initializing oxen repo! 🐂");

    let workspace_repo = init_workspace_repo(base_repo, &workspace_dir)?;

    // Serialize the workspace config to TOML
    let workspace_config = WorkspaceConfig {
        workspace_commit_id: commit.id.clone(),
        is_editable,
        workspace_name: workspace_name.clone(),
        workspace_id: Some(workspace_id.to_string()),
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
        name: workspace_name,
        base_repo: base_repo.clone(),
        workspace_repo,
        commit: commit.clone(),
        is_editable,
    })
}

fn check_non_editable_workspace(workspace: &Workspace, commit: &Commit) -> Result<(), OxenError> {
    if workspace.commit.id == commit.id && !workspace.is_editable {
        return Err(OxenError::basic_str(format!(
            "A non-editable workspace already exists for commit {}",
            commit.id
        )));
    }
    Ok(())
}

fn check_existing_workspace_name(
    workspace: &Workspace,
    workspace_name: &str,
) -> Result<(), OxenError> {
    if workspace.name == Some(workspace_name.to_string()) || *workspace_name == workspace.id {
        return Err(OxenError::basic_str(format!(
            "A workspace with the name {} already exists",
            workspace_name
        )));
    }
    Ok(())
}

pub fn list(repo: &LocalRepository) -> Result<Vec<Workspace>, OxenError> {
    let workspaces_dir = Workspace::workspaces_dir(repo);
    log::debug!("workspace::list got workspaces_dir: {:?}", workspaces_dir);
    if !workspaces_dir.exists() {
        // Return early if the workspaces directory does not exist
        return Ok(vec![]);
    }

    let workspaces_hashes = util::fs::list_dirs_in_dir(&workspaces_dir)
        .map_err(|e| OxenError::basic_str(format!("Error listing workspace directories: {}", e)))?;

    log::debug!(
        "workspace::list got workspaces_hashes: {:?}",
        workspaces_hashes
    );

    let mut workspaces = Vec::new();
    for workspace_hash in workspaces_hashes {
        // Construct the Workspace and add it to the list
        match get_by_dir(repo, workspace_hash) {
            Ok(workspace) => workspaces.push(workspace),
            Err(e) => {
                log::error!("Failed to create workspace: {}", e);
                continue;
            }
        }
    }

    Ok(workspaces)
}

pub fn get_non_editable_by_commit_id(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
) -> Result<Workspace, OxenError> {
    let workspaces = list(repo)?;
    for workspace in workspaces {
        if workspace.commit.id == commit_id.as_ref() && !workspace.is_editable {
            return Ok(workspace);
        }
    }
    Err(OxenError::basic_str(
        "No non-editable workspace found for the given commit ID",
    ))
}

pub fn delete(workspace: &Workspace) -> Result<(), OxenError> {
    let workspace_id = workspace.id.to_string();
    let workspace_dir = workspace.dir();
    if !workspace_dir.exists() {
        return Err(OxenError::workspace_not_found(workspace_id.into()));
    }

    log::debug!(
        "workspace::delete cleaning up workspace dir: {:?}",
        workspace_dir
    );
    match util::fs::remove_dir_all(&workspace_dir) {
        Ok(_) => log::debug!(
            "workspace::delete removed workspace dir: {:?}",
            workspace_dir
        ),
        Err(e) => log::error!("workspace::delete error removing workspace dir: {:?}", e),
    }

    Ok(())
}

pub fn clear(repo: &LocalRepository) -> Result<(), OxenError> {
    let workspaces_dir = Workspace::workspaces_dir(repo);
    if !workspaces_dir.exists() {
        return Ok(());
    }

    util::fs::remove_dir_all(&workspaces_dir)?;
    Ok(())
}

pub fn commit(
    workspace: &Workspace,
    new_commit: &NewCommitBody,
    branch_name: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    match workspace.workspace_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::commit::commit(workspace, new_commit, branch_name),
    }
}

fn init_workspace_repo(
    repo: &LocalRepository,
    workspace_dir: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let workspace_dir = workspace_dir.as_ref();
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::init_workspace_repo(repo, workspace_dir),
    }
}

pub fn populate_entries_with_workspace_data(
    directory: &Path,
    workspace: &Workspace,
    entries: &mut Vec<MetadataEntry>,
) -> Result<(), OxenError> {
    let workspace_changes =
        repositories::workspaces::status::status_from_dir(workspace, directory)?;

    let (additions_map, other_changes_map) = build_file_status_maps(&workspace_changes);
    for entry in entries.iter_mut() {
        entry.status = other_changes_map.get(&entry.filename).cloned();
    }
    for (file_path, status) in additions_map.iter() {
        if *status == StagedEntryStatus::Added {
            let file_path_from_workspace = workspace.dir().join(file_path);
            let mut metadata_from_path =
                repositories::metadata::from_path(&file_path_from_workspace)?;
            metadata_from_path.status = Some(status.clone());
            entries.push(metadata_from_path);
        }
    }

    Ok(())
}

pub fn populate_entry_with_workspace_data(
    file_path: &Path,
    entry: &mut MetadataEntry,
    workspace: &Workspace,
) -> Result<(), OxenError> {
    let workspace_changes =
        repositories::workspaces::status::status_from_dir(workspace, file_path)?;
    let (_additions_map, other_changes_map) = build_file_status_maps(&workspace_changes);
    entry.status = other_changes_map
        .get(file_path.to_str().unwrap())
        .cloned();
    Ok(())
}

pub fn get_added_entry(
    file_path: &Path,
    workspace: &Workspace,
    resource: &ParsedResource,
) -> Result<MetadataEntry, OxenError> {
    let workspace_changes =
        repositories::workspaces::status::status_from_dir(workspace, file_path)?;
    let (additions_map, _other_changes_map) = build_file_status_maps(&workspace_changes);
    let status = additions_map
        .get(file_path.to_str().unwrap())
        .cloned();
    let file_path_from_workspace = workspace.dir().join(file_path);
    if status == Some(StagedEntryStatus::Added) {
        let mut metadata_from_path = repositories::metadata::from_path(&file_path_from_workspace)?;

        metadata_from_path.status = Some(StagedEntryStatus::Added);
        metadata_from_path.resource = Some(resource.clone());
        Ok(metadata_from_path)
    } else {
        Err(OxenError::basic_str(
            "Entry is not in the workspace's staged database",
        ))
    }
}

/// Build a hashmap mapping file paths to their status from workspace_changes.staged_files.
///
/// Returns a tuple of two hashmaps:
/// - The first hashmap contains file paths mapped to their status if they are added.
/// - The second hashmap contains file paths mapped to their status if they are modified or removed.
///
/// This allows us to track files that were added to the workspace efficiently.
fn build_file_status_maps(
    workspace_changes: &StagedData,
) -> (
    HashMap<String, StagedEntryStatus>,
    HashMap<String, StagedEntryStatus>,
) {
    let mut additions_map = HashMap::new();
    let mut other_changes_map = HashMap::new();

    for (file_path, entry) in workspace_changes.staged_files.iter() {
        let key = file_path.to_str().unwrap().to_string();
        let status = entry.status.clone();
        if status == StagedEntryStatus::Added {
            additions_map.insert(key, status);
        } else {
            other_changes_map.insert(key, status);
        }
    }

    (additions_map, other_changes_map)
}
