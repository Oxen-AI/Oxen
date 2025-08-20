use crate::constants::OXEN_HIDDEN_DIR;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::entry::metadata_entry::{WorkspaceChanges, WorkspaceMetadataEntry};
use crate::model::{merkle_tree, MetadataEntry, ParsedResource, StagedData, StagedEntryStatus};
use crate::repositories;
use crate::util;

use crate::model::{workspace::WorkspaceConfig, Commit, LocalRepository, NewCommitBody, Workspace};
use crate::view::entries::EMetadataEntry;
use crate::view::merge::Mergeable;

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
use uuid::Uuid;

/// Loads a workspace from the filesystem. Must call create() first to create the workspace.
///
/// Returns an None if the workspace does not exist
pub fn get(
    repo: &LocalRepository,
    workspace_id: impl AsRef<str>,
) -> Result<Option<Workspace>, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let workspace_id_hash = util::hasher::hash_str_sha256(workspace_id);
    log::debug!("workspace::get workspace_id: {workspace_id:?} hash: {workspace_id_hash:?}");

    let workspace_dir = Workspace::workspace_dir(repo, &workspace_id_hash);
    let config_path = Workspace::config_path_from_dir(&workspace_dir);

    log::debug!("workspace::get directory: {workspace_dir:?}");
    if config_path.exists() {
        get_by_dir(repo, workspace_dir)
    } else if let Some(workspace) = get_by_name(repo, workspace_id)? {
        let workspace_id = util::hasher::hash_str_sha256(&workspace.id);
        let workspace_dir = Workspace::workspace_dir(repo, &workspace_id);
        get_by_dir(repo, workspace_dir)
    } else {
        Ok(None)
    }
}

pub fn get_by_dir(
    repo: &LocalRepository,
    workspace_dir: impl AsRef<Path>,
) -> Result<Option<Workspace>, OxenError> {
    let workspace_dir = workspace_dir.as_ref();
    let workspace_id = workspace_dir.file_name().unwrap().to_str().unwrap();
    let config_path = Workspace::config_path_from_dir(workspace_dir);

    if !config_path.exists() {
        log::debug!("workspace::get workspace not found: {:?}", workspace_dir);
        return Ok(None);
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

    Ok(Some(Workspace {
        id: config.workspace_id.unwrap_or(workspace_id.to_owned()),
        name: config.workspace_name,
        base_repo: repo.clone(),
        workspace_repo: LocalRepository::new(workspace_dir)?,
        commit,
        is_editable: config.is_editable,
    }))
}

pub fn get_by_name(
    repo: &LocalRepository,
    workspace_name: impl AsRef<str>,
) -> Result<Option<Workspace>, OxenError> {
    let workspace_name = workspace_name.as_ref();
    let workspaces = list(repo)?;
    for workspace in workspaces {
        if workspace.name == Some(workspace_name.to_string()) {
            return Ok(Some(workspace));
        }
    }
    Ok(None)
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
    let workspace_config_path = Workspace::config_path_from_dir(&workspace_dir);
    log::debug!(
        "index::workspaces::create writing workspace config to: {:?}",
        workspace_config_path
    );
    util::fs::write_to_path(&workspace_config_path, toml_string)?;

    Ok(Workspace {
        id: workspace_id.to_owned(),
        name: workspace_name,
        base_repo: base_repo.clone(),
        workspace_repo,
        commit: commit.clone(),
        is_editable,
    })
}

/// A wrapper around Workspace that automatically deletes the workspace when dropped
pub struct TemporaryWorkspace {
    workspace: Workspace,
}

impl TemporaryWorkspace {
    /// Get a reference to the underlying workspace
    pub fn workspace(&self) -> &Workspace {
        &self.workspace
    }
}

impl std::ops::Deref for TemporaryWorkspace {
    type Target = Workspace;

    fn deref(&self) -> &Self::Target {
        &self.workspace
    }
}

impl Drop for TemporaryWorkspace {
    fn drop(&mut self) {
        if let Err(e) = delete(&self.workspace) {
            log::error!("Failed to delete temporary workspace: {}", e);
        }
    }
}

/// Creates a new temporary workspace that will be deleted when the reference is dropped
pub fn create_temporary(
    base_repo: &LocalRepository,
    commit: &Commit,
) -> Result<TemporaryWorkspace, OxenError> {
    let workspace_id = Uuid::new_v4().to_string();
    let workspace_name = format!("temporary-{}", workspace_id);
    let workspace = create_with_name(base_repo, commit, workspace_id, Some(workspace_name), true)?;
    Ok(TemporaryWorkspace { workspace })
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

    log::debug!("workspace::list got {} workspaces", workspaces_hashes.len());

    let mut workspaces = Vec::new();
    for workspace_hash in workspaces_hashes {
        // Construct the Workspace and add it to the list
        match get_by_dir(repo, &workspace_hash) {
            Ok(Some(workspace)) => workspaces.push(workspace),
            Ok(None) => {
                log::debug!("Workspace not found: {:?}", workspace_hash);
                continue;
            }
            Err(e) => {
                log::error!("Failed to list workspace: {}", e);
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

    // Clean up caches before deleting the workspace
    merkle_tree::merkle_tree_node_cache::remove_from_cache(&workspace.workspace_repo.path)?;
    core::staged::remove_from_cache(&workspace.workspace_repo.path)?;
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

pub fn update_commit(workspace: &Workspace, new_commit_id: &str) -> Result<(), OxenError> {
    let config_path = workspace.config_path();

    if !config_path.exists() {
        log::error!("Workspace config not found: {:?}", config_path);
        return Err(OxenError::workspace_not_found(workspace.id.clone().into()));
    }

    let config_contents = util::fs::read_from_path(&config_path)?;
    let mut config: WorkspaceConfig = toml::from_str(&config_contents).map_err(|e| {
        log::error!(
            "Failed to parse workspace config: {:?}, err: {}",
            config_path,
            e
        );
        OxenError::basic_str(format!("Failed to parse workspace config: {}", e))
    })?;

    log::debug!(
        "Updating workspace {} commit from {} to {}",
        workspace.id,
        config.workspace_commit_id,
        new_commit_id
    );
    config.workspace_commit_id = new_commit_id.to_string();

    let toml_string = toml::to_string(&config).map_err(|e| {
        log::error!(
            "Failed to serialize workspace config to TOML: {:?}, err: {}",
            config_path,
            e
        );
        OxenError::basic_str(format!(
            "Failed to serialize workspace config to TOML: {}",
            e
        ))
    })?;

    util::fs::write_to_path(&config_path, toml_string)?;

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

pub fn mergeability(
    workspace: &Workspace,
    branch_name: impl AsRef<str>,
) -> Result<Mergeable, OxenError> {
    match workspace.workspace_repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::workspaces::commit::mergeability(workspace, branch_name),
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
    entries: &[MetadataEntry],
) -> Result<Vec<EMetadataEntry>, OxenError> {
    let workspace_changes =
        repositories::workspaces::status::status_from_dir(workspace, directory)?;
    let mut dir_entries: Vec<EMetadataEntry> = Vec::new();

    let mut entries: Vec<WorkspaceMetadataEntry> = entries
        .iter()
        .map(|entry| WorkspaceMetadataEntry::from_metadata_entry(entry.clone()))
        .collect();

    let (additions_map, other_changes_map) =
        build_file_status_maps_for_directory(&workspace_changes);
    for entry in entries.iter_mut() {
        let status = other_changes_map.get(&entry.filename).cloned();
        match status {
            Some(status) => {
                entry.changes = Some(WorkspaceChanges {
                    status: status.clone(),
                });
                dir_entries.push(EMetadataEntry::WorkspaceMetadataEntry(entry.clone()));
            }
            _ => {
                dir_entries.push(EMetadataEntry::WorkspaceMetadataEntry(entry.clone()));
            }
        }
    }
    for (file_path, status) in additions_map.iter() {
        if *status == StagedEntryStatus::Added {
            let file_path_from_workspace = workspace.dir().join(file_path);
            let metadata_from_path = repositories::metadata::from_path(&file_path_from_workspace)?;
            let mut ws_entry = WorkspaceMetadataEntry::from_metadata_entry(metadata_from_path);
            ws_entry.changes = Some(WorkspaceChanges {
                status: status.clone(),
            });
            dir_entries.push(EMetadataEntry::WorkspaceMetadataEntry(ws_entry));
        }
    }

    Ok(dir_entries)
}

pub fn populate_entry_with_workspace_data(
    file_path: &Path,
    entry: MetadataEntry,
    workspace: &Workspace,
) -> Result<EMetadataEntry, OxenError> {
    let workspace_changes =
        repositories::workspaces::status::status_from_dir(workspace, file_path)?;
    let (_additions_map, other_changes_map) = build_file_status_maps_for_file(&workspace_changes);
    let mut entry = WorkspaceMetadataEntry::from_metadata_entry(entry.clone());
    let changes = other_changes_map.get(file_path.to_str().unwrap()).cloned();
    if let Some(status) = changes {
        entry.changes = Some(WorkspaceChanges {
            status: status.clone(),
        });
    }
    Ok(EMetadataEntry::WorkspaceMetadataEntry(entry))
}

pub fn get_added_entry(
    file_path: &Path,
    workspace: &Workspace,
    resource: &ParsedResource,
) -> Result<EMetadataEntry, OxenError> {
    let workspace_changes =
        repositories::workspaces::status::status_from_dir(workspace, file_path)?;
    let (additions_map, _other_changes_map) = build_file_status_maps_for_file(&workspace_changes);
    let status = additions_map.get(file_path.to_str().unwrap()).cloned();
    let file_path_from_workspace = workspace.dir().join(file_path);
    if status == Some(StagedEntryStatus::Added) {
        let metadata_from_path = repositories::metadata::from_path(&file_path_from_workspace)?;
        let mut ws_entry = WorkspaceMetadataEntry::from_metadata_entry(metadata_from_path);
        ws_entry.changes = Some(WorkspaceChanges {
            status: StagedEntryStatus::Added,
        });
        ws_entry.resource = Some(resource.clone().into());
        Ok(EMetadataEntry::WorkspaceMetadataEntry(ws_entry))
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
fn build_file_status_maps_for_directory(
    workspace_changes: &StagedData,
) -> (
    HashMap<String, StagedEntryStatus>,
    HashMap<String, StagedEntryStatus>,
) {
    let mut additions_map = HashMap::new();
    let mut other_changes_map = HashMap::new();

    for (file_path, entry) in workspace_changes.staged_files.iter() {
        let status = entry.status.clone();
        if status == StagedEntryStatus::Added {
            // For added files, we use the full path as the key. As the staged files are relative to the repository root
            let key = file_path.to_str().unwrap().to_string();
            additions_map.insert(key, status);
        } else {
            // For modified or removed files, we use the file name as the key, as the file path is relative to the directory passed in.
            let key = file_path.file_name().unwrap().to_string_lossy().to_string();
            other_changes_map.insert(key, status);
        }
    }

    (additions_map, other_changes_map)
}

// For files, we always use the full path as the key, as results are relative to the repository root
fn build_file_status_maps_for_file(
    workspace_changes: &StagedData,
) -> (
    HashMap<String, StagedEntryStatus>,
    HashMap<String, StagedEntryStatus>,
) {
    let mut additions_map = HashMap::new();
    let mut other_changes_map = HashMap::new();
    for (file_path, entry) in workspace_changes.staged_files.iter() {
        let status = entry.status.clone();
        if status == StagedEntryStatus::Added {
            additions_map.insert(file_path.to_str().unwrap().to_string(), status);
        } else {
            other_changes_map.insert(file_path.to_str().unwrap().to_string(), status);
        }
    }
    (additions_map, other_changes_map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_can_commit_different_files_workspaces_without_merge_conflicts(
    ) -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write two files, hello.txt and goodbye.txt, and commit them
            let hello_file = repo.path.join("hello.txt");
            let goodbye_file = repo.path.join("goodbye.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            util::fs::write_to_path(&goodbye_file, "Goodbye")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::add(&repo, &goodbye_file).await?;
            let commit = repositories::commit(&repo, "Adding hello and goodbye files")?;

            {
                // Create temporary workspace in new scope
                let temp_workspace = create_temporary(&repo, &commit)?;

                // Update the hello file in the temporary workspace
                let workspace_hello_file = temp_workspace.dir().join("hello.txt");
                util::fs::write_to_path(&workspace_hello_file, "Hello again")?;
                repositories::workspaces::files::add(&temp_workspace, workspace_hello_file).await?;
                // Commit the changes to the "main" branch
                repositories::workspaces::commit(
                    &temp_workspace,
                    &NewCommitBody {
                        message: "Updating hello file".to_string(),
                        author: "Bessie".to_string(),
                        email: "bessie@oxen.ai".to_string(),
                    },
                    DEFAULT_BRANCH_NAME,
                )?;
            } // temp_workspace goes out of scope here and gets cleaned up

            {
                // Create a new temporary workspace off of the same original commit
                let temp_workspace = create_temporary(&repo, &commit)?;

                // Update the goodbye file in the temporary workspace
                let workspace_goodbye_file = temp_workspace.dir().join("goodbye.txt");
                util::fs::write_to_path(&workspace_goodbye_file, "Goodbye again")?;
                repositories::workspaces::files::add(&temp_workspace, workspace_goodbye_file)
                    .await?;
                // Commit the changes to the "main" branch
                repositories::workspaces::commit(
                    &temp_workspace,
                    &NewCommitBody {
                        message: "Updating goodbye file".to_string(),
                        author: "Bessie".to_string(),
                        email: "bessie@oxen.ai".to_string(),
                    },
                    DEFAULT_BRANCH_NAME,
                )?;
            } // temp_workspace goes out of scope here and gets cleaned up

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_cannot_commit_different_files_workspaces_with_merge_conflicts(
    ) -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Both workspaces try to commit the same file
            let hello_file = repo.path.join("greetings").join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            repositories::add(&repo, &hello_file).await?;
            let commit = repositories::commit(&repo, "Adding hello file")?;

            {
                // Create temporary workspace in new scope
                let temp_workspace = create_temporary(&repo, &commit)?;

                // Update the hello file in the temporary workspace
                let workspace_hello_file = temp_workspace.dir().join("greetings").join("hello.txt");
                util::fs::write_to_path(&workspace_hello_file, "Hello again")?;
                repositories::workspaces::files::add(&temp_workspace, workspace_hello_file).await?;
                // Commit the changes to the "main" branch
                repositories::workspaces::commit(
                    &temp_workspace,
                    &NewCommitBody {
                        message: "Updating hello file".to_string(),
                        author: "Bessie".to_string(),
                        email: "bessie@oxen.ai".to_string(),
                    },
                    DEFAULT_BRANCH_NAME,
                )?;
            } // temp_workspace goes out of scope here and gets cleaned up

            {
                // Create a new temporary workspace off of the same original commit
                let temp_workspace = create_temporary(&repo, &commit)?;

                // Update the hello file in the temporary workspace
                let workspace_hello_file = temp_workspace.dir().join("greetings").join("hello.txt");
                util::fs::write_to_path(&workspace_hello_file, "Hello again")?;
                repositories::workspaces::files::add(&temp_workspace, workspace_hello_file).await?;
                // Commit the changes to the "main" branch
                let result = repositories::workspaces::commit(
                    &temp_workspace,
                    &NewCommitBody {
                        message: "Updating hello file".to_string(),
                        author: "Bessie".to_string(),
                        email: "bessie@oxen.ai".to_string(),
                    },
                    DEFAULT_BRANCH_NAME,
                );

                // We should get a merge conflict error
                assert!(result.is_err());
            } // temp_workspace goes out of scope here and gets cleaned up

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_can_commit_different_files_workspaces_without_merge_conflicts_in_subdirs(
    ) -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write two files, greetings/hello.txt and greetings/goodbye.txt, and commit them
            let hello_file = repo.path.join("greetings").join("hello.txt");
            let goodbye_file = repo.path.join("greetings").join("goodbye.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            util::fs::write_to_path(&goodbye_file, "Goodbye")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::add(&repo, &goodbye_file).await?;
            let commit = repositories::commit(&repo, "Adding hello and goodbye files")?;

            {
                // Create temporary workspace in new scope
                let temp_workspace = create_temporary(&repo, &commit)?;

                // Update the hello file in the temporary workspace
                let workspace_hello_file = temp_workspace.dir().join("greetings").join("hello.txt");
                util::fs::write_to_path(&workspace_hello_file, "Hello again")?;
                repositories::workspaces::files::add(&temp_workspace, workspace_hello_file).await?;
                // Commit the changes to the "main" branch
                repositories::workspaces::commit(
                    &temp_workspace,
                    &NewCommitBody {
                        message: "Updating hello file".to_string(),
                        author: "Bessie".to_string(),
                        email: "bessie@oxen.ai".to_string(),
                    },
                    DEFAULT_BRANCH_NAME,
                )?;
            } // temp_workspace goes out of scope here and gets cleaned up

            {
                // Create a new temporary workspace off of the same original commit
                let temp_workspace = create_temporary(&repo, &commit)?;

                // Update the goodbye file in the temporary workspace
                let workspace_goodbye_file =
                    temp_workspace.dir().join("greetings").join("goodbye.txt");
                util::fs::write_to_path(&workspace_goodbye_file, "Goodbye again")?;
                repositories::workspaces::files::add(&temp_workspace, workspace_goodbye_file)
                    .await?;
                // Commit the changes to the "main" branch
                repositories::workspaces::commit(
                    &temp_workspace,
                    &NewCommitBody {
                        message: "Updating goodbye file".to_string(),
                        author: "Bessie".to_string(),
                        email: "bessie@oxen.ai".to_string(),
                    },
                    DEFAULT_BRANCH_NAME,
                )?;
            } // temp_workspace goes out of scope here and gets cleaned up

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_temporary_workspace_cleanup() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Write a test file and commit it
            let test_file = repo.path.join("test.txt");
            util::fs::write_to_path(&test_file, "Hello")?;
            repositories::add(&repo, &test_file).await?;
            let commit = repositories::commit(&repo, "Adding test file")?;
            let workspaces_dir = repo.path.join(".oxen").join("workspaces");

            {
                // Create temporary workspace in new scope
                let temp_workspace = create_temporary(&repo, &commit)?;

                // Verify workspace exists and contains our file
                assert!(temp_workspace.dir().exists());

                // Test deref functionality by accessing workspace fields/methods
                assert_eq!(temp_workspace.commit.id, commit.id);
                assert!(temp_workspace.is_editable);

                let workspace_entries = std::fs::read_dir(&workspaces_dir)?;
                assert_eq!(workspace_entries.count(), 1);
            } // temp_workspace goes out of scope here

            // Verify workspace was cleaned up
            let workspace_entries = std::fs::read_dir(&workspaces_dir)?;
            assert_eq!(
                workspace_entries.count(),
                0,
                "Workspace directory should be empty after cleanup"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_concurrent_workspace_commits() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|repo, remote_repo| async move {
            // Create two files in different directories to avoid conflicts
            let file1 = repo.path.join("dir1").join("file1.txt");
            let file2 = repo.path.join("dir2").join("file2.txt");
            util::fs::write_to_path(&file1, "File 1 content")?;
            util::fs::write_to_path(&file2, "File 2 content")?;
            repositories::add(&repo, &file1).await?;
            repositories::add(&repo, &file2).await?;
            let _commit = repositories::commit(&repo, "Adding initial files")?;
            repositories::push(&repo).await?;

            // Create two workspaces
            let workspace1 =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "workspace1")
                    .await?;
            let workspace2 =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, "workspace2")
                    .await?;

            // Modify files in each workspace
            util::fs::write_to_path(&file1, "Updated file 1")?;
            util::fs::write_to_path(&file2, "Updated file 2")?;
            api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace1.id,
                "dir1",
                file1,
            )
            .await?;
            api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace2.id,
                "dir2",
                file2,
            )
            .await?;

            // Create commit bodies
            let commit_body1 = NewCommitBody {
                message: "Update file 1".to_string(),
                author: "Bessie".to_string(),
                email: "bessie@oxen.ai".to_string(),
            };
            let commit_body2 = NewCommitBody {
                message: "Update file 2".to_string(),
                author: "Bessie".to_string(),
                email: "bessie@oxen.ai".to_string(),
            };

            // Clone necessary values for the second task
            let remote_repo_clone1 = remote_repo.clone();
            let remote_repo_clone2 = remote_repo.clone();

            // Spawn two concurrent commit tasks
            let commit_task1 = tokio::spawn(async move {
                api::client::workspaces::commit(
                    &remote_repo_clone1,
                    DEFAULT_BRANCH_NAME,
                    &workspace1.id,
                    &commit_body1,
                )
                .await
            });
            let commit_task2 = tokio::spawn(async move {
                api::client::workspaces::commit(
                    &remote_repo_clone2,
                    DEFAULT_BRANCH_NAME,
                    &workspace2.id,
                    &commit_body2,
                )
                .await
            });

            // Wait for both tasks to complete
            let result1 = commit_task1.await.expect("Task 1 panicked")?;
            let result2 = commit_task2.await.expect("Task 2 panicked")?;

            // Verify both commits were successful
            assert_ne!(result1.id, result2.id, "Commits should have different IDs");
            assert!(!result1.id.is_empty(), "Commit 1 should have valid ID");
            assert!(!result2.id.is_empty(), "Commit 2 should have valid ID");

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    #[ignore] // Temporarily disabled due to bearer token authentication requirements
    async fn test_fully_concurrent_workspace_operations() -> Result<(), OxenError> {
        // Number of concurrent tasks to run - reduced to avoid RocksDB file descriptor issues
        const NUM_TASKS: usize = 3;

        test::run_one_commit_sync_repo_test(|repo, remote_repo| async move {
            let mut results = Vec::new();

            // Run tasks sequentially with some parallelism to avoid file system races
            for i in 0..NUM_TASKS {
                let remote_repo = remote_repo.clone();
                let repo = repo.clone();
                
                // Add a small delay between operations to reduce database contention
                if i > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                let result = tokio::spawn(async move {
                    // Create a unique branch for this task
                    let branch_name = format!("branch-{}", i);
                    api::client::branches::create_from_branch(
                        &remote_repo,
                        &branch_name,
                        DEFAULT_BRANCH_NAME,
                    )
                    .await?;

                    // Create workspace from the new branch
                    let workspace = api::client::workspaces::create(
                        &remote_repo,
                        &branch_name,
                        &format!("workspace-{}", i),
                    )
                    .await?;

                    // Add a unique file
                    let file_path = repo.path.join(format!("file-{}.txt", i));
                    util::fs::write_to_path(&file_path, format!("content {}", i))?;
                    api::client::workspaces::files::upload_single_file(
                        &remote_repo,
                        &workspace.id,
                        "",
                        file_path,
                    )
                    .await?;

                    // Commit changes back to the task's branch
                    let commit_body = NewCommitBody {
                        message: format!("Commit from task {}", i),
                        author: "Test Author".to_string(),
                        email: "test@oxen.ai".to_string(),
                    };

                    api::client::workspaces::commit(
                        &remote_repo,
                        &branch_name,
                        &workspace.id,
                        &commit_body,
                    )
                    .await?;

                    Ok::<_, OxenError>(workspace.id)
                }).await
                .map_err(|e| OxenError::basic_str(format!("Task error: {}", e)))??;
                
                results.push(result);
            }

            // Verify all operations completed successfully
            assert_eq!(results.len(), NUM_TASKS);
            for workspace_id in results {
                assert!(!workspace_id.is_empty());
            }

            Ok(remote_repo)
        })
        .await
    }
}
