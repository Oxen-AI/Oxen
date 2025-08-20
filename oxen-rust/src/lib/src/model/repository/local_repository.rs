use crate::config::RepositoryConfig;
use crate::constants::SHALLOW_FLAG;
use crate::constants::{self, DEFAULT_VNODE_SIZE, MIN_OXEN_VERSION};
use crate::core::versions::MinOxenVersion;
use crate::error;
use crate::error::OxenError;
use crate::model::{MetadataEntry, Remote, RemoteRepository};
use crate::storage::{create_version_store, StorageConfig, VersionStore};
use crate::util;
use crate::view::RepositoryView;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalRepository {
    pub path: PathBuf,
    // Optional remotes to sync the data to
    remote_name: Option<String>, // name of the current remote ("origin" by default)
    min_version: Option<String>, // write the version if it is past v0.18.4
    remotes: Vec<Remote>,        // List of possible remotes
    vnode_size: Option<u64>,     // Size of the vnodes
    subtree_paths: Option<Vec<PathBuf>>, // If the user clones a subtree, we store the paths here so that we know we don't have the full tree
    pub depth: Option<i32>, // If the user clones with a depth, we store the depth here so that we know we don't have the full tree
    pub remote_mode: Option<bool>, // Flag for remote repositories
    pub workspace_name: Option<String>, // ID of the associated workspace for remote mode
    workspaces: Option<Vec<String>>, // List of workspaces for remote mode

    // Skip this field during serialization/deserialization
    #[serde(skip)]
    version_store: Option<Arc<dyn VersionStore>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalRepositoryWithEntries {
    pub local_repo: LocalRepository,
    pub entries: Option<Vec<MetadataEntry>>,
}

impl LocalRepository {
    /// Create a LocalRepository from a directory
    pub fn from_dir(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        let path = path.as_ref().to_path_buf();
        let config_path = util::fs::config_filepath(&path);
        let config = RepositoryConfig::from_file(&config_path)?;

        let mut repo = LocalRepository {
            path,
            remote_name: config.remote_name,
            min_version: config.min_version,
            remotes: config.remotes,
            vnode_size: config.vnode_size,
            subtree_paths: config.subtree_paths.clone(),
            depth: config.depth,
            version_store: None,
            remote_mode: config.remote_mode,
            workspace_name: config.workspace_name,
            workspaces: config.workspaces,
        };

        // Initialize the version store based on config
        let store = create_version_store(&repo.path, config.storage.as_ref())?;
        repo.version_store = Some(store);

        Ok(repo)
    }

    /// Create a LocalRepository with a pre-configured version store (composition)
    pub fn with_version_store(
        path: impl AsRef<Path>,
        version_store: Arc<dyn VersionStore>,
    ) -> Result<Self, OxenError> {
        let path = path.as_ref().to_path_buf();
        let config_path = util::fs::config_filepath(&path);
        let config = RepositoryConfig::from_file(&config_path).unwrap_or_default();

        Ok(LocalRepository {
            path,
            remote_name: config.remote_name,
            min_version: config.min_version,
            remotes: config.remotes,
            vnode_size: config.vnode_size,
            subtree_paths: config.subtree_paths,
            depth: config.depth,
            version_store: Some(version_store),
            remote_mode: config.remote_mode,
            workspace_name: config.workspace_name,
            workspaces: config.workspaces,
        })
    }

    /// Get a reference to the version store
    pub fn version_store(&self) -> Result<Arc<dyn VersionStore>, OxenError> {
        match &self.version_store {
            Some(store) => Ok(Arc::clone(store)),
            None => Err(OxenError::basic_str("Version store not initialized")),
        }
    }

    /// Initialize the version store if not already set
    pub fn init_version_store(&mut self) -> Result<(), OxenError> {
        if self.version_store.is_none() {
            // Load config to get storage settings
            let config_path = util::fs::config_filepath(&self.path);
            let config = RepositoryConfig::from_file(&config_path)?;

            // Create and initialize the store
            let store = create_version_store(&self.path, config.storage.as_ref())?;
            self.version_store = Some(store);
        }
        Ok(())
    }

    /// Initialize the default version store
    pub fn init_default_version_store(&mut self) -> Result<(), OxenError> {
        let store = create_version_store(&self.path, None)?;
        self.version_store = Some(store);
        Ok(())
    }


    /// Load a repository from the current directory
    /// this traverses up the directory tree until it finds a .oxen/ directory
    pub fn from_current_dir() -> Result<LocalRepository, OxenError> {
        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

        LocalRepository::from_dir(&repo_dir)
    }

    /// Instantiate a new repository at a given path
    /// Note: Does not create the repository on disk, or read the config file, just instantiates the struct
    /// To load the repository, use `LocalRepository::from_dir` or `LocalRepository::from_current_dir`
    pub fn new(path: impl AsRef<Path>) -> Result<LocalRepository, OxenError> {
        let mut repo = LocalRepository {
            path: path.as_ref().to_path_buf(),
            // No remotes are set yet
            remotes: vec![],
            remote_name: None,
            // New with a path should default to our current MIN_OXEN_VERSION
            min_version: Some(MIN_OXEN_VERSION.to_string()),
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            version_store: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
        };

        repo.init_default_version_store()?;
        Ok(repo)
    }

    /// Load an older version of a repository with older oxen core logic
    pub fn new_from_version(
        path: impl AsRef<Path>,
        min_version: impl AsRef<str>,
    ) -> Result<LocalRepository, OxenError> {
        let mut repo = LocalRepository {
            path: path.as_ref().to_path_buf(),
            remotes: vec![],
            remote_name: None,
            min_version: Some(min_version.as_ref().to_string()),
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            version_store: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
        };

        repo.init_default_version_store()?;
        Ok(repo)
    }

    pub fn from_view(view: RepositoryView) -> Result<LocalRepository, OxenError> {
        let mut repo = LocalRepository {
            path: std::env::current_dir()?.join(view.name),
            remotes: vec![],
            remote_name: None,
            min_version: None,
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            version_store: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
        };

        repo.init_default_version_store()?;
        Ok(repo)
    }

    pub fn from_remote(repo: RemoteRepository, path: &Path) -> Result<LocalRepository, OxenError> {
        let mut local_repo = LocalRepository {
            path: path.to_owned(),
            remotes: vec![repo.remote],
            remote_name: Some(String::from(constants::DEFAULT_REMOTE_NAME)),
            min_version: None,
            vnode_size: None,
            subtree_paths: None,
            depth: None,
            version_store: None,
            remote_mode: None,
            workspace_name: None,
            workspaces: None,
        };

        local_repo.init_default_version_store()?;
        Ok(local_repo)
    }

    pub fn min_version(&self) -> MinOxenVersion {
        match MinOxenVersion::or_earliest(self.min_version.clone()) {
            Ok(version) => version,
            Err(err) => {
                panic!("Invalid repo version\n{}", err)
            }
        }
    }

    pub fn set_remote_name(&mut self, name: impl AsRef<str>) {
        self.remote_name = Some(name.as_ref().to_string());
    }

    pub fn set_min_version(&mut self, version: MinOxenVersion) {
        self.min_version = Some(version.to_string());
    }

    pub fn remotes(&self) -> &Vec<Remote> {
        &self.remotes
    }

    pub fn dirname(&self) -> String {
        String::from(self.path.file_name().unwrap().to_str().unwrap())
    }

    pub fn vnode_size(&self) -> u64 {
        self.vnode_size.unwrap_or(DEFAULT_VNODE_SIZE)
    }

    pub fn set_vnode_size(&mut self, size: u64) {
        self.vnode_size = Some(size);
    }

    pub fn subtree_paths(&self) -> Option<Vec<PathBuf>> {
        self.subtree_paths.as_ref().map(|paths| {
            paths
                .iter()
                .map(|p| {
                    if p == &PathBuf::from(".") {
                        PathBuf::from("")
                    } else {
                        p.clone()
                    }
                })
                .collect()
        })
    }

    pub fn set_subtree_paths(&mut self, paths: Option<Vec<PathBuf>>) {
        self.subtree_paths = paths;
    }

    pub fn depth(&self) -> Option<i32> {
        self.depth
    }

    pub fn set_depth(&mut self, depth: Option<i32>) {
        self.depth = depth;
    }

    pub fn set_remote_mode(&mut self, is_remote: Option<bool>) {
        self.remote_mode = is_remote;
    }

    pub fn is_remote_mode(&self) -> bool {
        self.remote_mode.unwrap_or(false)
    }

    /// Save the repository configuration to disk
    pub fn save(&self) -> Result<(), OxenError> {
        let config_path = util::fs::config_filepath(&self.path);

        // Determine the current storage type and settings using the trait methods
        let storage = self.version_store.as_ref().map(|store| StorageConfig {
            type_: store.storage_type().to_string(),
            settings: store.storage_settings(),
        });

        let config = RepositoryConfig {
            remote_name: self.remote_name.clone(),
            remotes: self.remotes.clone(),
            subtree_paths: self.subtree_paths.clone(),
            depth: self.depth,
            min_version: self.min_version.clone(),
            vnode_size: self.vnode_size,
            storage,
            remote_mode: self.remote_mode,
            workspace_name: self.workspace_name.clone(),
            workspaces: self.workspaces.clone(),
        };

        config.save(&config_path)
    }

    pub fn set_remote(&mut self, name: impl AsRef<str>, url: impl AsRef<str>) -> Remote {
        self.remote_name = Some(name.as_ref().to_owned());
        let name = name.as_ref();
        let url = url.as_ref();
        let remote = Remote {
            name: name.to_owned(),
            url: url.to_owned(),
        };
        if self.has_remote(name) {
            // find remote by name and set
            for i in 0..self.remotes.len() {
                if self.remotes[i].name == name {
                    self.remotes[i] = remote.clone()
                }
            }
        } else {
            // we don't have the key, just push
            self.remotes.push(remote.clone());
        }
        remote
    }

    pub fn delete_remote(&mut self, name: impl AsRef<str>) {
        let name = name.as_ref();
        let mut new_remotes: Vec<Remote> = vec![];
        for i in 0..self.remotes.len() {
            if self.remotes[i].name != name {
                new_remotes.push(self.remotes[i].clone());
            }
        }
        self.remotes = new_remotes;
    }

    pub fn has_remote(&self, name: impl AsRef<str>) -> bool {
        let name = name.as_ref();
        for remote in self.remotes.iter() {
            if remote.name == name {
                return true;
            }
        }
        false
    }

    pub fn get_remote(&self, name: impl AsRef<str>) -> Option<Remote> {
        let name = name.as_ref();
        log::trace!("Checking for remote {name} have {}", self.remotes.len());
        for remote in self.remotes.iter() {
            log::trace!("comparing: {name} -> {}", remote.name);
            if remote.name == name {
                return Some(remote.clone());
            }
        }
        None
    }

    pub fn remote(&self) -> Option<Remote> {
        if let Some(name) = &self.remote_name {
            self.get_remote(name)
        } else {
            None
        }
    }

    pub fn add_workspace(&mut self, name: impl AsRef<str>) {
        let workspace_name = name.as_ref();
        let workspaces = self.workspaces.clone().unwrap_or_default();

        let mut new_workspaces = vec![];
        for workspace in workspaces {
            new_workspaces.push(workspace.clone());
        }

        new_workspaces.push(workspace_name.to_string());
        self.workspaces = Some(new_workspaces);
    }

    pub fn delete_workspace(&mut self, name: impl AsRef<str>) -> Result<(), OxenError> {
        let name = name.as_ref();

        if self.workspaces.is_none() {
            return Err(OxenError::basic_str(format!(
                "Error: Cannot delete workspace {:?} as it does not exist",
                name
            )));
        }

        // TODO: Allow deletions when workspace_name isn't set?
        // This seems like an impossible scenario...
        if self.workspace_name.is_some() && name == self.workspace_name.as_ref().unwrap() {
            return Err(OxenError::basic_str(
                "Error: Cannot delete current workspace",
            ));
        }

        let mut new_workspaces: Vec<String> = vec![];
        let prev_workspaces = self.workspaces.clone().unwrap();
        for workspace in prev_workspaces {
            if workspace != name {
                new_workspaces.push(workspace.clone());
            }
        }
        self.workspaces = Some(new_workspaces);
        Ok(())
    }

    pub fn has_workspace(&self, name: impl AsRef<str>) -> bool {
        let workspace_name = name.as_ref();
        self.workspaces.is_some()
            && self
                .workspaces
                .clone()
                .unwrap()
                .contains(&workspace_name.to_string())
    }

    // TODO: Right ow, this doesn't need to return a result
    // Define setting a workspace that's not in the workspaces vec to be an error?
    pub fn set_workspace(&mut self, name: impl AsRef<str>) -> Result<(), OxenError> {
        let workspace_name = name.as_ref();

        if let Some(ws_name) = self
            .workspaces
            .clone()
            .unwrap()
            .iter()
            .find(|ws| ws.starts_with(&format!("{}: ", workspace_name)))
        {
            self.workspace_name = Some(ws_name.to_string());
        } else {
            self.add_workspace(workspace_name);
            self.workspace_name = Some(workspace_name.to_string());
        }
        Ok(())
    }

    pub fn write_is_shallow(&self, shallow: bool) -> Result<(), OxenError> {
        let shallow_flag_path = util::fs::oxen_hidden_dir(&self.path).join(SHALLOW_FLAG);
        log::debug!("Write is shallow [{shallow}] to path: {shallow_flag_path:?}");
        if shallow {
            util::fs::write_to_path(&shallow_flag_path, "true")?;
        } else if shallow_flag_path.exists() {
            util::fs::remove_file(&shallow_flag_path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::{LocalRepository, RepoNew};
    use crate::test;
    use std::path::PathBuf;

    #[test]
    fn test_get_dirname_from_url() -> Result<(), OxenError> {
        let url = "http://0.0.0.0:3000/repositories/OxenData";
        let repo = RepoNew::from_url(url)?;
        assert_eq!(repo.name, "OxenData");
        assert_eq!(repo.namespace, "repositories");
        Ok(())
    }

    #[test]
    fn test_get_set_has_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|mut local_repo| {
            let url = "http://0.0.0.0:3000/repositories/OxenData";
            let remote_name = "origin";
            local_repo.set_remote(remote_name, url);
            let remote = local_repo.get_remote(remote_name).unwrap();
            assert_eq!(remote.name, remote_name);
            assert_eq!(remote.url, url);

            Ok(())
        })
    }

    #[test]
    fn test_delete_remote() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|mut local_repo| {
            let origin_url = "http://0.0.0.0:3000/repositories/OxenData";
            let origin_name = "origin";

            let other_url = "http://0.0.0.0:4000/repositories/OxenData";
            let other_name = "other";
            local_repo.set_remote(origin_name, origin_url);
            local_repo.set_remote(other_name, other_url);

            // Remove and make sure we cannot get again
            local_repo.delete_remote(origin_name);
            let remote = local_repo.get_remote(origin_name);
            assert!(remote.is_none());

            Ok(())
        })
    }

    // Note: Adding/Setting/Deleting workspaces does not currently require the repo to be in remote mode
    // Do we want to require that?
    #[test]
    fn test_add_workspace() -> Result<(), OxenError> {
        let repo_path = PathBuf::from("repo_path");
        let mut repo = LocalRepository::new(repo_path)?;

        let sample_name = "sample";
        repo.add_workspace(sample_name);

        let result = repo.has_workspace(sample_name);
        assert!(result);

        repo.set_workspace(sample_name)?;
        assert_eq!(repo.workspace_name, Some(sample_name.to_string()));

        Ok(())
    }

    #[test]
    fn test_delete_workspace() -> Result<(), OxenError> {
        let repo_path = PathBuf::from("repo_path");
        let mut repo = LocalRepository::new(repo_path)?;

        let sample_name = "sample";
        repo.add_workspace(sample_name);
        repo.set_workspace(sample_name)?;

        // Cannot delete current workspace_name
        let result = repo.delete_workspace(sample_name);
        assert!(result.is_err());

        let sample_2 = "second";
        repo.add_workspace(sample_2);
        repo.set_workspace(sample_2)?;

        // Can delete previous workspace_name
        repo.delete_workspace(sample_name)?;

        Ok(())
    }
}
