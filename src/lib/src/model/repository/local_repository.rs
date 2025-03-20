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
    depth: Option<i32>, // If the user clones with a depth, we store the depth here so that we know we don't have the full tree

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
        };

        // Initialize the version store based on config
        let store = create_version_store(&repo.path, config.storage.as_ref())?;
        repo.version_store = Some(store);

        Ok(repo)
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
        log::debug!("Checking for remote {name} have {}", self.remotes.len());
        for remote in self.remotes.iter() {
            log::debug!("comparing: {name} -> {}", remote.name);
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
    use crate::model::RepoNew;
    use crate::test;

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
}
