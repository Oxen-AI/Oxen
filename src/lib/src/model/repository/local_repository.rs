use crate::config::RepositoryConfig;
use crate::constants::SHALLOW_FLAG;
use crate::constants::{self, DEFAULT_VNODE_SIZE, MIN_OXEN_VERSION};
use crate::core::versions::MinOxenVersion;
use crate::error;
use crate::error::OxenError;
use crate::model::{Remote, RemoteRepository};
use crate::util;
use crate::view::RepositoryView;

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LocalRepository {
    pub path: PathBuf,
    // Optional remotes to sync the data to
    remote_name: Option<String>, // name of the current remote ("origin" by default)
    min_version: Option<String>, // write the version if it is past v0.18.4
    remotes: Vec<Remote>,        // List of possible remotes
    vnode_size: Option<u64>,
}

impl LocalRepository {
    /// Instantiate a new repository at a given path
    /// Note: Does not create the repository on disk, just instantiates the struct
    pub fn new(path: impl AsRef<Path>) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            path: path.as_ref().to_path_buf(),
            // No remotes are set yet
            remotes: vec![],
            remote_name: None,
            // New with a path should default to our current MIN_OXEN_VERSION
            min_version: Some(MIN_OXEN_VERSION.to_string()),
            vnode_size: None,
        })
    }

    /// Load an older version of a repository with older oxen core logic
    pub fn new_from_version(
        path: impl AsRef<Path>,
        min_version: impl AsRef<str>,
    ) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            path: path.as_ref().to_path_buf(),
            remotes: vec![],
            remote_name: None,
            min_version: Some(min_version.as_ref().to_string()),
            vnode_size: None,
        })
    }

    pub fn from_view(view: RepositoryView) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            path: std::env::current_dir()?.join(view.name),
            remotes: vec![],
            remote_name: None,
            min_version: None,
            vnode_size: None,
        })
    }

    pub fn from_remote(repo: RemoteRepository, path: &Path) -> Result<LocalRepository, OxenError> {
        Ok(LocalRepository {
            path: path.to_owned(),
            remotes: vec![repo.remote],
            remote_name: Some(String::from(constants::DEFAULT_REMOTE_NAME)),
            min_version: None,
            vnode_size: None,
        })
    }

    pub fn from_dir(dir: &Path) -> Result<LocalRepository, OxenError> {
        let config_path = util::fs::config_filepath(dir);
        if !config_path.exists() {
            return Err(OxenError::local_repo_not_found());
        }
        let cfg = RepositoryConfig::from_file(&config_path)?;
        let vnode_size = cfg.vnode_size();
        let repo = LocalRepository {
            path: dir.to_path_buf(),
            remotes: cfg.remotes,
            remote_name: cfg.remote_name,
            min_version: cfg.min_version,
            vnode_size: Some(vnode_size),
        };
        Ok(repo)
    }

    pub fn from_current_dir() -> Result<LocalRepository, OxenError> {
        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

        LocalRepository::from_dir(&repo_dir)
    }

    pub fn min_version(&self) -> MinOxenVersion {
        match MinOxenVersion::or_earliest(self.min_version.clone()) {
            Ok(version) => version,
            Err(err) => {
                panic!("Invalid repo version\n{}", err)
            }
        }
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

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let cfg = RepositoryConfig {
            remote_name: self.remote_name.clone(),
            remotes: self.remotes.clone(),
            min_version: self.min_version.clone(),
            vnode_size: Some(self.vnode_size.unwrap_or(DEFAULT_VNODE_SIZE)),
        };
        let toml = toml::to_string(&cfg)?;
        util::fs::write_to_path(path, toml)?;
        Ok(())
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        let filename = util::fs::config_filepath(&self.path);
        self.save(&filename)?;
        Ok(())
    }

    pub fn set_remote(&mut self, name: &str, url: &str) -> Remote {
        self.remote_name = Some(String::from(name));
        let remote = Remote {
            name: String::from(name),
            url: String::from(url),
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

    pub fn delete_remote(&mut self, name: &str) {
        let mut new_remotes: Vec<Remote> = vec![];
        for i in 0..self.remotes.len() {
            if self.remotes[i].name != name {
                new_remotes.push(self.remotes[i].clone());
            }
        }
        self.remotes = new_remotes;
    }

    pub fn has_remote(&self, name: &str) -> bool {
        for remote in self.remotes.iter() {
            if remote.name == name {
                return true;
            }
        }
        false
    }

    pub fn get_remote(&self, name: &str) -> Option<Remote> {
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

    pub fn is_shallow_clone(&self) -> bool {
        let shallow_flag_path = util::fs::oxen_hidden_dir(&self.path).join(SHALLOW_FLAG);
        shallow_flag_path.exists()
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
