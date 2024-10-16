use crate::api;
use crate::core::versions::MinOxenVersion;
use crate::view::repository::{RepositoryCreationView, RepositoryDataTypesView};
use crate::view::RepositoryView;
use crate::{error::OxenError, model::Remote};
use http::Uri;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct RemoteRepository {
    pub namespace: String,
    pub name: String,
    pub remote: Remote,
    pub min_version: Option<String>,
    pub is_empty: bool,
}

impl RemoteRepository {
    pub fn from_data_view(
        repository: &RepositoryDataTypesView,
        remote: &Remote,
    ) -> RemoteRepository {
        RemoteRepository {
            namespace: repository.namespace.clone(),
            name: repository.name.clone(),
            remote: remote.clone(),
            min_version: repository.min_version.clone(),
            is_empty: repository.is_empty,
        }
    }

    pub fn from_view(repository: &RepositoryView, remote: &Remote) -> RemoteRepository {
        RemoteRepository {
            namespace: repository.namespace.clone(),
            name: repository.name.clone(),
            remote: remote.clone(),
            min_version: repository.min_version.clone(),
            is_empty: repository.is_empty,
        }
    }

    pub fn from_creation_view(
        repository: &RepositoryCreationView,
        remote: &Remote,
    ) -> RemoteRepository {
        RemoteRepository {
            namespace: repository.namespace.clone(),
            name: repository.name.clone(),
            remote: remote.clone(),
            min_version: repository.min_version.clone(),
            is_empty: true,
        }
    }

    pub fn min_version(&self) -> MinOxenVersion {
        match MinOxenVersion::or_earliest(self.min_version.clone()) {
            Ok(version) => version,
            Err(err) => {
                panic!("Invalid repo version\n{}", err)
            }
        }
    }

    /// User friendly url for the remote repository
    /// Ex) http://localhost:3000/namespace/name
    pub fn url(&self) -> &str {
        &self.remote.url
    }

    // Host of the remote repository
    pub fn host(&self) -> String {
        // parse it from the url
        let uri = self.remote.url.parse::<Uri>().unwrap();
        uri.host().unwrap().to_string()
    }

    /// Underlying api url for the remote repository
    /// Ex) http://localhost:3000/api/repos/namespace/name
    pub fn api_url(&self) -> Result<String, OxenError> {
        api::endpoint::url_from_repo(self, "")
    }
}
