//! Request payload for creating a new repository. `oxen-cli` sends this to `oxen-server`.
//!
use http::Uri;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::constants::DEFAULT_HOST;
use crate::core::db::merkle_node::MerkleNodeBackend;
use crate::error::OxenError;
use crate::model::commit::Commit;
use crate::model::file::FileNew;
use crate::storage::StorageKind;

/// Only used between client and server for creating a new remote repository.
///
#[derive(Deserialize, Serialize, Debug, Clone, ToSchema, thiserror::Error)]
pub struct RepoNew {
    /// The namespace that the repository lives in: dictates application-level repository ownership.
    /// A namespace can be e.g. a user, a team, or an entire organization. Namespaces must be unique.
    pub namespace: String,
    /// The name of the repository. When cloned locally, this is the name of the directory.
    /// This name uniquely identifies it in the namespace.
    pub name: String,
    // All these are optional because you can create a repo with just a namespace and name
    // is_public only applies to OxenHub so is optional
    pub is_public: Option<bool>,
    // Host is where you are going to create the repo
    pub host: Option<String>,
    // scheme is the http scheme to use ie: http or https
    pub scheme: Option<String>,
    // Root commit to create on the server
    pub root_commit: Option<Commit>,
    // Description of the repo on the hub
    pub description: Option<String>,
    // Files that you want to seed the repo with
    pub files: Option<Vec<FileNew>>,
    /// Which storage backend the server should use for this repo (e.g. "local", "s3").
    #[serde(default)]
    pub storage_kind: Option<StorageKind>,
    /// Which engine backs the repo's Merkle node store. `None` uses the server's default.
    #[serde(default)]
    pub merkle_node_backend: Option<MerkleNodeBackend>,
}

impl std::fmt::Display for RepoNew {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.repo_id())
    }
}

impl RepoNew {
    /// The `namespace/repository` name.
    pub fn repo_id(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    /// Either the configured host or the default hostname.
    pub fn host(&self) -> String {
        self.host
            .clone()
            .unwrap_or_else(|| String::from(DEFAULT_HOST))
    }

    /// The URL scheme (i.e. "http" or "https").
    pub fn scheme(&self) -> String {
        self.scheme
            .clone()
            .unwrap_or_else(|| RepoNew::scheme_default(&self.host()))
    }

    /// repo_id is the "{namespace}/{repo_name}"
    pub fn new(repo_id: String, storage_kind: Option<StorageKind>) -> Result<RepoNew, OxenError> {
        if !repo_id.contains('/') {
            return Err(OxenError::basic_str(format!(
                "Invalid repo id: {repo_id:?}"
            )));
        }

        let mut split = repo_id.split('/');
        let namespace = split.next().unwrap().to_owned();
        let repo_name = split.next().unwrap().to_owned();
        Ok(RepoNew {
            namespace,
            name: repo_name,
            is_public: None,
            host: Some(String::from(DEFAULT_HOST)),
            scheme: Some(RepoNew::scheme_default(DEFAULT_HOST)),
            root_commit: None,
            description: None,
            files: None,
            storage_kind,
            merkle_node_backend: None,
        })
    }

    /// Default using scheme logic: local oxen-server is unencrypted (http). All else is encrypted (https).
    pub fn scheme_default(host: &str) -> String {
        if host.contains("localhost") || host.contains("127.0.0.1") || host.contains("0.0.0.0") {
            "http".to_string()
        } else {
            "https".to_string()
        }
    }

    pub fn from_namespace_name(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
        storage_kind: Option<StorageKind>,
    ) -> RepoNew {
        RepoNew {
            namespace: String::from(namespace.as_ref()),
            name: String::from(name.as_ref()),
            host: Some(String::from(DEFAULT_HOST)),
            scheme: Some(RepoNew::scheme_default(DEFAULT_HOST)),
            is_public: None,
            root_commit: None,
            description: None,
            files: None,
            storage_kind,
            merkle_node_backend: None,
        }
    }

    pub fn from_namespace_name_host(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
        host: impl AsRef<str>,
        storage_kind: Option<StorageKind>,
    ) -> RepoNew {
        RepoNew {
            namespace: String::from(namespace.as_ref()),
            name: String::from(name.as_ref()),
            is_public: None,
            host: Some(String::from(host.as_ref())),
            scheme: Some(RepoNew::scheme_default(host.as_ref())),
            root_commit: None,
            description: None,
            files: None,
            storage_kind,
            merkle_node_backend: None,
        }
    }

    pub fn from_root_commit(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
        root_commit: Commit,
    ) -> RepoNew {
        RepoNew {
            namespace: String::from(namespace.as_ref()),
            name: String::from(name.as_ref()),
            is_public: None,
            host: Some(String::from(DEFAULT_HOST)),
            scheme: Some(RepoNew::scheme_default(DEFAULT_HOST)),
            root_commit: Some(root_commit),
            description: None,
            files: None,
            storage_kind: None,
            merkle_node_backend: None,
        }
    }

    pub fn from_files(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
        files: Vec<FileNew>,
        storage_kind: Option<StorageKind>,
    ) -> RepoNew {
        RepoNew {
            namespace: String::from(namespace.as_ref()),
            name: String::from(name.as_ref()),
            is_public: None,
            host: Some(String::from(DEFAULT_HOST)),
            scheme: Some(RepoNew::scheme_default(DEFAULT_HOST)),
            root_commit: None,
            description: None,
            files: Some(files),
            storage_kind,
            merkle_node_backend: None,
        }
    }

    pub fn from_url(url: &str) -> Result<RepoNew, OxenError> {
        let uri = url.parse::<Uri>()?;

        let (namespace, name) = {
            let mut split_path: Vec<&str> = uri.path().split('/').collect();
            if split_path.len() < 3 {
                return Err(OxenError::NoNamespaceRepoInUrl(uri));
            }
            // Pop in reverse to get repo_name then namespace
            // unwraps are safe because of above length check
            let repo_name = split_path.pop().unwrap();
            let namespace = split_path.pop().unwrap();
            (namespace.to_string(), repo_name.to_string())
        };

        Ok(RepoNew {
            namespace,
            name,
            is_public: None,
            host: uri.host().map(|x| x.to_string()),
            scheme: uri.scheme().map(|x| x.to_string()),
            root_commit: None,
            description: None,
            files: None,
            storage_kind: None,
            merkle_node_backend: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merkle_node_backend_round_trips_through_json() {
        let mut repo = RepoNew::from_namespace_name("ns", "repo", None);
        repo.merkle_node_backend = Some(MerkleNodeBackend::Lmdb);
        let json = serde_json::to_string(&repo).expect("serialize");
        let parsed: RepoNew = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.merkle_node_backend, Some(MerkleNodeBackend::Lmdb));
    }

    /// An older client omits the field entirely; the server reads it as "no preference".
    #[test]
    fn missing_merkle_node_backend_deserializes_to_none() {
        let parsed: RepoNew =
            serde_json::from_str(r#"{"namespace":"ns","name":"repo"}"#).expect("deserialize");
        assert_eq!(parsed.merkle_node_backend, None);
    }
}
