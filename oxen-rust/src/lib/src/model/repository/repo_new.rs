use http::Uri;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::constants::DEFAULT_HOST;
use crate::error::OxenError;
use crate::model::commit::Commit;
use crate::model::file::FileNew;
use crate::opts::StorageOpts;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct RepoNew {
    pub namespace: String,
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
    // Storage backend opts for the repo
    #[serde(default)]
    #[schema(ignore)]
    pub storage_opts: Option<StorageOpts>,
}

impl std::fmt::Display for RepoNew {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.repo_id())
    }
}

impl std::error::Error for RepoNew {}

impl RepoNew {
    pub fn repo_id(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }

    pub fn is_public(&self) -> bool {
        self.is_public.unwrap_or(false)
    }

    pub fn host(&self) -> String {
        self.host
            .clone()
            .unwrap_or_else(|| String::from(DEFAULT_HOST))
    }

    pub fn scheme(&self) -> String {
        self.scheme
            .clone()
            .unwrap_or_else(|| RepoNew::scheme_default(self.host()))
    }

    /// repo_id is the "{namespace}/{repo_name}"
    pub fn new(repo_id: String, storage_opts: Option<StorageOpts>) -> Result<RepoNew, OxenError> {
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
            scheme: Some(RepoNew::scheme_default(String::from(DEFAULT_HOST))),
            root_commit: None,
            description: None,
            files: None,
            storage_opts,
        })
    }

    pub fn scheme_default(host: impl AsRef<str>) -> String {
        let host = host.as_ref();
        if host.contains("localhost") || host.contains("127.0.0.1") || host.contains("0.0.0.0") {
            "http".to_string()
        } else {
            "https".to_string()
        }
    }

    pub fn from_namespace_name(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
        storage_opts: Option<StorageOpts>,
    ) -> RepoNew {
        RepoNew {
            namespace: String::from(namespace.as_ref()),
            name: String::from(name.as_ref()),
            host: Some(String::from(DEFAULT_HOST)),
            scheme: Some(RepoNew::scheme_default(String::from(DEFAULT_HOST))),
            is_public: None,
            root_commit: None,
            description: None,
            files: None,
            storage_opts,
        }
    }

    pub fn from_namespace_name_host(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
        host: impl AsRef<str>,
        storage_opts: Option<StorageOpts>,
    ) -> RepoNew {
        RepoNew {
            namespace: String::from(namespace.as_ref()),
            name: String::from(name.as_ref()),
            is_public: None,
            host: Some(String::from(host.as_ref())),
            scheme: Some(RepoNew::scheme_default(host)),
            root_commit: None,
            description: None,
            files: None,
            storage_opts,
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
            scheme: Some(RepoNew::scheme_default(String::from(DEFAULT_HOST))),
            root_commit: Some(root_commit),
            description: None,
            files: None,
            storage_opts: None,
        }
    }

    pub fn from_files(
        namespace: impl AsRef<str>,
        name: impl AsRef<str>,
        files: Vec<FileNew>,
        storage_opts: Option<StorageOpts>,
    ) -> RepoNew {
        RepoNew {
            namespace: String::from(namespace.as_ref()),
            name: String::from(name.as_ref()),
            is_public: None,
            host: Some(String::from(DEFAULT_HOST)),
            scheme: Some(RepoNew::scheme_default(String::from(DEFAULT_HOST))),
            root_commit: None,
            description: None,
            files: Some(files),
            storage_opts,
        }
    }

    pub fn from_url(url: &str) -> Result<RepoNew, OxenError> {
        let uri = url.parse::<Uri>()?;
        let mut split_path: Vec<&str> = uri.path().split('/').collect();

        if split_path.len() < 3 {
            return Err(OxenError::basic_str("Invalid repo url"));
        }

        // Pop in reverse to get repo_name then namespace
        let repo_name = split_path.pop().unwrap();
        let namespace = split_path.pop().unwrap();
        Ok(RepoNew {
            namespace: namespace.to_string(),
            name: repo_name.to_string(),
            is_public: None,
            host: Some(uri.host().unwrap().to_string()),
            scheme: Some(uri.scheme().unwrap().to_string()),
            root_commit: None,
            description: None,
            files: None,
            storage_opts: None,
        })
    }
}
