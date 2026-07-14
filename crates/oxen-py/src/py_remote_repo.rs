use liboxen::api::client::file::{GetFileOpts, get_file};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use liboxen::api::requests::RepoNew;
use liboxen::config::UserConfig;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::file::{FileContents, FileNew};
use liboxen::model::{RemoteRepository, User};
use liboxen::opts::{PaginateOpts, SortOpts};
use liboxen::storage::StorageKind;
use liboxen::{api, repositories};
use tokio_stream::StreamExt;

use std::borrow::Cow;
use std::path::PathBuf;
use std::str::FromStr;

use crate::error::PyOxenError;
use crate::py_branch::PyBranch;
use crate::py_commit::{PyCommit, PyPaginatedCommits};
use crate::py_diff::PyDiffEntry;
use crate::py_entry::PyEntry;
use crate::py_merge::PyMergeable;
use crate::py_paginated_dir_entries::PyPaginatedDirEntries;
use crate::py_user::PyUser;
use crate::py_workspace::PyWorkspaceResponse;

#[derive(Clone)]
#[pyclass]
pub struct PyRemoteRepo {
    // Identity of the repo this handle points at, known even before it exists on the server.
    pub namespace: String,
    pub name: String,
    pub url: String,
    #[pyo3(get)]
    pub host: String,
    #[pyo3(get)]
    pub scheme: String,
    // The server's repo metadata; None until the repo exists (fetched on construction or create).
    pub repo: Option<RemoteRepository>,
    // revision and commit_id are Option's in case you call .create(empty=True)
    #[pyo3(get)]
    pub revision: Option<String>,
    #[pyo3(get)]
    pub commit_id: Option<String>,
}

impl PyRemoteRepo {
    /// The server metadata for this repo, erroring if the repo does not exist on the server yet.
    pub(crate) fn repo(&self) -> Result<&RemoteRepository, OxenError> {
        self.repo.as_ref().ok_or_else(|| {
            OxenError::internal_error(format!(
                "Remote repository {}/{} does not exist",
                self.namespace, self.name
            ))
        })
    }
}

#[pymethods]
impl PyRemoteRepo {
    #[new]
    #[pyo3(signature = (repo, host, revision="main", scheme="https"))]
    fn py_new(
        repo: String,
        host: String,
        revision: &str,
        scheme: &str,
    ) -> Result<Self, PyOxenError> {
        let (namespace, name) = match repo.split_once('/') {
            Some((namespace, name)) => (namespace.to_string(), name.to_string()),
            None => {
                return Err(OxenError::basic_str(format!(
                    "Invalid repo name, must be in format namespace/repo_name. Got {repo}"
                ))
                .into());
            }
        };

        let url = liboxen::api::endpoint::remote_url_from_namespace_name_scheme(
            &host, &namespace, &name, scheme,
        );

        // Fetch the server's metadata; a repo that does not exist yet (awaiting `create`) leaves
        // it None. Any other error -- including a server too old to report storage_kind -- fails.
        let (remote_repo, commit_id) =
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let remote_repo = match api::client::repositories::get_by_name_host_and_scheme(
                    &repo, &host, scheme,
                )
                .await
                {
                    Ok(remote_repo) => Some(remote_repo),
                    Err(OxenError::RemoteRepoNotFound(_)) => None,
                    Err(err) => return Err(err),
                };
                let commit_id = match &remote_repo {
                    Some(remote_repo) => api::client::revisions::get(remote_repo, revision)
                        .await?
                        .and_then(|parsed| parsed.commit.map(|commit| commit.id)),
                    None => None,
                };
                Ok::<_, OxenError>((remote_repo, commit_id))
            })?;

        Ok(Self {
            namespace,
            name,
            url,
            scheme: scheme.to_string(),
            host,
            repo: remote_repo,
            revision: Some(revision.to_string()),
            commit_id,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "RemoteRepo(namespace='{}', name='{}', url='{}' commit='{:?}')",
            self.namespace(),
            self.name(),
            self.url(),
            self.commit_id
        )
    }

    fn __str__(&self) -> String {
        format!("{}/{}", self.namespace(), self.name())
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn set_revision(&mut self, new_revision: String) {
        self.revision = Some(new_revision);
    }

    fn set_commit_id(&mut self, commit_id: String) {
        self.commit_id = Some(commit_id);
    }

    fn list_workspaces(&self) -> Result<Vec<PyWorkspaceResponse>, PyOxenError> {
        let workspaces = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::workspaces::list(self.repo()?).await })?;
        Ok(workspaces
            .iter()
            .map(|w| PyWorkspaceResponse {
                id: w.id.clone(),
                name: w.name.clone(),
                commit_id: w.commit.id.clone(),
            })
            .collect())
    }

    #[pyo3(signature = (empty, is_public, storage_backend=None))]
    fn create(
        &mut self,
        empty: bool,
        is_public: bool,
        storage_backend: Option<String>,
    ) -> Result<PyRemoteRepo, PyOxenError> {
        let storage_kind = storage_backend
            .map(|s| StorageKind::from_str(&s))
            .transpose()?;
        let (result, default_branch) =
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                if empty {
                    let mut repo = RepoNew::from_namespace_name_host(
                        self.namespace.clone(),
                        self.name.clone(),
                        self.host.clone(),
                        storage_kind,
                    );
                    repo.is_public = Some(is_public);
                    repo.scheme = Some(self.scheme.clone());
                    let repo = api::client::repositories::create_empty(repo).await?;
                    Ok::<_, OxenError>((repo, None))
                } else {
                    let config = UserConfig::get()?;
                    let user = config.to_user();
                    let files: Vec<FileNew> = vec![FileNew {
                        path: PathBuf::from("README.md"),
                        contents: FileContents::Text(format!("# {}\n", self.name)),
                        user: user.clone(),
                    }];
                    let mut repo =
                        RepoNew::from_files(&self.namespace, &self.name, files, storage_kind);
                    repo.host = Some(self.host.clone());
                    repo.is_public = Some(is_public);
                    repo.scheme = Some(self.scheme.clone());
                    let repo = api::client::repositories::create(repo).await?;
                    // The non-empty create makes an initial commit; look up the default branch so
                    // the handle reflects it.
                    let branch =
                        api::client::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME).await?;
                    let branch = branch.ok_or_else(|| {
                        OxenError::internal_error(format!(
                            "Branch {DEFAULT_BRANCH_NAME} not found after repository creation"
                        ))
                    })?;
                    Ok((repo, Some(branch)))
                }
            })?;

        self.repo = Some(result);
        // A non-empty create points the handle at the initial commit on the default branch. An
        // empty create leaves the handle untouched: `revision` is the branch future operations
        // target (the constructor defaults it to "main" before the branch exists), so clearing
        // it would break follow-up calls like `log()` once the first commit lands.
        if let Some(branch) = default_branch {
            self.revision = Some(branch.name);
            self.commit_id = Some(branch.commit_id);
        }

        Ok(PyRemoteRepo {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            url: self.url.clone(),
            host: self.host.clone(),
            scheme: self.scheme.clone(),
            repo: self.repo.clone(),
            revision: self.revision.clone(),
            commit_id: self.commit_id.clone(),
        })
    }

    fn exists(&self) -> Result<bool, PyOxenError> {
        let name = format!("{}/{}", self.namespace, self.name);
        let exists = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            match api::client::repositories::get_by_name_host_and_scheme(
                &name,
                &self.host,
                &self.scheme,
            )
            .await
            {
                Ok(_) => Ok(true),
                Err(OxenError::RemoteRepoNotFound(_)) => Ok(false),
                Err(err) => Err(err),
            }
        })?;

        Ok(exists)
    }

    fn delete(&self) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::repositories::delete(self.repo()?).await })?;

        Ok(())
    }

    fn download(
        &self,
        remote_path: PathBuf,
        local_path: PathBuf,
        revision: &str,
    ) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            if !revision.is_empty() {
                repositories::download(self.repo()?, &remote_path, &local_path, revision).await
            } else if let Some(revision) = &self.revision {
                repositories::download(self.repo()?, &remote_path, &local_path, &revision).await
            } else {
                Err(OxenError::basic_str(
                    "Invalid Revision: Cannot download without a version.",
                ))
            }
        })?;

        Ok(())
    }

    fn download_zip(
        &self,
        directory: PathBuf,
        local_path: PathBuf,
        revision: &str,
    ) -> Result<u64, PyOxenError> {
        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            if !revision.is_empty() {
                api::client::export::download_dir_as_zip(
                    self.repo()?,
                    revision,
                    &directory,
                    &local_path,
                )
                .await
            } else if let Some(revision) = &self.revision {
                api::client::export::download_dir_as_zip(
                    self.repo()?,
                    revision,
                    &directory,
                    &local_path,
                )
                .await
            } else {
                Err(OxenError::basic_str(
                    "Invalid Revision: Cannot download zip file without a revision.",
                ))
            }
        })?;

        // Return total bytes downloaded
        Ok(result)
    }

    fn get_file(&self, remote_path: PathBuf, revision: &str) -> Result<Cow<'_, [u8]>, PyOxenError> {
        let bytes = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut stream =
                get_file(self.repo()?, revision, &remote_path, GetFileOpts::default()).await?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                bytes.extend_from_slice(&chunk?);
            }
            Ok::<_, OxenError>(bytes)
        })?;

        Ok(bytes.into())
    }

    fn put_file(
        &self,
        branch: &str,
        directory: &str,
        local_path: PathBuf,
        file_name: &str,
        commit_message: &str,
        user: PyUser,
    ) -> Result<(), PyOxenError> {
        let commit_body = NewCommitBody {
            message: commit_message.to_string(),
            author: user.name().to_string(),
            email: user.email().to_string(),
        };
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::file::put_file(
                self.repo()?,
                &branch,
                &directory,
                &local_path,
                Some(file_name),
                Some(commit_body),
            )
            .await
        })?;

        Ok(())
    }

    fn delete_file(
        &self,
        branch: &str,
        file_path: PathBuf,
        commit_message: Option<&str>,
        user: PyUser,
    ) -> Result<(), PyOxenError> {
        let commit_body = commit_message.map(|commit_message| NewCommitBody {
            message: commit_message.to_string(),
            author: user.name().to_string(),
            email: user.email().to_string(),
        });

        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::file::delete_file(self.repo()?, &branch, &file_path, commit_body).await
        })?;

        Ok(())
    }

    #[pyo3(signature = (revision="main", path=None, page_num=1, page_size=10))]
    fn log(
        &self,
        revision: &str,
        path: Option<&str>,
        page_num: usize,
        page_size: usize,
    ) -> Result<PyPaginatedCommits, PyOxenError> {
        let page_opts = PaginateOpts {
            page_num,
            page_size,
        };

        let paginated_commits = if let Some(path) = path {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                api::client::commits::list_commits_for_path(
                    self.repo()?,
                    revision,
                    path,
                    &page_opts,
                )
                .await
            })?
        } else {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                api::client::commits::list_commit_history_paginated(
                    self.repo()?,
                    revision,
                    &page_opts,
                )
                .await
            })?
        };

        Ok(paginated_commits.into())
    }

    fn list_branches(&self) -> Result<Vec<PyBranch>, PyOxenError> {
        let branches = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::branches::list(self.repo()?).await })?;
        Ok(branches
            .iter()
            .map(|b| PyBranch::new(b.name.clone(), b.commit_id.clone()))
            .collect())
    }

    #[pyo3(signature = (path, page_num=1, page_size=10, sort_by=None, reverse=false, depth=None))]
    fn ls(
        &self,
        path: PathBuf,
        page_num: usize,
        page_size: usize,
        sort_by: Option<&str>,
        reverse: bool,
        depth: Option<isize>,
    ) -> Result<PyPaginatedDirEntries, PyOxenError> {
        let Some(revision) = &self.revision else {
            return Ok(PyPaginatedDirEntries::empty());
        };

        let sort_opts = SortOpts::from_query(sort_by, reverse)?;

        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::dir::list_with_opts(
                self.repo()?,
                revision,
                &path,
                page_num,
                page_size,
                sort_opts.as_ref(),
                depth,
            )
            .await
        })?;

        // Convert remote status to a PyStagedData using the from method
        Ok(PyPaginatedDirEntries::from(result))
    }

    fn file_exists(&self, path: PathBuf, revision: &str) -> Result<bool, PyOxenError> {
        let exists = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            match api::client::metadata::get_file(self.repo()?, &revision, &path).await {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(e) => Err(e),
            }
        })?;

        Ok(exists)
    }

    fn file_has_changes(
        &self,
        local_path: PathBuf,
        remote_path: PathBuf,
        revision: &str,
    ) -> PyResult<bool> {
        match pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::metadata::get_file(self.repo()?, &revision, &remote_path).await
        }) {
            Ok(Some(remote_metadata)) => {
                let remote_hash = remote_metadata.entry.hash();
                let local_hash = liboxen::util::hasher::hash_file_contents(&local_path)
                    .map_err(|e| PyValueError::new_err(format!("Error hashing local file: {e}")))?;
                Ok(remote_hash != local_hash)
            }
            Ok(None) => Err(PyValueError::new_err(format!(
                "File does not exist: {}",
                remote_path.display()
            ))),
            Err(e) => Err(PyValueError::new_err(format!(
                "Error getting file metadata: {e}",
            ))),
        }
    }

    fn metadata(&self, path: PathBuf) -> Result<Option<PyEntry>, PyOxenError> {
        let Some(revision) = &self.revision else {
            return Ok(None);
        };

        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::metadata::get_file(self.repo()?, &revision, &path).await
        })?;

        Ok(result.map(|e| PyEntry::from(e.entry)))
    }

    fn get_branch(&self, branch_name: String) -> PyResult<PyBranch> {
        log::info!("Get branch... {branch_name}");

        let branch = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            log::info!("From repo... {}", self.url);
            api::client::branches::get_by_name(self.repo()?, &branch_name).await
        });

        match branch {
            Ok(Some(branch)) => Ok(PyBranch::from(branch)),
            _ => Err(PyValueError::new_err("could not get branch")),
        }
    }

    fn branch_exists(&self, branch_name: String) -> PyResult<bool> {
        let branch = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::branches::get_by_name(self.repo()?, &branch_name).await
        });

        match branch {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(PyValueError::new_err(format!("Error getting branch: {e}"))),
        }
    }

    fn get_commit(&self, commit_id: String) -> PyResult<PyCommit> {
        let commit = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::commits::get_by_id(self.repo()?, &commit_id).await });
        match commit {
            Ok(Some(commit)) => Ok(PyCommit { commit }),
            _ => Err(PyValueError::new_err("could not get commit id {commit_id}")),
        }
    }

    fn create_branch(&self, new_name: String) -> PyResult<PyBranch> {
        let Some(commit_id) = &self.commit_id else {
            return Err(PyValueError::new_err(
                "Must have commit id to create branch",
            ));
        };

        let branch = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::branches::create_from_commit_id(self.repo()?, &new_name, &commit_id).await
        });

        match branch {
            Ok(branch) => Ok(PyBranch::from(branch)),
            _ => Err(PyValueError::new_err("Could not get or create branch")),
        }
    }

    fn delete_branch(&self, branch_name: String) -> PyResult<()> {
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::branches::delete(self.repo()?, &branch_name).await });

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(PyValueError::new_err(format!(
                "Could not delete branch: {e}"
            ))),
        }
    }

    fn merge(
        &mut self,
        base_branch: String,
        head_branch: String,
        user: PyUser,
    ) -> Result<PyCommit, PyOxenError> {
        let author: User = user.into();
        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::merger::merge(self.repo()?, &base_branch, &head_branch, &author).await
        })?;

        // Make sure to advance internal commit id
        self.commit_id = Some(result.merge.id.clone());

        Ok(PyCommit {
            commit: result.merge,
        })
    }

    fn mergeable(
        &self,
        base_branch: String,
        head_branch: String,
    ) -> Result<PyMergeable, PyOxenError> {
        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::merger::mergeable(self.repo()?, &base_branch, &head_branch).await
        })?;

        Ok(result.into())
    }

    fn checkout(&mut self, revision: String) -> PyResult<String> {
        let branch = self.get_branch(revision.clone());
        if let Ok(branch) = branch {
            self.set_revision(branch.name().to_string());
            self.commit_id = Some(branch.commit_id().to_string());
            return Ok(branch.name().to_string());
        }

        let commit = self.get_commit(revision.clone());
        match commit {
            Ok(commit) => {
                let commit_id = commit.commit.id.clone();
                self.set_revision(commit_id.clone());
                self.commit_id = Some(commit_id.clone());
                Ok(commit_id)
            }
            _ => Err(PyValueError::new_err(format!(
                "{revision} is not a valid branch name or commit id. Consider creating it with `create_branch`"
            ))),
        }
    }

    fn diff_file(&self, base: &str, head: &str, path: &str) -> Result<PyDiffEntry, PyOxenError> {
        let diff = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::diff::diff_entries(self.repo()?, &base, &head, path).await
        })?;

        Ok(diff.into())
    }
}
