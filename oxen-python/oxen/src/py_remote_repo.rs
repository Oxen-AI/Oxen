use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::commit::NewCommitBody;
use liboxen::model::file::{FileContents, FileNew};
use liboxen::model::{Remote, RemoteRepository, RepoNew};
use liboxen::opts::PaginateOpts;
use liboxen::{api, repositories};

use std::borrow::Cow;
use std::path::PathBuf;

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
    pub repo: RemoteRepository,
    #[pyo3(get)]
    pub host: String,
    #[pyo3(get)]
    pub scheme: String,
    // revision and commit_id are Option's in case you call .create(empty=True)
    #[pyo3(get)]
    pub revision: Option<String>,
    #[pyo3(get)]
    pub commit_id: Option<String>,
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
        let (namespace, repo_name) = match repo.split_once('/') {
            Some((namespace, repo_name)) => (namespace.to_string(), repo_name.to_string()),
            None => {
                return Err(OxenError::basic_str(format!(
                    "Invalid repo name, must be in format namespace/repo_name. Got {}",
                    repo
                ))
                .into())
            }
        };

        let remote_repo = RemoteRepository {
            namespace: namespace.to_owned(),
            name: repo_name.to_owned(),
            remote: Remote {
                url: liboxen::api::endpoint::remote_url_from_namespace_name_scheme(
                    &host, &namespace, &repo_name, scheme,
                ),
                name: String::from(liboxen::constants::DEFAULT_REMOTE_NAME),
            },
            is_empty: false,
            min_version: Some(liboxen::constants::MIN_OXEN_VERSION.to_string()),
        };

        let parsed_revision = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            liboxen::api::client::revisions::get(&remote_repo, revision).await
        })?;

        Ok(Self {
            repo: remote_repo,
            scheme: scheme.to_string(),
            host,
            revision: Some(revision.to_string()),
            commit_id: parsed_revision.map(|r| r.commit.unwrap().id),
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
        self.repo.url()
    }

    fn namespace(&self) -> &str {
        &self.repo.namespace
    }

    fn name(&self) -> &str {
        &self.repo.name
    }

    fn set_revision(&mut self, new_revision: String) {
        self.revision = Some(new_revision);
    }

    fn set_commit_id(&mut self, commit_id: String) {
        self.commit_id = Some(commit_id);
    }

    fn list_workspaces(&self) -> Result<Vec<PyWorkspaceResponse>, PyOxenError> {
        let workspaces = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::workspaces::list(&self.repo).await })?;
        Ok(workspaces
            .iter()
            .map(|w| PyWorkspaceResponse {
                id: w.id.clone(),
                name: w.name.clone(),
                commit_id: w.commit.id.clone(),
            })
            .collect())
    }

    fn create(&mut self, empty: bool, is_public: bool) -> Result<PyRemoteRepo, PyOxenError> {
        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            if empty {
                let mut repo = RepoNew::from_namespace_name_host(
                    self.repo.namespace.clone(),
                    self.repo.name.clone(),
                    self.host.clone(),
                );
                repo.is_public = Some(is_public);
                repo.scheme = Some(self.scheme.clone());
                api::client::repositories::create_empty(repo).await
            } else {
                let config = UserConfig::get()?;
                let user = config.to_user();
                let files: Vec<FileNew> = vec![FileNew {
                    path: PathBuf::from("README.md"),
                    contents: FileContents::Text(format!("# {}\n", &self.repo.name)),
                    user: user.clone(),
                }];
                let mut repo = RepoNew::from_files(&self.repo.namespace, &self.repo.name, files);
                repo.host = Some(self.host.clone());
                repo.is_public = Some(is_public);
                repo.scheme = Some(self.scheme.clone());
                api::client::repositories::create(repo).await
            }
        })?;

        self.repo = result;

        Ok(PyRemoteRepo {
            repo: self.repo.clone(),
            host: self.host.clone(),
            scheme: self.scheme.clone(),
            revision: self.revision.clone(),
            commit_id: self.commit_id.clone(),
        })
    }

    fn exists(&self) -> Result<bool, PyOxenError> {
        let exists = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::repositories::exists(&self.repo).await })?;

        Ok(exists)
    }

    fn delete(&self) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::repositories::delete(&self.repo).await })?;

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
                repositories::download(&self.repo, &remote_path, &local_path, revision).await
            } else {
                if let Some(revision) = &self.revision {
                    repositories::download(&self.repo, &remote_path, &local_path, &revision).await
                } else {
                    Err(OxenError::basic_str(
                        "Invalid Revision: Cannot download without a version.",
                    )
                    .into())
                }
            }
        })?;

        Ok(())
    }

    fn get_file(&self, remote_path: PathBuf, revision: &str) -> Result<Cow<[u8]>, PyOxenError> {
        let bytes = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::file::get_file(&self.repo, &revision, &remote_path).await
        })?;

        Ok(bytes.to_vec().into())
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
                &self.repo,
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
                api::client::commits::list_commits_for_path(&self.repo, revision, path, &page_opts)
                    .await
            })?
        } else {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                api::client::commits::list_commit_history_paginated(
                    &self.repo, revision, &page_opts,
                )
                .await
            })?
        };

        Ok(paginated_commits.into())
    }

    fn list_branches(&self) -> Result<Vec<PyBranch>, PyOxenError> {
        let branches = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::branches::list(&self.repo).await })?;
        Ok(branches
            .iter()
            .map(|b| PyBranch::new(b.name.clone(), b.commit_id.clone()))
            .collect())
    }

    fn ls(
        &self,
        path: PathBuf,
        page_num: usize,
        page_size: usize,
    ) -> Result<PyPaginatedDirEntries, PyOxenError> {
        let Some(revision) = &self.revision else {
            return Ok(PyPaginatedDirEntries::empty());
        };

        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::dir::list(&self.repo, &revision, &path, page_num, page_size).await
        })?;

        // Convert remote status to a PyStagedData using the from method
        Ok(PyPaginatedDirEntries::from(result))
    }

    fn file_exists(&self, path: PathBuf, revision: &str) -> Result<bool, PyOxenError> {
        let exists = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            match api::client::metadata::get_file(&self.repo, &revision, &path).await {
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
            api::client::metadata::get_file(&self.repo, &revision, &remote_path).await
        }) {
            Ok(Some(remote_metadata)) => {
                let remote_hash = remote_metadata.entry.hash();
                let local_hash =
                    liboxen::util::hasher::hash_file_contents(&local_path).map_err(|e| {
                        PyValueError::new_err(format!("Error hashing local file: {}", e))
                    })?;
                Ok(remote_hash != local_hash)
            }
            Ok(None) => Err(PyValueError::new_err(format!(
                "File does not exist: {}",
                remote_path.display()
            ))),
            Err(e) => Err(PyValueError::new_err(format!(
                "Error getting file metadata: {}",
                e
            ))),
        }
    }

    fn metadata(&self, path: PathBuf) -> Result<Option<PyEntry>, PyOxenError> {
        let Some(revision) = &self.revision else {
            return Ok(None);
        };

        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::metadata::get_file(&self.repo, &revision, &path).await
        })?;

        Ok(result.map(|e| PyEntry::from(e.entry)))
    }

    fn get_branch(&self, branch_name: String) -> PyResult<PyBranch> {
        log::info!("Get branch... {branch_name}");

        let branch = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            log::info!("From repo... {}", self.repo.remote.url);
            api::client::branches::get_by_name(&self.repo, &branch_name).await
        });

        match branch {
            Ok(Some(branch)) => Ok(PyBranch::from(branch)),
            _ => Err(PyValueError::new_err("could not get branch")),
        }
    }

    fn branch_exists(&self, branch_name: String) -> PyResult<bool> {
        let branch = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::branches::get_by_name(&self.repo, &branch_name).await });

        match branch {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(PyValueError::new_err(format!(
                "Error getting branch: {}",
                e
            ))),
        }
    }

    fn get_commit(&self, commit_id: String) -> PyResult<PyCommit> {
        let commit = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::commits::get_by_id(&self.repo, &commit_id).await });
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
            api::client::branches::create_from_commit_id(&self.repo, &new_name, &commit_id).await
        });

        match branch {
            Ok(branch) => Ok(PyBranch::from(branch)),
            _ => Err(PyValueError::new_err("Could not get or create branch")),
        }
    }

    fn delete_branch(&self, branch_name: String) -> PyResult<()> {
        let result = pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::branches::delete(&self.repo, &branch_name).await });

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(PyValueError::new_err(format!(
                "Could not delete branch: {}",
                e
            ))),
        }
    }

    fn merge(&mut self, base_branch: String, head_branch: String) -> Result<PyCommit, PyOxenError> {
        let result = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::merger::merge(&self.repo, &base_branch, &head_branch).await
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
            api::client::merger::mergeable(&self.repo, &base_branch, &head_branch).await
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
            },
            _ => Err(PyValueError::new_err(format!("{} is not a valid branch name or commit id. Consider creating it with `create_branch`", revision)))
        }
    }

    fn diff_file(&self, base: &str, head: &str, path: &str) -> Result<PyDiffEntry, PyOxenError> {
        let diff = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::diff::diff_entries(&self.repo, &base, &head, path).await
        })?;

        Ok(diff.into())
    }
}
