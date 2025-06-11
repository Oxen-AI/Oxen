//! Python bindings for the `Repo` struct.
//!

use liboxen::error::OxenError;
use liboxen::model::Branch;
use liboxen::model::LocalRepository;
use liboxen::opts::CloneOpts;
use liboxen::opts::RmOpts;
use pyo3::prelude::*;

use liboxen::api;
use liboxen::opts::FetchOpts;
use liboxen::repositories;

use liboxen::core::refs::with_ref_manager;

use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_branch::PyBranch;
use crate::py_commit::PyCommit;
// use crate::py_diff::PyDiff;
use crate::py_staged_data::PyStagedData;

#[pyclass]
pub struct PyRepo {
    path: PathBuf,
}

#[pymethods]
impl PyRepo {
    #[new]
    #[pyo3(signature = (path))]
    fn py_new(path: PathBuf) -> PyResult<Self> {
        Ok(Self { path })
    }

    pub fn path(&self) -> PyResult<String> {
        Ok(self.path.to_string_lossy().to_string())
    }

    pub fn init(&self) -> Result<(), PyOxenError> {
        repositories::init(&self.path)?;
        Ok(())
    }

    #[pyo3(signature = (url, branch, all, filters=None))]
    pub fn clone(
        &mut self,
        url: &str,
        branch: &str,
        all: bool,
        filters: Option<Vec<String>>,
    ) -> Result<(), PyOxenError> {
        let repo = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let opts = CloneOpts {
                url: url.to_string(),
                dst: self.path.clone(),
                fetch_opts: FetchOpts {
                    branch: branch.to_string(),
                    subtree_paths: filters.map(|f| f.iter().map(PathBuf::from).collect()),
                    depth: None,
                    all,
                    ..FetchOpts::new()
                },
            };
            repositories::clone(&opts).await
        })?;

        // cd repo_path
        self.path = repo.path;

        Ok(())
    }

    fn current_branch(&self) -> Result<Option<PyBranch>, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let branch = repositories::branches::current_branch(&repo)?.map(PyBranch::from);
        Ok(branch)
    }

    pub fn add(&self, path: PathBuf) -> Result<(), PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        repositories::add(&repo, path).unwrap();
        Ok(())
    }

    pub fn add_schema_metadata(
        &self,
        path: &str,
        column: &str,
        metadata: &str,
    ) -> Result<(), PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;

        // make sure metadata is valid json, return oxen error if not
        let metadata: serde_json::Value = serde_json::from_str(metadata).map_err(|e| {
            OxenError::basic_str(format!(
                "Metadata must be valid JSON: ''\n{}",
                // metadata.as_ref(),
                e
            ))
        })?;

        for (path, schema) in
            repositories::data_frames::schemas::add_column_metadata(&repo, path, column, &metadata)?
        {
            println!("{:?}\n{}", path, schema.verbose_str());
        }

        Ok(())
    }

    pub fn rm(&self, path: PathBuf, recursive: bool, staged: bool) -> Result<(), PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let rm_opts = RmOpts {
            path,
            recursive,
            staged,
        };

        repositories::rm(&repo, &rm_opts)?;

        Ok(())
    }

    pub fn status(&self) -> Result<PyStagedData, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let status = repositories::status(&repo)?;
        Ok(PyStagedData { data: status })
    }

    pub fn commit(&self, message: &str) -> Result<PyCommit, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let commit = repositories::commit(&repo, message)?;
        Ok(PyCommit { commit })
    }

    pub fn branch(&self, name: &str, delete: bool) -> Result<PyBranch, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let branch = if delete {
            repositories::branches::delete(&repo, name)
        } else {
            repositories::branches::get_by_name(&repo, name)?
                .ok_or(OxenError::local_branch_not_found(name))
        };
        Ok(PyBranch::from(branch?))
    }

    pub fn branch_exists(&self, name: &str) -> Result<bool, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let has_branch = with_ref_manager(&repo, |manager| Ok(manager.has_branch(name)))?;
        Ok(has_branch)
    }

    pub fn checkout(&self, revision: &str, create: bool) -> Result<PyBranch, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let branch = if create {
            repositories::branches::create_checkout(&repo, revision)?
        } else {
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                repositories::checkout(&repo, revision)
                    .await?
                    .ok_or(OxenError::local_branch_not_found(revision))
            })?
        };

        Ok(PyBranch::from(branch))
    }

    pub fn log(&self) -> Result<Vec<PyCommit>, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let commits = repositories::commits::list(&repo)?;
        Ok(commits
            .iter()
            .map(|c| PyCommit { commit: c.clone() })
            .collect())
    }

    fn list_branches(&self) -> Result<Vec<PyBranch>, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        let branches = repositories::branches::list(&repo)?;
        Ok(branches
            .iter()
            .map(|b| PyBranch::new(b.name.clone(), b.commit_id.clone()))
            .collect())
    }

    pub fn set_remote(&self, name: &str, url: &str) -> Result<(), PyOxenError> {
        let mut repo = LocalRepository::from_dir(&self.path)?;
        liboxen::command::config::set_remote(&mut repo, name, url)?;
        Ok(())
    }

    pub fn push(&self, remote: &str, branch: &str, delete: bool) -> Result<PyBranch, PyOxenError> {
        let result: Result<Branch, OxenError> =
            pyo3_async_runtimes::tokio::get_runtime().block_on(async {
                let repo = LocalRepository::from_dir(&self.path)?;
                if delete {
                    // Delete the remote branch
                    api::client::branches::delete_remote(&repo, remote, branch).await
                } else {
                    // Push to the remote branch
                    repositories::push::push_remote_branch(&repo, remote, branch).await
                }
            });

        let py_branch = PyBranch::from(result?);
        Ok(py_branch)
    }

    pub fn pull(&self, remote: &str, branch: &str, all: bool) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let repo = LocalRepository::from_dir(&self.path)?;
            let fetch_opts = FetchOpts {
                remote: remote.to_string(),
                branch: branch.to_string(),
                subtree_paths: None,
                depth: None,
                all,
                ..FetchOpts::new()
            };
            repositories::pull_remote_branch(&repo, &fetch_opts).await
        })?;
        Ok(())
    }

    pub fn merge(&self, branch: &str) -> Result<Option<PyCommit>, PyOxenError> {
        let repo = LocalRepository::from_dir(&self.path)?;
        match repositories::merge::merge(&repo, branch)? {
            Some(commit) => Ok(Some(PyCommit { commit })),
            None => Ok(None),
        }
    }

    // pub fn diff(&self, path: &str) -> Result<PyDiff, PyOxenError> {
    //     let repo = LocalRepository::from_dir(&self.path)?;
    //     let diff =
    // }
}
