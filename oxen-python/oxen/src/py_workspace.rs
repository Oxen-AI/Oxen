use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::model::NewCommitBody;
use pyo3::prelude::*;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::error::PyOxenError;
use crate::py_commit::PyCommit;
use crate::py_remote_repo::PyRemoteRepo;
use crate::py_staged_data::PyStagedData;

#[derive(Clone)]
#[pyclass]
pub struct PyWorkspace {
    pub repo: PyRemoteRepo,
    pub branch_name: String,
    pub commit_id: String,
    pub id: String,
    pub name: Option<String>,
}

#[pyclass]
pub struct PyWorkspaceResponse {
    pub id: String,
    pub name: Option<String>,
    pub commit_id: String,
}

#[pymethods]
impl PyWorkspaceResponse {
    fn __repr__(&self) -> String {
        format!(
            "Workspace(id='{}', name='{}', commit_id='{}')",
            self.id,
            self.name.as_deref().unwrap_or("None"),
            self.commit_id
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[pymethods]
impl PyWorkspace {
    #[new]
    #[pyo3(signature = (repo, branch_name, workspace_id, workspace_name, path))]
    fn new(
        repo: PyRemoteRepo,
        branch_name: String,
        workspace_id: Option<String>,
        workspace_name: Option<String>,
        path: Option<String>,
    ) -> Result<Self, PyOxenError> {
        let workspace_id = workspace_id.unwrap_or_else(|| format!("workspace-{}", Uuid::new_v4()));
        let workspace_identifier = workspace_name.clone().unwrap_or(workspace_id.clone());

        // Get the workspace by name
        let workspace = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::get(&repo.repo, &workspace_identifier).await
        })?;

        if let Some(workspace) = workspace {
            return Ok(Self {
                repo,
                branch_name: branch_name.clone(),
                id: workspace.id,
                name: workspace.name,
                commit_id: workspace.commit.id,
            });
        }

        let workspace = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::create_with_path(
                &repo.repo,
                &branch_name,
                &workspace_id,
                Path::new(&path.unwrap_or("/".to_string())),
                workspace_name,
            )
            .await
        })?;

        Ok(Self {
            repo,
            branch_name,
            id: workspace.id,
            name: workspace.name,
            commit_id: workspace.commit.id,
        })
    }

    pub fn get_identifier(&self) -> String {
        if let Some(name) = &self.name {
            name.clone()
        } else {
            self.id.clone()
        }
    }

    fn id(&self) -> String {
        self.id.clone()
    }

    fn name(&self) -> Option<String> {
        self.name.clone()
    }

    fn branch(&self) -> String {
        self.branch_name.clone()
    }

    fn commit_id(&self) -> String {
        self.commit_id.clone()
    }

    fn status(&self, path: PathBuf) -> Result<PyStagedData, PyOxenError> {
        let remote_status = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::changes::list(
                &self.repo.repo,
                &self.get_identifier(),
                &path,
                liboxen::constants::DEFAULT_PAGE_NUM,
                liboxen::constants::DEFAULT_PAGE_SIZE,
            )
            .await
        })?;

        // Convert remote status to a PyStagedData using the from method
        Ok(PyStagedData::from(remote_status))
    }

    fn add(&self, src: PathBuf, dst: String) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let paths = vec![src];
            api::client::workspaces::files::add(
                &self.repo.repo,
                &self.get_identifier(),
                &dst,
                paths,
            )
            .await
        })?;
        Ok(())
    }

    fn add_many(&self, src: Vec<PathBuf>, dst: String) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::files::add(&self.repo.repo, &self.get_identifier(), &dst, src)
                .await
        })?;
        Ok(())
    }

    fn rm(&self, path: PathBuf) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::files::rm(&self.repo.repo, &self.id, path).await
        })?;
        Ok(())
    }

    fn delete(&self) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime()
            .block_on(async { api::client::workspaces::delete(&self.repo.repo, &self.id).await })?;
        Ok(())
    }

    #[pyo3(signature = (message, branch_name=None))]
    fn commit(
        &self,
        message: String,
        branch_name: Option<String>,
    ) -> Result<PyCommit, PyOxenError> {
        let branch_name = branch_name.unwrap_or(self.branch_name.clone());
        let user = UserConfig::get()?.to_user();
        let commit = NewCommitBody {
            message,
            author: user.name,
            email: user.email,
        };
        let workspace_id = self.get_identifier();
        let commit = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let commit = api::client::workspaces::commit(
                &self.repo.repo,
                &branch_name,
                &workspace_id,
                &commit,
            )
            .await?;
            Ok(PyCommit { commit })
        });

        commit
    }
}
