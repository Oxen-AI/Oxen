use liboxen::api;
use liboxen::config::embedding_config::EmbeddingStatus;
use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::{NewCommitBody, RemoteRepository};
use liboxen::opts::{DFOpts, PaginateOpts};
use liboxen::view::JsonDataFrameViews;
use pyo3::prelude::*;
use std::path::{Path, PathBuf};

use crate::error::PyOxenError;
use crate::py_commit::PyCommit;
use crate::py_workspace::PyWorkspace;

#[pyfunction]
pub fn is_indexed(
    workspace: PyWorkspace,
    workspace_id: String,
    path: PathBuf,
) -> Result<bool, PyOxenError> {
    let repo = workspace.repo.repo;
    let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        api::client::workspaces::data_frames::is_indexed(&repo, &workspace_id, &path).await
    })?;
    Ok(data)
}

#[pyfunction]
pub fn index(
    workspace: PyWorkspace,
    workspace_id: String,
    path: PathBuf,
) -> Result<(), PyOxenError> {
    let repo = workspace.repo.repo;
    pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        api::client::workspaces::data_frames::index(&repo, &workspace_id, &path).await
    })?;
    Ok(())
}

fn _get(
    repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<JsonDataFrameViews, PyOxenError> {
    let path = path.as_ref();
    let opts = DFOpts::empty();

    let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        api::client::workspaces::data_frames::get(repo, &workspace_id, path, &opts).await
    })?;

    let Some(data_frame) = data.data_frame else {
        return Err(
            OxenError::basic_str(format!("Failed to get data frame for path: {:?}", path)).into(),
        );
    };

    Ok(data_frame)
}

#[pyclass]
pub struct PyWorkspaceDataFrame {
    workspace: PyWorkspace,
    path: PathBuf,
    _first_page: JsonDataFrameViews,
}

#[pyclass]
pub struct PyColumn {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub dtype: String,
}

// Implement the __repr__ method for PyColumn
#[pymethods]
impl PyColumn {
    fn __repr__(&self) -> String {
        format!("PyColumn(name={}, dtype={})", self.name, self.dtype)
    }

    fn __str__(&self) -> String {
        format!("PyColumn(name={}, dtype={})", self.name, self.dtype)
    }
}

#[pymethods]
impl PyWorkspaceDataFrame {
    #[new]
    #[pyo3(signature = (workspace, path))]
    fn new(workspace: PyWorkspace, path: PathBuf) -> Result<Self, PyOxenError> {
        let workspace_id = workspace.get_identifier();
        // Index the data frame (will simply return if already indexed)
        index(workspace.clone(), workspace_id.clone(), path.clone())?;
        // Fetch the first page so that it is
        // quick to look up size and other pagination params

        let df = _get(&workspace.repo.repo, &workspace_id, &path)?;
        Ok(Self {
            workspace,
            path,
            _first_page: df,
        })
    }

    fn size(&mut self) -> Result<(usize, usize), PyOxenError> {
        let df = _get(&self.workspace.repo.repo, &self.workspace.id, &self.path)?;
        let size = &df.view.size;
        let width = size.width;
        let height = size.height;
        self._first_page = df;
        Ok((width, height))
    }

    fn get_columns(&self) -> Result<Vec<PyColumn>, PyOxenError> {
        let df = _get(&self.workspace.repo.repo, &self.workspace.id, &self.path)?;
        let columns = &df.view.schema.fields;
        let columns = columns
            .iter()
            .map(|c| PyColumn {
                name: c.name.clone(),
                dtype: c.dtype.clone(),
            })
            .collect();
        Ok(columns)
    }

    fn page_size(&self) -> usize {
        self._first_page.view.pagination.page_size
    }

    fn total_pages(&self) -> usize {
        self._first_page.view.pagination.total_pages
    }

    pub fn is_indexed(&self) -> Result<bool, PyOxenError> {
        let is_indexed = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::is_indexed(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
            )
            .await
        })?;
        Ok(is_indexed)
    }

    pub fn index(&self) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::index(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
            )
            .await
        })?;

        Ok(())
    }

    #[pyo3(signature = (page=None))]
    fn list(&self, page: Option<usize>) -> Result<String, PyOxenError> {
        let mut opts = DFOpts::empty();
        opts.page = page;

        let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::get(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                &opts,
            )
            .await
        })?;

        // Extract the serde_json::Value from the JsonDataFrameView
        let view = data.data_frame.unwrap().view.data;

        // convert json to String
        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    /// Query the data frame using SQL
    fn sql_query(&self, sql: String) -> Result<String, PyOxenError> {
        let mut opts = DFOpts::empty();
        opts.sql = Some(sql);

        match pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::get(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                &opts,
            )
            .await
        }) {
            Ok(data) => {
                // Extract the serde_json::Value from the JsonDataFrameView
                let view = data.data_frame.unwrap().view.data;

                // convert json to String
                let result: String = serde_json::to_string(&view).unwrap();
                Ok(result)
            }
            Err(e) => {
                Err(OxenError::basic_str(format!("Failed to query data frame: {}", e)).into())
            }
        }
    }

    fn is_nearest_neighbors_enabled(&self, column: String) -> Result<bool, PyOxenError> {
        let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::embeddings::get(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
            )
            .await
        })?;

        let is_enabled = data
            .columns
            .iter()
            .any(|c| c.name == column && c.status == EmbeddingStatus::Complete);
        Ok(is_enabled)
    }

    fn enable_nearest_neighbors(&self, column: String) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let use_background_thread = false;
            api::client::workspaces::data_frames::embeddings::index(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                &column,
                use_background_thread,
            )
            .await
        })?;
        Ok(())
    }

    fn sort_by_embedding(
        &self,
        column: String,
        embedding: Vec<f32>,
        page_num: usize,
        page_size: usize,
    ) -> Result<String, PyOxenError> {
        let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let paginate_opts = PaginateOpts {
                page_num,
                page_size,
            };
            api::client::workspaces::data_frames::embeddings::neighbors(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                &column,
                &embedding,
                &paginate_opts,
            )
            .await
        })?;
        // Extract the serde_json::Value from the JsonDataFrameView
        let view = data.data_frame.unwrap().view.data;

        // convert json to String
        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    /// Get the nearest neighbors to the embedding
    fn nearest_neighbors_search(
        &self,
        find_embedding_where: String,
        sort_by_similarity_to: String,
        page_num: usize,
        page_size: usize,
    ) -> Result<String, PyOxenError> {
        let mut opts = DFOpts::empty();
        opts.find_embedding_where = Some(find_embedding_where);
        opts.sort_by_similarity_to = Some(sort_by_similarity_to);
        opts.page = Some(page_num);
        opts.page_size = Some(page_size);

        let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::get(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                &opts,
            )
            .await
        })?;

        // Extract the serde_json::Value from the JsonDataFrameView
        let view = data.data_frame.unwrap().view.data;

        // convert json to String
        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    fn get_row_by_idx(&self, row: usize) -> Result<String, PyOxenError> {
        let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let mut opts = DFOpts::empty();
            opts.sql = Some(format!("SELECT * FROM df LIMIT 1 OFFSET {}", row));

            let response = api::client::workspaces::data_frames::get(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                &opts,
            )
            .await?;

            // convert view to json string
            match serde_json::to_string(&response.data_frame.unwrap().view.data) {
                Ok(json) => Ok(json),
                Err(e) => Err(OxenError::basic_str(format!(
                    "Could not convert view to json: {}",
                    e
                ))),
            }
        })?;
        Ok(data)
    }

    fn get_row_by_id(&self, id: String) -> Result<String, PyOxenError> {
        let data = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::rows::get(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                id.as_str(),
            )
            .await
        })?;
        let view = data.data_frame.view.data;

        // convert json to String
        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    fn insert_row(&self, data: String) -> Result<String, PyOxenError> {
        let Ok(_) = serde_json::from_str::<serde_json::Value>(&data) else {
            return Err(
                OxenError::basic_str(format!("Failed to parse json data: {}", data)).into(),
            );
        };

        let workspace_id = self.workspace.get_identifier();

        let (_, Some(row_id)) = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::rows::add(
                &self.workspace.repo.repo,
                &workspace_id,
                &self.path,
                data.to_string(),
            )
            .await
        })?
        else {
            return Err(OxenError::basic_str("Failed to insert data").into());
        };

        Ok(row_id)
    }

    fn update_row(&self, id: String, data: String) -> Result<String, PyOxenError> {
        let Ok(_) = serde_json::from_str::<serde_json::Value>(&data) else {
            return Err(
                OxenError::basic_str(format!("Failed to parse json data: {}", data)).into(),
            );
        };

        let view = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::rows::update(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                id.as_str(),
                data,
            )
            .await
        })?;
        let view = view.data_frame.view.data;

        let result: String = serde_json::to_string(&view).unwrap();
        Ok(result)
    }

    fn delete_row(&self, id: String) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::rows::delete(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                id.as_str(),
            )
            .await
        })?;
        Ok(())
    }

    fn add_column(&self, name: String, data_type: String) -> Result<(), PyOxenError> {
        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            let data = serde_json::json!({
                "name": name,
                "data_type": data_type,
            });

            let data = data.to_string();
            api::client::workspaces::data_frames::columns::create(
                &self.workspace.repo.repo,
                &self.workspace.id,
                &self.path,
                data,
            )
            .await
        })?;
        Ok(())
    }

    fn restore(&self) -> Result<(), PyOxenError> {
        let repo = &self.workspace.repo.repo;

        pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::data_frames::restore(repo, &self.workspace.id, &self.path)
                .await
        })?;

        Ok(())
    }

    fn commit(&self, branch: &str, message: &str) -> Result<PyCommit, PyOxenError> {
        let user = UserConfig::get()?;
        let repo = &self.workspace.repo.repo;

        let commit = NewCommitBody {
            message: message.to_string(),
            author: user.name,
            email: user.email,
        };

        let commit = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
            api::client::workspaces::commit(repo, branch, &self.workspace.id, &commit).await
        })?;
        Ok(commit.into())
    }
}
