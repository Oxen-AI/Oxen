//! Oxen Notebook Utils
//!

use pyo3::prelude::*;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_NOTEBOOK_BASE_IMAGE};
use liboxen::opts::NotebookOpts;
use liboxen::view::notebook::Notebook;
use crate::error::PyOxenError;
use crate::py_remote_repo::PyRemoteRepo;

#[pyclass]
pub struct PyNotebook {
    #[pyo3(get)]
    pub id: String,
    #[pyo3(get)]
    pub namespace: String,
    #[pyo3(get)]
    pub name: String,
}

#[pymethods]
impl PyNotebook {
    #[new]
    fn new(id: String, namespace: String, name: String) -> Self {
        Self { id, namespace, name }
    }

    pub fn url(&self) -> String {
        format!("https://hub.oxen.ai/{}/{}/notebooks/{}", self.namespace, self.name, self.id)
    }

    // implement __str__
    fn __str__(&self) -> String {
        format!("PyNotebook({})", self.id)
    }

    // implement __repr__
    fn __repr__(&self) -> String {
        format!("PyNotebook({})", self.id)
    }
}

/// Start a notebook
#[pyfunction]
#[pyo3(signature = (
    repo,
    notebook,
    branch=DEFAULT_BRANCH_NAME,
    base_image=DEFAULT_NOTEBOOK_BASE_IMAGE,
    mode="edit",
    cpu_cores=2,
    memory_mb=2048,
    timeout_secs=3600,
    gpu_model=None,
    notebook_base_image_id=None,
    build_script=None,
    script_args=None
))]
pub fn py_start_notebook(
    repo: PyRemoteRepo,
    notebook: String,
    branch: &str,
    base_image: &str,
    mode: &str,
    cpu_cores: u32,
    memory_mb: u32,
    timeout_secs: u32,
    gpu_model: Option<String>,
    notebook_base_image_id: Option<String>,
    build_script: Option<String>,
    script_args: Option<String>,
) -> Result<PyNotebook, PyOxenError> {
    let mut opts = NotebookOpts {
        notebook,
        branch: branch.to_string(),
        base_image: base_image.to_string(),
        mode: mode.to_string(),
        gpu_model,
        cpu_cores,
        memory_mb,
        timeout_secs,
        notebook_base_image_id,
        build_script,
        script_args,
    };
    let notebook = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        let base_images = api::client::notebooks::list_base_images(&repo.repo).await?;
        let base_image_id = base_images
            .iter()
            .find(|i| i.image_definition == opts.base_image);

        let Some(base_image_id) = base_image_id else {
            let error = format!("Base image not supported: {}", opts.base_image);
            return Err(OxenError::basic_str(error));
        };

        opts.base_image = base_image_id.id.to_owned();

        let notebook = api::client::notebooks::create(&repo.repo, &opts).await?;
        let notebook = api::client::notebooks::run(&repo.repo, &notebook).await?;
        Ok(notebook)
    })?;
    let notebook = PyNotebook { id: notebook.id, namespace: repo.repo.namespace, name: repo.repo.name };
    println!("✅ Notebook {} started", notebook.id);
    Ok(notebook)
}

/// Stop a notebook
#[pyfunction]
#[pyo3(signature = (repo, notebook_id))]
pub fn py_stop_notebook(
    repo: PyRemoteRepo,
    notebook_id: String
) -> Result<(), PyOxenError> {
    let notebook: Result<Notebook, OxenError> = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        let notebook = Notebook { id: notebook_id };
        let notebook = api::client::notebooks::stop(&repo.repo, &notebook).await?;
        Ok(notebook)
    });
    println!("🛑 Notebook {} stopped", notebook.unwrap().id);
    Ok(())
}
