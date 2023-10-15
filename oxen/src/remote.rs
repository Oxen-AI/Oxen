
use pyo3::prelude::*;
use std::path::PathBuf;

use liboxen::config::UserConfig;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::model::{file::FileNew, RepoNew};
use liboxen::error::OxenError;
use crate::error::PyOxenError;
use crate::py_remote_repo::PyRemoteRepo;

#[pyfunction]
pub fn get_repo(name: String, host: String) -> Result<Option<PyRemoteRepo>, PyOxenError> {
    let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
        liboxen::api::remote::repositories::get_by_name_and_host(name, &host).await
    })?;

    if let Some(repo) = result {
        return Ok(Some(PyRemoteRepo {
            repo: repo.clone(),
            host: host.clone(),
            revision: DEFAULT_BRANCH_NAME.to_string(),
        }));
    }

    Ok(None)
}

#[pyfunction]
pub fn create_repo(
    name: String,
    description: String,
    is_public: bool,
    host: String,
    files: Vec<(String, String)>
) -> Result<PyRemoteRepo, PyOxenError> {
    // Check that name is valid ex: :namespace/:repo_name
    if !name.contains("/") {
        return Err(OxenError::basic_str(format!(
            "Invalid repository name: {}",
            name
        )).into());
    }

    let namespace = name.split("/").collect::<Vec<&str>>()[0].to_string();
    let repo_name = name.split("/").collect::<Vec<&str>>()[1].to_string();

    let result = pyo3_asyncio::tokio::get_runtime().block_on(async {
        let config = UserConfig::get()?;
        let user = config.to_user();

        if files.is_empty() {
            let mut repo = RepoNew::from_namespace_name_host(namespace, repo_name, host.clone());
            if !description.is_empty() {
                repo.description = Some(description);
            }
            repo.is_public = Some(is_public);

            liboxen::api::remote::repositories::create_empty(repo).await
        } else {
            let files: Vec<FileNew> = files.iter().map(|(path, contents)| {
                FileNew {
                    path: PathBuf::from(path),
                    contents: contents.to_string(),
                    user: user.clone()
                }
            }).collect();
            let mut repo = RepoNew::from_files(&namespace, &repo_name, files);
            if !description.is_empty() {
                repo.description = Some(description);
            }
            repo.is_public = Some(is_public);

            liboxen::api::remote::repositories::create(repo).await
        }
    })?;
    Ok(PyRemoteRepo {
        repo: result.clone(),
        host: host.clone(),
        revision: DEFAULT_BRANCH_NAME.to_string(),
    })
}
