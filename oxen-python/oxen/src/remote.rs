use liboxen::model::file::FileContents;
use pyo3::prelude::*;
use std::path::PathBuf;

use crate::error::PyOxenError;
use crate::py_remote_repo::PyRemoteRepo;
use liboxen::config::UserConfig;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::model::{file::FileNew, RepoNew};

#[pyfunction]
#[pyo3(signature = (name, host, scheme="https"))]
pub fn get_repo(
    name: String,
    host: String,
    scheme: &str,
) -> Result<Option<PyRemoteRepo>, PyOxenError> {
    let Some(remote_repo) = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        liboxen::api::client::repositories::get_by_name_and_host(name, &host).await
    })?
    else {
        return Ok(None);
    };

    let branch_name = DEFAULT_BRANCH_NAME.to_string();
    let Some(revision) = pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        liboxen::api::client::revisions::get(&remote_repo, &branch_name).await
    })?
    else {
        return Ok(None);
    };

    return Ok(Some(PyRemoteRepo {
        repo: remote_repo.clone(),
        host: host.clone(),
        scheme: scheme.to_string(),
        revision: Some(branch_name.to_string()),
        commit_id: revision.commit.map(|r| r.id),
    }));
}

#[pyfunction]
pub fn create_repo(
    name: String,
    description: String,
    is_public: bool,
    host: String,
    scheme: String,
    files: Vec<(String, String)>,
) -> Result<PyRemoteRepo, PyOxenError> {
    // Check that name is valid ex: :namespace/:repo_name
    if !name.contains("/") {
        return Err(OxenError::basic_str(format!("Invalid repository name: {}", name)).into());
    }

    let namespace = name.split("/").collect::<Vec<&str>>()[0].to_string();
    let repo_name = name.split("/").collect::<Vec<&str>>()[1].to_string();

    pyo3_async_runtimes::tokio::get_runtime().block_on(async {
        let config = UserConfig::get()?;
        let user = config.to_user();

        if files.is_empty() {
            let mut repo = RepoNew::from_namespace_name_host(namespace, repo_name, host.clone());
            if !description.is_empty() {
                repo.description = Some(description);
            }
            repo.is_public = Some(is_public);
            repo.scheme = Some(scheme.clone());

            let repo = liboxen::api::client::repositories::create_empty(repo).await?;
            Ok(PyRemoteRepo {
                repo: repo.clone(),
                host: host.clone(),
                scheme: scheme.to_string(),
                // Empty repo does not have a revision or commit_id
                revision: None,
                commit_id: None,
            })
        } else {
            let files: Vec<FileNew> = files
                .iter()
                .map(|(path, contents)| FileNew {
                    path: PathBuf::from(path),
                    contents: FileContents::Text(contents.to_string()),
                    user: user.clone(),
                })
                .collect();
            let mut repo = RepoNew::from_files(&namespace, &repo_name, files);
            if !description.is_empty() {
                repo.description = Some(description);
            }
            repo.is_public = Some(is_public);
            repo.scheme = Some(scheme.clone());

            let repo = liboxen::api::client::repositories::create(repo).await?;
            let branch = liboxen::api::client::branches::get_by_name(&repo, &DEFAULT_BRANCH_NAME)
                .await?
                .unwrap();

            Ok(PyRemoteRepo {
                repo: repo.clone(),
                host: host.clone(),
                scheme: scheme.to_string(),
                revision: Some(DEFAULT_BRANCH_NAME.to_string()),
                commit_id: Some(branch.commit_id),
            })
        }
    })
}
