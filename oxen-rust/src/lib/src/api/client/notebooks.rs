use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::opts::NotebookOpts;
use crate::view::notebook::{
    Notebook, NotebookBaseImage, NotebookBaseImagesResponse, NotebookRequest, NotebookResponse,
};

/// Create a new notebook object that is ready to be run
pub async fn create(
    repository: &RemoteRepository,
    opts: &NotebookOpts,
) -> Result<Notebook, OxenError> {
    let branch = &opts.branch;
    let path = &opts.notebook;

    let url =
        api::endpoint::url_from_repo(repository, &format!("/notebooks/file/{branch}/{path}"))?;

    let params = serde_json::to_string(&NotebookRequest::new(opts))?;
    log::debug!("notebooks::create {url}\n{params}");

    let client = client::new_for_url(&url)?;
    let res = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(params)
        .send()
        .await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: NotebookResponse = serde_json::from_str(&body)?;
    Ok(response.notebook)
}

/// Run a notebook object that has already been created
pub async fn run(
    repository: &RemoteRepository,
    notebook: &Notebook,
) -> Result<Notebook, OxenError> {
    let notebook_id = &notebook.id;

    let url = api::endpoint::url_from_repo(repository, &format!("/notebooks/{notebook_id}"))?;
    log::debug!("notebooks::start {url}");

    let client = client::new_for_url(&url)?;
    let res = client.post(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: NotebookResponse = serde_json::from_str(&body)?;
    Ok(response.notebook)
}

/// Get a notebook object that has already been created
pub async fn get(
    repository: &RemoteRepository,
    notebook_id: impl AsRef<str>,
) -> Result<Notebook, OxenError> {
    let notebook_id = notebook_id.as_ref();

    let url = api::endpoint::url_from_repo(repository, &format!("/notebooks/{notebook_id}"))?;
    log::debug!("notebooks::start {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: NotebookResponse = serde_json::from_str(&body)?;
    Ok(response.notebook)
}

/// Stop a notebook by id
pub async fn stop(
    repository: &RemoteRepository,
    notebook: &Notebook,
) -> Result<Notebook, OxenError> {
    let notebook_id = &notebook.id;

    let url = api::endpoint::url_from_repo(repository, &format!("/notebooks/{notebook_id}"))?;
    log::debug!("notebooks::stop {url}");

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: NotebookResponse = serde_json::from_str(&body)?;
    Ok(response.notebook)
}

/// List the available images
pub async fn list_base_images(
    repository: &RemoteRepository,
) -> Result<Vec<NotebookBaseImage>, OxenError> {
    let base_url = repository.api_url()?;
    // Replace the namespace/name with ""
    let base_url = base_url.replace(
        &format!("/repos/{}/{}", repository.namespace, repository.name),
        "",
    );

    let url = format!("{base_url}/notebook_base_images");
    log::debug!("notebooks::list_base_images {url}");

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: NotebookBaseImagesResponse = serde_json::from_str(&body)?;
    Ok(response.notebook_base_images)
}
