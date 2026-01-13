use actix_web::{HttpRequest, HttpResponse};

use crate::controllers;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::model::merkle_tree::node::EMerkleTreeNode;
use liboxen::view::FileWithHash;
use liboxen::{constants, repositories};

#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/export/download/{resource}",
    tag = "Files",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "Wiki-Text"),
        ("resource" = String, Path, description = "Path to directory and branch/commit (e.g. main/data)", example = "main/data"),
    ),
    responses(
        (status = 200, description = "Zip archive of the directory", content_type = "application/zip"),
        (status = 400, description = "Download size exceeds limit"),
        (status = 404, description = "Repository or resource not found")
    )
)]
pub async fn download_zip(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let resource = parse_resource(&req, &repo)?;
    let directory = resource.path.clone();
    let commit = resource.commit.ok_or(OxenHttpError::NotFound)?;

    let Some(dir_node) =
        repositories::tree::get_dir_with_children_recursive(&repo, &commit, &directory, None)?
    else {
        return Err(OxenHttpError::NotFound);
    };

    log::debug!("download_dir_as_zip found dir node: {dir_node:?}");
    let files = dir_node.list_files()?;

    let total_bytes: u64 = files
        .iter()
        .filter_map(|(_, node)| {
            if let EMerkleTreeNode::File(file_node) = &node.node {
                Some(file_node.num_bytes())
            } else {
                None
            }
        })
        .sum();
    if total_bytes > constants::MAX_ZIP_DOWNLOAD_SIZE {
        return Err(OxenHttpError::BadRequest(format!("Download request exceeded size limit ({} bytes) for zip file downloads: found {} bytes", constants::MAX_ZIP_DOWNLOAD_SIZE, total_bytes).into()));
    }

    let files_with_hash: Vec<FileWithHash> = files
        .into_iter()
        .map(|f| FileWithHash {
            path: f.0,
            hash: f.1.hash.to_string(),
        })
        .collect();

    log::debug!(
        "download_dir_as_zip found {} files with total size {}",
        files_with_hash.len(),
        total_bytes
    );

    let response = controllers::versions::stream_versions_zip(&repo, files_with_hash).await?;

    Ok(response)
}
