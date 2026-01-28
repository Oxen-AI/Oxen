use actix_web::{http, web, HttpRequest, HttpResponse};
use utoipa;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param, TreeDepthQuery};

use liboxen::repositories;
use liboxen::view::entries::TreeEntriesResponse;

/// List a directory's contents in a nested tree structure
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/tree/{resource}",
    operation_id = "list_directory_tree",
    tag = "Directories",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the directory (including branch/commit ID)", example = "main/data/train"),
        TreeDepthQuery
    ),
    responses(
        (status = 200, description = "Nested tree structure of directory entries", body = TreeEntriesResponse),
        (status = 404, description = "Directory or repository not found")
    )
)]
pub async fn get(
    req: HttpRequest,
    query: web::Query<TreeDepthQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    // Parse path params
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    // Get the repository
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    // Parse the resource to get revision and path
    let resource = parse_resource(&req, &repo)?;

    let depth = query.depth.unwrap_or(1);
    let revision = resource.version.to_str().unwrap_or_default().to_string();

    // Call the repository tree listing function
    let entries =
        repositories::entries::list_directory_tree(&repo, &revision, &resource.path, depth).await?;

    let response = TreeEntriesResponse::ok_from(entries);
    Ok(HttpResponse::Ok()
        .status(http::StatusCode::OK)
        .json(response))
}
