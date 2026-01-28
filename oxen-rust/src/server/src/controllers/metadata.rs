use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;

use liboxen::view::entries::EMetadataEntry;
use liboxen::view::entry_metadata::EMetadataEntryResponseView;
use liboxen::view::StatusMessage;
use liboxen::{current_function, repositories};

use actix_web::{HttpRequest, HttpResponse};
use utoipa;

/// Get file metadata
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/metadata/{resource}",
    tag = "Metadata",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the file/dir (including branch/commit info)", example = "main/images/train/dog_1.jpg"),
    ),
    responses(
        (status = 200, description = "Metadata for the entry found", body = EMetadataEntryResponseView),
        (status = 404, description = "Entry not found"),
        (status = 400, description = "Invalid resource path")
    )
)]
pub async fn file(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;
    let workspace_ref = resource.workspace.as_ref();

    let commit = if let Some(workspace) = workspace_ref {
        workspace.commit.clone()
    } else {
        resource.clone().commit.unwrap()
    };

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = repositories::commits::get_by_id(&repo, &commit.id)?
        .ok_or(OxenError::revision_not_found(commit.id.clone().into()))?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let meta = if let Some(workspace) = resource.workspace.as_ref() {
        match repositories::entries::get_meta_entry(&repo, &commit, &resource.path) {
            Ok(entry) => {
                let mut entry = repositories::workspaces::populate_entry_with_workspace_data(
                    resource.path.as_ref(),
                    entry.clone(),
                    workspace,
                )?;
                entry.set_resource(Some(resource.clone()));
                EMetadataEntryResponseView {
                    status: StatusMessage::resource_found(),
                    entry,
                }
            }
            Err(_) => {
                let added_entry = repositories::workspaces::get_added_entry(
                    &repo,
                    &resource.path,
                    workspace,
                    &resource,
                )?;
                EMetadataEntryResponseView {
                    status: StatusMessage::resource_found(),
                    entry: added_entry,
                }
            }
        }
    } else {
        let mut entry = repositories::entries::get_meta_entry(&repo, &commit, &resource.path)?;
        entry.resource = Some(resource.clone());
        EMetadataEntryResponseView {
            status: StatusMessage::resource_found(),
            entry: EMetadataEntry::MetadataEntry(entry),
        }
    };

    Ok(HttpResponse::Ok().json(meta))
}

/// Update file metadata
#[utoipa::path(
    put,
    path = "/api/repos/{namespace}/{repo_name}/metadata/{resource}",
    tag = "Metadata",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Path to the file (including version to update)", example = "versions/files/b4/158b417c800c7322d711f1816f5c00e1215b4d7c001c9b6892556d11/data"),
    ),
    responses(
        (status = 200, description = "Metadata updated", body = StatusMessage),
        (status = 400, description = "Missing version in resource path"),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn update_metadata(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    let version_str = resource
        .version
        .to_str()
        .ok_or(OxenHttpError::BadRequest("Missing resource version".into()))?;

    repositories::entries::update_metadata(&repo, version_str)?;
    Ok(HttpResponse::Ok().json(StatusMessage::resource_updated()))
}
