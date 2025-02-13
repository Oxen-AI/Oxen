use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;

use liboxen::view::entry_metadata::MetadataEntryResponseView;
use liboxen::view::StatusMessage;
use liboxen::{current_function, repositories};

use actix_web::{HttpRequest, HttpResponse};

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
            Ok(mut entry) => {
                repositories::workspaces::populate_entry_with_workspace_data(
                    resource.path.as_ref(),
                    &mut entry,
                    workspace,
                )?;
                entry.resource = Some(resource.clone());
                MetadataEntryResponseView {
                    status: StatusMessage::resource_found(),
                    entry: entry.into(),
                }
            }
            Err(_) => {
                let added_entry = repositories::workspaces::get_added_entry(
                    &resource.path,
                    workspace,
                    &resource,
                )?;
                MetadataEntryResponseView {
                    status: StatusMessage::resource_found(),
                    entry: added_entry.into(),
                }
            }
        }
    } else {
        let mut entry = repositories::entries::get_meta_entry(&repo, &commit, &resource.path)?;
        entry.resource = Some(resource.clone());
        MetadataEntryResponseView {
            status: StatusMessage::resource_found(),
            entry: entry.into(),
        }
    };

    Ok(HttpResponse::Ok().json(meta))
}

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
