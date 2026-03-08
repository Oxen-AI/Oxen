use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param, PageNumQuery};

use liboxen::constants;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::remote_staged_status::RemoteStagedStatus;
use liboxen::view::{RemoteStagedStatusResponse, StatusMessage, StatusMessageDescription};

use actix_web::{web, HttpRequest, HttpResponse};

use std::path::PathBuf;

#[tracing::instrument(skip_all, fields(namespace, repo_name))]
pub async fn list_root(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    metrics::counter!("oxen_server_workspaces_changes_list_root_total").increment(1);
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    log::debug!("/changes looking up repo: {namespace}/{repo_name}");

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!("/changes looking up workspace_id: {workspace_id}");
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        log::debug!("/changes could not find workspace_id: {workspace_id}");
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let path = PathBuf::from(".");
    let staged = repositories::workspaces::status::status_from_dir(&workspace, &path)?;

    staged.print();

    let response = RemoteStagedStatusResponse {
        status: StatusMessage::resource_found(),
        staged: RemoteStagedStatus::from_staged(
            &workspace.workspace_repo,
            &staged,
            page_num,
            page_size,
        ),
    };
    Ok(HttpResponse::Ok().json(response))
}

#[tracing::instrument(skip_all, fields(namespace, repo_name))]
pub async fn list(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    metrics::counter!("oxen_server_workspaces_changes_list_total").increment(1);
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    log::debug!("/changes looking up repo: {namespace}/{repo_name}");

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);
    let page_num = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!("/changes looking up workspace_id: {workspace_id}");
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        log::debug!("/changes could not find workspace_id: {workspace_id}");
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let staged = repositories::workspaces::status::status_from_dir(&workspace, &path)?;

    staged.print();

    let response = RemoteStagedStatusResponse {
        status: StatusMessage::resource_found(),
        staged: RemoteStagedStatus::from_staged(
            &workspace.workspace_repo,
            &staged,
            page_num,
            page_size,
        ),
    };
    Ok(HttpResponse::Ok().json(response))
}

#[tracing::instrument(skip_all, fields(namespace, repo_name))]
pub async fn delete(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    metrics::counter!("oxen_server_workspaces_changes_delete_total").increment(1);
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let path = PathBuf::from(path_param(&req, "path")?);

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    // This may not be in the commit if it's added, so have to parse tabular-ness from the path.
    if util::fs::is_tabular(&path) {
        repositories::workspaces::data_frames::restore(&repo, &workspace, &path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else if repositories::workspaces::files::exists(&workspace, &path)? {
        repositories::workspaces::files::delete(&workspace, &path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
    }
}
