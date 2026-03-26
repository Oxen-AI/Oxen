use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{PageNumQuery, app_data, path_param};

use liboxen::constants;
use liboxen::core::staged::get_staged_db_manager;
use liboxen::model::LocalRepository;
use liboxen::model::Workspace;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::remote_staged_status::RemoteStagedStatus;
use liboxen::view::{
    FilePathsResponse, RemoteStagedStatusResponse, StatusMessage, StatusMessageDescription,
};

use actix_web::{HttpRequest, HttpResponse, web};

use std::path::PathBuf;

pub async fn list_root(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
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

pub async fn list(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
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

/// Unstage a file from the workspace
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/changes/{path}",
    description = "Unstage a file from workspace staging",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745"),
        ("path" = String, Path, description = "The path to the file to delete (unstage)", example = "images/train/dog_1.jpg")
    ),
    responses(
        (status = 200, description = "File marked for deletion", body = StatusMessage),
        (status = 404, description = "Workspace or File not found")
    )
)]
pub async fn unstage(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
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

    unstage_from_workspace(&repo, &workspace, &path).await
}

/// Unstage files
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/changes",
    description = "Unstage files from a workspace. Accepts both files and directories.",
    tag = "Workspace Files",
    params(
        ("namespace" = String, Path, description = "The namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "The name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "The UUID of the workspace", example = "580c0587-c157-417b-9118-8686d63d2745")
    ),
    request_body(
        content = Vec<String>,
        description = "List of paths to unstage from the workspace staging area",
        example = json!(["images/train/revert_me.jpg", "data/config.json"])
    ),
    responses(
        (status = 200, description = "Files unstaged from staging", body = StatusMessage),
        (status = 206, description = "Some files could not be unstaged (returns paths of files not found)", body = FilePathsResponse),
        (status = 404, description = "Workspace not found")
    )
)]
pub async fn unstage_many(
    req: HttpRequest,
    payload: web::Json<Vec<PathBuf>>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    log::debug!("unstage_many found repo {repo_name}, workspace_id {workspace_id}");

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let paths_to_remove: Vec<PathBuf> = payload.into_inner();

    let mut err_paths = vec![];

    for path in paths_to_remove {
        let is_staged = get_staged_db_manager(&workspace.workspace_repo)?
            .read_from_staged_db(&path)?
            .is_some();

        if !is_staged {
            continue;
        }

        match unstage_from_workspace(&repo, &workspace, &path).await {
            // Note: we can't delete the version file here because it may be
            // referenced elsewhere. In order to cleanup eagerly here we would
            // need the staged DB to track whether the version was newly added
            // or already existed.
            Ok(_) => {}
            Err(e) => {
                log::debug!("Failed to unstage file {path:?}: {e:?}");
                err_paths.push(path);
            }
        }
    }

    if err_paths.is_empty() {
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::PartialContent().json(FilePathsResponse {
            paths: err_paths,
            status: StatusMessage::resource_not_found(),
        }))
    }
}

async fn unstage_from_workspace(
    repo: &LocalRepository,
    workspace: &Workspace,
    path: &PathBuf,
) -> Result<HttpResponse, OxenHttpError> {
    // This may not be in the commit if it's added, so have to parse tabular-ness from the path.
    if util::fs::is_tabular(path) {
        repositories::workspaces::data_frames::restore(repo, workspace, path).await?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else if repositories::workspaces::files::exists(workspace, path)? {
        repositories::workspaces::files::unstage(workspace, path)?;
        Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
    } else {
        Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
    }
}
