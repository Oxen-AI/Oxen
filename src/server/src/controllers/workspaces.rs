use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use liboxen::error::OxenError;
use liboxen::model::NewCommitBody;
use liboxen::repositories;
use liboxen::view::workspaces::{ListWorkspaceResponseView, NewWorkspace, WorkspaceResponse};
use liboxen::view::{CommitResponse, StatusMessage, WorkspaceResponseView};

use actix_web::{HttpRequest, HttpResponse};

pub mod changes;
pub mod data_frames;
pub mod files;

pub async fn get_or_create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let data: Result<NewWorkspace, serde_json::Error> = serde_json::from_str(&body);
    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("Unable to parse body. Err: {}\n{}", err, body);
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let Some(branch) = repositories::branches::get_by_name(&repo, &data.branch_name)? else {
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Branch not found")));
    };

    // Return workspace if it already exists
    let workspace_id = data.workspace_id.clone();
    log::debug!("get_or_create workspace_id {:?}", workspace_id);
    if let Ok(workspace) = repositories::workspaces::get(&repo, &workspace_id) {
        return Ok(HttpResponse::Ok().json(WorkspaceResponseView {
            status: StatusMessage::resource_created(),
            workspace: WorkspaceResponse {
                id: workspace_id,
                commit: workspace.commit.into(),
            },
        }));
    }

    let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    // Create the workspace
    repositories::workspaces::create(&repo, &commit, &workspace_id, true)?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            id: workspace_id,
            commit: commit.into(),
        },
    }))
}

pub async fn list(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    log::debug!("workspaces::list got repo: {:?}", repo.path);
    let workspaces = repositories::workspaces::list(&repo)?;
    let workspace_views = workspaces
        .iter()
        .map(|workspace| WorkspaceResponse {
            id: workspace.id.clone(),
            commit: workspace.commit.clone().into(),
        })
        .collect();

    Ok(HttpResponse::Ok().json(ListWorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspaces: workspace_views,
    }))
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    repositories::workspaces::delete(&workspace)?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            id: workspace_id,
            commit: workspace.commit.into(),
        },
    }))
}

pub async fn commit(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let branch_name = path_param(&req, "branch")?;

    log::debug!(
        "workspace::commit {namespace}/{repo_name} workspace id {} to branch {} got body: {}",
        workspace_id,
        branch_name,
        body
    );

    let data: Result<NewCommitBody, serde_json::Error> = serde_json::from_str(&body);

    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("unable to parse commit data. Err: {}\n{}", err, body);
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    let workspace = repositories::workspaces::get(&repo, &workspace_id)?;

    match repositories::workspaces::commit(&workspace, &data, &branch_name) {
        Ok(commit) => {
            log::debug!("workspace::commit ✅ success! commit {:?}", commit);
            Ok(HttpResponse::Ok().json(CommitResponse {
                status: StatusMessage::resource_created(),
                commit,
            }))
        }
        Err(OxenError::WorkspaceBehind(branch)) => Err(OxenHttpError::WorkspaceBehind(branch)),
        Err(err) => {
            log::error!("unable to commit branch {:?}. Err: {}", branch_name, err);
            Ok(HttpResponse::UnprocessableEntity().json(StatusMessage::error(format!("{err:?}"))))
        }
    }
}
