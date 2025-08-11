use crate::errors::{OxenHttpError, WorkspaceBranch};
use crate::helpers::get_repo;
use crate::params::{app_data, path_param, NameParam};

use liboxen::error::OxenError;
use liboxen::model::NewCommitBody;
use liboxen::repositories;
use liboxen::view::merge::MergeableResponse;
use liboxen::view::workspaces::{ListWorkspaceResponseView, NewWorkspace, WorkspaceResponse};
use liboxen::view::{
    CommitResponse, StatusMessage, StatusMessageDescription, WorkspaceResponseView,
};

use actix_web::{web, HttpRequest, HttpResponse};

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
        return Ok(
            HttpResponse::BadRequest().json(StatusMessage::error(format!(
                "Branch not found: {}",
                data.branch_name
            ))),
        );
    };

    // Return workspace if it already exists
    let workspace_id = data.workspace_id.clone();
    let workspace_name = data.name.clone();
    let workspace_identifier;
    if let Some(workspace_name) = workspace_name {
        workspace_identifier = workspace_name;
    } else {
        workspace_identifier = workspace_id.clone();
    }
    log::debug!("get_or_create workspace_id {:?}", workspace_id);
    if let Ok(Some(workspace)) = repositories::workspaces::get(&repo, &workspace_identifier) {
        return Ok(HttpResponse::Ok().json(WorkspaceResponseView {
            status: StatusMessage::resource_found(),
            workspace: WorkspaceResponse {
                id: workspace_id,
                name: workspace.name.clone(),
                commit: workspace.commit.into(),
            },
        }));
    }

    let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    // Create the workspace
    repositories::workspaces::create_with_name(
        &repo,
        &commit,
        &workspace_id,
        data.name.clone(),
        true,
    )?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            id: workspace_id,
            name: data.name.clone(),
            commit: commit.into(),
        },
    }))
}

pub async fn get(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_found(),
        workspace: WorkspaceResponse {
            id: workspace.id,
            name: workspace.name,
            commit: workspace.commit.into(),
        },
    }))
}

pub async fn create(
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
        return Ok(
            HttpResponse::BadRequest().json(StatusMessage::error(format!(
                "Branch not found: {}",
                data.branch_name
            ))),
        );
    };

    let workspace_id = &data.workspace_id;
    let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    // Create the workspace
    repositories::workspaces::create_with_name(
        &repo,
        &commit,
        workspace_id,
        data.name.clone(),
        true,
    )?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            id: workspace_id.clone(),
            name: data.name.clone(),
            commit: commit.into(),
        },
    }))
}

pub async fn create_with_new_branch(
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

    // If the branch doesn't exist, create it 
    let branch = if let Some(branch) = repositories::branches::get_by_name(&repo, &data.branch_name)? {
        branch
    } else {
        repositories::branches::create_from_head(&repo, &data.branch_name)?
    };

    let workspace_id = &data.workspace_id;
    let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?.unwrap();

    // Create the workspace
    repositories::workspaces::create_with_name(
        &repo,
        &commit,
        workspace_id,
        data.name.clone(),
        true,
    )?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            id: workspace_id.clone(),
            name: data.name.clone(),
            commit: commit.into(),
        },
    }))
}

pub async fn list(
    req: HttpRequest,
    params: web::Query<NameParam>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
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
            name: workspace.name.clone(),
            commit: workspace.commit.clone().into(),
        })
        .filter(|workspace| {
            // TODO: Would be faster to have a map of names to namespaces, but this works for now
            //       if getting a workspace is slow then we can optimize it
            if let Some(name) = &params.name {
                workspace.name == Some(name.to_string())
            } else {
                true
            }
        })
        .collect();

    Ok(HttpResponse::Ok().json(ListWorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspaces: workspace_views,
    }))
}

pub async fn clear(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    repositories::workspaces::clear(&repo)?;
    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    repositories::workspaces::delete(&workspace)?;

    Ok(HttpResponse::Ok().json(WorkspaceResponseView {
        status: StatusMessage::resource_created(),
        workspace: WorkspaceResponse {
            id: workspace_id,
            name: workspace.name,
            commit: workspace.commit.into(),
        },
    }))
}

pub async fn mergeability(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let branch_name = path_param(&req, "branch")?;

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };
    let mergeable = repositories::workspaces::mergeability(&workspace, &branch_name)?;

    let response = MergeableResponse {
        status: StatusMessage::resource_found(),
        mergeable,
    };
    Ok(HttpResponse::Ok().json(response))
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

    let Some(workspace) = repositories::workspaces::get(&repo, &workspace_id)? else {
        return Ok(HttpResponse::NotFound()
            .json(StatusMessageDescription::workspace_not_found(workspace_id)));
    };

    let Some(branch) = repositories::branches::get_by_name(&repo, &branch_name)? else {
        return Ok(HttpResponse::NotFound().json(StatusMessageDescription::not_found(branch_name)));
    };

    match repositories::workspaces::commit(&workspace, &data, &branch_name) {
        Ok(commit) => {
            log::debug!("workspace::commit ✅ success! commit {:?}", commit);
            Ok(HttpResponse::Ok().json(CommitResponse {
                status: StatusMessage::resource_created(),
                commit,
            }))
        }
        Err(OxenError::WorkspaceBehind(workspace)) => {
            Err(OxenHttpError::WorkspaceBehind(Box::new(WorkspaceBranch {
                workspace: *workspace.clone(),
                branch,
            })))
        }
        Err(err) => {
            log::error!("unable to commit branch {:?}. Err: {}", branch_name, err);
            Ok(HttpResponse::UnprocessableEntity().json(StatusMessage::error(format!("{err:?}"))))
        }
    }
}
