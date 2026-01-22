use crate::errors::{OxenHttpError, WorkspaceBranch};
use crate::helpers::get_repo;
use crate::params::{app_data, path_param, NameParam};

use liboxen::constants::INITIAL_COMMIT_MSG;
use liboxen::error::OxenError;
use liboxen::model::{NewCommitBody, User};
use liboxen::repositories;
use liboxen::view::merge::MergeableResponse;
use liboxen::view::workspaces::{ListWorkspaceResponseView, NewWorkspace, WorkspaceResponse};
use liboxen::view::{
    CommitResponse, StatusMessage, StatusMessageDescription, WorkspaceResponseView,
};

use actix_web::{web, HttpRequest, HttpResponse};
use utoipa;

pub mod changes;
pub mod data_frames;
pub mod files;

/// Get or create workspace
#[utoipa::path(
    put,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/get_or_create",
    operation_id = "get_or_create_workspace",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = NewWorkspace,
        description = "Workspace creation details, including base branch and optional name/ID.",
        example = json!({
            "branch_name": "main",
            "name": "bessie_workspace",
            "workspace_id": "b3f27f05-0955-4076-805f-39575853b27b"
        })
    ),
    responses(
        (status = 200, description = "Workspace found or created", body = WorkspaceResponseView),
        (status = 400, description = "Invalid payload or branch not found"),
        (status = 404, description = "Repository not found")
    )
)]
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
            log::error!("Unable to parse body. Err: {err}\n{body}");
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    // Try to get the branch, or create it if the repo is empty
    let branch = match repositories::branches::get_by_name(&repo, &data.branch_name)? {
        Some(branch) => branch,
        None => {
            // Branch doesn't exist - check if repo is empty
            if repositories::commits::head_commit_maybe(&repo)?.is_some() {
                // Repo has commits but branch doesn't exist - this is an error
                return Ok(
                    HttpResponse::BadRequest().json(StatusMessage::error(format!(
                        "Branch not found: {}. For non-empty repositories, create the branch first.",
                        data.branch_name
                    ))),
                );
            }

            // Repo is empty - create initial commit with the requested branch
            log::debug!(
                "get_or_create: empty repo, creating initial commit on branch {}",
                data.branch_name
            );
            let user = User {
                name: "Oxen".to_string(),
                email: "oxen@oxen.ai".to_string(),
            };
            repositories::commits::create_initial_commit(
                &repo,
                &data.branch_name,
                &user,
                INITIAL_COMMIT_MSG,
            )?;

            // Now get the newly created branch
            repositories::branches::get_by_name(&repo, &data.branch_name)?
                .ok_or_else(|| {
                    OxenError::basic_str("Failed to create initial branch")
                })?
        }
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
    log::debug!("get_or_create workspace_id {workspace_id:?}");
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

/// Get workspace
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}",
    operation_id = "get_workspace",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "ID of the workspace", example = "b3f27f05-0955-4076-805f-39575853b27b"),
    ),
    responses(
        (status = 200, description = "Workspace found", body = WorkspaceResponseView),
        (status = 404, description = "Workspace not found")
    )
)]
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

/// Create a new workspace
///
/// **DEPRECATED**: Use PUT (get_or_create) instead. This endpoint now delegates to get_or_create
/// for consistent idempotent behavior. The POST method is retained for backward compatibility.
#[deprecated(note = "Use PUT /workspaces (get_or_create) instead")]
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/workspaces",
    operation_id = "create_workspace",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = NewWorkspace,
        description = "Workspace creation details. DEPRECATED: Use PUT method instead for idempotent get-or-create behavior.",
        example = json!({
            "branch_name": "main",
            "name": "bessie_workspace",
            "workspace_id": "b3f27f05-0955-4076-805f-39575853b27b"
        })
    ),
    responses(
        (status = 200, description = "Workspace created or found", body = WorkspaceResponseView),
        (status = 400, description = "Invalid payload or branch not found"),
        (status = 404, description = "Repository not found")
    )
)]
#[allow(deprecated)]
pub async fn create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    // Delegate to get_or_create for consistent behavior
    get_or_create(req, body).await
}

/// Create workspace from new branch
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/new_branch",
    operation_id = "create_workspace_new_branch",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = NewWorkspace,
        description = "Workspace creation details. Creates the branch if it doesn't exist.",
        example = json!({
            "branch_name": "daisy-dev",
            "name": "daisy_workspace",
            "workspace_id": "4a7f05c3-1d0e-4f0e-8f9f-095a43b27b3f"
        })
    ),
    responses(
        (status = 200, description = "Workspace created with new branch", body = WorkspaceResponseView),
        (status = 400, description = "Invalid payload"),
        (status = 404, description = "Repository not found")
    )
)]
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
            log::error!("Unable to parse body. Err: {err}\n{body}");
            return Ok(HttpResponse::BadRequest().json(StatusMessage::error(err.to_string())));
        }
    };

    // If the branch doesn't exist, create it
    let branch =
        if let Some(branch) = repositories::branches::get_by_name(&repo, &data.branch_name)? {
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

/// List all workspaces
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/workspaces",
    operation_id = "list_workspaces",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        NameParam // Query parameter for optional name filtering
    ),
    responses(
        (status = 200, description = "List of workspaces", body = ListWorkspaceResponseView),
        (status = 404, description = "Repository not found")
    )
)]
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
            //       if getting a workspace is slow then we can optimize it
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

/// Clear workspaces for repo
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/clear",
    operation_id = "clear_workspaces",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace for the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "Workspaces cleared", body = StatusMessage),
        (status = 404, description = "Workspace not found")
    )
)]
pub async fn clear(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    repositories::workspaces::clear(&repo)?;
    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}

/// Delete workspace
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}",
    operation_id = "delete_workspace",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "ID of the workspace", example = "b3f27f05-0955-4076-805f-39575853b27b"),
    ),
    responses(
        (status = 200, description = "Workspace deleted", body = WorkspaceResponseView),
        (status = 404, description = "Workspace or Repository not found")
    )
)]
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

/// Check workspace mergeability
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/merge/{branch}",
    operation_id = "check_workspace_mergeability",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "ID of the workspace", example = "b3f27f05-0955-4076-805f-39575853b27b"),
        ("branch" = String, Path, description = "Target branch name to merge into", example = "main"),
    ),
    responses(
        (status = 200, description = "Mergeability status found", body = MergeableResponse),
        (status = 404, description = "Workspace or target branch not found")
    )
)]
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

/// Commit workspace
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/workspaces/{workspace_id}/commit/{branch}",
    operation_id = "commit_workspace",
    tag = "Workspaces",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("workspace_id" = String, Path, description = "ID of the workspace", example = "b3f27f05-0955-4076-805f-39575853b27b"),
        ("branch" = String, Path, description = "Target branch name to commit to", example = "main"),
    ),
    request_body(
        content = NewCommitBody,
        description = "Commit details for the workspace merge.",
        example = json!({
            "author": "bessie",
            "email": "bessie@oxen.ai",
            "message": "Commit changes from bessie_workspace"
        })
    ),
    responses(
        (status = 200, description = "Workspace committed successfully", body = CommitResponse),
        (status = 404, description = "Workspace or branch not found"),
        (status = 422, description = "Unprocessable Entity, e.g., workspace is behind main branch")
    )
)]
pub async fn commit(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let workspace_id = path_param(&req, "workspace_id")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let branch_name = path_param(&req, "branch")?;

    log::debug!(
        "workspace::commit {namespace}/{repo_name} workspace id {workspace_id} to branch {branch_name} got body: {body}"
    );

    let data: Result<NewCommitBody, serde_json::Error> = serde_json::from_str(&body);

    let data = match data {
        Ok(data) => data,
        Err(err) => {
            log::error!("unable to parse commit data. Err: {err}\n{body}");
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

    match repositories::workspaces::commit(&workspace, &data, &branch_name).await {
        Ok(commit) => {
            log::debug!("workspace::commit ✅ success! commit {commit:?}");
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
            log::error!("unable to commit branch {branch_name:?}. Err: {err}");
            Ok(HttpResponse::UnprocessableEntity().json(StatusMessage::error(format!("{err:?}"))))
        }
    }
}
