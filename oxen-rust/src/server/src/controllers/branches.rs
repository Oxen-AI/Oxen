use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param, PageNumQuery};

use actix_web::{web, HttpRequest, HttpResponse};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util::{self, paginate};
use liboxen::view::entries::ResourceVersion;
use liboxen::view::{
    BranchLockResponse, BranchNewFromBranchName, BranchNewFromCommitId, BranchRemoteMerge,
    BranchResponse, BranchUpdate, CommitEntryVersion, CommitResponse, ListBranchesResponse,
    PaginatedEntryVersions, PaginatedEntryVersionsResponse, StatusMessage,
};
use liboxen::{constants, repositories};

/// List all branches
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches",
    operation_id = "list_branches",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (
            status = 200,
            description = "List of branches", 
            body = ListBranchesResponse,
            example = json!({
                "branches": [
                    {
                        "commit": {
                            "author": "Bessie Oxington",
                            "email": "hello@oxen.ai",
                            "id": "592d564750031fa1431000472c2d721d",
                            "message": "update README",
                            "timestamp": "2024-11-25T21:11:12Z"
                        },
                        "commit_id": "592d564750031fa1431000472c2d721d",
                        "name": "main"
                    },
                    {
                        "commit": {
                            "author": "Daisy Oxington",
                            "email": "daisy@oxen.ai",
                            "id": "abc1234567890def1234567890fedcba",
                            "message": "added new validation data",
                            "timestamp": "2024-11-25T20:00:00Z"
                        },
                        "commit_id": "abc1234567890def1234567890fedcba",
                        "name": "development"
                    }
                ],
                "oxen_version": "0.22.2",
                "status": "success",
                "status_message": "resource_found"
            })
        ),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let branches = repositories::branches::list(&repo)?;

    let view = ListBranchesResponse {
        status: StatusMessage::resource_found(),
        branches,
    };
    Ok(HttpResponse::Ok().json(view))
}

/// Get an existing branch
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}",
    operation_id = "get_branch",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch", example = "main"),
    ),
    responses(
        (status = 200, description = "Branch found", body = BranchResponse),
        (status = 404, description = "Branch not found")
    )
)]
pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    log::debug!("show branch {branch_name:?}");
    let branch = repositories::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;
    log::debug!("show branch found {branch:?}");

    let view = BranchResponse {
        status: StatusMessage::resource_found(),
        branch,
    };

    Ok(HttpResponse::Ok().json(view))
}

/// Create a new branch
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/branches",
    operation_id = "create_branch",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = BranchNewFromBranchName,
        description = "Branch creation details. Can be created from another branch or a commit ID.",
        example = json!({
            "new_name": "development",
            "from_name": "main"
        })
    ),
    responses(
        (status = 200, description = "Branch created", body = BranchResponse),
        (status = 400, description = "Invalid request body"),
        (status = 404, description = "Repository or source branch/commit not found")
    )
)]
pub async fn create(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    log::debug!("Create branch: {body}");

    // Try to deserialize the body into a BranchNewFromBranchName
    let data: Result<BranchNewFromBranchName, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        log::debug!("Create from branch!");
        return create_from_branch(&repo, &data);
    }

    // Try to deserialize the body into a BranchNewFromCommitId
    let data: Result<BranchNewFromCommitId, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        log::debug!("Create from commit!");
        return create_from_commit(&repo, &data);
    }

    Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid request body")))
}

fn create_from_branch(
    repo: &LocalRepository,
    data: &BranchNewFromBranchName,
) -> Result<HttpResponse, OxenHttpError> {
    let maybe_new_branch: Option<liboxen::model::Branch> =
        repositories::branches::get_by_name(repo, &data.new_name)?;
    if let Some(branch) = maybe_new_branch {
        let view = BranchResponse {
            status: StatusMessage::resource_found(),
            branch,
        };
        return Ok(HttpResponse::Ok().json(view));
    }

    let from_branch = repositories::branches::get_by_name(repo, &data.from_name)?
        .ok_or(OxenHttpError::NotFound)?;

    let new_branch = repositories::branches::create(repo, &data.new_name, from_branch.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_created(),
        branch: new_branch,
    }))
}

fn create_from_commit(
    repo: &LocalRepository,
    data: &BranchNewFromCommitId,
) -> Result<HttpResponse, OxenHttpError> {
    let new_branch = repositories::branches::create(repo, &data.new_name, &data.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_created(),
        branch: new_branch,
    }))
}

/// Delete a branch
#[utoipa::path(
    delete,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}",
    operation_id = "delete_branch",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch to delete", example = "development"),
    ),
    responses(
        (status = 200, description = "Branch deleted", body = BranchResponse),
        (status = 404, description = "Branch not found")
    )
)]
pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let branch = repositories::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    repositories::branches::force_delete(&repository, &branch.name)?;
    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_deleted(),
        branch,
    }))
}

/// Update a branch to a new commit
#[utoipa::path(
    put,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}",
    operation_id = "update_branch",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch to update", example = "main"),
    ),
    request_body(
        content = BranchUpdate,
        description = "The commit ID to update the branch head to.",
        example = json!({
            "commit_id": "84c76a5b2e9a2637f9091991475c404d"
        })
    ),
    responses(
        (status = 200, description = "Branch updated", body = BranchResponse),
        (status = 400, description = "Bad Request"),
        (status = 404, description = "Branch or Commit not found")
    )
)]
pub async fn update(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let data: Result<BranchUpdate, serde_json::Error> = serde_json::from_str(&body);
    let data = data.map_err(|err| OxenHttpError::BadRequest(format!("{err:?}").into()))?;

    let branch = repositories::branches::update(&repository, branch_name, data.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_updated(),
        branch,
    }))
}

/// Merge a commit into a branch
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/merge",
    operation_id = "merge_branch",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch to merge into (the target branch)", example = "main"),
    ),
    request_body(
        content = BranchRemoteMerge,
        description = "Client and Server commit IDs for performing the merge.",
        example = json!({
            "client_commit_id": "abc1234567890def1234567890fedcba",
            "server_commit_id": "84c76a5b2e9a2637f9091991475c404d"
        })
    ),
    responses(
        (status = 200, description = "Merge successful or merge conflicts encountered. Returns the new head commit.", body = CommitResponse),
        (status = 400, description = "Bad Request (e.g., malformed body)"),
        (status = 404, description = "Branch or Commit not found")
    )
)]
pub async fn maybe_create_merge(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let branch_name = path_param(&req, "branch_name")?;
    let branch = repositories::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    let data: Result<BranchRemoteMerge, serde_json::Error> = serde_json::from_str(&body);
    let data = data.map_err(|err| OxenHttpError::BadRequest(format!("{err:?}").into()))?;
    let incoming_commit_id = data.client_commit_id;
    let incoming_commit = repositories::commits::get_by_id(&repository, &incoming_commit_id)?
        .ok_or(OxenError::resource_not_found(&incoming_commit_id))?;

    let current_commit_id = data.server_commit_id;
    let current_commit = repositories::commits::get_by_id(&repository, &current_commit_id)?
        .ok_or(OxenError::resource_not_found(&current_commit_id))?;

    log::debug!("maybe_create_merge got client head commit {incoming_commit_id:?}");

    let maybe_merge_commit = repositories::merge::merge_commit_into_base_on_branch(
        &repository,
        &incoming_commit,
        &current_commit,
        &branch,
    )
    .await?;

    // Return what will become the new head of the repo after push is complete.
    if let Some(merge_commit) = maybe_merge_commit {
        log::debug!("returning merge commit {merge_commit:?}");
        // Update branch head
        Ok(HttpResponse::Ok().json(CommitResponse {
            status: StatusMessage::resource_created(),
            commit: merge_commit,
        }))
    } else {
        // If there are merge conflicts, we can't complete this merge and want to reset the branch to the previous remote head
        // as if this push never happened
        log::debug!("returning current commit {current_commit_id:?}.");
        Ok(HttpResponse::Ok().json(CommitResponse {
            status: StatusMessage::resource_found(),
            commit: current_commit,
        }))
    }
}

/// Get Latest Commit for Branch
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/latest_commit",
    operation_id = "get_latest_commit",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch", example = "main"),
    ),
    responses(
        (status = 200, description = "Latest synced commit found", body = CommitResponse),
        (status = 404, description = "Branch not found")
    )
)]
pub async fn latest_synced_commit(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let commit = repositories::branches::latest_synced_commit(&repository, &branch_name)?;

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_found(),
        commit,
    }))
}

/// Lock a branch
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/lock",
    operation_id = "lock_branch",
    description = "Locks a branch, preventing writes until it is unlocked",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch to lock", example = "main"),
    ),
    responses(
        (status = 200, description = "Branch locked successfully", body = BranchLockResponse),
        (status = 409, description = "Failed to lock branch (already locked or unavailable)", body = BranchLockResponse),
    )
)]
pub async fn lock(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    match repositories::branches::lock(&repository, &branch_name) {
        Ok(_) => Ok(HttpResponse::Ok().json(BranchLockResponse {
            status: StatusMessage::resource_updated(),
            branch_name: branch_name.clone(),
            is_locked: true,
        })),
        Err(e) => {
            // Log the error for debugging
            log::error!("Failed to lock branch: {e}");

            Ok(HttpResponse::Conflict().json(BranchLockResponse {
                status: StatusMessage::error(e.to_string()),
                branch_name: branch_name.clone(),
                is_locked: false,
            }))
        }
    }
}

/// Unlock a branch
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/unlock",
    operation_id = "unlock_branch",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch to unlock", example = "main"),
    ),
    responses(
        (status = 200, description = "Branch unlocked", body = BranchLockResponse),
    )
)]
pub async fn unlock(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    repositories::branches::unlock(&repository, &branch_name)?;

    Ok(HttpResponse::Ok().json(BranchLockResponse {
        status: StatusMessage::resource_updated(),
        branch_name,
        is_locked: false,
    }))
}

/// Check if a branch is locked
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/lock",
    operation_id = "is_branch_locked",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch to check", example = "main"),
    ),
    responses(
        (status = 200, description = "Branch lock status returned", body = BranchLockResponse),
    )
)]
pub async fn is_locked(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let is_locked = repositories::branches::is_locked(&repository, &branch_name)?;

    Ok(HttpResponse::Ok().json(BranchLockResponse {
        status: StatusMessage::resource_found(),
        branch_name,
        is_locked,
    }))
}

/// Get all versions of a file on a branch
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/versions/{path}",
    operation_id = "list_file_versions",
    tag = "Branches",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("branch_name" = String, Path, description = "Name of the branch", example = "main"),
        ("path" = String, Path, description = "Path to the file or dir", example = "images/train.jpg"),
        PageNumQuery
    ),
    responses(
        (status = 200, description = "List of entry versions found", body = PaginatedEntryVersionsResponse),
        (status = 404, description = "Repository, branch or path not found")
    )
)]
pub async fn list_entry_versions(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;

    // Get branch
    let repo = get_repo(&app_data.path, namespace.clone(), &repo_name)?;
    let branch = repositories::branches::get_by_name(&repo, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    let path = PathBuf::from(path_param(&req, "path")?);
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    let commits_with_versions =
        repositories::branches::list_entry_versions_on_branch(&repo, &branch.name, &path)?;
    log::debug!(
        "list_entry_versions_on_branch found {:?} versions",
        commits_with_versions.len()
    );

    let mut commit_versions: Vec<CommitEntryVersion> = Vec::new();

    for (commit, _entry) in commits_with_versions {
        // For each version, get the schema hash if one exists.
        // Use the original path, not the entry path, to get the full path
        let maybe_schema_hash = if util::fs::is_tabular(&path) {
            let maybe_schema =
                repositories::data_frames::schemas::get_by_path(&repo, &commit, &path)?;
            match maybe_schema {
                Some(schema) => Some(schema.hash),
                None => {
                    log::error!("Could not get schema for tabular file {:?}", &path);
                    None
                }
            }
        } else {
            None
        };

        commit_versions.push(CommitEntryVersion {
            commit: commit.clone(),
            resource: ResourceVersion {
                version: commit.id.clone(),
                path: path.to_string_lossy().into(),
            },
            schema_hash: maybe_schema_hash,
        });
    }

    let (paginated_commit_versions, pagination) = paginate(commit_versions, page, page_size);

    let response = PaginatedEntryVersionsResponse {
        status: StatusMessage::resource_found(),
        versions: PaginatedEntryVersions {
            versions: paginated_commit_versions,
            pagination,
        },
        branch,
        path,
    };

    Ok(HttpResponse::Ok().json(response))
}
