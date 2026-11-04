use std::path::PathBuf;

use crate::errors::OxenHttpError;
use crate::helpers::{get_repo, get_repo_async};
use crate::params::{PageNumQuery, app_data, path_param};
use crate::tasks;

use actix_web::{HttpRequest, HttpResponse, web};

use liboxen::core::repo_locks;
use liboxen::error::OxenError;
use liboxen::model::metadata::generic_metadata::GenericMetadata;
use liboxen::model::{EntryDataType, LocalRepository};
use liboxen::util::paginate;
use liboxen::view::entries::ResourceVersion;
use liboxen::view::{
    BranchNewFromBranchName, BranchNewFromCommitId, BranchRemoteMerge, BranchResponse,
    BranchUpdate, CommitEntryVersion, CommitResponse, ListBranchesResponse, PaginatedEntryVersions,
    PaginatedEntryVersionsResponse, StatusMessage,
};
use liboxen::{constants, repositories};

/// List all branches
#[tracing::instrument(skip_all)]
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches",
    tag = "Branches",
    description = "List all branches in the repository with their current commit information.",
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
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(app_data, namespace, name)?;

    let branches = repositories::branches::list(&repo).await?;

    let view = ListBranchesResponse {
        status: StatusMessage::resource_found(),
        branches,
    };
    Ok(HttpResponse::Ok().json(view))
}

/// Get an existing branch
#[tracing::instrument(skip_all)]
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}",
    tag = "Branches",
    description = "Get a branch by name, returning its details and current commit.",
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
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let branch_name = path_param(&req, "branch_name")?.to_string();
    let repository = get_repo(app_data, namespace, name)?;

    log::debug!("show branch {branch_name:?}");
    let branch = repositories::branches::get_by_name(&repository, &branch_name)?;
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
    tag = "Branches",
    description = "Create a new branch from another branch name or commit ID. Returns existing branch if name already exists.",
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
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();

    let repo = get_repo(app_data, namespace, repo_name)?;

    // Keep the write in flight until it finishes so a maintenance op (migration/prune) drains
    // against it; returns 429 while an exclusive op holds the repo.
    let _write = repo_locks::begin_write(&repo)?;

    log::debug!("Create branch: {body}");

    // Try to deserialize the body into a BranchNewFromBranchName
    let data: Result<BranchNewFromBranchName, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        log::debug!("Create from branch!");
        return create_from_branch(&repo, &data).await;
    }

    // Try to deserialize the body into a BranchNewFromCommitId
    let data: Result<BranchNewFromCommitId, serde_json::Error> = serde_json::from_str(&body);
    if let Ok(data) = data {
        log::debug!("Create from commit!");
        return create_from_commit(&repo, &data).await;
    }

    Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid request body")))
}

async fn create_from_branch(
    repo: &LocalRepository,
    data: &BranchNewFromBranchName,
) -> Result<HttpResponse, OxenHttpError> {
    match repositories::branches::get_by_name(repo, &data.new_name) {
        Ok(branch) => {
            let view = BranchResponse {
                status: StatusMessage::resource_found(),
                branch,
            };
            return Ok(HttpResponse::Ok().json(view));
        }
        Err(OxenError::BranchNotFound(_)) => {} // branch doesn't exist yet, continue
        Err(e) => return Err(e.into()),
    }

    let from_branch = repositories::branches::get_by_name(repo, &data.from_name)
        .map_err(|_| OxenHttpError::NotFound)?;

    // Verify the source branch's head before pointing a new ref at it, scoping the check to what it
    // adds over the default branch.
    repositories::branches::verify_advance_to(
        repo,
        &from_branch.commit_id,
        constants::DEFAULT_BRANCH_NAME,
    )
    .await?;

    let new_branch = repositories::branches::create(repo, &data.new_name, from_branch.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_created(),
        branch: new_branch,
    }))
}

async fn create_from_commit(
    repo: &LocalRepository,
    data: &BranchNewFromCommitId,
) -> Result<HttpResponse, OxenHttpError> {
    // Verify the new branch's commit before pointing a ref at it, scoping the check to what it
    // adds over the default branch.
    repositories::branches::verify_advance_to(
        repo,
        &data.commit_id,
        constants::DEFAULT_BRANCH_NAME,
    )
    .await?;

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
    tag = "Branches",
    description = "Force delete a branch by name.",
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
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let branch_name = path_param(&req, "branch_name")?.to_string();
    let repository = get_repo(app_data, namespace, name)?;
    let _write = repo_locks::begin_write(&repository)?;

    let branch = repositories::branches::get_by_name(&repository, &branch_name)?;

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
    tag = "Branches",
    description = "Update a branch to point to a different commit ID.",
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
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let branch_name = path_param(&req, "branch_name")?.to_string();
    let repository = get_repo(app_data, namespace, name)?;
    let _write = repo_locks::begin_write(&repository)?;

    let data: Result<BranchUpdate, serde_json::Error> = serde_json::from_str(&body);
    let data = data.map_err(|err| OxenHttpError::BadRequest(format!("{err:?}").into()))?;

    // Don't advance the ref onto a commit whose newly-added objects aren't all present. Scope the
    // check to what changed since the branch's current head.
    repositories::branches::verify_advance_to(&repository, &data.commit_id, &branch_name).await?;

    let branch = repositories::branches::update(&repository, &branch_name, data.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_updated(),
        branch,
    }))
}

/// Merge a commit into a branch
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/merge",
    tag = "Branches",
    description = "Merge a client commit into a branch during push. Returns merge commit on success, or original server commit if conflicts occur.",
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
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, namespace, name)?;
    let _write = repo_locks::begin_write(&repository)?;
    let branch_name = path_param(&req, "branch_name")?.to_string();
    let branch = repositories::branches::get_by_name(&repository, &branch_name)?;

    let data: Result<BranchRemoteMerge, serde_json::Error> = serde_json::from_str(&body);
    let data = data.map_err(|err| OxenHttpError::BadRequest(format!("{err:?}").into()))?;
    let incoming_commit_id = data.client_commit_id;
    let incoming_commit = repositories::commits::get_by_id(&repository, &incoming_commit_id)?
        .ok_or_else(|| OxenError::resource_not_found(&incoming_commit_id))?;

    let current_commit_id = data.server_commit_id;
    let current_commit = repositories::commits::get_by_id(&repository, &current_commit_id)?
        .ok_or_else(|| OxenError::resource_not_found(&current_commit_id))?;

    log::debug!("maybe_create_merge got client head commit {incoming_commit_id:?}");

    // Validate the incoming commit's added objects before merging it in, so the merge can't build
    // a head missing reachable objects. Scope the check to what the client added over the
    // current server head.
    repositories::branches::verify_reachable_objects(
        &repository,
        Some(&current_commit),
        &incoming_commit,
    )
    .await?;

    let merge_commit = match repositories::merge::merge_commit_into_base_on_branch(
        &repository,
        &incoming_commit,
        &current_commit,
        &branch,
    )
    .await
    {
        Ok(commit) => commit,
        Err(OxenError::UpstreamMergeConflict(_)) => {
            // If there are merge conflicts, we can't complete this merge and want to reset
            // the branch to the previous remote head as if this push never happened
            log::debug!("returning current commit {current_commit_id:?}.");
            return Ok(HttpResponse::Ok().json(CommitResponse {
                status: StatusMessage::resource_found(),
                commit: current_commit,
            }));
        }
        Err(e) => return Err(e.into()),
    };

    log::debug!("returning merge commit {merge_commit:?}");
    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit: merge_commit,
    }))
}

/// Get all versions of a file on a branch
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/branches/{branch_name}/versions/{path}",
    tag = "Branches",
    description = "List paginated historical versions of a file across commits on a branch, including schema hash for tabular files.",
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
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let branch_name = path_param(&req, "branch_name")?.to_string();

    let repo = get_repo_async(app_data, &namespace, &repo_name).await?;

    let path = PathBuf::from(path_param(&req, "path")?);

    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // The branch lookup, the history walk, and each version's node are sync tree reads.
    let response = tasks::spawn_blocking(
        move || -> Result<PaginatedEntryVersionsResponse, OxenError> {
            let branch = repositories::branches::get_by_name(&repo, &branch_name)?;
            let commits_with_versions =
                repositories::branches::list_entry_versions_on_branch(&repo, &branch.name, &path)?;
            log::debug!(
                "list_entry_versions_on_branch found {:?} versions",
                commits_with_versions.len()
            );

            let (page_versions, pagination) = paginate(commits_with_versions, page, page_size);
            let mut paginated_commit_versions = Vec::with_capacity(page_versions.len());
            for (commit, _entry) in page_versions {
                // The schema hash of a data frame's version, from the type its commit recorded.
                // Use the original path, not the entry path, to get the full path
                let node = repositories::tree::get_file_by_path(&repo, &commit, &path)?;
                let maybe_schema_hash = match node
                    .as_ref()
                    .map(|node| (node.data_type(), node.metadata()))
                {
                    Some((_, Some(GenericMetadata::MetadataTabular(metadata)))) => {
                        Some(metadata.tabular.schema.hash)
                    }
                    Some((EntryDataType::Tabular, _)) => {
                        tracing::error!(
                            oxen.file_path = %path.display(),
                            oxen.commit_id = %commit.id,
                            "A committed data frame version has no tabular metadata"
                        );
                        None
                    }
                    _ => None,
                };

                paginated_commit_versions.push(CommitEntryVersion {
                    resource: ResourceVersion {
                        version: commit.id.clone(),
                        path: path.to_string_lossy().into(),
                    },
                    commit,
                    schema_hash: maybe_schema_hash,
                });
            }
            Ok(PaginatedEntryVersionsResponse {
                status: StatusMessage::resource_found(),
                versions: PaginatedEntryVersions {
                    versions: paginated_commit_versions,
                    pagination,
                },
                branch,
                path,
            })
        },
    )
    .await
    .map_err(OxenError::from)??;

    Ok(HttpResponse::Ok().json(response))
}
