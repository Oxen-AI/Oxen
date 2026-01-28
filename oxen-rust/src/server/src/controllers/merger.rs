use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_base_head, path_param, resolve_base_head_branches};

use actix_web::{HttpRequest, HttpResponse};

use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::view::merge::{
    MergeConflictFile, MergeResult, MergeSuccessResponse, Mergeable, MergeableResponse,
};
use liboxen::view::StatusMessage;

/// Check if branches are mergeable
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/merge/{base_head}",
    tag = "Merge",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "satellite-images"),
        ("base_head" = String, Path, description = "The base and head revisions separated by '..'", example = "main..feature/add-labels"),
    ),
    responses(
        (status = 200, description = "Merge status returned successfully", body = MergeableResponse),
        (status = 404, description = "Repository or one of the revisions not found")
    )
)]
pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head_branches(&repository, &base, &head)?;
    let base = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;

    // Check if mergeable
    let conflicts =
        repositories::merge::list_conflicts_between_branches(&repository, &base, &head).await?;
    let conflicts: Vec<MergeConflictFile> = conflicts
        .into_iter()
        .map(|path| MergeConflictFile {
            path: path.to_string_lossy().to_string(),
        })
        .collect();
    let is_mergeable = conflicts.is_empty();

    // Get commits
    let commits = repositories::merge::list_commits_between_branches(&repository, &base, &head)?;

    // Create response object
    let response = MergeableResponse {
        status: StatusMessage::resource_found(),
        mergeable: Mergeable {
            is_mergeable,
            conflicts,
            commits,
        },
    };

    Ok(HttpResponse::Ok().json(response))
}

/// Merge branches
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/merge/{base_head}",
    tag = "Merge",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "satellite-images"),
        ("base_head" = String, Path, description = "The base and head revisions separated by '..'", example = "main..feature/add-labels"),
    ),
    responses(
        (status = 200, description = "Branches merged successfully", body = MergeSuccessResponse),
        (status = 409, description = "Merge conflict", body = StatusMessage),
        (status = 404, description = "Repository or one of the revisions not found"),
    )
)]
pub async fn merge(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repo = get_repo(&app_data.path, namespace, name)?;

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (maybe_base_branch, maybe_head_branch) = resolve_base_head_branches(&repo, &base, &head)?;
    let base_branch =
        maybe_base_branch.ok_or(OxenError::revision_not_found(base.clone().into()))?;
    let head_branch =
        maybe_head_branch.ok_or(OxenError::revision_not_found(head.clone().into()))?;

    // .unwrap() safe because branches must have commits
    let base_commit = repositories::commits::get_by_id(&repo, &base_branch.commit_id)?.unwrap();
    let head_commit = repositories::commits::get_by_id(&repo, &head_branch.commit_id)?.unwrap();

    // Check if mergeable
    match repositories::merge::merge_into_base(&repo, &head_branch, &base_branch).await {
        Ok(Some(merge_commit)) => {
            // If the merge was successful, update the branch
            repositories::branches::update(&repo, &base_branch.name, &merge_commit.id)?;

            let response = MergeSuccessResponse {
                status: StatusMessage::resource_found(),
                commits: MergeResult {
                    base: base_commit,
                    head: head_commit,
                    merge: merge_commit,
                },
            };

            Ok(HttpResponse::Ok().json(response))
        }
        Ok(None) => {
            log::debug!("Merge has conflicts");
            Err(OxenError::merge_conflict(format!(
                "Unable to merge {head} into {base} due to conflicts"
            )))?
        }
        Err(err) => {
            log::debug!("Err merging branches {err:?}");
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}
