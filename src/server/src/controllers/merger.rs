use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_base_head, path_param, resolve_base_head_branches};

use actix_web::{HttpRequest, HttpResponse};

use liboxen::core::index::{CommitReader, Merger};
use liboxen::error::OxenError;
use liboxen::view::merge::{MergeConflictFile, MergeSuccessResponse, Mergeable, MergeableResponse};
use liboxen::view::StatusMessage;

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
    let merger = Merger::new(&repository)?;
    let is_mergeable = !merger.has_conflicts(&base, &head)?;

    // Get merge conflicts
    let commit_reader = CommitReader::new(&repository)?;
    let conflicts = merger
        .list_conflicts_between_branches(&commit_reader, &base, &head)?
        .iter()
        .map(|p| MergeConflictFile {
            path: p.to_string_lossy().to_string(),
        })
        .collect();

    // Get commits
    let commits = merger.list_commits_between_branches(&commit_reader, &base, &head)?;

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

pub async fn merge(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
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
    let merger = Merger::new(&repository)?;
    match merger.merge_into_base(&head, &base) {
        Ok(Some(_merge_commit)) => {
            let response = MergeSuccessResponse {
                status: StatusMessage::resource_found(),
                base_commit: base.commit_id,
                head_commit: head.commit_id,
            };

            Ok(HttpResponse::Ok().json(response))
        }
        Ok(None) => {
            log::debug!("Merge has conflicts");
            Ok(HttpResponse::BadRequest().json(StatusMessage::bad_request()))
        }
        Err(err) => {
            log::debug!("Err merging branches {:?}", err);
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}
