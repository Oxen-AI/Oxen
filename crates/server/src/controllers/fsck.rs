use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{HttpRequest, HttpResponse, web};
use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::repositories::fsck::{RebuildDirHashesStats, rebuild_dir_hash_db};
use liboxen::view::versions::CleanCorruptedVersionsResult;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
pub struct RebuildDirHashesQuery {
    /// Commit id to rebuild. Takes precedence over `branch` if both are set.
    pub commit_id: Option<String>,
    /// Branch name whose tip commit should be rebuilt.
    pub branch: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct RebuildDirHashesResponse {
    pub status_message: String,
    pub stats: RebuildDirHashesStats,
}

/// POST /fsck/rebuild-dir-hashes
///
/// Rebuilds the target commit's `dir_hash_db` from its merkle tree. Use when path-based
/// directory lookups return "Resource not found" for directories that are still present in the
/// tree.
///
/// Commit selection (first match wins):
///   1. `?commit_id=<id>`
///   2. `?branch=<name>` — rebuild the tip of this branch
///   3. default — rebuild the repository's HEAD commit
pub async fn rebuild_dir_hashes(
    req: HttpRequest,
    query: web::Query<RebuildDirHashesQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let commit = if let Some(commit_id) = &query.commit_id {
        repositories::commits::get_by_id(&repo, commit_id)?
            .ok_or_else(|| OxenError::RevisionNotFound(commit_id.clone().into()))?
    } else if let Some(branch_name) = &query.branch {
        let branch = repositories::branches::get_by_name(&repo, branch_name)?;
        repositories::commits::get_by_id(&repo, &branch.commit_id)?
            .ok_or_else(|| OxenError::RevisionNotFound(branch.commit_id.clone().into()))?
    } else {
        repositories::commits::head_commit(&repo)?
    };

    log::info!(
        "rebuild_dir_hashes: {namespace}/{repo_name} commit={}",
        commit.id
    );

    // The rebuild is synchronous CPU + IO heavy work; run it off the actix worker thread so we
    // don't starve concurrent requests. `JoinError` converts into `OxenError` via `#[from]`;
    // `OxenError` then converts into `OxenHttpError`.
    let stats = tokio::task::spawn_blocking(move || rebuild_dir_hash_db(&repo, &commit))
        .await
        .map_err(OxenError::from)??;

    let response = RebuildDirHashesResponse {
        status_message: format!(
            "Rebuilt dir_hash_db for commit {} ({} directories)",
            stats.commit_id, stats.dirs_written
        ),
        stats,
    };
    Ok(HttpResponse::Ok().json(response))
}

#[derive(Deserialize, Debug)]
pub struct CleanQuery {
    /// When true, scan and report only; do not remove any files. Default: false (apply).
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Debug)]
pub struct CleanResponse {
    pub status_message: String,
    pub result: CleanCorruptedVersionsResult,
}

/// POST /fsck/clean
///
/// Scans the version store for corrupted files (i.e. files whose contents no longer hash to
/// their filename) and removes them. Pass `?dry_run=true` to report without removing.
pub async fn clean(
    req: HttpRequest,
    query: web::Query<CleanQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;

    let dry_run = query.dry_run;
    log::info!("fsck clean: {namespace}/{repo_name} dry_run={dry_run}");

    let version_store = repo.version_store()?;
    let result = version_store.clean_corrupted_versions(dry_run).await?;

    let status_message = if dry_run {
        format!(
            "Scanned {} version files; {} corrupted (dry run, nothing removed)",
            result.scanned, result.corrupted
        )
    } else {
        format!(
            "Scanned {} version files; {} corrupted, {} removed",
            result.scanned, result.corrupted, result.cleaned
        )
    };

    Ok(HttpResponse::Ok().json(CleanResponse {
        status_message,
        result,
    }))
}
