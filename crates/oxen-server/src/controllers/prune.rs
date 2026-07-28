use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{HttpRequest, HttpResponse, web};
use liboxen::core::repo_locks;
use liboxen::repositories;
use liboxen::view::http::STATUS_SUCCESS;
use liboxen::view::oxen_response::ErrorResponse;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct PruneRequest {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PruneStatsResponse {
    pub nodes_scanned: usize,
    pub nodes_kept: usize,
    pub nodes_removed: usize,
    pub versions_scanned: usize,
    pub versions_kept: usize,
    pub versions_removed: usize,
    pub bytes_freed: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PruneResponse {
    pub status: String,
    pub status_message: String,
    pub status_description: Option<String>,
    pub error: Option<ErrorResponse>,
    pub stats: PruneStatsResponse,
}

/// POST /prune
/// Trigger a prune operation on the repository
pub async fn prune(
    req: HttpRequest,
    body: web::Json<PruneRequest>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let repo_name = path_param(&req, "repo_name")?.to_string();

    // Get the repository
    let repository = get_repo(app_data, &namespace, &repo_name)?;

    let dry_run = body.dry_run;

    log::info!("Prune requested for {namespace}/{repo_name} (dry_run: {dry_run})");

    // Prune deletes nodes and version files; hold the whole-repo exclusive lock so no write lands
    // mid-prune. A dry run only reports what would be removed, so it needs no lock.
    let stats = if dry_run {
        repositories::prune::prune(&repository, true).await?
    } else {
        repo_locks::with_repo_exclusive(&repository, repositories::prune::prune(&repository, false))
            .await?
    };

    let status_message = if dry_run {
        "Prune dry-run completed successfully. No files were deleted.".to_string()
    } else {
        "Prune completed successfully.".to_string()
    };

    let response = PruneResponse {
        status: STATUS_SUCCESS.to_string(),
        status_message,
        status_description: None,
        error: None,
        stats: PruneStatsResponse {
            nodes_scanned: stats.nodes_scanned,
            nodes_kept: stats.nodes_kept,
            nodes_removed: stats.nodes_removed,
            versions_scanned: stats.versions_scanned,
            versions_kept: stats.versions_kept,
            versions_removed: stats.versions_removed,
            bytes_freed: stats.bytes_freed,
        },
    };

    Ok(HttpResponse::Ok().json(response))
}
