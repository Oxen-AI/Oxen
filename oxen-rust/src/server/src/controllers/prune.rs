use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{web, HttpRequest, HttpResponse};
use liboxen::repositories;
use liboxen::view::http::{STATUS_ERROR, STATUS_SUCCESS};
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
    pub message: String,
    pub stats: PruneStatsResponse,
}

/// POST /prune
/// Trigger a prune operation on the repository
pub async fn prune(
    req: HttpRequest,
    body: web::Json<PruneRequest>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    // Get the repository
    let repository = get_repo(&app_data.path, &namespace, &repo_name)?;

    let dry_run = body.dry_run;

    log::info!(
        "Prune requested for {}/{} (dry_run: {})",
        namespace,
        repo_name,
        dry_run
    );

    // Run the prune operation
    match repositories::prune::prune(&repository, dry_run).await {
        Ok(stats) => {
            let message = if dry_run {
                "Prune dry-run completed successfully. No files were deleted.".to_string()
            } else {
                "Prune completed successfully.".to_string()
            };

            let response = PruneResponse {
                status: STATUS_SUCCESS.to_string(),
                message,
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
        Err(err) => {
            log::error!("Prune failed: {}", err);
            let response = PruneResponse {
                status: STATUS_ERROR.to_string(),
                message: format!("Prune failed: {}", err),
                stats: PruneStatsResponse {
                    nodes_scanned: 0,
                    nodes_kept: 0,
                    nodes_removed: 0,
                    versions_scanned: 0,
                    versions_kept: 0,
                    versions_removed: 0,
                    bytes_freed: 0,
                },
            };
            Ok(HttpResponse::InternalServerError().json(response))
        }
    }
}
