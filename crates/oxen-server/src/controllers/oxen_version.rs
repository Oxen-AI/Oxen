use actix_web::{HttpRequest, HttpResponse};
use liboxen::constants::MIN_OXEN_CLIENT_VERSION;
use liboxen::view::StatusMessage;
use liboxen::view::oxen_version::OxenVersionResponse;

/// Check Oxen server status
#[utoipa::path(
    get,
    path = "/api/version",
    tag = "Health",
    description = "Check if the Oxen server is running and responsive.",
    responses(
        (status = 200, description = "Server is running", body = StatusMessage)
    )
)]
pub async fn index(_req: HttpRequest) -> HttpResponse {
    let response = StatusMessage::resource_found();
    HttpResponse::Ok().json(response)
}

pub async fn min_version(_req: HttpRequest) -> HttpResponse {
    let response = OxenVersionResponse {
        status: StatusMessage::resource_found(),
        version: MIN_OXEN_CLIENT_VERSION.to_string(),
    };
    HttpResponse::Ok().json(response)
}
