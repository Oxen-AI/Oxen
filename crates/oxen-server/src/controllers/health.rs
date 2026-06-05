use crate::errors::OxenHttpError;
use crate::params::app_data;
use actix_web::{HttpRequest, HttpResponse};
use liboxen::util;
use liboxen::view::{HealthResponse, StatusMessage};

pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    match util::fs::disk_usage_for_path(&app_data.path) {
        Ok(disk_usage) => {
            let response = HealthResponse {
                status: StatusMessage::resource_found(),
                disk_usage,
            };
            Ok(HttpResponse::Ok().json(response))
        }
        Err(err) => {
            log::error!("Error getting disk usage: {err:?}");
            Err(OxenHttpError::InternalServerError)
        }
    }
}
