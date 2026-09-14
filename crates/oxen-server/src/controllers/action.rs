use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};
use actix_web::{HttpRequest, HttpResponse};
use liboxen::repositories;
use liboxen::view::http::STATUS_SUCCESS;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ActionResponse {
    action: String,
    status: String,
    state: String,
}

impl ActionResponse {
    fn new(action: &str, state: &str) -> Self {
        ActionResponse {
            action: action.to_string(),
            status: STATUS_SUCCESS.to_string(),
            state: state.to_string(),
        }
    }
}

pub async fn completed(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let action = path_param(&req, "action")?;
    log::debug!("{action} action completed");
    Ok(HttpResponse::Ok().json(ActionResponse::new(action, "completed")))
}

/// Record that a push finished, and start a recalculation of the repository's size.
pub async fn completed_push(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?.to_string();
    let name = path_param(&req, "repo_name")?.to_string();
    let repository = get_repo(app_data, &namespace, &name)?;

    log::debug!("push action completed");
    if let Err(err) = repositories::size::update_size(&repository) {
        log::error!("Failed to start a size recalculation: {err}");
    }

    Ok(HttpResponse::Ok().json(ActionResponse::new("push", "completed")))
}

pub async fn started(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let action = path_param(&req, "action")?;
    Ok(HttpResponse::Ok().json(ActionResponse::new(action, "started")))
}
