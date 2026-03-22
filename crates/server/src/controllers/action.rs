use crate::{errors::OxenHttpError, params::path_param};
use actix_web::{HttpRequest, HttpResponse};
use liboxen::view::http::STATUS_SUCCESS;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ActionResponse {
    action: String,
    status: String,
    state: String,
}

pub async fn completed(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let action = path_param(&req, "action")?;
    log::debug!("{action} action completed");
    let resp = ActionResponse {
        action: action.to_string(),
        state: "completed".to_string(),
        status: STATUS_SUCCESS.to_string(),
    };
    Ok(HttpResponse::Ok().json(resp))
}

pub async fn started(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let action = path_param(&req, "action")?;
    let resp = ActionResponse {
        action: action.to_string(),
        status: STATUS_SUCCESS.to_string(),
        state: "started".to_string(),
    };
    Ok(HttpResponse::Ok().json(resp))
}
