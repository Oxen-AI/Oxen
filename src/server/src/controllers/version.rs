use crate::params::app_data;
use actix_web::{HttpRequest, HttpResponse};
use liboxen::constants::MIN_OXEN_VERSION;
use liboxen::repositories;
use liboxen::view::version::VersionResponse;
use liboxen::view::StatusMessage;
use serde::Serialize;

pub async fn index(_req: HttpRequest) -> HttpResponse {
    let response = StatusMessage::resource_found();
    HttpResponse::Ok().json(response)
}

pub async fn min_version(_req: HttpRequest) -> HttpResponse {
    let response = VersionResponse {
        status: StatusMessage::resource_found(),
        version: MIN_OXEN_VERSION.to_string(),
    };
    HttpResponse::Ok().json(response)
}

#[derive(Serialize, Debug)]
struct ResolveResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub repository_api_url: String,
}

pub async fn resolve(req: HttpRequest) -> HttpResponse {
    let app_data = app_data(&req).unwrap();

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    if let (Some(name), Some(namespace)) = (name, namespace) {
        match repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(_)) => match req.url_for("repo_root", [namespace, name]) {
                Ok(url) => {
                    log::debug!("resolved repo URL: {}", url);
                    HttpResponse::Ok().json(ResolveResponse {
                        status: StatusMessage::resource_found(),
                        repository_api_url: url.to_string(),
                    })
                }
                Err(err) => {
                    log::debug!("Error generating repo URL: {:?}", err);
                    HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                }
            },
            Ok(None) => {
                log::debug!("404 Could not find repo: {}", name);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::debug!("Err finding repo: {} => {:?}", name, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Could not find `name` or `namespace` param...";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}
