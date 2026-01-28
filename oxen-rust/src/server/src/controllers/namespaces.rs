use crate::errors::OxenHttpError;
use crate::params::app_data;

use liboxen::namespaces;
use liboxen::view::{ListNamespacesResponse, NamespaceResponse, NamespaceView, StatusMessage};

use actix_web::{HttpRequest, HttpResponse, Result};
use utoipa;

/// List namespaces
#[utoipa::path(
    get,
    path = "/api/namespaces",
    tag = "Namespaces",
    responses(
        (status = 200, description = "List of namespaces", body = ListNamespacesResponse),
    )
)]
pub async fn index(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespaces: Vec<NamespaceView> = namespaces::list(&app_data.path)
        .into_iter()
        .map(|namespace| NamespaceView { namespace })
        .collect();

    let view = ListNamespacesResponse {
        status: StatusMessage::resource_found(),
        namespaces,
    };

    Ok(HttpResponse::Ok().json(view))
}

/// Get namespace
#[utoipa::path(
    get,
    path = "/api/namespaces/{namespace}",
    tag = "Namespaces",
    params(
        ("namespace" = String, Path, description = "Name of the namespace"),
    ),
    responses(
        (status = 200, description = "Namespace details", body = NamespaceResponse),
        (status = 400, description = "Missing namespace parameter"),
        (status = 404, description = "Namespace not found"),
    )
)]
pub async fn show(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace: Option<&str> = req.match_info().get("namespace");

    if let Some(namespace) = namespace {
        match namespaces::get(&app_data.path, namespace) {
            Ok(Some(namespace)) => Ok(HttpResponse::Ok().json(NamespaceResponse {
                status: StatusMessage::resource_found(),
                namespace,
            })),

            Ok(None) => {
                log::debug!("404 Could not find namespace: {namespace}");
                Err(OxenHttpError::NotFound)
            }
            Err(err) => {
                log::debug!("Err finding namespace: {namespace} => {err:?}");
                Err(OxenHttpError::InternalServerError)
            }
        }
    } else {
        let msg = "Could not find `namespace` param";
        Err(OxenHttpError::BadRequest(msg.into()))
    }
}
