use crate::errors::OxenHttpError;
use crate::params::{app_data, path_param};
use crate::tasks;

use liboxen::error::OxenError;
use liboxen::namespaces;
use liboxen::view::{ListNamespacesResponse, NamespaceResponse, NamespaceView, StatusMessage};

use actix_web::{HttpRequest, HttpResponse, Result, web};
use serde::Deserialize;
use utoipa::{self, IntoParams};

#[derive(Deserialize, Debug, IntoParams)]
pub struct NamespaceQuery {
    /// The namespace's directory in the legacy layout, when it is not named for the namespace.
    legacy_directory: Option<String>,
}

/// List namespaces
#[utoipa::path(
    get,
    path = "/api/namespaces",
    tag = "Namespaces",
    description = "List all namespaces on the server.",
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
    description = "Get details of a specific namespace by name.",
    params(
        ("namespace" = String, Path, description = "Name of the namespace"),
        NamespaceQuery
    ),
    responses(
        (status = 200, description = "Namespace details", body = NamespaceResponse),
        (
            status = 400,
            description = "Missing namespace parameter, or a `legacy_directory` that is not a \
                           single path segment"
        ),
        (status = 404, description = "Namespace not found"),
    )
)]
pub async fn show(
    req: HttpRequest,
    query: web::Query<NamespaceQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace: Option<&str> = path_param(&req, "namespace").ok();

    if let Some(namespace) = namespace {
        let found = {
            let (data_dir, name) = (app_data.path.clone(), namespace.to_string());
            let legacy_directory = query.into_inner().legacy_directory;
            tasks::spawn_blocking(move || {
                namespaces::get(&data_dir, &name, legacy_directory.as_deref())
            })
            .await
            .map_err(OxenError::from)??
        };
        match found {
            Some(namespace) => Ok(HttpResponse::Ok().json(NamespaceResponse {
                status: StatusMessage::resource_found(),
                namespace,
            })),

            None => {
                log::debug!("404 Could not find namespace: {namespace}");
                Err(OxenHttpError::NotFound)
            }
        }
    } else {
        let msg = "Could not find `namespace` param";
        Err(OxenHttpError::BadRequest(msg.into()))
    }
}
