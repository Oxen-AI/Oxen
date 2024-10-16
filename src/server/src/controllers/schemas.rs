use std::path::Path;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::repositories;
use liboxen::view::schema::SchemaWithPath;

use actix_web::{HttpRequest, HttpResponse};
use liboxen::error::OxenError;
use liboxen::view::entries::ResourceVersion;
use liboxen::view::{ListSchemaResponse, StatusMessage};

pub async fn list_or_get(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    // Try to see if they are asking for a specific file
    if let Ok(resource) = parse_resource(&req, &repo) {
        if resource.path != Path::new("") {
            let commit = &resource.clone().commit.unwrap();

            log::debug!(
                "schemas::list_or_get file {:?} commit {}",
                resource.path,
                commit
            );

            let schema =
                repositories::data_frames::schemas::get_by_path(&repo, commit, &resource.path)?;

            let mut schema_w_paths: Vec<SchemaWithPath> = vec![];
            if let Some(schema) = schema {
                schema_w_paths.push(SchemaWithPath::new(
                    resource.path.to_string_lossy().into(),
                    schema,
                ));
            }

            let resource = ResourceVersion {
                path: resource.path.to_string_lossy().into(),
                version: resource.version.to_string_lossy().into(),
            };
            let response = ListSchemaResponse {
                status: StatusMessage::resource_found(),
                schemas: schema_w_paths,
                commit: Some(commit.clone()),
                resource: Some(resource),
            };
            return Ok(HttpResponse::Ok().json(response));
        }
    }

    // Otherwise, list all schemas
    let revision = path_param(&req, "resource")?;

    let commit = repositories::revisions::get(&repo, &revision)?
        .ok_or(OxenError::revision_not_found(revision.to_owned().into()))?;

    log::debug!(
        "schemas::list_or_get revision {} commit {}",
        revision,
        commit
    );

    let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
    let mut schema_w_paths: Vec<SchemaWithPath> = schemas
        .into_iter()
        .map(|(path, schema)| SchemaWithPath::new(path.to_string_lossy().into(), schema))
        .collect();
    schema_w_paths.sort_by(|a, b| a.schema.hash.cmp(&b.schema.hash));

    let response = ListSchemaResponse {
        status: StatusMessage::resource_found(),
        schemas: schema_w_paths,
        commit: Some(commit),
        resource: None,
    };
    Ok(HttpResponse::Ok().json(response))
}
