use actix_web::{web, HttpRequest, HttpResponse};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;
use liboxen::model::db_import::DbImportConfig;
use liboxen::model::NewCommitBody;
use liboxen::repositories;
use liboxen::view::{CommitResponse, StatusMessage};

#[derive(ToSchema, Deserialize)]
#[schema(
    title = "DbImportBody",
    description = "Body for importing data from an external database",
    example = json!({
        "connection_url": "postgres://user@host:5432/mydb",
        "password": "secret",
        "query": "SELECT * FROM users WHERE active = true",
        "output_path": "data/active_users.csv",
        "output_format": "csv",
        "batch_size": 10000,
        "commit_message": "Import active users",
        "name": "ox",
        "email": "ox@oxen.ai"
    })
)]
pub struct DbImportBody {
    pub connection_url: String,
    pub password: Option<String>,
    pub query: String,
    pub output_path: Option<String>,
    pub output_format: Option<String>,
    pub batch_size: Option<usize>,
    pub commit_message: Option<String>,
    pub name: Option<String>,
    pub email: Option<String>,
}

/// Import data from an external database
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/import/db/{resource}",
    tag = "Import",
    description = "Import data from an external database (PostgreSQL, MySQL, SQLite) by executing a SQL query and committing the results as a file.",
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "datasets"),
        ("resource" = String, Path, description = "Branch name and optional path", example = "main/data/results.csv"),
    ),
    request_body(
        content = DbImportBody,
        description = "Database import configuration",
    ),
    responses(
        (status = 200, description = "Data imported and committed", body = CommitResponse),
        (status = 400, description = "Bad Request / Invalid query or connection")
    )
)]
pub async fn import_db(
    req: HttpRequest,
    body: web::Json<DbImportBody>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    // Resource must specify a branch
    let branch = resource
        .branch
        .clone()
        .ok_or(OxenError::local_branch_not_found(
            resource.version.to_string_lossy(),
        ))?;

    // Determine output path: use body.output_path or fall back to resource path
    let output_path = if let Some(ref path) = body.output_path {
        std::path::PathBuf::from(path)
    } else {
        resource.path.clone()
    };

    if output_path.as_os_str().is_empty() {
        return Err(OxenHttpError::BadRequest(
            "output_path is required".to_string().into(),
        ));
    }

    // Determine output format from extension or body
    let output_format = if let Some(ref fmt) = body.output_format {
        fmt.clone()
    } else {
        output_path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("csv")
            .to_string()
    };

    let config = DbImportConfig::new(
        &body.connection_url,
        body.password.clone(),
        &body.query,
        &output_path,
        &output_format,
        body.batch_size,
    )
    .map_err(|e| OxenHttpError::BadRequest(format!("{e}").into()))?;

    let commit_body = NewCommitBody {
        author: body.name.clone().unwrap_or_default(),
        email: body.email.clone().unwrap_or_default(),
        message: body.commit_message.clone().unwrap_or_else(|| {
            format!(
                "Import from {} to {}",
                config.db_type,
                output_path.display()
            )
        }),
    };

    let commit =
        repositories::workspaces::db_import::import_db(&repo, &branch.name, &config, &commit_body)
            .await?;

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
}
