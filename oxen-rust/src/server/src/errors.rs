use actix_web::{error, http::StatusCode, HttpResponse};
use derive_more::{Display, Error};
use liboxen::constants;
use liboxen::error::{OxenError, PathBufError, StringError};
use liboxen::model::Branch;
use liboxen::view::http::{
    MSG_BAD_REQUEST, MSG_CONFLICT, MSG_RESOURCE_ALREADY_EXISTS, MSG_RESOURCE_NOT_FOUND,
    MSG_UPDATE_REQUIRED, STATUS_ERROR,
};
use liboxen::view::{SQLParseError, StatusMessage, StatusMessageDescription};

use serde_json::json;
use std::io;

#[derive(Debug, Display, Error)]
pub enum OxenHttpError {
    InternalServerError,
    BadRequest(StringError),
    NotFound,
    AppDataDoesNotExist,
    PathParamDoesNotExist(StringError),
    SQLParseError(StringError),
    NotQueryable,
    DatasetNotIndexed(PathBufError),
    DatasetAlreadyIndexed(PathBufError),
    UpdateRequired(StringError),
    WorkspaceBehind(Branch),

    // Translate OxenError to OxenHttpError
    InternalOxenError(OxenError),

    // External
    ActixError(actix_web::Error),
    SerdeError(serde_json::Error),
    RedisError(redis::RedisError),
    PolarsError(polars::error::PolarsError),
}

impl From<OxenError> for OxenHttpError {
    fn from(error: OxenError) -> Self {
        OxenHttpError::InternalOxenError(error)
    }
}

impl From<polars::error::PolarsError> for OxenHttpError {
    fn from(error: polars::error::PolarsError) -> Self {
        OxenHttpError::PolarsError(error)
    }
}

impl From<io::Error> for OxenHttpError {
    fn from(error: io::Error) -> Self {
        OxenHttpError::InternalOxenError(OxenError::IO(error))
    }
}

impl From<redis::RedisError> for OxenHttpError {
    fn from(error: redis::RedisError) -> Self {
        OxenHttpError::RedisError(error)
    }
}

impl From<actix_web::Error> for OxenHttpError {
    fn from(error: actix_web::Error) -> Self {
        OxenHttpError::ActixError(error)
    }
}

impl From<serde_json::Error> for OxenHttpError {
    fn from(error: serde_json::Error) -> Self {
        OxenHttpError::SerdeError(error)
    }
}

impl From<std::string::FromUtf8Error> for OxenHttpError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        OxenHttpError::BadRequest(StringError::new(error.to_string()))
    }
}

impl error::ResponseError for OxenHttpError {
    fn error_response(&self) -> HttpResponse {
        match self {
            OxenHttpError::InternalServerError => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::BadRequest(desc) => HttpResponse::BadRequest()
                .json(StatusMessageDescription::bad_request(desc.to_string())),
            OxenHttpError::SQLParseError(query) => {
                HttpResponse::BadRequest().json(SQLParseError::new(query.to_string()))
            }
            OxenHttpError::AppDataDoesNotExist => {
                log::error!("AppData does not exist");
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::PathParamDoesNotExist(param) => {
                log::error!(
                    "Param {} does not exist in resource path, make sure it matches in routes.rs",
                    param
                );
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::NotFound => {
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            OxenHttpError::NotQueryable => {
                let error_json = json!({
                    "error": {
                        "type": "not_queryable",
                        "title": "DataFrame is too large.",
                        "detail": format!("This DataFrame is too large to query. Upgrade your plan to query larger DataFrames larger than {}", constants::MAX_QUERYABLE_ROWS),
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_BAD_REQUEST,
                });
                HttpResponse::BadRequest().json(error_json)
            }
            OxenHttpError::DatasetNotIndexed(path) => {
                let error_json = json!({
                    "error": {
                        "type": "dataset_not_indexed",
                        "title":
                            "Dataset must be indexed.",
                        "detail":
                            format!("This dataset {} is not yet indexed for SQL and NLP querying.", path),
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_BAD_REQUEST,
                });
                HttpResponse::BadRequest().json(error_json)
            }
            OxenHttpError::WorkspaceBehind(branch) => {
                let error_json = json!({
                    "error": {
                        "type": MSG_CONFLICT,
                        "title": "Workspace is behind",
                        "detail": format!("This workspace is behind on branch '{}'", branch.name)
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_CONFLICT,
                });

                HttpResponse::NotFound().json(error_json)
            }
            OxenHttpError::DatasetAlreadyIndexed(path) => {
                let error_json = json!({
                    "error": {
                        "type": "dataset_already_indexed",
                        "title":
                            "Dataset is already indexed.",
                        "detail":
                            format!("This dataset {} is already indexed for SQL and NLP querying.", path),
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_RESOURCE_ALREADY_EXISTS,
                });
                HttpResponse::BadRequest().json(error_json)
            }
            OxenHttpError::ActixError(_) => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::PolarsError(error) => {
                log::error!("Polars processing error: {}", error);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::SerdeError(_) => {
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::RedisError(_) => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::UpdateRequired(version) => {
                let version_str = version.to_string();
                let error_json = json!({
                    "error": {
                        "type": "update_required",
                        "title": format!("Oxen CLI out of date. Pushing to OxenHub requires version >= {version_str}.")
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_UPDATE_REQUIRED,
                });
                HttpResponse::UpgradeRequired().json(error_json)
            }
            OxenHttpError::InternalOxenError(error) => {
                // Catch specific OxenError's and return the appropriate response
                match error {
                    OxenError::RepoNotFound(repo) => {
                        log::debug!("Repo not found: {}", repo);

                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "Repository '{}' not found",
                            repo
                        )))
                    }
                    OxenError::ParsedResourceNotFound(resource) => {
                        log::debug!("Resource not found: {}", resource);

                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Resource not found",
                                "detail": format!("Could not find path: {}", resource)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });

                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::BranchNotFound(branch) => {
                        log::debug!("Branch not found: {}", branch);

                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Branch does not exist",
                                "detail": format!("Could not find branch: {}", branch)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });

                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::RevisionNotFound(commit_id) => {
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "File does not exist",
                                "detail": format!("Could not find file in commit: {}", commit_id)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });

                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::PathDoesNotExist(path) => {
                        log::debug!("Path does not exist: {}", path);

                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Path does not exist",
                                "detail": format!("Could not find path: {}", path)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });

                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::WorkspaceNotFound(workspace) => {
                        log::error!("Workspace not found: {}", workspace);

                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Workspace does not exist",
                                "detail": format!("Could not find workspace: {}", workspace)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });

                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::CommitEntryNotFound(msg) => {
                        log::error!("{msg}");

                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Entry does not exist",
                                "detail": format!("{}",msg )
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });

                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::InvalidSchema(schema) => {
                        log::error!("Invalid schema: {}", schema);

                        HttpResponse::BadRequest().json(StatusMessageDescription::bad_request(
                            format!("Schema is invalid: '{}'", schema),
                        ))
                    }
                    OxenError::RemoteAheadOfLocal(desc) => {
                        log::error!("Remote ahead of local: {}", desc);

                        HttpResponse::BadRequest()
                            .json(StatusMessageDescription::bad_request(format!("{}", desc)))
                    }
                    OxenError::IncompleteLocalHistory(desc) => {
                        log::error!("Cannot push repo with incomplete local history: {}", desc);

                        HttpResponse::BadRequest()
                            .json(StatusMessageDescription::bad_request(format!("{}", desc)))
                    }
                    OxenError::IncompatibleSchemas(schema) => {
                        log::error!("Incompatible schemas: {}", schema);

                        let schema_vals = &schema
                            .fields
                            .iter()
                            .map(|f| format!("{}: {}", f.name, f.dtype))
                            .collect::<Vec<String>>()
                            .join(", ");
                        let error =
                            format!("Schema does not match. Valid Fields [{}]", schema_vals);

                        let error_json = json!({
                            "error": {
                                "type": "schema_error",
                                "title":
                                    "Incompatible Schemas",
                                "detail":
                                    format!("{}", error)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::ColumnNameAlreadyExists(column_name) => {
                        log::error!("Column Name Already Exists schemas: {}", column_name);
                        let error_json = json!({
                            "error": {
                                "type": "column_error",
                                "title":
                                    "Column Name Already Exists",
                                "detail":
                                    format!("Column name '{}' already exists in schema", column_name)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::DUCKDB(error) => {
                        log::error!("DuckDB error: {}", error);

                        let error_json = json!({
                            "error": {
                                "type": "query_error",
                                "title":
                                    "Could not execute query on Data",
                                "detail":
                                    format!("{}", error)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    err => {
                        log::error!("Internal server error: {:?}", err);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                }
            }
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            OxenHttpError::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::AppDataDoesNotExist => StatusCode::BAD_REQUEST,
            OxenHttpError::PathParamDoesNotExist(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::BadRequest(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::SQLParseError(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::NotFound => StatusCode::NOT_FOUND,
            OxenHttpError::NotQueryable => StatusCode::BAD_REQUEST,
            OxenHttpError::WorkspaceBehind(_) => StatusCode::CONFLICT,
            OxenHttpError::DatasetNotIndexed(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::DatasetAlreadyIndexed(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::UpdateRequired(_) => StatusCode::UPGRADE_REQUIRED,
            OxenHttpError::ActixError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::SerdeError(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::RedisError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::PolarsError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::InternalOxenError(error) => match error {
                OxenError::RepoNotFound(_) => StatusCode::NOT_FOUND,
                OxenError::RevisionNotFound(_) => StatusCode::NOT_FOUND,
                OxenError::InvalidSchema(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}
