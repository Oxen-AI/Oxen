use actix_multipart::MultipartError;
use actix_web::{HttpResponse, error};
use derive_more::{Display, Error};
use liboxen::constants;
use liboxen::error::{OxenError, PathBufError, StringError};
use liboxen::model::{Branch, Workspace};
use liboxen::view::http::{
    MSG_BAD_REQUEST, MSG_CONFLICT, MSG_INTERNAL_SERVER_ERROR, MSG_RESOURCE_ALREADY_EXISTS,
    MSG_RESOURCE_NOT_FOUND, MSG_UPDATE_REQUIRED, STATUS_ERROR,
};
use liboxen::view::{SQLParseError, StatusMessage, StatusMessageDescription};

use serde_json::json;
use std::io;

#[derive(Debug)]
pub struct WorkspaceBranch {
    pub workspace: Workspace,
    pub branch: Branch,
}

impl std::fmt::Display for WorkspaceBranch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkspaceBranch(workspace={:?}, branch={})",
            self.workspace, self.branch
        )
    }
}

impl std::error::Error for WorkspaceBranch {}

#[derive(Debug, Display, Error)]
pub enum OxenHttpError {
    InternalServerError,
    BadRequest(StringError),
    MultipartError(MultipartError),
    NotFound,
    AppDataDoesNotExist,
    PathParamDoesNotExist(StringError),
    SQLParseError(StringError),
    NotQueryable,
    DatasetNotIndexed(PathBufError),
    DatasetAlreadyIndexed(PathBufError),
    UpdateRequired(StringError),
    MigrationRequired(StringError),
    WorkspaceBehind(Box<WorkspaceBranch>),
    BasicError(StringError),
    FailedToReadRequestPayload,

    // Translate OxenError to OxenHttpError
    InternalOxenError(OxenError),

    // External
    ActixError(actix_web::Error),
    SerdeError(serde_json::Error),
}

impl From<OxenError> for OxenHttpError {
    fn from(error: OxenError) -> Self {
        OxenHttpError::InternalOxenError(error)
    }
}

impl From<io::Error> for OxenHttpError {
    fn from(error: io::Error) -> Self {
        OxenHttpError::InternalOxenError(OxenError::IO(error))
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
    // NOTICE: We are **NOT** using the status_code() method in error_response().
    //
    //         We instead have opted to directly implement the OxenHttpError -> HTTP status code
    //         mapping directly in the error_response() creation method.
    //
    //         Do not add a `status_code()` method definition here :)

    fn error_response(&self) -> HttpResponse {
        log::debug!("OxenHttpError: {self:?}");
        match self {
            OxenHttpError::InternalServerError => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::MultipartError(_) => {
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::FailedToReadRequestPayload => HttpResponse::BadRequest().json(
                StatusMessageDescription::bad_request("Failed to read request payload"),
            ),
            OxenHttpError::BadRequest(desc) => {
                let error_json = json!({
                    "error": {
                        "type": "bad_request",
                        "title":
                            "Bad Request",
                        "detail":
                            desc.to_string()
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_BAD_REQUEST,
                });
                HttpResponse::BadRequest().json(error_json)
            }
            OxenHttpError::SQLParseError(query) => {
                HttpResponse::BadRequest().json(SQLParseError::new(query.to_string()))
            }
            OxenHttpError::AppDataDoesNotExist => {
                log::error!("AppData does not exist");
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::PathParamDoesNotExist(param) => {
                log::error!(
                    "Param {param} does not exist in resource path, make sure it matches in routes.rs"
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
            OxenHttpError::BasicError(error) => {
                let error_json = json!({
                    "error": {
                        "type": "basic_error",
                        "title": "Basic error",
                        "detail": format!("{}", error)
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_BAD_REQUEST,
                });
                HttpResponse::BadRequest().json(error_json)
            }
            OxenHttpError::WorkspaceBehind(workspace_branch) => {
                let workspace = &workspace_branch.workspace;
                let branch = &workspace_branch.branch;
                let error_json = json!({
                    "error": {
                        "type": MSG_CONFLICT,
                        "title": "Workspace is behind",
                        "detail": format!("This workspace '{}' is behind on branch '{}' commit {} < {}", workspace.id, branch.name, workspace.commit.id, branch.commit_id)
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
            OxenHttpError::SerdeError(_) => {
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::UpdateRequired(version) => {
                let version_str = version.to_string();
                let error_json = json!({
                    "error": {
                        "type": "update_required",
                        "detail": format!("Oxen CLI out of date. Pushing to OxenHub requires version >= {version_str}."),
                        "title": "Update Required",
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_UPDATE_REQUIRED,
                });
                HttpResponse::UpgradeRequired().json(error_json)
            }
            OxenHttpError::MigrationRequired(version) => {
                let version_str = version.to_string();
                let error_json = json!({
                    "error": {
                        "type": "migration_required",
                        "detail": format!("Oxen Server is running a newer minimum required version: {version_str}. A migration may be in progress, hang tight."),
                        "title": "Migration Required",
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
                        log::debug!("Repo not found: {repo}");
                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "Repository '{repo}' not found"
                        )))
                    }
                    OxenError::ResourceNotFound(resource) => {
                        log::debug!("Resource not found: {resource}");
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
                    OxenError::ParsedResourceNotFound(resource) => {
                        log::debug!("Resource not found: {resource}");
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
                    OxenError::RevisionNotFound(revision) => {
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Revision not found",
                                "detail": format!("Could not find branch or commit: {}", revision)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::PathDoesNotExist(path) => {
                        log::debug!("Path does not exist: {path}");
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
                        log::error!("Workspace not found: {workspace}");
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
                    OxenError::RemoteRepoNotFound(remote) => {
                        log::debug!("Remote repo not found: {remote}");
                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "Remote repository not found: {remote}"
                        )))
                    }
                    OxenError::CommitEntryNotFound(msg) => {
                        log::error!("{msg}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Entry does not exist",
                                "detail": format!("{}", msg)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::UpstreamMergeConflict(desc) => {
                        log::error!("Upstream merge conflict: {desc}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_CONFLICT,
                                "title": "Merge conflict",
                                "detail": format!("{desc}")
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_CONFLICT,
                        });
                        HttpResponse::Conflict().json(error_json)
                    }
                    OxenError::InvalidSchema(schema) => {
                        log::error!("Invalid schema: {schema}");
                        HttpResponse::BadRequest().json(StatusMessageDescription::bad_request(
                            format!("Schema is invalid: '{schema}'"),
                        ))
                    }

                    OxenError::IncompatibleSchemas(schema) => {
                        log::error!("Incompatible schemas: {schema}");

                        let schema_vals = &schema
                            .fields
                            .iter()
                            .map(|f| format!("{}: {}", f.name, f.dtype))
                            .collect::<Vec<String>>()
                            .join(", ");
                        let error = format!("Schema does not match. Valid Fields [{schema_vals}]");

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
                        log::error!("Column Name Already Exists: {column_name}");
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
                    OxenError::ColumnNameNotFound(column_name) => {
                        log::error!("Column Name Not Found: {column_name}");
                        let error_json = json!({
                            "error": {
                                "type": "column_error",
                                "title":
                                    "Column Name Not Found",
                                "detail":
                                    format!("Column name '{}' not found in schema", column_name)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::InvalidRepoName(name) => {
                        log::debug!("Invalid repo name: {name}");
                        let error_json = json!({
                            "error": {
                                "type": "invalid_repo_name",
                                "title":
                                    "Invalid Repository Name",
                                "detail":
                                    format!("Invalid repository or namespace name '{name}'. Must match [a-zA-Z0-9][a-zA-Z0-9_.-]+"),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::ImportFileError(desc) => {
                        let error_json = json!({
                            "error": {
                                "type": "bad_request",
                                "title":
                                    "Bad Request",
                                "detail":
                                    desc.to_string()
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::DUCKDB(error) => {
                        log::error!("DuckDB error: {error}");
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
                    OxenError::PolarsError(error) => {
                        log::error!("Polars error: {error:?}");
                        let error_json = json!({
                            "error": {
                                "type": "data_frame_error",
                                "title": "Error Reading DataFrame",
                                "detail":
                                    format!("{}", error),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::InternalServerError().json(error_json)
                    }
                    OxenError::DataFrameError(error) => {
                        log::error!("DataFrame error: {error}");
                        let error_json = json!({
                            "error": {
                                "type": "data_frame_error",
                                "title": "Error Reading DataFrame",
                                "detail": format!("{}", error),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_INTERNAL_SERVER_ERROR,
                        });
                        HttpResponse::InternalServerError().json(error_json)
                    }
                    thumbnail_error @ OxenError::ThumbnailingNotEnabled => {
                        log::error!("Thumbnailing not enabled: {thumbnail_error}");
                        let error_json = json!({
                            "error": {
                                "type": "thumbnailing_not_enabled",
                                "title": "Thumbnailing Not Enabled",
                                "detail": format!("{thumbnail_error}"),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_INTERNAL_SERVER_ERROR,
                        });
                        HttpResponse::InternalServerError().json(error_json)
                    }
                    OxenError::Basic(error) => {
                        let error_json = json!({
                            "error": {
                                "type": MSG_INTERNAL_SERVER_ERROR,
                                "title": format!("{}", error),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_INTERNAL_SERVER_ERROR,
                        });
                        HttpResponse::InternalServerError().json(error_json)
                    }
                    e @ OxenError::NoRowsFound => {
                        log::error!("No rows found: {e}");
                        let error_json = json!({
                            "error": {
                                "type": "no_rows_found",
                                "title": "No rows found",
                                "detail": format!("{e}"),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_INTERNAL_SERVER_ERROR,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::LocalRepoNotFound(path) => {
                        log::debug!("Local repo not found: {path}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Local repository not found",
                                "detail": format!("No oxen repository found at {path}")
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::HeadNotFound => {
                        log::debug!("HEAD not found");
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "HEAD not found",
                                "detail": "HEAD not found."
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::NoCommitsFound => {
                        log::debug!("No commits found");
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "No commits found",
                                "detail": "No commits found."
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::QueryableWorkspaceNotFound => {
                        log::debug!("Queryable workspace not found");
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Queryable workspace not found",
                                "detail": "Queryable workspace not found."
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::WorkspaceBehind(workspace) => {
                        log::error!("Workspace behind: {workspace}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_CONFLICT,
                                "title": "Workspace is behind",
                                "detail": format!("Workspace '{}' is behind at commit {}", workspace.id, workspace.commit.id)
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_CONFLICT,
                        });
                        HttpResponse::Conflict().json(error_json)
                    }
                    err => {
                        log::error!("Internal server error: {err:?}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_INTERNAL_SERVER_ERROR,
                                "title": "Internal server error",
                                "detail": format!("{}", err),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_INTERNAL_SERVER_ERROR,
                        });
                        HttpResponse::InternalServerError().json(error_json)
                    }
                }
            }
        }
    }
}
