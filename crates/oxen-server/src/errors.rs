use actix_multipart::MultipartError;
use actix_web::{HttpResponse, error};
use derive_more::{Display, Error};
use liboxen::constants;
use liboxen::core::db::data_frames::DataFrameError;
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
    EndpointDeprecated(StringError),
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

// Convert into its [`OxenError`] wrapper and treat it as an [`OxenHttpError::InternalOxenError`].
impl From<DataFrameError> for OxenHttpError {
    fn from(error: DataFrameError) -> Self {
        Self::InternalOxenError(error.into())
    }
}

impl From<OxenError> for OxenHttpError {
    fn from(error: OxenError) -> Self {
        OxenHttpError::InternalOxenError(error)
    }
}

// Convert into its [`OxenError`] wrapper and treat it as an [`OxenHttpError::InternalOxenError`].
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
            OxenHttpError::SerdeError(_) => handle_serde(),
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
            OxenHttpError::EndpointDeprecated(detail) => {
                let error_json = json!({
                    "error": {
                        "type": "endpoint_deprecated",
                        "detail": detail.to_string(),
                        "title": "Endpoint Deprecated",
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
                    OxenError::MerkleNodeNotFound(hash) => {
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Merkle node not found",
                                "detail": format!("Could not find Merkle tree node with hash: {hash}")
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
                    OxenError::WorkspaceStagedDbCorrupted {
                        workspace_id,
                        source,
                    } => {
                        log::error!("Workspace '{workspace_id}' staged db corrupted: {source}");
                        let error_json = json!({
                            "error": {
                                "type": "workspace_staged_db_corrupted",
                                "title": "Workspace staged data is inconsistent",
                                "detail": "The staged database for this workspace is in an inconsistent state and cannot be read. The workspace may need to be recreated.",
                                "workspace_id": workspace_id,
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_INTERNAL_SERVER_ERROR,
                        });
                        HttpResponse::InternalServerError().json(error_json)
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
                    // The client's workspace is stale and must be re-indexed or
                    // unstaged first — a 409 so it isn't blindly auto-retried.
                    OxenError::WorkspaceStaleStagedIndex(desc) => {
                        log::warn!("Workspace stale staged index: {desc}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_CONFLICT,
                                "title": "Stale workspace data frame",
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
                    OxenError::DUCKDB(error) => handle_duckdb(error),
                    OxenError::PolarsError(error) => handle_polars(error),
                    OxenError::DataFrameError(error) => match error {
                        DataFrameError::DuckDb(error) => handle_duckdb(error),
                        DataFrameError::Polars(error) => handle_polars(error),
                        DataFrameError::SerdeJson(_) => handle_serde(),
                        DataFrameError::ColumnNameAlreadyExists(column_name) => {
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
                        DataFrameError::ReservedColumnName(column_name) => {
                            log::error!("Reserved column name: {column_name}");
                            let error_json = json!({
                                "error": {
                                    "type": "column_error",
                                    "title": "Column Name Is Reserved",
                                    "detail":
                                        format!("Column name '{}' is reserved for Oxen's internal use", column_name)
                                },
                                "status": STATUS_ERROR,
                                "status_message": MSG_BAD_REQUEST,
                            });
                            HttpResponse::BadRequest().json(error_json)
                        }
                        DataFrameError::SqlParse(e) => {
                            log::error!("SQL parse error: {e}");
                            let error_json = json!({
                                "error": {
                                    "type": "sql_parse_error",
                                    "title": "Invalid SQL",
                                    "detail": format!("{e}"),
                                },
                                "status": STATUS_ERROR,
                                "status_message": MSG_BAD_REQUEST,
                            });
                            HttpResponse::BadRequest().json(error_json)
                        }
                        DataFrameError::ColumnNameNotFound(column_name) => {
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
                        e @ DataFrameError::NoRowsFound => {
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
                        _ => {
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
                    },
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
                    OxenError::TabularExportMissingMetadata(path) => {
                        let error_json = json!({
                            "error": {
                                "type": MSG_BAD_REQUEST,
                                "title": "Cannot commit an empty data frame",
                                "detail": format!(
                                    "The data frame '{}' has no rows to commit (it may be empty after row deletions), so it has no tabular schema. Add at least one row before committing.",
                                    path.to_string_lossy()
                                )
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::Basic(error) | OxenError::InternalError(error) => {
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
                    OxenError::VersionsMissingOnServer { hashes } => {
                        log::warn!(
                            "Versions missing on server: {} hash(es) absent from version store",
                            hashes.len()
                        );
                        // Embed the missing hashes both in `detail` (so existing clients
                        // see them in the rendered error) and as a typed `missing_hashes`
                        // field (so future clients can parse them programmatically without
                        // string-extracting from `detail`).
                        let error_json = json!({
                            "error": {
                                "type": "version_blobs_missing",
                                "title": "Version blobs missing on server",
                                "detail": format!(
                                    "Server is missing {} content blob(s) for this batch download. Missing hashes: {}",
                                    hashes.len(),
                                    hashes.join(", ")
                                ),
                                "missing_hashes": hashes,
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::ReachableObjectsMissing {
                        missing_nodes,
                        missing_versions,
                    } => {
                        log::warn!(
                            "Refusing branch advance: commit references missing reachable objects ({missing_nodes} node(s), {missing_versions} blob(s))"
                        );
                        let error_json = json!({
                            "error": {
                                "type": "reachable_objects_missing",
                                "title": "Commit references objects missing on server",
                                "detail": format!(
                                    "Refusing to advance the branch: the commit references {missing_nodes} merkle node(s) and {missing_versions} version blob(s) the server is missing. Re-push the missing objects with `oxen push --missing-files`."
                                ),
                                "missing_node_count": missing_nodes,
                                "missing_version_count": missing_versions,
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::DirHashIndexMissing { commit } => {
                        log::warn!(
                            "Refusing branch advance: commit {commit} is missing its directory index"
                        );
                        let error_json = json!({
                            "error": {
                                "type": "dir_hash_index_missing",
                                "title": "Commit directory index missing on server",
                                "detail": format!(
                                    "Refusing to advance the branch: commit {commit} is missing its directory index on the server, so its tree can't be served by path. Re-push the commit to repopulate it."
                                ),
                                "commit": commit,
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    err => {
                        // Surface the error's message so unmapped variants return a real reason
                        // instead of a bare "internal_server_error" that lives only in the logs.
                        log::error!("Internal server error: {err:?}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_INTERNAL_SERVER_ERROR,
                                "title": format!("{err}"),
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

/// Convert a [`duckdb::Error`] into a HTTP bad request error.
fn handle_duckdb(error: &impl std::error::Error) -> HttpResponse {
    log::error!("DuckDB error: {error}");
    let error_json = json!({
        "error": {
            "type": "query_error",
            "title": "Could not execute query on Data",
            "detail": format!("{}", error),
        },
        "status": STATUS_ERROR,
        "status_message": MSG_BAD_REQUEST,
    });
    HttpResponse::BadRequest().json(error_json)
}

/// Convert a [`polars::error::PolarsError`] into a HTTP 500 error.
fn handle_polars(error: &impl std::error::Error) -> HttpResponse {
    log::error!("Polars error: {error:?}");
    let error_json = json!({
        "error": {
            "type": "data_frame_error",
            "title": "Error Reading DataFrame",
            "detail": format!("{}", error),
        },
        "status": STATUS_ERROR,
        "status_message": MSG_BAD_REQUEST,
    });
    HttpResponse::InternalServerError().json(error_json)
}

/// Convert a [`serde_json::Error`] into a HTTP bad request error.
fn handle_serde() -> HttpResponse {
    HttpResponse::BadRequest().json(StatusMessage::bad_request())
}
