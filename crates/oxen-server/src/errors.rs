use actix_multipart::MultipartError;
use actix_web::{HttpResponse, error};
use derive_more::{Display, Error};
use liboxen::constants;
use liboxen::core::db::data_frames::DataFrameError;
use liboxen::core::db::merkle_node::merkle_node_db::MerkleDbError;
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

    /// Log level tracks who caused the failure: `error!` for a server-side defect, `warn!` for a
    /// request the client got wrong, `debug!` only when there is no identifying detail worth
    /// recording. `error!` is what reaches Sentry as an issue and `debug!` is filtered out in
    /// production, so a 4xx arm at `error!` turns ordinary client traffic into alerts, and a
    /// named-resource miss at `debug!` leaves nothing to debug from.
    fn error_response(&self) -> HttpResponse {
        log::debug!("OxenHttpError: {self:?}");
        match self {
            OxenHttpError::InternalServerError => {
                // Silent: this variant carries no detail, so the failure is reported before it
                // is constructed, either at the construction site or by the callee.
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
            OxenHttpError::ActixError(error) => {
                let status = error.as_response_error().status_code();
                if status.is_server_error() {
                    log::error!("Actix error: {error}");
                    HttpResponse::build(status).json(StatusMessage::internal_server_error())
                } else {
                    log::warn!("Request rejected before it reached a handler: {error}");
                    let error_json = json!({
                        "error": {
                            "type": "bad_request",
                            "title": "Bad Request",
                            "detail": format!("{error}"),
                        },
                        "status": STATUS_ERROR,
                        "status_message": MSG_BAD_REQUEST,
                    });
                    HttpResponse::build(status).json(error_json)
                }
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
                    OxenError::InvalidRepoIdentifier(identifier) => {
                        // Logged louder than an ordinary miss: this error doesn't typically happen
                        // by accident, and could specify an arbitrary location on disk.
                        log::warn!("Rejected invalid repo identifier: {identifier:?}");
                        HttpResponse::BadRequest().json(StatusMessageDescription::bad_request(
                            "A namespace and a repository name must each be a single path segment",
                        ))
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
                    OxenError::LockTimeout(msg) => {
                        log::warn!("Repository busy (exclusive lock held): {msg}");
                        let error_json = json!({
                            "error": {
                                "type": "lock_timeout",
                                "title": "Repository is busy",
                                "detail": msg.to_string(),
                            },
                            "status": STATUS_ERROR,
                            "status_message": "too_many_requests",
                        });
                        HttpResponse::TooManyRequests()
                            .insert_header(("Retry-After", "5"))
                            .json(error_json)
                    }
                    OxenError::NoMergeBase { base, head } => {
                        log::debug!("No merge base between {base} and {head}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_BAD_REQUEST,
                                "title": "No merge base",
                                "detail": format!(
                                    "'{base}' and '{head}' share no history, so there is no merge base to compare from"
                                ),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
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
                    // The next four are things a caller got wrong. Each returns a terminal 4xx at
                    // `warn!`, so it stops both the client retrying and the alert that a 5xx at
                    // `error!` raises.
                    OxenError::DiffPathInNeitherRevision { path, base, head } => {
                        log::warn!("Diff requested for {path}, absent from {base} and {head}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_RESOURCE_NOT_FOUND,
                                "title": "Path not in either revision",
                                "detail": format!("{path} does not exist in {base} or in {head}, so there is nothing to diff."),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_RESOURCE_NOT_FOUND,
                        });
                        HttpResponse::NotFound().json(error_json)
                    }
                    OxenError::NotAFile(path) => {
                        log::warn!("Single-file endpoint given a directory: {path}");
                        let error_json = json!({
                            "error": {
                                "type": "not_a_file",
                                "title": "Not a single file",
                                "detail": format!("This endpoint serves one file at a time, and {path} is a directory."),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::NoChanges => {
                        log::warn!("Commit refused, nothing staged differs from the parent commit");
                        let error_json = json!({
                            "error": {
                                "type": "no_changes",
                                "title": "No changes to commit",
                                "detail": "Nothing staged differs from the parent commit, so there is nothing to commit.",
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::UnprocessableEntity().json(error_json)
                    }
                    OxenError::DestinationAlreadyStaged(path) => {
                        log::warn!("Move refused, destination already staged: {path}");
                        let error_json = json!({
                            "error": {
                                "type": "destination_already_staged",
                                "title": "Destination already staged",
                                "detail": format!("{path} already has a staged entry. Unstage it first, or choose another destination."),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_CONFLICT,
                        });
                        HttpResponse::Conflict().json(error_json)
                    }
                    // Only the unsupported-format case is the caller's: they uploaded an image
                    // this build cannot decode. Every other image failure stays a server error.
                    OxenError::ImageError(_) if error.is_image_too_large() => {
                        log::warn!("Image exceeds the decoder's allocation ceiling: {error}");
                        let error_json = json!({
                            "error": {
                                "type": "image_too_large",
                                "title": "Image Too Large",
                                "detail": "This image is larger than the server will decode.",
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::PayloadTooLarge().json(error_json)
                    }
                    OxenError::ImageError(_) if error.is_unsupported_image_format() => {
                        log::warn!("Unsupported image format: {error}");
                        let error_json = json!({
                            "error": {
                                "type": "unsupported_image_format",
                                "title": "Unsupported Image Format",
                                "detail": format!("This image cannot be decoded by the server: {error}"),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::UnsupportedMediaType().json(error_json)
                    }
                    OxenError::PathStagedForRemoval(path) => {
                        log::warn!("Edit refused, path staged for removal: {path}");
                        let error_json = json!({
                            "error": {
                                "type": MSG_CONFLICT,
                                "title": "Path is staged for removal",
                                "detail": format!("Unstage the removal of '{path}' before editing it"),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_CONFLICT,
                        });
                        HttpResponse::Conflict().json(error_json)
                    }
                    OxenError::NotADataFrame(path) => {
                        log::debug!("Not a tabular data frame: {path}");
                        let error_json = json!({
                            "error": {
                                "type": "not_a_data_frame",
                                "title": "Not a tabular data frame",
                                "detail": format!("Schema operations need a tabular file: '{path}'")
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    OxenError::VersionStoreBlobMissing { hash } => {
                        // Not a 404: the commit still lists this file, so the resource exists and
                        // the server cannot produce its bytes. Reported separately from generic
                        // IO so it is visible as data loss rather than lost among read failures.
                        tracing::error!(
                            hash = %hash,
                            "Version store is missing data for a hash the tree references"
                        );
                        let error_json = json!({
                            "error": {
                                "type": "version_blob_missing",
                                "title": "Version data missing",
                                "detail": format!("No stored data for hash {hash}"),
                                "hash": hash,
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_INTERNAL_SERVER_ERROR,
                        });
                        HttpResponse::InternalServerError().json(error_json)
                    }
                    OxenError::WorkspaceNotFound(workspace) => {
                        log::warn!("Workspace not found: {workspace}");
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
                        tracing::error!(
                            workspace_id = %workspace_id,
                            cause = %source,
                            "Workspace staged db is corrupted"
                        );
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
                        log::warn!("{msg}");
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
                        log::warn!("Upstream merge conflict: {desc}");
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
                        log::warn!("Invalid schema: {schema}");
                        HttpResponse::BadRequest().json(StatusMessageDescription::bad_request(
                            format!("Schema is invalid: '{schema}'"),
                        ))
                    }
                    OxenError::IncompatibleSchemas(schema) => {
                        log::warn!("Incompatible schemas: {schema}");

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
                    OxenError::UnsupportedRepoVersion(version) => {
                        log::warn!("Unsupported repo on-disk version: {version}");
                        let error_json = json!({
                            "error": {
                                "type": "unsupported_repo_version",
                                "title":
                                    "Unsupported Repository Version",
                                "detail":
                                    format!("This repository is stored in the Oxen v{version} on-disk format, which this server can no longer read. Migrate it up to the current format with an older Oxen release."),
                            },
                            "status": STATUS_ERROR,
                            "status_message": MSG_BAD_REQUEST,
                        });
                        HttpResponse::BadRequest().json(error_json)
                    }
                    // Distinct from UnsupportedRepoVersion above: that one trusts the repo's
                    // declared min_version, which can disagree with the bytes actually on disk.
                    // This arm is reached when a node itself turns out to predate the format.
                    OxenError::MerkleDbError(MerkleDbError::PreV025Node { dtype, hash }) => {
                        // Already logged where the node was classified, which also covers the
                        // read paths that never reach an HTTP response.
                        let error_json = json!({
                            "error": {
                                "type": "pre_v0_25_node_format",
                                "title":
                                    "Retired Repository Storage Format",
                                "detail":
                                    format!("This repository contains Merkle nodes written before Oxen v0.25.0, which this server cannot read (first encountered: {dtype:?} node {hash})."),
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
                    OxenError::PolarsError(polars_error) => {
                        handle_polars(polars_error, error.is_polars_io_error())
                    }
                    OxenError::DataFrameError(data_frame_error) => match data_frame_error {
                        DataFrameError::DuckDb(error) => handle_duckdb(error),
                        DataFrameError::Polars(polars_error) => {
                            handle_polars(polars_error, error.is_polars_io_error())
                        }
                        DataFrameError::SerdeJson(_) => handle_serde(),
                        DataFrameError::ColumnNameAlreadyExists(column_name) => {
                            log::warn!("Column Name Already Exists: {column_name}");
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
                            log::warn!("Reserved column name: {column_name}");
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
                            log::warn!("SQL parse error: {e}");
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
                            log::warn!("Column Name Not Found: {column_name}");
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
                            log::debug!("No rows found: {e}");
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
                        // Both release images build with `liboxen/ffmpeg`, so reaching this arm means
                        // a build lost the feature and every video-thumbnail request 500s.
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
                        log::warn!("Workspace behind: {workspace}");
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
                        //
                        // The error text stays in the message here, unlike the arms above. Error
                        // reporting groups by message, and every unmapped variant reaches this
                        // one line — a constant message would collapse every unrelated failure
                        // into a single bucket. An unmapped error carrying an id still splits per
                        // id; the fix for that is giving it its own arm, not flattening this one.
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
    log::warn!("DuckDB error: {error}");
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

/// Convert a [`polars::error::PolarsError`] into a HTTP 400, or a 500 when the failure was the
/// server's own IO rather than the caller's data.
fn handle_polars(error: &impl std::error::Error, is_server_side: bool) -> HttpResponse {
    let status_message = if is_server_side {
        tracing::error!(cause = ?error, "Polars error reading a data frame");
        MSG_INTERNAL_SERVER_ERROR
    } else {
        tracing::warn!(cause = ?error, "Malformed data frame or query");
        MSG_BAD_REQUEST
    };
    let error_json = json!({
        "error": {
            "type": "data_frame_error",
            "title": "Error Reading DataFrame",
            "detail": format!("{}", error),
        },
        "status": STATUS_ERROR,
        "status_message": status_message,
    });
    if is_server_side {
        HttpResponse::InternalServerError().json(error_json)
    } else {
        HttpResponse::BadRequest().json(error_json)
    }
}

/// Convert a [`serde_json::Error`] into a HTTP bad request error.
fn handle_serde() -> HttpResponse {
    HttpResponse::BadRequest().json(StatusMessage::bad_request())
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::ResponseError;
    use actix_web::http::StatusCode;
    use std::path::PathBuf;

    #[test]
    fn test_unsupported_repo_version_is_a_client_error() {
        // A 5xx here reads as transient to clients, which retry it with backoff. The repo's
        // on-disk format never changes on its own, so the status has to be terminal.
        let error = OxenHttpError::from(OxenError::UnsupportedRepoVersion("0.19.0".into()));
        let status = error.error_response().status();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!status.is_server_error());
    }

    #[test]
    fn test_pre_v0_25_node_is_a_client_error() {
        // Same reasoning as above: a node that predates the format never becomes readable by
        // retrying, so the status has to be terminal rather than the 500 this used to return.
        let error = OxenHttpError::from(OxenError::MerkleDbError(MerkleDbError::PreV025Node {
            dtype: liboxen::model::MerkleTreeNodeType::VNode,
            hash: liboxen::model::MerkleHash::new(42),
        }));
        let status = error.error_response().status();

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!status.is_server_error());
    }

    #[test]
    fn test_client_mistakes_are_not_server_errors() {
        // Each of these is something a caller got wrong. As a 5xx each one alerted us and invited
        // the client to retry a request that can never succeed, so the status has to be a terminal
        // 4xx, reported at `warn!` rather than `error!`.
        let cases = [
            (
                OxenError::NotAFile(PathBuf::from("some/dir").into()),
                StatusCode::BAD_REQUEST,
            ),
            (OxenError::NoChanges, StatusCode::UNPROCESSABLE_ENTITY),
            (
                OxenError::DestinationAlreadyStaged(PathBuf::from("a.txt").into()),
                StatusCode::CONFLICT,
            ),
        ];

        for (error, expected) in cases {
            let rendered = format!("{error}");
            let status = OxenHttpError::from(error).error_response().status();
            assert_eq!(status, expected, "wrong status for {rendered}");
            assert!(!status.is_server_error(), "{rendered} reported as a 5xx");
        }
    }

    /// Reports two blob-missing errors with different hashes and returns what the reporting
    /// backend received, driving the real `error_response` through the same tracing layer `main`
    /// installs.
    ///
    /// The subscriber here is scoped rather than global, so the `log` -> `tracing` bridge is not
    /// active: a site written with `log::error!` produces no event at all rather than a
    /// differently-grouped one. Reverting a converted site therefore fails the count assertion
    /// below instead of the message one.
    fn reported_events_for_two_hashes() -> Vec<sentry::protocol::Event<'static>> {
        use tracing_subscriber::layer::SubscriberExt;

        let transport = sentry::test::TestTransport::new();
        let options = sentry::ClientOptions {
            dsn: Some(
                "https://public@sentry.invalid/1"
                    .parse()
                    .expect("the test DSN should parse"),
            ),
            transport: Some(std::sync::Arc::new(std::sync::Arc::clone(&transport))),
            ..Default::default()
        };
        let hub = std::sync::Arc::new(sentry::Hub::new(
            Some(std::sync::Arc::new(options.into())),
            std::sync::Arc::new(Default::default()),
        ));

        sentry::Hub::run(hub, || {
            let subscriber =
                tracing_subscriber::registry().with(sentry::integrations::tracing::layer());
            tracing::subscriber::with_default(subscriber, || {
                for hash in ["aaa111", "bbb222"] {
                    let error = OxenError::VersionStoreBlobMissing {
                        hash: hash.to_string(),
                    };
                    let _ = OxenHttpError::from(error).error_response();
                }
            });
        });

        transport.fetch_and_clear_events()
    }

    #[test]
    fn missing_blob_reports_group_together_and_keep_the_hash() {
        // Reports are grouped by message, so a hash in the message would file every missing blob
        // as its own issue and a resolved one would never reopen — the recurrence arrives under a
        // new fingerprint. The hash still has to survive somewhere or the report is unactionable.
        let events = reported_events_for_two_hashes();
        assert_eq!(events.len(), 2, "expected one report per call");

        assert_eq!(
            events[0].message, events[1].message,
            "two hashes must share one message, or they group as separate issues"
        );

        let hashes: Vec<String> = events
            .iter()
            .map(|event| {
                format!(
                    "{:?}",
                    event
                        .contexts
                        .get("Rust Tracing Fields")
                        .expect("the hash should ride along as a structured field")
                )
            })
            .collect();
        assert!(hashes[0].contains("aaa111"), "got {}", hashes[0]);
        assert!(hashes[1].contains("bbb222"), "got {}", hashes[1]);
    }

    #[test]
    fn test_actix_errors_keep_the_status_actix_chose() {
        // actix classifies its own errors; answering all of them with 500 turned a client that
        // truncated its upload into an alert. The status has to come from actix, not be replaced.
        let incomplete = OxenHttpError::from(actix_web::Error::from(
            actix_web::error::PayloadError::Incomplete(None),
        ));
        let status = incomplete.error_response().status();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(!status.is_server_error());

        // Overflow is the one payload case actix rates differently, and it must survive too.
        let overflow = OxenHttpError::from(actix_web::Error::from(
            actix_web::error::PayloadError::Overflow,
        ));
        assert_eq!(
            overflow.error_response().status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn test_diff_path_in_neither_revision_is_not_found() {
        // Diffing a path neither revision contains is the caller naming something that is not
        // there, not a server failure.
        let error = OxenError::DiffPathInNeitherRevision {
            path: PathBuf::from("a.csv").into(),
            base: "abc123".to_string(),
            head: "def456".to_string(),
        };
        let status = OxenHttpError::from(error).error_response().status();

        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!status.is_server_error());
    }

    #[test]
    fn test_unsupported_image_format_is_a_client_error() {
        // An image this build cannot decode is the caller's input. Every other image failure is
        // still a server error, so the arm is guarded rather than matching all of `ImageError`.
        let error = OxenError::ImageError(image::ImageError::Unsupported(
            image::error::UnsupportedError::from_format_and_kind(
                image::error::ImageFormatHint::Name("AVIF".to_string()),
                image::error::UnsupportedErrorKind::Format(image::error::ImageFormatHint::Name(
                    "AVIF".to_string(),
                )),
            ),
        ));

        let status = OxenHttpError::from(error).error_response().status();
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert!(!status.is_server_error());
    }
}
