//! Errors for the oxen library
//!
//! Enumeration for all errors that can occur in the oxen library
//!

use duckdb::arrow::error::ArrowError;
use std::io;
use std::num::ParseIntError;
use std::path::Path;
use std::path::PathBuf;
use std::path::StripPrefixError;
use tokio::task::JoinError;

use crate::model::RepoNew;
use crate::model::Schema;
use crate::model::Workspace;
use crate::model::{Commit, ParsedResource};

pub mod path_buf_error;
pub mod string_error;

pub use crate::error::path_buf_error::PathBufError;
pub use crate::error::string_error::StringError;

pub const HEAD_NOT_FOUND: &str = "HEAD not found";

pub const EMAIL_AND_NAME_NOT_FOUND: &str = "oxen not configured, set email and name with:\n\noxen config --name YOUR_NAME --email YOUR_EMAIL\n";

pub const AUTH_TOKEN_NOT_FOUND: &str = "oxen authentication token not found, obtain one from your administrator and configure with:\n\noxen config --auth <HOST> <TOKEN>\n";

#[derive(thiserror::Error, Debug)]
pub enum OxenError {
    // User
    #[error("{0}")]
    UserConfigNotFound(Box<StringError>),

    // Repo
    #[error("Repository '{0}' not found")]
    RepoNotFound(Box<RepoNew>),
    #[error("No oxen repository found at {0}")]
    LocalRepoNotFound(Box<PathBufError>),
    #[error("Repository '{0}' already exists")]
    RepoAlreadyExists(Box<RepoNew>),
    #[error("Repository already exists at destination: {0}")]
    RepoAlreadyExistsAtDestination(Box<StringError>),
    #[error("Invalid repository or namespace name '{0}'. Must match [a-zA-Z0-9][a-zA-Z0-9_.-]+")]
    InvalidRepoName(StringError),

    // Fork
    #[error("{0}")]
    ForkStatusNotFound(StringError),

    // Remotes
    #[error("Remote repository not found: {0}")]
    RemoteRepoNotFound(Box<StringError>),
    #[error("{0}")]
    RemoteAheadOfLocal(StringError),
    #[error("{0}")]
    IncompleteLocalHistory(StringError),
    #[error("{0}")]
    RemoteBranchLocked(StringError),
    #[error("{0}")]
    UpstreamMergeConflict(StringError),

    // Branches/Commits
    #[error("{0}")]
    BranchNotFound(Box<StringError>),
    #[error("Revision not found: {0}")]
    RevisionNotFound(Box<StringError>),
    #[error("Root commit does not match: {0}")]
    RootCommitDoesNotMatch(Box<Commit>),
    #[error("{0}")]
    NothingToCommit(StringError),
    #[error("{0}")]
    NoCommitsFound(StringError),
    #[error("{0}")]
    HeadNotFound(StringError),

    // Workspaces
    #[error("Workspace not found: {0}")]
    WorkspaceNotFound(Box<StringError>),
    #[error("No queryable workspace found")]
    QueryableWorkspaceNotFound(),
    #[error("Workspace is behind: {0}")]
    WorkspaceBehind(Box<Workspace>),

    // Resources (paths, uris, etc.)
    #[error("Resource not found: {0}")]
    ResourceNotFound(StringError),
    #[error("Path does not exist: {0}")]
    PathDoesNotExist(Box<PathBufError>),
    #[error("Resource not found: {0}")]
    ParsedResourceNotFound(Box<PathBufError>),

    // Versioning
    #[error("{0}")]
    MigrationRequired(StringError),
    #[error("{0}")]
    OxenUpdateRequired(StringError),
    #[error("Invalid version: {0}")]
    InvalidVersion(StringError),

    // Entry
    #[error("{0}")]
    CommitEntryNotFound(StringError),

    // Schema
    #[error("Invalid schema: {0}")]
    InvalidSchema(Box<Schema>),
    #[error("Incompatible schemas: {0}")]
    IncompatibleSchemas(Box<Schema>),
    #[error("{0}")]
    InvalidFileType(StringError),
    #[error("{0}")]
    ColumnNameAlreadyExists(StringError),
    #[error("{0}")]
    ColumnNameNotFound(StringError),
    #[error("{0}")]
    UnsupportedOperation(StringError),

    // Metadata
    #[error("{0}")]
    ImageMetadataParseError(StringError),
    #[error("{0}")]
    ThumbnailingNotEnabled(StringError),

    // SQL
    #[error("SQL parse error: {0}")]
    SQLParseError(StringError),
    #[error("{0}")]
    NoRowsFound(StringError),

    // CLI Interaction
    #[error("{0}")]
    OperationCancelled(StringError),

    // fs / io
    #[error("{0}")]
    StripPrefixError(StringError),

    // Dataframe Errors
    #[error("{0}")]
    DataFrameError(StringError),

    // File Import Error
    #[error("{0}")]
    ImportFileError(StringError),

    // External Library Errors
    #[error("{0}")]
    IO(#[from] io::Error),
    #[error("Authentication failed: {0}")]
    Authentication(StringError),
    #[error("{0}")]
    ArrowError(#[from] ArrowError),
    #[error("{0}")]
    BinCodeError(#[from] bincode::Error),
    #[error("Configuration error: {0}")]
    TomlSer(#[from] toml::ser::Error),
    #[error("Configuration error: {0}")]
    TomlDe(#[from] toml::de::Error),
    #[error("Invalid URI: {0}")]
    URI(#[from] http::uri::InvalidUri),
    #[error("Invalid URL: {0}")]
    URL(#[from] url::ParseError),
    #[error("JSON error: {0}")]
    JSON(#[from] serde_json::Error),
    #[error("Network error: {0}")]
    HTTP(#[from] reqwest::Error),
    #[error("UTF-8 encoding error: {0}")]
    UTF8Error(#[from] std::str::Utf8Error),
    #[error("Database error: {0}")]
    DB(#[from] rocksdb::Error),
    #[error("Query error: {0}")]
    DUCKDB(#[from] duckdb::Error),
    #[error("Environment variable error: {0}")]
    ENV(#[from] std::env::VarError),
    #[error("Image processing error: {0}")]
    ImageError(#[from] image::ImageError),
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),
    #[error("Connection pool error: {0}")]
    R2D2Error(#[from] r2d2::Error),
    #[error("Directory traversal error: {0}")]
    JwalkError(#[from] jwalk::Error),
    #[error("Pattern error: {0}")]
    PatternError(#[from] glob::PatternError),
    #[error("Glob error: {0}")]
    GlobError(#[from] glob::GlobError),
    #[error("DataFrame error: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),
    #[error("Invalid integer: {0}")]
    ParseIntError(#[from] ParseIntError),
    #[error("Decode error: {0}")]
    RmpDecodeError(#[from] rmp_serde::decode::Error),

    // Fallback
    #[error("{0}")]
    Basic(StringError),
}

impl OxenError {
    /// Returns a user-facing hint for this error, or None if none applies.
    pub fn hint(&self) -> Option<String> {
        use OxenError::*;
        use std::io::ErrorKind::PermissionDenied;

        let hint = match self {
            LocalRepoNotFound(_) => "Run `oxen init` to create a new repository here.",
            Authentication(_) => {
                "Check your token with `oxen config --auth <HOST> <TOKEN>` and try again."
            }
            RemoteRepoNotFound(_) => {
                "Verify the remote URL is correct. Check your remotes with `oxen remote -v`."
            }
            BranchNotFound(_) => "List available branches with `oxen branch --all`.",
            RevisionNotFound(_) => {
                "Check available branches with `oxen branch --all` or commits with `oxen log`."
            }
            NothingToCommit(_) => "Stage changes with `oxen add <path>` before committing.",
            HeadNotFound(_) | NoCommitsFound(_) => {
                "This repository has no commits yet. Add files and create your first commit."
            }
            PathDoesNotExist(_)
            | ResourceNotFound(_)
            | ParsedResourceNotFound(_)
            | CommitEntryNotFound(_) => "Check the path and current branch with `oxen status`.",
            HTTP(req_err) => {
                if req_err.is_connect() || req_err.is_timeout() {
                    "Check your internet connection and that the remote host is reachable."
                } else if req_err.is_status() {
                    if let Some(status) = req_err.status() {
                        return Some(format!("Server returned HTTP {status}."));
                    } else {
                        return None;
                    }
                } else {
                    "Check your internet connection and remote configuration with `oxen remote -v`."
                }
            }
            IO(io_err) if io_err.kind() == PermissionDenied => {
                "Check file permissions and try again."
            }
            DB(_) | ArrowError(_) | BinCodeError(_) | RedisError(_) | R2D2Error(_)
            | RmpDecodeError(_) => {
                "This is an internal error. Run with RUST_LOG=debug for more details."
            }
            _ => return None,
        }
        .to_string();
        Some(hint)
    }

    pub fn basic_str(s: String) -> Self {
        OxenError::Basic(StringError::from(s))
    }

    pub fn thumbnailing_not_enabled(s: &str) -> Self {
        OxenError::ThumbnailingNotEnabled(StringError::from(s))
    }

    pub fn authentication(s: String) -> Self {
        OxenError::Authentication(StringError::from(s))
    }

    pub fn migration_required(s: String) -> Self {
        OxenError::MigrationRequired(StringError::from(s))
    }

    pub fn invalid_version(s: String) -> Self {
        OxenError::InvalidVersion(StringError::from(s))
    }

    pub fn oxen_update_required(s: String) -> Self {
        OxenError::OxenUpdateRequired(StringError::from(s))
    }

    pub fn user_config_not_found(value: StringError) -> Self {
        OxenError::UserConfigNotFound(Box::new(value))
    }

    pub fn repo_not_found(repo: RepoNew) -> Self {
        OxenError::RepoNotFound(Box::new(repo))
    }

    pub fn file_import_error(s: String) -> Self {
        OxenError::ImportFileError(StringError::from(s))
    }

    pub fn remote_not_set(name: &str) -> Self {
        OxenError::basic_str(format!(
            "Remote not set, you can set a remote by running:\n\noxen config --set-remote {name} <url>\n"
        ))
    }

    pub fn remote_ahead_of_local() -> Self {
        OxenError::RemoteAheadOfLocal(StringError::from(
            "\nRemote ahead of local, must pull changes. To fix run:\n\n  oxen pull\n",
        ))
    }

    pub fn upstream_merge_conflict() -> Self {
        OxenError::UpstreamMergeConflict(StringError::from(
            "\nRemote has conflicts with local branch. To fix run:\n\n  oxen pull\n\nThen resolve conflicts and commit changes.\n",
        ))
    }

    pub fn merge_conflict(desc: String) -> Self {
        OxenError::UpstreamMergeConflict(StringError::from(desc))
    }

    pub fn incomplete_local_history() -> Self {
        OxenError::IncompleteLocalHistory(StringError::from(
            "\nCannot push to an empty repository with an incomplete local history. To fix, pull the complete history from your remote:\n\n  oxen pull <remote> <branch> --all\n",
        ))
    }

    pub fn remote_branch_locked() -> Self {
        OxenError::RemoteBranchLocked(StringError::from(
            "\nRemote branch is locked - another push is in progress. Wait a bit before pushing again, or try pushing to a new branch.\n",
        ))
    }

    pub fn operation_cancelled() -> Self {
        OxenError::OperationCancelled(StringError::from("\nOperation cancelled.\n"))
    }

    pub fn resource_not_found(value: String) -> Self {
        OxenError::ResourceNotFound(StringError::from(value))
    }

    pub fn path_does_not_exist(path: PathBuf) -> Self {
        OxenError::PathDoesNotExist(Box::new(path.into()))
    }

    pub fn image_metadata_error(s: String) -> Self {
        OxenError::ImageMetadataParseError(StringError::from(s))
    }

    pub fn sql_parse_error(s: String) -> Self {
        OxenError::SQLParseError(StringError::from(s))
    }

    pub fn parsed_resource_not_found(resource: ParsedResource) -> Self {
        OxenError::ParsedResourceNotFound(Box::new(resource.resource.into()))
    }

    pub fn invalid_repo_name(s: String) -> Self {
        OxenError::InvalidRepoName(StringError::from(s))
    }

    pub fn is_auth_error(&self) -> bool {
        matches!(self, OxenError::Authentication(_))
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            OxenError::PathDoesNotExist(_)
                | OxenError::ResourceNotFound(_)
                | OxenError::RemoteRepoNotFound(_)
        )
    }

    pub fn repo_already_exists(repo: RepoNew) -> Self {
        OxenError::RepoAlreadyExists(Box::new(repo))
    }

    pub fn repo_already_exists_at_destination(value: StringError) -> Self {
        OxenError::RepoAlreadyExistsAtDestination(Box::new(value))
    }

    pub fn fork_status_not_found() -> Self {
        OxenError::ForkStatusNotFound(StringError::from("No fork status found"))
    }

    pub fn revision_not_found(value: StringError) -> Self {
        OxenError::RevisionNotFound(Box::new(value))
    }

    pub fn workspace_not_found(value: StringError) -> Self {
        OxenError::WorkspaceNotFound(Box::new(value))
    }

    pub fn workspace_behind(workspace: &Workspace) -> Self {
        OxenError::WorkspaceBehind(Box::new(workspace.clone()))
    }

    pub fn root_commit_does_not_match(commit: Commit) -> Self {
        OxenError::RootCommitDoesNotMatch(Box::new(commit))
    }

    pub fn no_commits_found() -> Self {
        OxenError::NoCommitsFound(StringError::from("\n No commits found.\n"))
    }

    pub fn local_repo_not_found(dir: PathBuf) -> OxenError {
        OxenError::LocalRepoNotFound(Box::new(dir.into()))
    }

    pub fn email_and_name_not_set() -> OxenError {
        OxenError::user_config_not_found(EMAIL_AND_NAME_NOT_FOUND.to_string().into())
    }

    pub fn remote_repo_not_found(url: String) -> OxenError {
        OxenError::RemoteRepoNotFound(Box::new(StringError::from(url)))
    }

    pub fn head_not_found() -> OxenError {
        OxenError::HeadNotFound(StringError::from(HEAD_NOT_FOUND))
    }

    pub fn home_dir_not_found() -> OxenError {
        OxenError::basic_str("Home directory not found".to_string())
    }

    pub fn cache_dir_not_found() -> OxenError {
        OxenError::basic_str("Cache directory not found".to_string())
    }

    pub fn must_be_on_valid_branch() -> OxenError {
        OxenError::basic_str(
            "Repository is in a detached HEAD state, checkout a valid branch to continue.\n\n  oxen checkout <branch>\n".to_string(),
        )
    }

    pub fn no_schemas_staged() -> OxenError {
        OxenError::basic_str(
            "No schemas staged\n\nAuto detect schema on file with:\n\n  oxen add path/to/file.csv\n\nOr manually add a schema override with:\n\n  oxen schemas add path/to/file.csv 'name:str, age:i32'\n".to_string(),
        )
    }

    pub fn no_schemas_committed() -> OxenError {
        OxenError::basic_str(
            "No schemas committed\n\nAuto detect schema on file with:\n\n  oxen add path/to/file.csv\n\nOr manually add a schema override with:\n\n  oxen schemas add path/to/file.csv 'name:str, age:i32'\n\nThen commit the schema with:\n\n  oxen commit -m 'Adding schema for path/to/file.csv'\n".to_string(),
        )
    }

    pub fn schema_does_not_exist_for_file(path: &Path) -> OxenError {
        let err = format!("Schema does not exist for file {:?}", path);
        OxenError::basic_str(err)
    }

    pub fn schema_does_not_exist(path: &Path) -> OxenError {
        let err = format!("Schema does not exist {:?}", path);
        OxenError::basic_str(err)
    }

    pub fn schema_does_not_have_field(field: &str) -> OxenError {
        let err = format!("Schema does not have field {:?}", field);
        OxenError::basic_str(err)
    }

    pub fn schema_has_changed(old_schema: Schema, current_schema: Schema) -> OxenError {
        let err =
            format!("\nSchema has changed\n\nOld\n{old_schema}\n\nCurrent\n{current_schema}\n");
        OxenError::basic_str(err)
    }

    pub fn remote_branch_not_found(name: &str) -> OxenError {
        let err = format!("Remote branch '{}' not found", name);
        OxenError::BranchNotFound(Box::new(StringError::from(err)))
    }

    pub fn local_branch_not_found(name: &str) -> OxenError {
        let err = format!("Branch '{}' not found", name);
        OxenError::BranchNotFound(Box::new(StringError::from(err)))
    }

    pub fn commit_db_corrupted(commit_id: &str) -> OxenError {
        let err = format!("Commit db corrupted, could not find commit: {}", commit_id);
        OxenError::basic_str(err)
    }

    pub fn commit_id_does_not_exist(commit_id: &str) -> OxenError {
        let err = format!("Could not find commit: {}", commit_id);
        OxenError::basic_str(err)
    }

    pub fn local_parent_link_broken(commit_id: &str) -> OxenError {
        let err = format!("Broken link to parent commit: {}", commit_id);
        OxenError::basic_str(err)
    }

    pub fn entry_does_not_exist(path: PathBuf) -> OxenError {
        OxenError::ParsedResourceNotFound(Box::new(path.into()))
    }

    pub fn file_error(path: &Path, error: std::io::Error) -> OxenError {
        let err = format!("File does not exist: {:?} error {:?}", path, error);
        OxenError::basic_str(err)
    }

    pub fn file_create_error(path: &Path, error: std::io::Error) -> OxenError {
        let err = format!("Could not create file: {:?} error {:?}", path, error);
        OxenError::basic_str(err)
    }

    pub fn dir_create_error(path: &Path, error: std::io::Error) -> OxenError {
        let err = format!("Could not create directory: {:?} error {:?}", path, error);
        OxenError::basic_str(err)
    }

    pub fn file_open_error(path: &Path, error: std::io::Error) -> OxenError {
        let err = format!("Could not open file: {:?} error {:?}", path, error,);
        OxenError::basic_str(err)
    }

    pub fn file_read_error(path: &Path, error: std::io::Error) -> OxenError {
        let err = format!("Could not read file: {:?} error {:?}", path, error,);
        OxenError::basic_str(err)
    }

    pub fn file_metadata_error(path: &Path, error: std::io::Error) -> OxenError {
        let err = format!("Could not get file metadata: {:?} error {:?}", path, error);
        OxenError::basic_str(err)
    }

    pub fn file_copy_error(src: &Path, dst: &Path, err: impl std::fmt::Debug) -> OxenError {
        let err = format!(
            "File copy error: {err:?}\nCould not copy from `{:?}` to `{:?}`",
            src, dst
        );
        OxenError::basic_str(err)
    }

    pub fn file_rename_error(src: &Path, dst: &Path, err: impl std::fmt::Debug) -> OxenError {
        let err = format!(
            "File rename error: {err:?}\nCould not move from `{:?}` to `{:?}`",
            src, dst
        );
        OxenError::basic_str(err)
    }

    pub fn workspace_add_file_not_in_repo(path: &Path) -> OxenError {
        let err = format!(
            "File is outside of the repo {:?}\n\nYou must specify a path you would like to add the file at with the -d flag.\n\n  oxen workspace add /path/to/file.png -d my-images/\n",
            path
        );
        OxenError::basic_str(err)
    }

    pub fn cannot_overwrite_files(paths: &[PathBuf]) -> OxenError {
        let paths_str = paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect::<Vec<String>>()
            .join("\n  ");

        OxenError::basic_str(format!(
            "\nError: your local changes to the following files would be overwritten. Please commit the following changes before continuing:\n\n  {paths_str}\n"
        ))
    }

    pub fn entry_does_not_exist_in_commit(path: &Path, commit_id: &str) -> OxenError {
        let err = format!("Entry {:?} does not exist in commit {}", path, commit_id);
        OxenError::CommitEntryNotFound(err.into())
    }

    pub fn must_supply_valid_api_key() -> OxenError {
        OxenError::basic_str(
            "Must supply valid API key. Create an account at https://oxen.ai and then set the API key with:\n\n  oxen config --auth hub.oxen.ai <API_KEY>\n".to_string(),
        )
    }

    pub fn file_has_no_parent(path: &Path) -> OxenError {
        let err = format!("File has no parent: {:?}", path);
        OxenError::basic_str(err)
    }

    pub fn file_has_no_name(path: &Path) -> OxenError {
        let err = format!("File has no file_name: {:?}", path);
        OxenError::basic_str(err)
    }

    pub fn could_not_convert_path_to_str(path: &Path) -> OxenError {
        let err = format!("File has no name: {:?}", path);
        OxenError::basic_str(err)
    }

    pub fn local_revision_not_found(name: &str) -> OxenError {
        let err = format!("Local branch or commit reference `{}` not found", name);
        OxenError::basic_str(err)
    }

    pub fn could_not_find_merge_conflict(path: &Path) -> OxenError {
        let err = format!("Could not find merge conflict for path: {:?}", path);
        OxenError::basic_str(err)
    }

    pub fn could_not_decode_value_for_key_error(key: &str) -> OxenError {
        let err = format!("Could not decode value for key: {:?}", key);
        OxenError::basic_str(err)
    }

    pub fn invalid_set_remote_url(url: &str) -> OxenError {
        let err = format!(
            "\nRemote invalid, must be fully qualified URL, got: {:?}\n\n  oxen config --set-remote origin https://hub.oxen.ai/<namespace>/<reponame>\n",
            url
        );
        OxenError::basic_str(err)
    }

    pub fn invalid_file_type(file_type: &str) -> OxenError {
        let err = format!("Invalid file type: {:?}", file_type);
        OxenError::InvalidFileType(StringError::from(err))
    }

    pub fn column_name_already_exists(column_name: &str) -> OxenError {
        let err = format!("Column name already exists: {column_name:?}");
        OxenError::ColumnNameAlreadyExists(StringError::from(err))
    }

    pub fn column_name_not_found(column_name: &str) -> OxenError {
        let err = format!("Column name not found: {column_name:?}");
        OxenError::ColumnNameNotFound(StringError::from(err))
    }

    pub fn incompatible_schemas(schema: Schema) -> OxenError {
        OxenError::IncompatibleSchemas(Box::new(schema))
    }

    pub fn parse_error(value: &str) -> OxenError {
        let err = format!("Parse error: {:?}", value);
        OxenError::basic_str(err)
    }

    pub fn unknown_subcommand(parent: &str, name: &str) -> OxenError {
        OxenError::basic_str(format!("Unknown {} subcommand '{}'", parent, name))
    }
}

// Manual From impls for types that need transformation
impl From<String> for OxenError {
    fn from(error: String) -> Self {
        OxenError::Basic(StringError::from(error))
    }
}

impl From<StripPrefixError> for OxenError {
    fn from(error: StripPrefixError) -> Self {
        OxenError::basic_str(format!("Error stripping prefix: {error}"))
    }
}

impl From<JoinError> for OxenError {
    fn from(error: JoinError) -> Self {
        OxenError::basic_str(error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for OxenError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        OxenError::basic_str(format!("UTF8 conversion error: {error}"))
    }
}
