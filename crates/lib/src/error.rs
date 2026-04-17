//! Errors for the oxen library
//!
//! Enumeration for all errors that can occur in the oxen library
//!

use aws_sdk_s3::error::BuildError;
use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_smithy_runtime_api::client::result::SdkError;
use duckdb::arrow::error::ArrowError;
use std::io;
use std::num::ParseIntError;
use std::path::Path;
use std::path::PathBuf;
use tokio::task::JoinError;

use crate::model::ParsedResource;
use crate::model::RepoNew;
use crate::model::Schema;
use crate::model::Workspace;
use crate::model::merkle_tree::node_type::InvalidMerkleTreeNodeType;

pub mod path_buf_error;
pub mod string_error;

pub use crate::error::path_buf_error::PathBufError;
pub use crate::error::string_error::StringError;

pub const AUTH_TOKEN_NOT_FOUND: &str = "oxen authentication token not found, obtain one from your administrator and configure with:\n\noxen config --auth <HOST> <TOKEN>\n";

#[derive(thiserror::Error, Debug)]
pub enum OxenError {
    //
    // Configuration
    //
    /// The user configuration file cannot be found at $HOME/.config/oxen/user_config.toml
    #[error(
        "oxen not configured, set email and name with:\n\noxen config --name YOUR_NAME --email YOUR_EMAIL\n"
    )]
    UserConfigNotFound,

    //
    // Repo
    //
    /// When an operation assumes a repository exists but it cannot be found.
    #[error("Repository '{0}' not found")]
    RepoNotFound(Box<RepoNew>),

    /// When a local repository cannot be found at the given path.
    #[error("No oxen repository found at {0}")]
    LocalRepoNotFound(PathBufError),

    /// Error during repository creation: attempt to create a repository that already exists.
    #[error("Repository '{0}' already exists")]
    RepoAlreadyExists(Box<RepoNew>),

    /// Error when creating a repository: repo names are restricted.
    #[error("Invalid repository or namespace name '{0}'. Must match [a-zA-Z0-9][a-zA-Z0-9_.-]+")]
    InvalidRepoName(StringError),

    /// When `get_fork_status` cannot obtain the fork status for a repository.
    #[error("No fork status found.")]
    ForkStatusNotFound,
    //
    // Remotes
    //
    /// A remote repository with a given name was not located on the server.
    #[error("Remote repository not found: {0}")]
    RemoteRepoNotFound(StringError),

    /// A merge cannot occur because there's a conflict with its upstream tracking branch.
    #[error("{0}")]
    UpstreamMergeConflict(StringError),

    /// A remote with the given name was not found.
    #[error(
        "No remote named '{0}' is set. You can set a remote by running:\n\noxen config --set-remote '{0}' <url>\n"
    )]
    RemoteNotSet(String),

    //
    // Branches/Commits
    //
    /// A branch with a given name was not found in the repository.
    #[error("{0}")]
    BranchNotFound(StringError),

    /// A given revision (commit hash) was not found in the repository.
    #[error("Revision not found: {0}")]
    RevisionNotFound(StringError),

    /// The repository is empty: it has no commits.
    #[error("No commits found.")]
    NoCommitsFound,

    /// The repository's current branch (head) cannot be located.
    #[error("HEAD not found.")]
    HeadNotFound,

    /// Missing a file name
    #[error("{0}")]
    MissingFileName(StringError),

    //
    // Workspaces
    //
    /// The workspace wasn't found (either locally or on a remote server).
    #[error("Workspace not found: {0}")]
    WorkspaceNotFound(StringError),

    /// No queryable workspace was found.
    #[error("No queryable workspace found")]
    QueryableWorkspaceNotFound,

    /// The workspace is behind the remote repository and cannot be automatically updated.
    #[error("Workspace is behind: {0}")]
    WorkspaceBehind(Box<Workspace>),

    /// A workspace with this name already exists.
    #[error("A workspace with the name '{0}' already exists")]
    WorkspaceAlreadyExists(String),

    #[error("{0}")]
    WorkspaceNameIndex(#[from] crate::core::workspaces::workspace_name_index::WsError),

    //
    // Resources (paths, uris, etc.)
    //
    /// A resource (entry, path, commit, etc.) was not found.
    #[error("Resource not found: {0}")]
    ResourceNotFound(StringError),

    /// The given path was not found, either in the repository or in the filesystem.
    #[error("Path does not exist: {0}")]
    PathDoesNotExist(PathBufError),

    /// A parsed resource was not found.
    #[error("Resource not found: {0}")]
    ParsedResourceNotFound(PathBufError),

    //
    // Versioning
    //
    /// The repository must be migrated before it can be used.
    /// This is due to the repository being at an older version than the oxen server or client
    /// being used on it.
    #[error("{0}")]
    MigrationRequired(StringError),

    /// The oxen client or server must be updated before it can be used.
    #[error("{0}")]
    OxenUpdateRequired(StringError),

    /// The version is invalid or unsupported.
    #[error("Invalid version: {0}")]
    InvalidVersion(StringError),

    // Version Store
    /// An error uploading a file to the version store
    #[error("{0}")]
    Upload(StringError),

    /// An error deleting keys
    #[error("delete_objects: some keys failed to delete: {0:?}")]
    DeleteFailure(Vec<(String, String)>),

    // Entry
    /// A commit entry is not present in the repository.
    #[error("{0}")]
    CommitEntryNotFound(StringError),

    //
    // Merkle Tree Operations
    //
    /// A failure during serialization or deserialization of a merkle tree node: it has an unknown
    /// u8 marker for its node type.
    #[error("{0}")]
    MerkleTreeError(#[from] InvalidMerkleTreeNodeType),

    //
    // Schema (dataframes)
    //
    /// The schema is invalid or unsupported for dataframe operations.
    #[error("Invalid schema: {0}")]
    InvalidSchema(Box<Schema>),

    /// The schemas of the data frames are incompatible.
    #[error("Incompatible schemas: {0}")]
    IncompatibleSchemas(Box<Schema>),

    /// The file type is unsupported for data frame operations.
    #[error("{0}")]
    InvalidFileType(StringError),

    /// A column name already exists in the dataframe's schema and cannot be added again.
    #[error("{0}")]
    ColumnNameAlreadyExists(StringError),

    /// A column name was requested in a dataframe, but no such column exists.
    #[error("{0}")]
    ColumnNameNotFound(StringError),

    /// An operation is not supported for the the dataframe.
    #[error("{0}")]
    UnsupportedOperation(StringError),

    //
    // Metadata
    //
    /// Thumbnails can only be created when the ffmpeg feature is enabled.
    #[error(
        "Video thumbnail generation requires the 'ffmpeg' feature to be enabled. Build with --features liboxen/ffmpeg to enable this functionality."
    )]
    ThumbnailingNotEnabled,

    //
    // Dataframes
    //
    /// No rows were found for a given SQL query.
    #[error("Query returned no rows")]
    NoRowsFound,

    /// An error encountered during dataframe operations.
    /// Contains a human-readable description of the error.
    #[error("{0}")]
    DataFrameError(StringError),

    /// Adding a file into a workspace
    #[error("{0}")]
    ImportFileError(StringError),

    /// An error encountered during SQL parsing.
    #[error("{0}")]
    SQLParseError(StringError),

    //
    //
    // Wrappers
    //
    //
    /// An error encountered dealing with AWS
    #[error("AWS error: {0}")]
    AwsError(Box<dyn std::error::Error + Send + Sync>),

    /// Wraps the error from std::path::strip_prefix.
    #[error("Error stripping prefix: {0}")]
    StripPrefixError(#[from] std::path::StripPrefixError),

    /// Wraps errors encountered from file reading & writing operations.
    #[error("{0}")]
    IO(#[from] io::Error),

    /// Encountered when authentication fails. Contains the authentication error message.
    #[error("Authentication failed: {0}")]
    Authentication(StringError),

    /// Wraps errors from the Arrow library, which are encountered in dataframe operations.
    #[error("{0}")]
    ArrowError(#[from] ArrowError),

    /// Wraps errors from bincode when serializing and deserializing Rust objects into binary data.
    #[error("{0}")]
    BinCodeError(#[from] bincode::Error),

    /// Wraps errors encountered when trying to serialize TOML data.
    #[error("Configuration error: {0}")]
    TomlSer(#[from] toml::ser::Error),

    /// Wraps errors encountered when deserializing invalid TOML data.
    #[error("Configuration error: {0}")]
    TomlDe(#[from] toml::de::Error),

    /// Wraps errors encountered when parsing malformed URIs.
    #[error("Invalid URI: {0}")]
    URI(#[from] http::uri::InvalidUri),

    /// Wraps errors encountered when parsing malformed URLs.
    #[error("Invalid URL: {0}")]
    URL(#[from] url::ParseError),

    /// Wraps JSON serialization and deserialization errors.
    #[error("JSON error: {0}")]
    JSON(#[from] serde_json::Error),

    /// Wraps any HTTP client errors we encounter.
    #[error("Network error: {0}")]
    HTTP(#[from] reqwest::Error),

    /// Wraps any error we encounter from handling non-UTF-8 strings.
    ///
    /// Most often occurs when interacting with filesystem paths as much
    /// of the oxen codebase relies on paths being valid UTF-8 strings.
    #[error("UTF-8 encoding error: {0}")]
    UTF8Error(#[from] std::str::Utf8Error),

    /// Wraps any error we encounter from converting a byte slice to a UTF-8 string.
    #[error("UTF-8 conversion error: {0}")]
    Utf8ConvError(#[from] std::string::FromUtf8Error),

    /// Wraps any error we encounter from interacting with RocksDB.
    #[error("Database error: {0}")]
    DB(#[from] rocksdb::Error),

    /// Wraps any error we encounter from interacting with DuckDB.
    #[error("Query error: {0}")]
    DUCKDB(#[from] duckdb::Error),

    /// Wraps any error we encounter from interacting with environment variables.
    #[error("Environment variable error: {0}")]
    ENV(#[from] std::env::VarError),

    /// Wraps any error that we get from using the image crate (image processing).
    #[error("Image processing error: {0}")]
    ImageError(#[from] image::ImageError),

    /// Wraps any error that we get from Redis client use.
    #[error("Redis error: {0}")]
    RedisError(#[from] redis::RedisError),

    /// Wraps any error that we get from using r2d2 (the connection pool).
    #[error("Connection pool error: {0}")]
    R2D2Error(#[from] r2d2::Error),

    /// Wraps any error that we get from using jwalk (directory traversal).
    #[error("Directory traversal error: {0}")]
    JwalkError(#[from] jwalk::Error),

    /// Wraps any error that we get from parsing malformed glob patterns.
    #[error("Pattern error: {0}")]
    PatternError(#[from] glob::PatternError),

    /// Wraps any error that we encounter when walking paths emitted from a glob pattern.
    #[error("Glob error: {0}")]
    GlobError(#[from] glob::GlobError),

    /// Wraps any error that we get from using polars (dataframe operations).
    #[error("DataFrame error: {0}")]
    PolarsError(#[from] polars::prelude::PolarsError),

    /// Wraps any error that we get from parsing integers from strings.
    #[error("Invalid integer: {0}")]
    ParseIntError(#[from] ParseIntError),

    /// Wraps any error that we get from encoding message pack data.
    #[error("Encode error: {0}")]
    RmpEncodeError(#[from] rmp_serde::encode::Error),

    /// Wraps any error that we get from decoding message pack data.
    #[error("Decode error: {0}")]
    RmpDecodeError(#[from] rmp_serde::decode::Error),

    /// Wraps any error that we get from joining tasks.
    #[error("{0}")]
    JoinError(#[from] JoinError),

    #[error(
        "Cannot push commit '{commit_id}' (\"{commit_message}\"): file data is not available locally.\nThis usually means the repository was cloned without full history.\n{help}"
    )]
    CannotPushShallowClone {
        commit_id: String,
        commit_message: String,
        help: String,
    },

    // Fallback
    // TODO: remove all uses of `Basic` and replace with specific errors.
    #[error("{0}")]
    Basic(StringError),

    #[error("{0}")]
    InternalError(StringError),
}

impl OxenError {
    //
    //
    // User-Facing
    //
    //

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
            HeadNotFound | NoCommitsFound => {
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

    //
    //
    // Property Identification
    //  Does this error belong to some semantic category?
    //
    //

    /// Is this error's source an authentication problem?
    pub fn is_auth_error(&self) -> bool {
        matches!(self, OxenError::Authentication(_))
    }

    /// Is this error considered as something not existing?
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            OxenError::PathDoesNotExist(_)
                | OxenError::ResourceNotFound(_)
                | OxenError::RemoteRepoNotFound(_)
                | OxenError::LocalRepoNotFound(_)
                | OxenError::ParsedResourceNotFound(_)
                | OxenError::WorkspaceNotFound(_)
                | OxenError::QueryableWorkspaceNotFound
        )
    }

    //
    //
    // Constructors
    //
    //

    /// Make a new OxenError::Authentication error.
    pub fn authentication(s: impl AsRef<str>) -> Self {
        OxenError::Authentication(StringError::from(s.as_ref()))
    }

    /// Make a new OxenError::InvalidVersion error.
    pub fn invalid_version(s: impl AsRef<str>) -> Self {
        OxenError::InvalidVersion(StringError::from(s.as_ref()))
    }

    /// Makes an OxenError::Upload error.
    pub fn upload(s: &str) -> Self {
        OxenError::Upload(StringError::from(s))
    }

    /// Make a new OxenError::RepoNotFound error.
    pub fn repo_not_found(repo: RepoNew) -> Self {
        OxenError::RepoNotFound(Box::new(repo))
    }

    /// Make a new OxenError::FileImportError error.
    pub fn file_import_error(s: impl AsRef<str>) -> Self {
        OxenError::ImportFileError(StringError::from(s.as_ref()))
    }

    /// Makes a new OxenError::ResourceNotFound error.
    pub fn resource_not_found(value: impl AsRef<str>) -> Self {
        OxenError::ResourceNotFound(StringError::from(value.as_ref()))
    }

    /// Make a new OxenError::PathDoesNotExist error.
    pub fn path_does_not_exist(path: impl AsRef<Path>) -> Self {
        OxenError::PathDoesNotExist(path.as_ref().into())
    }

    /// Make a new ParsedResourceNotFound error.
    pub fn parsed_resource_not_found(resource: ParsedResource) -> Self {
        OxenError::ParsedResourceNotFound(resource.resource.into())
    }

    /// Make a new OxenError::LocalRepoNotFound error.
    pub fn local_repo_not_found(dir: impl AsRef<Path>) -> OxenError {
        OxenError::LocalRepoNotFound(dir.as_ref().into())
    }

    /// Make a new OxenError::UserConfigNotFound error.
    pub fn email_and_name_not_set() -> OxenError {
        OxenError::UserConfigNotFound
    }

    /// Make a new OxenError::BranchNotFound error.
    pub fn remote_branch_not_found(name: impl AsRef<str>) -> OxenError {
        let err = format!("Remote branch '{}' not found", name.as_ref());
        OxenError::BranchNotFound(err.into())
    }

    /// Make a new OxenError::BranchNotFound error.
    pub fn local_branch_not_found(name: impl AsRef<str>) -> OxenError {
        let err = format!("Branch '{}' not found", name.as_ref());
        OxenError::BranchNotFound(err.into())
    }

    /// Make a new OxenError::ParsedResourceNotFound error.
    pub fn entry_does_not_exist(path: impl AsRef<Path>) -> OxenError {
        OxenError::ParsedResourceNotFound(path.as_ref().into())
    }

    /// Make a new OxenError::CommitEntryNotFound error.
    pub fn entry_does_not_exist_in_commit(
        path: impl AsRef<Path>,
        commit_id: impl AsRef<str>,
    ) -> OxenError {
        let err = format!(
            "Entry {:?} does not exist in commit {}",
            path.as_ref(),
            commit_id.as_ref()
        );
        OxenError::CommitEntryNotFound(err.into())
    }

    /// Make a new OxenError::InvalidFileType error.
    pub fn invalid_file_type(file_type: impl AsRef<str>) -> OxenError {
        let err = format!("Invalid file type: {:?}", file_type.as_ref());
        OxenError::InvalidFileType(StringError::from(err))
    }

    /// Make a new OxenError::ColumnNameAlreadyExists error.
    pub fn column_name_already_exists(column_name: &str) -> OxenError {
        let err = format!("Column name already exists: {column_name:?}");
        OxenError::ColumnNameAlreadyExists(StringError::from(err))
    }

    /// Make a new OxenError::ColumnNameNotFound error.
    pub fn column_name_not_found(column_name: &str) -> OxenError {
        let err = format!("Column name not found: {column_name:?}");
        OxenError::ColumnNameNotFound(StringError::from(err))
    }

    /// Make a new OxenError::IncompatibleSchemas error.
    pub fn incompatible_schemas(schema: Schema) -> OxenError {
        OxenError::IncompatibleSchemas(Box::new(schema))
    }

    /// Make a new OxenError::Basic error.
    pub fn basic_str(s: impl AsRef<str>) -> Self {
        OxenError::Basic(StringError::from(s.as_ref()))
    }

    /// Make a new OxenError::InternalError error.
    pub fn internal_error(s: impl AsRef<str>) -> Self {
        OxenError::InternalError(StringError::from(s.as_ref()))
    }

    //
    // OxenError::Basic constructors
    //
    //   TODO: these should all be replaced with specific variants.
    //
    //   CONTEXT:
    //         It is very useful to be able to have fully structured errors for every specific
    //         condition that can go wrong in oxen.
    //
    //         For readability & maintainability, it will be useful to break-out all of these error
    //         variants into their own specific sub-error enums. Areas of operation that have similar
    //         failure reasons (e.g. code working on the same data structure), are priority candidates
    //         for their own sub-error enums.
    //
    //         It will be useful then to define `From` the sub-error enums into an `OxenError`. This
    //         will allow using more specific per-oxen-module `Result<T, oxen sub-error>` functions
    //         in contexts that use an `OxenError`. It also allows for a helper type to be written
    //         that can preserve the original error type while still allowing conversion to `OxenError`.
    //

    pub fn home_dir_not_found() -> OxenError {
        OxenError::basic_str("Home directory not found")
    }

    pub fn cache_dir_not_found() -> OxenError {
        OxenError::basic_str("Cache directory not found")
    }

    pub fn must_be_on_valid_branch() -> OxenError {
        OxenError::basic_str(
            "Repository is in a detached HEAD state, checkout a valid branch to continue.\n\n  oxen checkout <branch>\n",
        )
    }

    pub fn no_schemas_staged() -> OxenError {
        OxenError::basic_str(
            "No schemas staged\n\nAuto detect schema on file with:\n\n  oxen add path/to/file.csv\n\nOr manually add a schema override with:\n\n  oxen schemas add path/to/file.csv 'name:str, age:i32'\n",
        )
    }

    pub fn no_schemas_committed() -> OxenError {
        OxenError::basic_str(
            "No schemas committed\n\nAuto detect schema on file with:\n\n  oxen add path/to/file.csv\n\nOr manually add a schema override with:\n\n  oxen schemas add path/to/file.csv 'name:str, age:i32'\n\nThen commit the schema with:\n\n  oxen commit -m 'Adding schema for path/to/file.csv'\n",
        )
    }

    pub fn schema_does_not_exist(path: impl AsRef<Path>) -> OxenError {
        OxenError::basic_str(format!("Schema does not exist {:?}", path.as_ref()))
    }

    pub fn commit_id_does_not_exist(commit_id: impl AsRef<str>) -> OxenError {
        OxenError::basic_str(format!("Could not find commit: {}", commit_id.as_ref()))
    }

    pub fn file_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        OxenError::basic_str(format!(
            "File does not exist: {:?} error {:?}",
            path.as_ref(),
            error
        ))
    }

    pub fn file_create_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        OxenError::basic_str(format!(
            "Could not create file: {:?} error {:?}",
            path.as_ref(),
            error
        ))
    }

    pub fn file_open_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        OxenError::basic_str(format!(
            "Could not open file: {:?} error {:?}",
            path.as_ref(),
            error
        ))
    }

    pub fn file_read_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        OxenError::basic_str(format!(
            "Could not read file: {:?} error {:?}",
            path.as_ref(),
            error
        ))
    }

    pub fn file_metadata_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        OxenError::basic_str(format!(
            "Could not get file metadata: {:?} error {:?}",
            path.as_ref(),
            error
        ))
    }

    pub fn file_copy_error(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        err: impl std::fmt::Debug,
    ) -> OxenError {
        OxenError::basic_str(format!(
            "File copy error: {err:?}\nCould not copy from `{:?}` to `{:?}`",
            src.as_ref(),
            dst.as_ref()
        ))
    }

    pub fn file_rename_error(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        err: impl std::fmt::Debug,
    ) -> OxenError {
        OxenError::basic_str(format!(
            "File rename error: {err:?}\nCould not move from `{:?}` to `{:?}`",
            src.as_ref(),
            dst.as_ref()
        ))
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

    pub fn must_supply_valid_api_key() -> OxenError {
        OxenError::basic_str(
            "Must supply valid API key. Create an account at https://oxen.ai and then set the API key with:\n\n  oxen config --auth hub.oxen.ai <API_KEY>\n",
        )
    }

    pub fn file_has_no_name(path: impl AsRef<Path>) -> OxenError {
        OxenError::basic_str(format!("File has no file_name: {:?}", path.as_ref()))
    }

    pub fn could_not_convert_path_to_str(path: impl AsRef<Path>) -> OxenError {
        OxenError::basic_str(format!("File has no name: {:?}", path.as_ref()))
    }

    pub fn local_revision_not_found(name: impl AsRef<str>) -> OxenError {
        OxenError::basic_str(format!(
            "Local branch or commit reference `{}` not found",
            name.as_ref()
        ))
    }

    pub fn could_not_find_merge_conflict(path: impl AsRef<Path>) -> OxenError {
        OxenError::basic_str(format!(
            "Could not find merge conflict for path: {:?}",
            path.as_ref()
        ))
    }

    pub fn could_not_decode_value_for_key_error(key: impl AsRef<str>) -> OxenError {
        OxenError::basic_str(format!(
            "Could not decode value for key: {:?}",
            key.as_ref()
        ))
    }

    pub fn invalid_set_remote_url(url: impl AsRef<str>) -> OxenError {
        OxenError::basic_str(format!(
            "\nRemote invalid, must be fully qualified URL, got: {:?}\n\n  oxen config --set-remote origin https://hub.oxen.ai/<namespace>/<reponame>\n",
            url.as_ref()
        ))
    }

    pub fn parse_error(value: impl AsRef<str>) -> OxenError {
        OxenError::basic_str(format!("Parse error: {:?}", value.as_ref()))
    }

    pub fn unknown_subcommand(parent: impl AsRef<str>, name: impl AsRef<str>) -> OxenError {
        OxenError::basic_str(format!(
            "Unknown {} subcommand '{}'",
            parent.as_ref(),
            name.as_ref()
        ))
    }
}

// Manual From impls for types that need transformation
impl From<String> for OxenError {
    fn from(error: String) -> Self {
        OxenError::Basic(StringError::from(error))
    }
}

/// AWS SDK Error
impl<E> From<SdkError<E, HttpResponse>> for OxenError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(e: SdkError<E, HttpResponse>) -> Self {
        OxenError::AwsError(Box::new(e))
    }
}

/// AWS Build Error
impl From<BuildError> for OxenError {
    fn from(e: BuildError) -> Self {
        OxenError::AwsError(Box::new(e))
    }
}
