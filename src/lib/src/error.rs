//! Errors for the oxen library
//!
//! Enumeration for all errors that can occur in the oxen library
//!

use derive_more::{Debug, Error};
use duckdb::arrow::error::ArrowError;
use std::fmt;
use std::io;
use std::num::ParseIntError;
use std::path::Path;
use std::path::StripPrefixError;

use crate::model::Branch;
use crate::model::Schema;
use crate::model::{Commit, ParsedResource};
use crate::model::{Remote, RepoNew};

pub mod path_buf_error;
pub mod string_error;

pub use crate::error::path_buf_error::PathBufError;
pub use crate::error::string_error::StringError;

use polars::prelude::PolarsError;

pub const NO_REPO_FOUND: &str = "No oxen repository exists, looking for directory: .oxen";

pub const HEAD_NOT_FOUND: &str = "HEAD not found";

pub const EMAIL_AND_NAME_NOT_FOUND: &str =
    "oxen not configured, set email and name with:\n\noxen config --name YOUR_NAME --email YOUR_EMAIL\n";

pub const AUTH_TOKEN_NOT_FOUND: &str =
    "oxen authentication token not found, obtain one from your administrator and configure with:\n\noxen config --auth <HOST> <TOKEN>\n";

#[derive(Error, Debug)]
pub enum OxenError {
    /// Internal Oxen Errors
    // User
    UserConfigNotFound(Box<StringError>),

    // Repo
    RepoNotFound(Box<RepoNew>),
    LocalRepoNotFound(Box<PathBufError>),
    RepoAlreadyExists(Box<RepoNew>),
    RepoAlreadyExistsAtDestination(Box<StringError>),

    // Fork
    ForkStatusNotFound(StringError),

    // Remotes
    RemoteRepoNotFound(Box<Remote>),
    RemoteAheadOfLocal(StringError),
    IncompleteLocalHistory(StringError),
    RemoteBranchLocked(StringError),
    UpstreamMergeConflict(StringError),

    // Branches/Commits
    BranchNotFound(Box<StringError>),
    RevisionNotFound(Box<StringError>),
    RootCommitDoesNotMatch(Box<Commit>),
    NothingToCommit(StringError),
    NoCommitsFound(StringError),
    HeadNotFound(StringError),

    // Workspaces
    WorkspaceNotFound(Box<StringError>),
    QueryableWorkspaceNotFound(),
    WorkspaceBehind(Branch),

    // Resources (paths, uris, etc.)
    ResourceNotFound(StringError),
    PathDoesNotExist(Box<PathBufError>),
    ParsedResourceNotFound(Box<PathBufError>),

    // Versioning
    MigrationRequired(StringError),
    OxenUpdateRequired(StringError),
    InvalidVersion(StringError),

    // Entry
    CommitEntryNotFound(StringError),

    // Schema
    InvalidSchema(Box<Schema>),
    IncompatibleSchemas(Box<Schema>),
    InvalidFileType(StringError),
    ColumnNameAlreadyExists(StringError),
    ColumnNameNotFound(StringError),
    UnsupportedOperation(StringError),

    // Metadata
    ImageMetadataParseError(StringError),

    // SQL
    SQLParseError(StringError),
    NoRowsFound(StringError),

    // CLI Interaction
    OperationCancelled(StringError),

    // fs / io
    StripPrefixError(StringError),

    // Dataframe Errors
    DataFrameError(StringError),

    // File Import Error
    ImportFileError(StringError),

    // External Library Errors
    IO(io::Error),
    Authentication(StringError),
    ArrowError(ArrowError),
    BinCodeError(bincode::Error),
    TomlSer(toml::ser::Error),
    TomlDe(toml::de::Error),
    URI(http::uri::InvalidUri),
    URL(url::ParseError),
    JSON(serde_json::Error),
    HTTP(reqwest::Error),
    UTF8Error(std::str::Utf8Error),
    DB(rocksdb::Error),
    DUCKDB(duckdb::Error),
    ENV(std::env::VarError),
    ImageError(image::ImageError),
    RedisError(redis::RedisError),
    R2D2Error(r2d2::Error),
    JwalkError(jwalk::Error),
    PatternError(glob::PatternError),
    GlobError(glob::GlobError),
    PolarsError(polars::prelude::PolarsError),
    ParseIntError(ParseIntError),
    RmpDecodeError(rmp_serde::decode::Error),

    // Fallback
    Basic(StringError),
}

impl fmt::Display for OxenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OxenError::OxenUpdateRequired(err) | OxenError::Basic(err) => write!(f, "{}", err),
            _ => {
                write!(f, "{:?}", self)
            }
        }
    }
}

impl OxenError {
    pub fn basic_str(s: impl AsRef<str>) -> Self {
        OxenError::Basic(StringError::from(s.as_ref()))
    }

    pub fn authentication(s: impl AsRef<str>) -> Self {
        OxenError::Authentication(StringError::from(s.as_ref()))
    }

    pub fn migration_required(s: impl AsRef<str>) -> Self {
        OxenError::MigrationRequired(StringError::from(s.as_ref()))
    }

    pub fn invalid_version(s: impl AsRef<str>) -> Self {
        OxenError::InvalidVersion(StringError::from(s.as_ref()))
    }

    pub fn oxen_update_required(s: impl AsRef<str>) -> Self {
        OxenError::OxenUpdateRequired(StringError::from(s.as_ref()))
    }

    pub fn user_config_not_found(value: StringError) -> Self {
        OxenError::UserConfigNotFound(Box::new(value))
    }

    pub fn repo_not_found(repo: RepoNew) -> Self {
        OxenError::RepoNotFound(Box::new(repo))
    }

    pub fn file_import_error(s: impl AsRef<str>) -> Self {
        OxenError::ImportFileError(StringError::from(s.as_ref()))
    }

    pub fn remote_not_set(name: impl AsRef<str>) -> Self {
        let name = name.as_ref();
        OxenError::basic_str(
            format!("Remote not set, you can set a remote by running:\n\noxen config --set-remote {} <url>\n", name)
        )
    }

    pub fn remote_not_found(remote: Remote) -> Self {
        OxenError::RemoteRepoNotFound(Box::new(remote))
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

    pub fn merge_conflict(desc: impl AsRef<str>) -> Self {
        OxenError::UpstreamMergeConflict(StringError::from(desc.as_ref()))
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

    pub fn resource_not_found(value: impl AsRef<str>) -> Self {
        OxenError::ResourceNotFound(StringError::from(value.as_ref()))
    }

    pub fn path_does_not_exist(path: impl AsRef<Path>) -> Self {
        OxenError::PathDoesNotExist(Box::new(path.as_ref().into()))
    }

    pub fn image_metadata_error(s: impl AsRef<str>) -> Self {
        OxenError::ImageMetadataParseError(StringError::from(s.as_ref()))
    }

    pub fn sql_parse_error(s: impl AsRef<str>) -> Self {
        OxenError::SQLParseError(StringError::from(s.as_ref()))
    }

    pub fn parsed_resource_not_found(resource: ParsedResource) -> Self {
        OxenError::ParsedResourceNotFound(Box::new(resource.resource.into()))
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

    pub fn workspace_behind(branch: Branch) -> Self {
        OxenError::WorkspaceBehind(branch)
    }

    pub fn root_commit_does_not_match(commit: Commit) -> Self {
        OxenError::RootCommitDoesNotMatch(Box::new(commit))
    }

    pub fn no_commits_found() -> Self {
        OxenError::NoCommitsFound(StringError::from("\n No commits found.\n"))
    }

    pub fn local_repo_not_found(dir: impl AsRef<Path>) -> OxenError {
        OxenError::LocalRepoNotFound(Box::new(dir.as_ref().into()))
    }

    pub fn email_and_name_not_set() -> OxenError {
        OxenError::user_config_not_found(EMAIL_AND_NAME_NOT_FOUND.to_string().into())
    }

    pub fn auth_token_not_set() -> OxenError {
        OxenError::basic_str(AUTH_TOKEN_NOT_FOUND)
    }

    pub fn remote_repo_not_found(url: impl AsRef<str>) -> OxenError {
        let err = format!("Remote repository does not exist {}", url.as_ref());
        OxenError::basic_str(err)
    }

    pub fn head_not_found() -> OxenError {
        OxenError::HeadNotFound(StringError::from(HEAD_NOT_FOUND))
    }

    pub fn home_dir_not_found() -> OxenError {
        OxenError::basic_str("Home directory not found")
    }

    pub fn must_be_on_valid_branch() -> OxenError {
        OxenError::basic_str("Repository is in a detached HEAD state, checkout a valid branch to continue.\n\n  oxen checkout <branch>\n")
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

    pub fn schema_does_not_exist_for_file(path: impl AsRef<Path>) -> OxenError {
        let err = format!("Schema does not exist for file {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn schema_does_not_exist(path: impl AsRef<Path>) -> OxenError {
        let err = format!("Schema does not exist {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn schema_does_not_have_field(field: impl AsRef<str>) -> OxenError {
        let err = format!("Schema does not have field {:?}", field.as_ref());
        OxenError::basic_str(err)
    }

    pub fn schema_has_changed(old_schema: Schema, current_schema: Schema) -> OxenError {
        let err =
            format!("\nSchema has changed\n\nOld\n{old_schema}\n\nCurrent\n{current_schema}\n");
        OxenError::basic_str(err)
    }

    pub fn remote_branch_not_found(name: impl AsRef<str>) -> OxenError {
        let err = format!("Remote branch '{}' not found", name.as_ref());
        OxenError::BranchNotFound(Box::new(StringError::from(err)))
    }

    pub fn local_branch_not_found(name: impl AsRef<str>) -> OxenError {
        let err = format!("Branch '{}' not found", name.as_ref());
        OxenError::BranchNotFound(Box::new(StringError::from(err)))
    }

    pub fn commit_db_corrupted(commit_id: impl AsRef<str>) -> OxenError {
        let err = format!(
            "Commit db corrupted, could not find commit: {}",
            commit_id.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn commit_id_does_not_exist(commit_id: impl AsRef<str>) -> OxenError {
        let err = format!("Could not find commit: {}", commit_id.as_ref());
        OxenError::basic_str(err)
    }

    pub fn local_parent_link_broken(commit_id: impl AsRef<str>) -> OxenError {
        let err = format!("Broken link to parent commit: {}", commit_id.as_ref());
        OxenError::basic_str(err)
    }

    pub fn entry_does_not_exist(path: impl AsRef<Path>) -> OxenError {
        OxenError::ParsedResourceNotFound(Box::new(path.as_ref().into()))
    }

    pub fn file_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        let err = format!("File does not exist: {:?} error {:?}", path.as_ref(), error);
        OxenError::basic_str(err)
    }

    pub fn file_create_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        let err = format!(
            "Could not create file: {:?} error {:?}",
            path.as_ref(),
            error
        );
        OxenError::basic_str(err)
    }

    pub fn dir_create_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        let err = format!(
            "Could not create directory: {:?} error {:?}",
            path.as_ref(),
            error
        );
        OxenError::basic_str(err)
    }

    pub fn file_open_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        let err = format!("Could not open file: {:?} error {:?}", path.as_ref(), error,);
        OxenError::basic_str(err)
    }

    pub fn file_read_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        let err = format!("Could not read file: {:?} error {:?}", path.as_ref(), error,);
        OxenError::basic_str(err)
    }

    pub fn file_metadata_error(path: impl AsRef<Path>, error: std::io::Error) -> OxenError {
        let err = format!(
            "Could not get file metadata: {:?} error {:?}",
            path.as_ref(),
            error
        );
        OxenError::basic_str(err)
    }

    pub fn file_copy_error(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        err: impl Debug,
    ) -> OxenError {
        let err = format!(
            "File copy error: {err:?}\nCould not copy from `{:?}` to `{:?}`",
            src.as_ref(),
            dst.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn file_rename_error(
        src: impl AsRef<Path>,
        dst: impl AsRef<Path>,
        err: impl Debug,
    ) -> OxenError {
        let err = format!(
            "File rename error: {err:?}\nCould not move from `{:?}` to `{:?}`",
            src.as_ref(),
            dst.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn workspace_add_file_not_in_repo(path: impl AsRef<Path>) -> OxenError {
        let err = format!(
            "File is outside of the repo {:?}\n\nYou must specify a path you would like to add the file at with the -p flag.\n\n  oxen workspace add /path/to/file.png -p my-images/\n",
            path.as_ref()
        );
        OxenError::basic_str(err)
    }

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

    pub fn file_has_no_parent(path: impl AsRef<Path>) -> OxenError {
        let err = format!("File has no parent: {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn file_has_no_name(path: impl AsRef<Path>) -> OxenError {
        let err = format!("File has no file_name: {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn could_not_convert_path_to_str(path: impl AsRef<Path>) -> OxenError {
        let err = format!("File has no name: {:?}", path.as_ref());
        OxenError::basic_str(err)
    }

    pub fn local_revision_not_found(name: impl AsRef<str>) -> OxenError {
        let err = format!(
            "Local branch or commit reference `{}` not found",
            name.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn could_not_find_merge_conflict(path: impl AsRef<Path>) -> OxenError {
        let err = format!(
            "Could not find merge conflict for path: {:?}",
            path.as_ref()
        );
        OxenError::basic_str(err)
    }

    pub fn could_not_decode_value_for_key_error(key: impl AsRef<str>) -> OxenError {
        let err = format!("Could not decode value for key: {:?}", key.as_ref());
        OxenError::basic_str(err)
    }

    pub fn invalid_set_remote_url(url: impl AsRef<str>) -> OxenError {
        let err = format!("\nRemote invalid, must be fully qualified URL, got: {:?}\n\n  oxen config --set-remote origin https://hub.oxen.ai/<namespace>/<reponame>\n", url.as_ref());
        OxenError::basic_str(err)
    }

    pub fn invalid_file_type(file_type: impl AsRef<str>) -> OxenError {
        let err = format!("Invalid file type: {:?}", file_type.as_ref());
        OxenError::InvalidFileType(StringError::from(err))
    }

    pub fn column_name_already_exists(column_name: &str) -> OxenError {
        let err = format!("Column name already exists: {:?}", column_name);
        OxenError::ColumnNameAlreadyExists(StringError::from(err))
    }

    pub fn column_name_not_found(column_name: &str) -> OxenError {
        let err = format!("Column name not found: {:?}", column_name);
        OxenError::ColumnNameNotFound(StringError::from(err))
    }

    pub fn incompatible_schemas(schema: Schema) -> OxenError {
        OxenError::IncompatibleSchemas(Box::new(schema))
    }

    pub fn parse_error(value: impl AsRef<str>) -> OxenError {
        let err = format!("Parse error: {:?}", value.as_ref());
        OxenError::basic_str(err)
    }
}

// if you do not want to call .map_err, implement the std::convert::From trait
impl From<io::Error> for OxenError {
    fn from(error: io::Error) -> Self {
        OxenError::IO(error)
    }
}

impl From<String> for OxenError {
    fn from(error: String) -> Self {
        OxenError::Basic(StringError::from(error))
    }
}

impl From<toml::ser::Error> for OxenError {
    fn from(error: toml::ser::Error) -> Self {
        OxenError::TomlSer(error)
    }
}

impl From<toml::de::Error> for OxenError {
    fn from(error: toml::de::Error) -> Self {
        OxenError::TomlDe(error)
    }
}

impl From<http::uri::InvalidUri> for OxenError {
    fn from(error: http::uri::InvalidUri) -> Self {
        OxenError::URI(error)
    }
}

impl From<url::ParseError> for OxenError {
    fn from(error: url::ParseError) -> Self {
        OxenError::URL(error)
    }
}

impl From<serde_json::Error> for OxenError {
    fn from(error: serde_json::Error) -> Self {
        OxenError::JSON(error)
    }
}

impl From<std::str::Utf8Error> for OxenError {
    fn from(error: std::str::Utf8Error) -> Self {
        OxenError::UTF8Error(error)
    }
}

impl From<bincode::Error> for OxenError {
    fn from(error: bincode::Error) -> Self {
        OxenError::BinCodeError(error)
    }
}

impl From<r2d2::Error> for OxenError {
    fn from(error: r2d2::Error) -> Self {
        OxenError::R2D2Error(error)
    }
}

impl From<jwalk::Error> for OxenError {
    fn from(error: jwalk::Error) -> Self {
        OxenError::JwalkError(error)
    }
}

impl From<redis::RedisError> for OxenError {
    fn from(error: redis::RedisError) -> Self {
        OxenError::RedisError(error)
    }
}

impl From<glob::PatternError> for OxenError {
    fn from(error: glob::PatternError) -> Self {
        OxenError::PatternError(error)
    }
}

impl From<PolarsError> for OxenError {
    fn from(err: PolarsError) -> Self {
        OxenError::PolarsError(err)
    }
}

impl From<ArrowError> for OxenError {
    fn from(error: ArrowError) -> Self {
        OxenError::ArrowError(error)
    }
}

impl From<glob::GlobError> for OxenError {
    fn from(error: glob::GlobError) -> Self {
        OxenError::GlobError(error)
    }
}

impl From<reqwest::Error> for OxenError {
    fn from(error: reqwest::Error) -> Self {
        OxenError::HTTP(error)
    }
}

impl From<rocksdb::Error> for OxenError {
    fn from(error: rocksdb::Error) -> Self {
        OxenError::DB(error)
    }
}

impl From<duckdb::Error> for OxenError {
    fn from(error: duckdb::Error) -> Self {
        OxenError::DUCKDB(error)
    }
}

impl From<std::env::VarError> for OxenError {
    fn from(error: std::env::VarError) -> Self {
        OxenError::ENV(error)
    }
}

impl From<StripPrefixError> for OxenError {
    fn from(error: StripPrefixError) -> Self {
        OxenError::basic_str(format!("Error stripping prefix: {}", error))
    }
}
impl From<ParseIntError> for OxenError {
    fn from(error: ParseIntError) -> Self {
        OxenError::basic_str(error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for OxenError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        OxenError::basic_str(format!("UTF8 conversion error: {}", error))
    }
}

impl From<image::ImageError> for OxenError {
    fn from(error: image::ImageError) -> Self {
        OxenError::ImageError(error)
    }
}

impl From<rmp_serde::decode::Error> for OxenError {
    fn from(error: rmp_serde::decode::Error) -> Self {
        OxenError::RmpDecodeError(error)
    }
}
