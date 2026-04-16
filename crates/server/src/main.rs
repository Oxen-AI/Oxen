use dotenvy::dotenv;
use dotenvy::from_filename;
use liboxen::config::UserConfig;
use liboxen::constants::OXEN_VERSION;
use liboxen::error::OxenError;
use liboxen::model::User;
use liboxen::model::merkle_tree::merkle_tree_node_cache;
use liboxen::model::metadata::metadata_image::ImgResize;

use liboxen::util;

pub mod app_data;
pub mod auth;
pub mod controllers;
pub mod errors;
pub mod helpers;
pub mod middleware;
pub mod params;
pub mod routes;
pub mod services;
#[cfg(test)]
pub(crate) mod test;

pub(crate) mod metrics;

extern crate liboxen;
extern crate log;
extern crate lru;

use actix_web::middleware::{Condition, DefaultHeaders, Logger};
use actix_web::{App, HttpServer, web};
use actix_web_httpauth::middleware::HttpAuthentication;
use thiserror::Error;

use middleware::{MetricsMiddleware, RequestIdMiddleware};
use tracing_actix_web::TracingLogger;

// Note: These 'view' imports are all for the auto-generated docs with utoipa
use liboxen::model::metadata::{
    MetadataAudio, MetadataDir, MetadataImage, MetadataTabular, MetadataText, MetadataVideo,
    generic_metadata::GenericMetadata,
};
use liboxen::model::{Commit, CommitStats, RepoNew};
use liboxen::view::commit::CommitTreeValidationResponse;
use liboxen::view::compare::{
    CompareCommits, CompareCommitsResponse, CompareDupes, CompareEntries, CompareEntryResponse,
    CompareTabular, CompareTabularResponse, TabularCompareBody, TabularCompareTargetBody,
};
use liboxen::view::data_frames::FromDirectoryRequest;
use liboxen::view::diff::{DirDiffStatus, DirDiffTreeSummary, DirTreeDiffResponse};
use liboxen::view::entries::{ListCommitEntryResponse, ResourceVersion};
use liboxen::view::entry_metadata::EMetadataEntryResponseView;
use liboxen::view::fork::{ForkRequest, ForkStartResponse, ForkStatus};
use liboxen::view::merge::{
    MergeConflictFile, MergeResult, MergeSuccessResponse, Mergeable, MergeableResponse,
};
use liboxen::view::repository::{
    DataTypeView, RepositoryCreationResponse, RepositoryCreationView, RepositoryDataTypesResponse,
    RepositoryDataTypesView, RepositoryListView, RepositoryStatsResponse, RepositoryStatsView,
};
use liboxen::view::tree::merkle_hashes::MerkleHashes;
use liboxen::view::versions::{VersionFile, VersionFileResponse};
use liboxen::view::workspaces::{ListWorkspaceResponseView, NewWorkspace, WorkspaceResponse};
use liboxen::view::{
    CommitEntryVersion, CommitResponse, CommitStatsResponse, DataTypeCount, ErrorFileInfo,
    ErrorFilesResponse, FilePathsResponse, FileWithHash, ListCommitResponse,
    ListNamespacesResponse, ListRepositoryResponse, MerkleHashesResponse, NamespaceResponse,
    NamespaceView, PaginatedCommits, PaginatedEntryVersions, PaginatedEntryVersionsResponse,
    ParseResourceResponse, RepositoryResponse, RepositoryView, RootCommitResponse, StatusMessage,
};

use tracing::level_filters::LevelFilter;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

use clap::{Parser, Subcommand};

use std::env;
use std::path::{Path, PathBuf};

use crate::metrics::MetricsGuard;

const VERSION: &str = liboxen::constants::OXEN_VERSION;

const ABOUT: &str = "Oxen Server is the storage backend for Oxen, the AI and machine learning data management toolchain";

const SUPPORT: &str = "
    📖 Documentation on running oxen-server can be found at:
            https://docs.oxen.ai/getting-started/oxen-server

    💬 For more support, or to chat with the Oxen team, join our Discord:
            https://discord.gg/s3tBEn7Ptg
";

const START_SERVER_USAGE: &str = "Usage: `oxen-server start -i 0.0.0.0 -p 3000`";

// Exports for the utoipa docs
// To add new endpoints to the docs, register their respective controller modules and schemas below
// TODO: we should be able to automatically discover these,
// see: https://github.com/juhaku/utoipa/blob/master/utoipa-actix-web/README.md
// If that doesn't work, we should break these out into separate schemas in the
// corresponding 'services' module and use the 'nest' attribute to include them
// in the top-level schema
// see: https://docs.rs/utoipa/latest/utoipa/derive.OpenApi.html#nest-attribute-syntax
#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "Namespaces", description = "Namespace management endpoints"),
        (name = "Repositories", description = "Repository management endpoints.")
    ),
    paths(
        // Health
        crate::controllers::oxen_version::index,
        // Namespaces
        crate::controllers::namespaces::index,
        crate::controllers::namespaces::show,
        // Repositories
        crate::controllers::repositories::index,
        crate::controllers::repositories::show,
        crate::controllers::repositories::create,
        crate::controllers::repositories::delete,
        crate::controllers::repositories::stats,
        crate::controllers::repositories::update_size,
        crate::controllers::repositories::get_size,
        crate::controllers::repositories::transfer_namespace,
        // Workspaces
        crate::controllers::workspaces::get_or_create,
        crate::controllers::workspaces::get,
        crate::controllers::workspaces::list,
        crate::controllers::workspaces::clear,
        crate::controllers::workspaces::delete,
        crate::controllers::workspaces::update_metadata,
        crate::controllers::workspaces::mergeability,
        crate::controllers::workspaces::commit,
        // Workspaces - changes
        crate::controllers::workspaces::changes::unstage,
        crate::controllers::workspaces::changes::unstage_many,
        // Workspaces - files
        crate::controllers::workspaces::files::get,
        crate::controllers::workspaces::files::add,
        crate::controllers::workspaces::files::add_version_files,
        crate::controllers::workspaces::files::rm_files,
        // Branches
        crate::controllers::branches::index,
        crate::controllers::branches::show,
        crate::controllers::branches::create,
        crate::controllers::branches::delete,
        crate::controllers::branches::update,
        crate::controllers::branches::maybe_create_merge,
        crate::controllers::branches::list_entry_versions,
        // Commits
        crate::controllers::commits::index,
        crate::controllers::commits::history,
        crate::controllers::commits::list_all,
        crate::controllers::commits::list_missing,
        crate::controllers::commits::list_missing_files,
        crate::controllers::commits::mark_commits_as_synced,
        crate::controllers::commits::show,
        crate::controllers::commits::parents,
        crate::controllers::commits::download_commits_db,
        crate::controllers::commits::download_dir_hashes_db,
        crate::controllers::commits::download_commit_entries_db,
        crate::controllers::commits::create,
        crate::controllers::commits::upload_chunk,
        crate::controllers::commits::upload_tree,
        crate::controllers::commits::root_commit,
        crate::controllers::commits::upload,
        crate::controllers::commits::complete,
        // Merge
        crate::controllers::merger::show,
        crate::controllers::merger::merge,
        // Diff
        crate::controllers::diff::commits,
        crate::controllers::diff::entries,
        crate::controllers::diff::dir_tree,
        crate::controllers::diff::dir_entries,
        crate::controllers::diff::file,
        crate::controllers::diff::create_df_diff,
        crate::controllers::diff::update_df_diff,
        crate::controllers::diff::get_df_diff,
        crate::controllers::diff::delete_df_diff,
        crate::controllers::diff::get_derived_df,
        // Fork
        crate::controllers::fork::fork,
        crate::controllers::fork::get_status,
        // Files (Repository)
        crate::controllers::file::get,
        crate::controllers::file::put,
        crate::controllers::file::delete,
        crate::controllers::file::mv,
        // Import
        crate::controllers::import::upload_zip,
        crate::controllers::import::import,
        // Export
        crate::controllers::export::download_zip,
        // DataFrames
        crate::controllers::data_frames::get,
        crate::controllers::data_frames::index,
        crate::controllers::data_frames::from_directory,
        // Directories
        crate::controllers::dir::get,
        // Metadata
        crate::controllers::metadata::file,
        crate::controllers::metadata::update_metadata,
        // Versions
        crate::controllers::versions::metadata,
        crate::controllers::versions::download,
        crate::controllers::versions::batch_download,
        crate::controllers::versions::batch_upload,
    ),
    components(
        // TODO: I'm not sure if these are all necessary to include
        schemas(
            // Misc
            StatusMessage,
            ParseResourceResponse,
            ImgResize,
            // Namespaces Schemas
            ListNamespacesResponse,
            NamespaceResponse,
            NamespaceView,
            // Repository Schemas
            ListRepositoryResponse, RepositoryResponse, RepositoryView,
            RepositoryCreationResponse, RepositoryCreationView, RepositoryDataTypesResponse,
            RepositoryDataTypesView, RepositoryListView, RepositoryStatsResponse,
            RepositoryStatsView, DataTypeView, DataTypeCount,
            RepoNew, User,
            // Commit Schemas
            CommitResponse, ListCommitResponse, PaginatedCommits, RootCommitResponse,
            MerkleHashesResponse, MerkleHashes, ListCommitEntryResponse, Commit,
            CommitStatsResponse, CommitStats, CommitTreeValidationResponse,
            // Workspace Schemas
            ListWorkspaceResponseView, NewWorkspace, WorkspaceResponse, MergeableResponse,
            // Merge Schemas
            MergeSuccessResponse, MergeResult, Mergeable, MergeConflictFile,
            // Compare Schemas
            CompareCommits, CompareCommitsResponse, CompareDupes, CompareEntries, CompareEntryResponse,
            CompareTabular, CompareTabularResponse, DirDiffStatus, DirDiffTreeSummary, DirTreeDiffResponse,
            TabularCompareBody, TabularCompareTargetBody,
            // Fork Schemas
            ForkRequest, ForkStartResponse, ForkStatus,
            // File/Entry Schemas
            CommitEntryVersion, ResourceVersion, PaginatedEntryVersions, PaginatedEntryVersionsResponse,
            FilePathsResponse, ErrorFilesResponse, ErrorFileInfo, FileWithHash,
            // Upload & Request Bodies
            crate::controllers::workspaces::files::FileUpload,
            crate::controllers::file::FileUploadBody,
            crate::controllers::import::ZipUploadBody,
            crate::controllers::import::ImportFileBody,
            FromDirectoryRequest,
            // Metadata Schemas
            EMetadataEntryResponseView,
            GenericMetadata, MetadataDir, MetadataText, MetadataImage,
            MetadataVideo, MetadataAudio, MetadataTabular,
            // Version Schemas,
            VersionFile, VersionFileResponse,
        ),
    ),
    modifiers(
        &SecurityAddon
    ),
    servers(
        (url = "https://hub.oxen.ai", description = "Production API"),
        (url = "http://localhost:3000", description = "Local Development")
    ),
    security(
        ("api_key" = [])
    ),
)]
struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.as_mut().unwrap();
        components.add_security_scheme(
            "api_key",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

#[derive(Parser)]
#[command(version=VERSION, about=ABOUT, long_about=format!("{ABOUT}\n{SUPPORT}"), subcommand_required=true, arg_required_else_help=true, allow_external_subcommands=true)]
struct ServerCli {
    #[command(subcommand)]
    command: ServerCommand,
}

/// All server CLI subcommands.
#[derive(Subcommand)]
enum ServerCommand {
    /// Starts the server on the given host and port
    #[command(name = "start", override_usage=START_SERVER_USAGE)]
    Start {
        /// The server's IP address.
        #[arg(
            short = 'i',
            long = "ip",
            default_value = "0.0.0.0",
            help = "What host to bind the server to"
        )]
        ip: String,

        /// The port to serve from.
        #[arg(
            short = 'p',
            long = "port",
            default_value = "3000",
            help = "What port to bind the server to"
        )]
        port: u16,

        /// Whether or not to use auth on the routes. Defaults to off.
        #[arg(
            short = 'a',
            long = "auth",
            help = "Start the server with token-based authentication enforced"
        )]
        auth: bool,
    },

    /// Create a new user in the server and output the config file for that user
    #[command(name = "add-user")]
    AddUser {
        #[arg(
            short = 'e',
            long = "email",
            required = true,
            help = "User's email address"
        )]
        email: String,

        #[arg(
            short = 'n',
            long = "name",
            required = true,
            help = "User's name that will show up in the commits"
        )]
        name: String,

        #[arg(
            short = 'o',
            long = "output",
            default_value = "user_config.toml",
            help = "Where to write the output config file to give to the user"
        )]
        output: PathBuf,
    },
}

#[actix_web::main]
async fn main() {
    // fail-fast if we cannot initialize logging
    let _tracing_guard = util::telemetry::init_tracing("oxen-server", LevelFilter::WARN)
        .expect("Failed to initialize tracing & logging for oxen-server.");
    // We want to show the error's display(), not the debug() representation.
    // actix_web::main() will show the error's debug() representation.
    if let Err(e) = server().await {
        log::error!("{e}");
    }
}

#[derive(Debug, Error)]
enum ServerError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Oxen(#[from] OxenError),
    #[cfg(feature = "metrics")]
    #[error("Invalid OXEN_METRICS_PORT value: {0} (parsing error: {1})")]
    InvalidPort(String, std::num::ParseIntError),
    #[cfg(feature = "metrics")]
    #[error("Failed to start Prometheus metrics server: {0}")]
    Metrics(#[from] metrics_exporter_prometheus::BuildError),
}

/// The actual main oxen-server loop.
async fn server() -> Result<(), ServerError> {
    dotenv().ok();

    match from_filename(Path::new("src").join("server").join("env.local")) {
        Ok(_) => log::debug!("Loaded .env file from current directory"),
        Err(e) => log::debug!("Failed to load .env file: {e}"),
    }

    util::perf::init_perf_logging();

    let sync_dir = match env::var("SYNC_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => PathBuf::from("data"),
    };

    match ServerCli::parse().command {
        ServerCommand::Start { ip, port, auth } => {
            let _metrics_guard = init_metrics()?;

            // KEEP as println! -- do not log!
            println!("🐂 v{VERSION}");
            println!("{SUPPORT}");

            start(
                &ip,
                port,
                &ServerConfig {
                    // TODO: why is this not checking the value of the env var?
                    disable_merkle_cache: env::var("OXEN_DISABLE_MERKLE_CACHE").is_ok(),
                    enable_auth: auth,
                },
                &sync_dir,
            )
            .await?;
            Ok(())
        }

        ServerCommand::AddUser {
            email,
            name,
            output,
        } => {
            log::debug!("Saving to sync dir: {sync_dir:?}");
            let token = add_user(&email, &name, output.as_path(), &sync_dir)?;
            // KEEP as println! -- do not log!
            println!(
                "User access token created:\n\n{token}\n\nTo give user access have them run the command `oxen config --auth <HOST> <TOKEN>`"
            );
            Ok(())
        }
    }
}

/// Initialize the Prometheus metrics server on the port specified by `OXEN_METRICS_PORT`.
/// Metrics are **opt-in**: if the variable is not set, no metrics server is started.
/// Returns `Ok(None)` if `OXEN_METRICS_PORT` is unset or `'off'`.
///
/// # Errors
///   - `OXEN_METRICS_PORT` is set to a value that cannot be parsed as a `u16`
///   - The port is already bound by another process
///   - The Prometheus exporter fails to start
///
/// Callers should propagate or handle the returned error.
#[cfg(feature = "metrics")]
fn init_metrics() -> Result<Option<MetricsGuard>, ServerError> {
    let enable_metrics = match env::var("OXEN_METRICS_PORT").as_deref() {
        Ok(val) if val.to_lowercase() == "off" => {
            log::info!("Prometheus metrics explicitly disabled (OXEN_METRICS_PORT=off).");
            None
        }
        Ok(val) => {
            let port: u16 = val
                .parse()
                .map_err(|e| ServerError::InvalidPort(val.to_string(), e))?;
            Some(port)
        }
        // Not set: opt-in only, no metrics server
        Err(_) => None,
    };

    if let Some(port) = enable_metrics {
        let guard = crate::metrics::init_metrics_prometheus(port)?;
        log::info!(
            "Prometheus metrics at http://0.0.0.0:{port}/metrics \
             (set OXEN_METRICS_PORT to change, OXEN_METRICS_PORT='off' to disable)"
        );
        Ok(Some(guard))
    } else {
        Ok(None)
    }
}

/// Returns `Ok(None)` when the "metrics" feature is not enabled.
#[cfg(not(feature = "metrics"))]
fn init_metrics() -> Result<Option<MetricsGuard>, ServerError> {
    if let Ok(val) = env::var("OXEN_METRICS_PORT")
        && !val.eq_ignore_ascii_case("off")
    {
        log::error!(
            "OXEN_METRICS_PORT is set but the 'metrics' feature is not enabled. \
                 Re-compile with `--features metrics` to enable metrics collection and the \
                 Prometheus-compatible /metrics endpoint. (Ignoring.)"
        );
    }
    Ok(None)
}

#[derive(Debug, Clone)]
struct ServerConfig {
    disable_merkle_cache: bool,
    enable_auth: bool,
}

async fn start(
    host: &str,
    port: u16,
    config: &ServerConfig,
    sync_dir: &Path,
) -> Result<(), std::io::Error> {
    let ServerConfig {
        disable_merkle_cache,
        enable_auth,
    } = *config;

    // Configure merkle tree node caching
    if disable_merkle_cache {
        log::info!("Merkle tree node caching disabled");
    } else {
        log::info!("Merkle tree node caching enabled");
        merkle_tree_node_cache::enable();
        log::info!(
            "Merkle tree node cache size: {}",
            merkle_tree_node_cache::CACHE_SIZE.get()
        );
    }

    let data = app_data::OxenAppData::new(PathBuf::from(sync_dir));

    {
        let running = format!("Running on {host}:{port}");
        eprintln!("{running}");
        log::info!("{running}");
    }
    log::info!("Syncing to directory: {}", sync_dir.display());

    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .route(
                "/api/version",
                web::get().to(controllers::oxen_version::index),
            )
            .route(
                "/api/min_version",
                web::get().to(controllers::oxen_version::min_version),
            )
            .route("/api/health", web::get().to(controllers::health::index))
            .route(
                "/api/namespaces",
                web::get().to(controllers::namespaces::index),
            )
            .route(
                "/api/namespaces/{namespace}",
                web::get().to(controllers::namespaces::show),
            )
            .route(
                "/api/migrations/{migration_tstamp}",
                web::get().to(controllers::migrations::list_unmigrated),
            )
            .wrap(Condition::new(
                enable_auth,
                HttpAuthentication::bearer(auth::validator::validate),
            ))
            .service(
                SwaggerUi::new("/swagger-ui/{_:.*}")
                    .url("/api/_spec/oxen_server_openapi.json", ApiDoc::openapi()),
            )
            .service(web::scope("/api/repos").configure(routes::config))
            .default_service(web::route().to(controllers::not_found::index))
            .wrap(DefaultHeaders::new().add(("oxen-version", OXEN_VERSION)))
            .wrap(Logger::new(
                "%a \"%r\" %s %b \"%{Referer}i\" \"%{User-Agent}i\" %T %{x-oxen-request-id}o",
            ))
            .wrap(RequestIdMiddleware)
            .wrap(MetricsMiddleware)
            .wrap(TracingLogger::default())
    })
    .bind((host.to_owned(), port))?
    .run()
    .await
}

/// Creates the user and returns their auth token.
fn add_user(email: &str, name: &str, output: &Path, sync_dir: &Path) -> Result<String, OxenError> {
    let keygen = auth::access_keys::AccessKeyManager::new(sync_dir)?;
    let (user, token) = keygen.create(&User {
        name: name.to_string(),
        email: email.to_string(),
    })?;

    let cfg = UserConfig::from_user(&user);
    cfg.save(output)?;

    Ok(token)
}
