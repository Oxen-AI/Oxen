use dotenvy::dotenv;
use dotenvy::from_filename;
use liboxen::api::requests::RepoNew;
use liboxen::config::UserConfig;
use liboxen::constants::OXEN_VERSION;
use liboxen::error::OxenError;
use liboxen::model::User;
use liboxen::model::merkle_tree::merkle_tree_node_cache;
use liboxen::model::metadata::metadata_image::ImgResize;

use liboxen::util;
use liboxen::util::telemetry;

// Imported as modules rather than as items: the `crate::`-rooted paths below — notably the utoipa
// `paths(...)` list — resolve through this crate's root.
use oxen_server::{app_data, auth, config, controllers, crash_diagnostics, metrics, routes};

extern crate liboxen;
extern crate log;
extern crate lru;

use actix_web::middleware::{Condition, DefaultHeaders, Logger};
use actix_web::{App, HttpServer, web};
use actix_web_httpauth::middleware::HttpAuthentication;
use thiserror::Error;

use oxen_server::middleware::{
    MetricsMiddleware, RequestIdMiddleware, RequestStartLogMiddleware, request_id,
};
use tracing_actix_web::TracingLogger;

// Note: These 'view' imports are all for the auto-generated docs with utoipa
use liboxen::model::metadata::{
    MetadataAudio, MetadataDir, MetadataImage, MetadataTabular, MetadataText, MetadataVideo,
    generic_metadata::GenericMetadata,
};
use liboxen::model::{Commit, CommitStats};
use liboxen::view::commit::CommitTreeValidationResponse;
use liboxen::view::compare::{
    CommitSide, CompareCommit, CompareCommits, CompareCommitsResponse, CompareDupes,
    CompareEntries, CompareEntryResponse, CompareTabular, CompareTabularResponse,
    TabularCompareBody, TabularCompareTargetBody,
};
use liboxen::view::data_frames::FromDirectoryRequest;
use liboxen::view::diff::{DirDiffStatus, DirDiffTreeSummary, DirTreeDiffResponse};
use liboxen::view::entries::{ListCommitEntryResponse, ResourceVersion};
use liboxen::view::entry_metadata::EMetadataEntryResponseView;
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

use sentry::integrations::tracing as sentry_tracing;
use tracing::level_filters::LevelFilter;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi};
use utoipa_swagger_ui::SwaggerUi;

use clap::{Parser, Subcommand};

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use liboxen::constants;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::config::Config;
use crate::config::storage_policy::StoragePolicyError;
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
        (name = "Repositories", description = "Repository management endpoints."),
        (
            name = "Workspace Data Frames",
            description = "Editing a data frame inside a workspace, including its schema and column metadata."
        )
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
        crate::controllers::workspaces::mergeability,
        crate::controllers::workspaces::commit,
        // Workspaces - changes
        crate::controllers::workspaces::changes::list_root,
        crate::controllers::workspaces::changes::list,
        crate::controllers::workspaces::changes::unstage,
        crate::controllers::workspaces::changes::unstage_many,
        // Workspaces - files
        crate::controllers::workspaces::files::get,
        crate::controllers::workspaces::files::add,
        crate::controllers::workspaces::files::add_version_files,
        crate::controllers::workspaces::files::rm_files,
        // Workspace Data Frames
        crate::controllers::workspaces::data_frames::put_schema_metadata,
        crate::controllers::workspaces::data_frames::columns::add_column_metadata,
        // Branches
        crate::controllers::branches::index,
        crate::controllers::branches::show,
        crate::controllers::branches::create,
        crate::controllers::branches::delete,
        crate::controllers::branches::update,
        crate::controllers::branches::maybe_create_merge,
        crate::controllers::branches::list_entry_versions,
        // Commits
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
            CommitSide, CompareCommit, CompareCommits, CompareCommitsResponse, CompareDupes,
            CompareEntries, CompareEntryResponse,
            CompareTabular, CompareTabularResponse, DirDiffStatus, DirDiffTreeSummary, DirTreeDiffResponse,
            TabularCompareBody, TabularCompareTargetBody,
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

    #[arg(
        long = "config-dir",
        global = true,
        help = "Directory for oxen's user and auth config files \
                (overrides $OXEN_CONFIG_DIR; defaults to ~/.config/oxen/)"
    )]
    config_dir: Option<PathBuf>,
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

        /// Optional path to a TOML config file controlling server-wide settings.
        #[arg(
            long = "config",
            help = "Path to a TOML config file controlling server-wide settings \
                    (currently: storage policy). When omitted, built-in defaults apply."
        )]
        config: Option<PathBuf>,

        /// Run the server in test mode. Not for production use.
        #[arg(
            short = 't',
            long = "test",
            help = "Run the server in test mode. Currently this only relaxes the import SSRF \
                    guard to allow loopback download targets so tests can serve fixtures from a \
                    local mock HTTP server. Do not use in production."
        )]
        test: bool,
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

    /// Report which repositories hold Merkle nodes predating the v0.25.0 on-disk format
    #[command(name = "scan-node-format")]
    ScanNodeFormat {
        #[arg(long = "namespace", help = "Limit the scan to a single namespace")]
        namespace: Option<String>,

        #[arg(
            long = "limit",
            help = "Stop after scanning this many repositories, for sampling a large fleet"
        )]
        limit: Option<usize>,
    },
}

/// Initialize Sentry crash reporting when `dsn` is set (from `SENTRY_DSN`). Hold the returned
/// guard for the process lifetime; `None` — no DSN — leaves Sentry disabled, so self-hosted and
/// OSS deployments report nothing.
fn init_sentry(dsn: Option<String>) -> Option<sentry::ClientInitGuard> {
    let dsn = dsn.filter(|dsn| !dsn.is_empty())?;
    let options = sentry::ClientOptions {
        release: Some(OXEN_VERSION.into()),
        environment: env::var("SENTRY_ENVIRONMENT").ok().map(Into::into),
        // A data server must never let Sentry attach request/user PII, and reports errors only —
        // no performance tracing.
        send_default_pii: false,
        traces_sample_rate: 0.0,
        attach_stacktrace: true,
        ..Default::default()
    };
    Some(sentry::init((dsn, options)))
}

/// The tracing layer that forwards events to Sentry, or `None` when Sentry is disabled, in which
/// case no event-processing cost is paid at all.
///
/// Keeps `sentry-tracing`'s default event mapping: an `error!` becomes an issue, `warn!` and `info!`
/// become breadcrumbs giving whatever issue follows some context, and `debug!` and below are
/// dropped. The level a failure is logged at is therefore what decides whether it alerts — see the
/// level convention on `OxenHttpError::error_response`. Spans are filtered out entirely: with
/// performance tracing disabled every span would build a Sentry transaction that is never sampled.
fn sentry_tracing_layer(enabled: bool) -> Option<telemetry::BoxedLayer> {
    if !enabled {
        return None;
    }
    Some(Box::new(sentry_tracing::layer().span_filter(|_| false)))
}

#[actix_web::main]
async fn main() {
    crash_diagnostics::install().expect("failed to install crash diagnostics");

    // Capture panics when a Sentry DSN is configured; held for the whole process so the client
    // flushes on exit. Sits after `crash_diagnostics::install`, whose panic hook Sentry's chains
    // onto, and before tracing init, which needs to know whether to compose the Sentry layer.
    let dsn = env::var("SENTRY_DSN").ok();
    let dsn_configured = dsn.as_ref().is_some_and(|dsn| !dsn.is_empty());
    let _sentry_guard = init_sentry(dsn);
    // `sentry::init` returns a guard even for a DSN it could not parse, so ask the client itself.
    let sentry_enabled = _sentry_guard
        .as_ref()
        .is_some_and(|guard| guard.is_enabled());
    // fail-fast if we cannot initialize logging
    let _tracing_guard = telemetry::init_tracing_with_layer(
        "oxen-server",
        LevelFilter::WARN,
        sentry_tracing_layer(sentry_enabled),
    )
    .expect("Failed to initialize tracing & logging for oxen-server.");
    // Logged here rather than next to `init_sentry` because no subscriber exists yet at that point.
    if dsn_configured && !sentry_enabled {
        log::error!("SENTRY_DSN is set but Sentry is disabled; no errors will be reported");
    }
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
    #[error("Failed to read config file {path}: {source}")]
    ConfigRead {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse config file {path}: {source}")]
    ConfigParse {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("{0}")]
    StoragePolicy(#[from] StoragePolicyError),
    #[error("No such namespace {namespace:?} under sync dir {sync_dir:?}")]
    NamespaceNotFound {
        namespace: String,
        sync_dir: PathBuf,
    },
    #[cfg(feature = "metrics")]
    #[error("Invalid OXEN_METRICS_PORT value: {0} (parsing error: {1})")]
    InvalidPort(String, std::num::ParseIntError),
    #[cfg(feature = "metrics")]
    #[error("Failed to start Prometheus metrics server: {0}")]
    Metrics(#[from] metrics_exporter_prometheus::BuildError),
}

/// Load the server's TOML config file. Missing path → built-in defaults.
fn load_server_config(path: Option<&Path>) -> Result<Config, ServerError> {
    let Some(path) = path else {
        return Ok(Config::default());
    };
    let contents = std::fs::read_to_string(path).map_err(|source| ServerError::ConfigRead {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_str(&contents).map_err(|source| ServerError::ConfigParse {
        path: path.to_path_buf(),
        source,
    })
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

    let cli = ServerCli::parse();
    if let Some(dir) = cli.config_dir {
        util::fs::set_oxen_config_dir(dir);
    }

    match cli.command {
        ServerCommand::Start {
            ip,
            port,
            auth,
            config,
            test,
        } => {
            let _metrics_guard = init_metrics()?;
            let server_config = load_server_config(config.as_deref())?;

            // KEEP as println! -- do not log!
            println!("🐂 v{VERSION}");
            println!("{SUPPORT}");

            // Fail fast if the configured S3 bucket is unreachable, rather than letting the first
            // request 500. Local-only servers carry no S3 opts and skip the probe.
            if let Some(s3_opts) = server_config.storage.s3() {
                log::info!("Verifying S3 bucket '{}' is reachable...", s3_opts.bucket);
                liboxen::storage::verify_s3_bucket_reachable(s3_opts).await?;
            }

            start(
                &ip,
                port,
                ServerOpts {
                    // TODO: why is this not checking the value of the env var?
                    disable_merkle_cache: env::var("OXEN_DISABLE_MERKLE_CACHE").is_ok(),
                    enable_auth: auth,
                    test_mode: test,
                },
                &sync_dir,
                server_config,
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

        ServerCommand::ScanNodeFormat { namespace, limit } => {
            scan_node_format(&sync_dir, namespace.as_deref(), limit)
        }
    }
}

/// Whether `path` is a directory and not a symlink to one.
///
/// Uses `metadata.is_dir()` rather than `path.is_dir()` to avoid following symlinks — Oxen does
/// not track them, and a symlinked namespace or repo would otherwise be walked as though it were
/// a second copy, counting the same repository twice.
fn is_real_dir(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
}

/// Walk repositories under `sync_dir` and report which hold pre-v0.25.0 Merkle nodes.
///
/// Read-only. Repositories the current build refuses to open at all are counted separately from
/// those that open but contain unreadable nodes: the two have different remedies, and only the
/// second is invisible to a `min_version` check.
fn scan_node_format(
    sync_dir: &Path,
    namespace: Option<&str>,
    limit: Option<usize>,
) -> Result<(), ServerError> {
    let namespaces = match namespace {
        // Fail loudly on a namespace that isn't there. Left to the walk below it would read_dir,
        // fail, and be skipped, printing a zero summary indistinguishable from a clean namespace
        // — the one output a caller must be able to trust.
        Some(one) => {
            let namespace_dir = sync_dir.join(one);
            if !is_real_dir(&namespace_dir) {
                return Err(ServerError::NamespaceNotFound {
                    namespace: one.to_string(),
                    sync_dir: sync_dir.to_path_buf(),
                });
            }
            vec![namespace_dir]
        }
        None => {
            let mut dirs: Vec<PathBuf> = std::fs::read_dir(sync_dir)?
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .filter(|path| is_real_dir(path))
                .collect();
            dirs.sort();
            dirs
        }
    };

    // Outcomes are counted apart because they have different remedies: pre-0.25 repos get
    // migrated, damaged and unscannable ones need a person, and unopenable ones are the
    // `min_version` population a config sweep already finds. `unlistable` counts namespaces
    // rather than repos — it is the one that says the totals below are incomplete.
    let (mut scanned, mut affected, mut damaged, mut unopenable, mut unscannable, mut unlistable) =
        (0usize, 0usize, 0usize, 0usize, 0usize, 0usize);
    let mut totals: BTreeMap<String, usize> = BTreeMap::new();

    'outer: for namespace_dir in namespaces {
        // A namespace that cannot be listed hides an unknown number of repositories, so skipping
        // it quietly would understate every total below with nothing to say so. Reported and
        // counted rather than fatal: unlike the explicit-namespace case, which is a caller
        // mistake with nothing left to do, one bad namespace should not cost the whole run.
        let entries = match std::fs::read_dir(&namespace_dir) {
            Ok(entries) => entries,
            Err(err) => {
                unlistable += 1;
                let label = namespace_dir
                    .strip_prefix(sync_dir)
                    .unwrap_or(&namespace_dir)
                    .display();
                // KEEP as println! -- do not log!
                println!("{label}\tcannot list namespace: {err}");
                continue;
            }
        };
        let mut repo_dirs: Vec<PathBuf> = entries
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|path| is_real_dir(path) && is_real_dir(&path.join(constants::OXEN_HIDDEN_DIR)))
            .collect();
        repo_dirs.sort();

        for repo_dir in repo_dirs {
            if limit.is_some_and(|max| scanned >= max) {
                break 'outer;
            }
            scanned += 1;
            let label = repo_dir
                .strip_prefix(sync_dir)
                .unwrap_or(&repo_dir)
                .display()
                .to_string();

            let repo = match LocalRepository::from_dir(&repo_dir) {
                Ok(repo) => repo,
                Err(err) => {
                    unopenable += 1;
                    // KEEP as println! -- do not log!
                    println!("{label}\tcannot open: {err}");
                    continue;
                }
            };

            // One unreadable repo must not end a fleet-wide scan: the caller loses every count
            // gathered so far, and a truncated run looks much like a short one.
            let report = match repositories::fsck::scan_node_format(&repo) {
                Ok(report) => report,
                Err(err) => {
                    unscannable += 1;
                    // KEEP as println! -- do not log!
                    println!("{label}\tcannot scan: {err}");
                    continue;
                }
            };

            let has_legacy = report.is_affected();
            let has_damage = report.undecodable > 0;
            if !has_legacy && !has_damage {
                continue;
            }
            if has_legacy {
                affected += 1;
            }
            if has_damage {
                damaged += 1;
            }
            let mut by_type: Vec<String> = report
                .pre_v025
                .iter()
                .map(|(dtype, count)| {
                    *totals.entry(format!("{dtype:?}")).or_default() += count;
                    format!("{dtype:?}={count}")
                })
                .collect();
            by_type.sort();
            if report.undecodable > 0 {
                by_type.push(format!("undecodable={}", report.undecodable));
            }
            // KEEP as println! -- do not log!
            println!(
                "{label}\tnodes={}\tpre_v0_25={}\t{}",
                report.total_nodes,
                report.pre_v025_total(),
                by_type.join(" ")
            );
        }
    }

    // KEEP as println! -- do not log!
    println!(
        "\nscanned={scanned} pre_v0_25_repos={affected} damaged_repos={damaged} \
         could_not_open={unopenable} could_not_scan={unscannable} \
         could_not_list_namespaces={unlistable}"
    );
    if !totals.is_empty() {
        let breakdown: Vec<String> = totals
            .iter()
            .map(|(dtype, count)| format!("{dtype}={count}"))
            .collect();
        println!("pre-v0.25.0 nodes by type: {}", breakdown.join(" "));
    }
    Ok(())
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

/// CLI/env-derived flags collected at startup, distinct from the TOML-loaded
/// [`config::Config`] (which carries settings from disk).
#[derive(Debug, Clone)]
struct ServerOpts {
    disable_merkle_cache: bool,
    enable_auth: bool,
    /// Test mode (`--test`): relaxes the import SSRF guard to allow loopback targets. Never
    /// enabled in production.
    test_mode: bool,
}

async fn start(
    host: &str,
    port: u16,
    opts: ServerOpts,
    sync_dir: &Path,
    config: Config,
) -> Result<(), std::io::Error> {
    let ServerOpts {
        disable_merkle_cache,
        enable_auth,
        test_mode,
    } = opts;

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

    // Install DuckDB extensions before actix hands out any request threads. oxen-server
    // is a single-instance deploy, so every restart drains pent-up client retries into a
    // burst of concurrent first-use requests; two of them triggering autoload at once
    // race on the extension file's temp→final rename (`Could not move file: Access is
    // denied` on Windows, similar timing hazards elsewhere).
    match liboxen::core::df::duckdb_setup::preload_extensions() {
        Ok(()) => log::info!("DuckDB extensions preloaded"),
        Err(e) => log::error!("Failed to preload DuckDB extensions: {e}"),
    }

    let data = app_data::OxenAppData {
        path: PathBuf::from(sync_dir),
        config,
        test_mode,
    };

    {
        let running = format!("Running on {host}:{port}");
        eprintln!("{running}");
        log::info!("{running}");
    }
    log::info!("Syncing to directory: {}", sync_dir.display());

    // Actix handles SIGTERM/SIGINT/SIGQUIT by default and drains in-flight
    // requests within `shutdown_timeout`. We cap that here so that, after
    // `.run().await` returns, there is still time to perform other cleanup
    // operations before a supervisor force-kills the process.
    const ACTIX_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

    // actix's default HTTP/1 keep-alive is 5s. Keep it comfortably above the OxenHub
    // Finch client's idle-eviction window so the client always retires an idle pooled
    // connection before the server reaps it — a reused server-closed socket surfaces
    // to the client as a dropped connection (502 Bad Gateway).
    const ACTIX_KEEP_ALIVE_SECS: u64 = 75;

    let server_result = HttpServer::new(move || {
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
            .wrap(
                Logger::new(
                    "end %a \"%r\" %s %b \"%{Referer}i\" \"%{User-Agent}i\" %T req=%{request_id}xo",
                )
                .custom_response_replace("request_id", |res| request_id(res.request())),
            )
            .wrap(RequestStartLogMiddleware)
            // RequestId must stay outer of the Logger/RequestStartLog above (actix runs the last
            // .wrap outermost) so the request-id extension those two read is populated before them.
            .wrap(RequestIdMiddleware)
            .wrap(MetricsMiddleware)
            .wrap(TracingLogger::default())
            // Outermost: a per-request Sentry hub so a captured panic carries request context.
            // Server-error auto-capture is disabled here.
            .wrap(
                sentry_actix::Sentry::builder()
                    .capture_server_errors(false)
                    .finish(),
            )
    })
    .keep_alive(Duration::from_secs(ACTIX_KEEP_ALIVE_SECS))
    .bind((host.to_owned(), port))?
    .shutdown_timeout(ACTIX_SHUTDOWN_TIMEOUT_SECS)
    .run()
    .await;

    log::info!("HTTP server stopped — flushing DuckDB connection cache");
    liboxen::core::db::data_frames::df_db::flush_all_df_db_connections();
    log::info!("DuckDB connection cache flushed — exiting");

    server_result
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::storage_policy::StoragePolicy;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn load_server_config_returns_default_when_no_path() {
        let file = load_server_config(None).unwrap();
        assert_eq!(file.storage, StoragePolicy::default());
    }

    #[test]
    fn load_server_config_reads_real_file() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            r#"
            [storage]
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
            s3_region = "us-west-1"
            "#
        )
        .unwrap();
        let file = load_server_config(Some(f.path())).unwrap();
        let expected: StoragePolicy = toml::from_str(
            r#"
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
            s3_region = "us-west-1"
            "#,
        )
        .unwrap();
        assert_eq!(file.storage, expected);
    }

    #[test]
    fn load_server_config_surfaces_io_error() {
        let missing = std::path::Path::new("/no/such/file/oxen-server.toml");
        let err = load_server_config(Some(missing)).unwrap_err();
        assert!(matches!(err, ServerError::ConfigRead { .. }));
    }

    #[test]
    fn load_server_config_surfaces_parse_error() {
        let mut f = NamedTempFile::new().unwrap();
        // Validation failure (S3 without bucket) surfaces as a parse error because the
        // `TryFrom` impl fires during `toml::from_str`.
        writeln!(f, "[storage]\nbackends = [\"s3\"]\n").unwrap();
        let err = load_server_config(Some(f.path())).unwrap_err();
        let ServerError::ConfigParse { source, .. } = &err else {
            panic!("expected ConfigParse, got {err:?}");
        };
        assert!(
            source.to_string().contains("s3 bucket cannot be empty"),
            "expected EmptyS3Bucket message, got: {source}",
        );
    }

    #[test]
    fn init_sentry_is_disabled_without_dsn() {
        assert!(init_sentry(None).is_none());
        assert!(init_sentry(Some(String::new())).is_none());
    }

    #[test]
    fn sentry_tracing_layer_is_absent_without_sentry() {
        assert!(sentry_tracing_layer(false).is_none());
        assert!(sentry_tracing_layer(true).is_some());
    }
}
