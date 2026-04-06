use std::path::{Path, PathBuf};

use thiserror::Error;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::{SubscriberInitExt, TryInitError};
use tracing_subscriber::{EnvFilter, Layer, Registry};

/// An error that can occur during logging or metrics initialization.
#[derive(Debug, Error)]
pub enum TelemetryError {
    #[error("OXEN_LOG_DIR set but is empty, cannot enable JSON file logging.")]
    EmptyLogDir,
    #[error("Requested JSON file logging cannot be enabled because OXEN_LOG_DIR is a file: {0}")]
    LogDirIsFile(PathBuf),
    #[error("Failed to create log directory ({0}): {1}")]
    CreateLogDir(PathBuf, std::io::Error),
    #[error("Failed to initialize tracing: {0}")]
    InitFail(#[from] TryInitError),
}

/// Initialize tracing with the `tracing-log` bridge.
///
/// **Stderr** (always): human-readable formatted output, like `env_logger`.
///
/// **File** (opt-in via `OXEN_LOG_DIR`): JSON-per-line output to a rolling
/// daily log file. The env var accepts a directory path — rolling files are
/// written there (e.g. `/var/log/oxen`).
///
/// Filter level is controlled by `RUST_LOG`. The filter applies to both stderr
/// and file output.
///
/// Returns `Some(WorkerGuard)` when file logging is active. The caller
/// **must** hold the guard in a named binding for the lifetime of the
/// application — dropping it flushes the non-blocking writer. When file
/// logging is not enabled, returns `None`.
pub fn init_tracing(
    app_name: &str,
    default: LevelFilter,
) -> Result<Option<WorkerGuard>, TelemetryError> {
    let log_level = log_env_filter(default);

    // Always: human-readable stderr output
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);

    let maybe_log_dir = match std::env::var("OXEN_LOG_DIR").ok() {
        Some(log_dir) => {
            let created_log_dir = create_log_dir(&log_dir)?;
            Some(created_log_dir)
        }
        None => None,
    };

    let (maybe_json_layer, maybe_worker_guard) = if let Some(log_dir) = maybe_log_dir {
        let (jl, wg) = json_file_logging(app_name, &log_dir);
        (Some(jl), Some(wg))
    } else {
        (None, None)
    };

    // .try_init() also installs the tracing-log bridge (forwarding log::* calls into tracing)
    // when the `tracing-log` feature is enabled.
    tracing_subscriber::registry()
        .with(maybe_json_layer)
        .with(stderr_layer)
        .with(log_level)
        .try_init()?;

    Ok(maybe_worker_guard)
}

/// Create an [`EnvFilter`] from `RUST_LOG`, falling back to the default log level if not set.
fn log_env_filter(default: LevelFilter) -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::builder()
            .with_default_directive(default.into())
            .parse_lossy("")
    })
}

/// Accepts the OXEN_LOG_DIR's value and ensures it is a valid directory.
/// Returns None if it shouldn't be enabled. Panics on error!
fn create_log_dir(oxen_log_dir: &str) -> Result<PathBuf, TelemetryError> {
    let oxen_log_dir = oxen_log_dir.trim();
    if oxen_log_dir.is_empty() {
        Err(TelemetryError::EmptyLogDir)
    } else {
        let log_dir = PathBuf::from(oxen_log_dir);
        if log_dir.is_file() {
            Err(TelemetryError::LogDirIsFile(log_dir))
        } else {
            match std::fs::create_dir_all(&log_dir) {
                Ok(()) => Ok(log_dir),
                Err(e) => Err(TelemetryError::CreateLogDir(log_dir, e)),
            }
        }
    }
}

/// Configure ND-JSON file logging with daily log rotation in the given log directory.
fn json_file_logging(app_name: &str, log_dir: &Path) -> (impl Layer<Registry>, WorkerGuard) {
    let file_appender = tracing_appender::rolling::daily(log_dir, app_name);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let layer = tracing_subscriber::fmt::layer()
        .json()
        .with_writer(non_blocking)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(true)
        .with_line_number(true);

    (layer, guard)
}
