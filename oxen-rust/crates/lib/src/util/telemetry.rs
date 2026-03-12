use std::path::PathBuf;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initialize tracing with the `tracing-log` bridge.
///
/// **Stderr** (always): human-readable formatted output, like `env_logger`.
///
/// **File** (opt-in via `OXEN_LOG_FILE`): JSON-per-line output to a rolling
/// daily log file. The env var accepts:
///   - A directory path — rolling files are written there (e.g. `/var/log/oxen`)
///   - `"1"` or `"true"` — uses the default directory `~/.oxen/logs/`
///
/// Filter level is controlled by `RUST_LOG` (default: `info`). The filter
/// applies to both stderr and file output.
///
/// Returns `Some(WorkerGuard)` when file logging is active. The caller
/// **must** hold the guard in a named binding for the lifetime of the
/// application — dropping it flushes the non-blocking writer. When file
/// logging is not enabled, returns `None`.
pub fn init_tracing(app_name: &str) -> Option<WorkerGuard> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Optionally set up JSON file logging
    let log_file_setting = std::env::var("OXEN_LOG_FILE").ok();
    let (json_layer, guard) = if let Some(ref value) = log_file_setting {
        let log_dir = if value == "1" || value.eq_ignore_ascii_case("true") {
            log_directory()
        } else {
            PathBuf::from(value)
        };

        std::fs::create_dir_all(&log_dir).ok();
        let file_appender = tracing_appender::rolling::daily(&log_dir, app_name);
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(non_blocking)
            .with_target(true)
            .with_thread_ids(true)
            .with_file(true)
            .with_line_number(true);

        (Some(layer), Some(guard))
    } else {
        (None, None)
    };

    // Always: human-readable stderr output
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true);

    // .try_init() also installs the tracing-log bridge (forwarding log::*
    // calls into tracing) when the `tracing-log` feature is enabled.
    if let Err(e) = tracing_subscriber::registry()
        .with(env_filter)
        .with(json_layer)
        .with(stderr_layer)
        .try_init()
    {
        eprintln!("warning: failed to initialize tracing: {e}");
    }

    guard
}

/// Install the Prometheus metrics exporter, listening on the given port.
///
/// Metrics are available at `http://0.0.0.0:{port}/metrics`.
pub fn init_metrics_prometheus(port: u16) {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    if let Err(e) = builder.with_http_listener(([0, 0, 0, 0], port)).install() {
        eprintln!("warning: failed to install Prometheus metrics exporter on port {port}: {e}");
    }
}

/// Install a no-op metrics recorder (for CLI or when metrics are disabled).
pub fn init_metrics_noop() {
    // metrics crate uses a global recorder; if none is installed counters/histograms
    // are silently discarded. We don't need to install anything — the default
    // behaviour when no recorder is set is to no-op.
}

fn log_directory() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".oxen")
        .join("logs")
}

/// Record an error metric. Use at every `return Err(...)` site.
#[macro_export]
macro_rules! oxen_err_metric {
    ($module:expr, $error:expr) => {
        metrics::counter!("oxen_errors_total", "module" => $module, "error" => $error).increment(1);
    };
}

/// Record an operation counter.
#[macro_export]
macro_rules! oxen_counter {
    ($name:expr) => {
        metrics::counter!($name).increment(1);
    };
    ($name:expr, $($key:expr => $value:expr),+) => {
        metrics::counter!($name, $($key => $value),+).increment(1);
    };
}
