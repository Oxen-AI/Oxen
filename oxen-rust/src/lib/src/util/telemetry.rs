use std::path::PathBuf;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Initialize tracing with JSON file output and the `tracing-log` bridge.
///
/// Captures all existing `log::` calls as tracing events via `tracing_log::LogTracer`.
/// Writes JSON-formatted logs to rolling daily files under `~/.oxen/logs/`.
/// Filter level is controlled by the `OXEN_LOG` env var (default: `info`).
///
/// Returns a `WorkerGuard` that **must** be held in a named binding for the
/// lifetime of the application — dropping it flushes the non-blocking writer.
pub fn init_tracing(app_name: &str) -> WorkerGuard {
    // Bridge: forward all `log::` calls into tracing
    tracing_log::LogTracer::init().ok();

    let log_dir = log_directory();
    std::fs::create_dir_all(&log_dir).ok();

    let file_appender = tracing_appender::rolling::daily(&log_dir, app_name);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    let env_filter = EnvFilter::try_from_env("OXEN_LOG").unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(non_blocking)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true),
        )
        .init();

    guard
}

/// Install the Prometheus metrics exporter, listening on the given port.
///
/// Metrics are available at `http://0.0.0.0:{port}/metrics`.
pub fn init_metrics_prometheus(port: u16) {
    let builder = metrics_exporter_prometheus::PrometheusBuilder::new();
    builder
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("failed to install Prometheus metrics exporter");
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
