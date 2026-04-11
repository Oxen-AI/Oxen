use std::path::{Path, PathBuf};

use thiserror::Error;
use tracing::level_filters::LevelFilter;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::fmt::format::FmtSpan;
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
    #[error("Unknown OXEN_OTEL_PROTOCOL value: {0}")]
    UnknownProtocol(String),
    #[error("OXEN_OTEL_ENDPOINT must start with http:// (got: {0})")]
    InvalidEndpoint(String),
}

/// Holds all tracing-related guards. Drop flushes writers and shuts down
/// the OpenTelemetry pipeline (when enabled). The caller **must** keep this
/// alive for the application lifetime.
pub struct TracingGuard {
    _file_guard: Option<WorkerGuard>,
    #[cfg(feature = "otel")]
    _tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        // The global subscriber holds an Arc clone of the tracer provider
        // (via OpenTelemetryLayer → SdkTracer → SdkTracerProvider), so
        // dropping _tracer_provider alone never brings the Arc refcount to
        // zero — TracerProviderInner::drop() never fires. We call shutdown()
        // explicitly to flush the BatchSpanProcessor's pending spans.
        // This is idempotent: if atexit_flush already ran, the atomic
        // is_shutdown flag causes this to no-op.
        #[cfg(feature = "otel")]
        if let Some(ref provider) = self._tracer_provider
            && let Err(e) = provider.shutdown()
        {
            eprintln!("warning: OTel tracer provider shutdown failed: {e}");
        }
    }
}

/// Ensures the OTel tracer provider is shut down (and pending spans flushed)
/// when the process exits, even if [`TracingGuard`] is stored in a static
/// whose `Drop` impl will never run.
///
/// A clone of the [`SdkTracerProvider`] is stored in a process-global
/// [`OnceLock`] and a C `atexit` callback is registered to call `shutdown()`
/// on it. `shutdown()` is idempotent (guarded by an atomic flag), so it is
/// safe for both [`TracingGuard::drop`] and the `atexit` handler to call it.
#[cfg(feature = "otel")]
mod atexit_flush {
    use std::sync::OnceLock;

    static PROVIDER: OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> = OnceLock::new();

    extern "C" fn on_exit() {
        if let Some(provider) = PROVIDER.get() {
            let _ = provider.shutdown();
        }
    }

    /// Store a clone of the provider and register the `atexit` callback.
    /// Only the first call has any effect; subsequent calls are no-ops.
    ///
    /// # Safety
    ///
    /// This function declares the C standard library's `atexit` via
    /// `unsafe extern "C"` and marks it `safe` because:
    ///
    /// - `atexit` is defined by ISO C (C89 / C99 / C11) and is available on
    ///   every platform this project targets (Linux, macOS, Windows).
    /// - The registered callback (`on_exit`) is an `extern "C" fn` with no
    ///   captures, satisfying `atexit`'s function-pointer requirement.
    /// - `on_exit` only accesses `PROVIDER`, a `OnceLock` that is `Sync` and
    ///   fully initialized before the callback can ever fire.
    /// - `SdkTracerProvider::shutdown()` is synchronous, blocking, and
    ///   idempotent (guarded by an atomic flag), so it is safe to call from
    ///   the single-threaded `atexit` context even if [`TracingGuard::drop`]
    ///   already called it.
    pub(super) fn register(provider: opentelemetry_sdk::trace::SdkTracerProvider) -> bool {
        if PROVIDER.set(provider).is_err() {
            return false;
        }
        unsafe extern "C" {
            safe fn atexit(f: extern "C" fn()) -> core::ffi::c_int;
        }
        let registered = atexit(on_exit) == 0;
        if !registered {
            eprintln!("warning: failed to register OTel atexit flush handler");
        }
        registered
    }
}

/// Initialize tracing with the `tracing-log` bridge.
///
/// **Stderr** (always): human-readable formatted output, like `env_logger`.
///
/// **File** (opt-in via `OXEN_LOG_DIR`): JSON-per-line output to a rolling
/// daily log file. The env var accepts a directory path — rolling files are
/// written there (e.g. `/var/log/oxen`).
///
/// **FmtSpan events** (opt-in via `OXEN_FMT_SPAN`): emit span lifecycle
/// events (creation, close, etc.) as additional log lines on stderr. Accepted
/// values: `NEW`, `CLOSE`, `ENTER`, `EXIT`, `ACTIVE`, `FULL`, `NONE`,
/// `1`/`true` (alias for `CLOSE`), or `|`-combined (e.g. `NEW|CLOSE`).
///
/// **OpenTelemetry** (opt-in via `OXEN_OTEL_ENDPOINT`, requires `otel` feature):
/// export spans to an OTLP-compatible collector. See env var documentation in
/// the server README for configuration details. Also accepts the standard named
/// env var `OTEL_EXPORTER_OTLP_ENDPOINT`, but checks `OXEN_OTEL_ENDPOINT` first.
///
/// The `OXEN_OTEL_PROTOCOL` env var is either `"http"` or `"grpc"`: it controls
/// the protocol used for OTLP exports. If not set, defaults to `"grpc"`.
///
/// Filter level is controlled by `RUST_LOG`. The filter applies to all output layers.
///
/// Returns a [TracingGuard] when file logging is active. The caller
/// **must** hold the guard in a named binding for the lifetime of the
/// application — dropping it flushes the non-blocking writer.
pub fn init_tracing(app_name: &str, default: LevelFilter) -> Result<TracingGuard, TelemetryError> {
    let log_level = log_env_filter(default);

    // FmtSpan configuration for stderr
    let span_events = std::env::var("OXEN_FMT_SPAN")
        .ok()
        .map(|v| parse_fmt_span(&v))
        .unwrap_or(FmtSpan::NONE);

    // Always: human-readable stderr output (with optional span events)
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_span_events(span_events);

    let maybe_log_dir = match std::env::var("OXEN_LOG_DIR").ok() {
        Some(log_dir) => {
            let created_log_dir = create_log_dir(&log_dir)?;
            Some(created_log_dir)
        }
        None => None,
    };

    let (m_json_layer, m_worker_guard) = if let Some(ref log_dir) = maybe_log_dir {
        let (jl, wg) = json_file_logging(app_name, log_dir);
        (Some(jl), Some(wg))
    } else {
        (None, None)
    };

    // Base registry with all non-OTel layers
    let registry = tracing_subscriber::registry()
        .with(m_json_layer)
        .with(stderr_layer)
        .with(log_level);

    // OpenTelemetry layer (feature-gated). Composed separately because the
    // concrete subscriber type (`S`) changes with each `.with()` call and
    // `OpenTelemetryLayer<S, T>` must match the exact inner subscriber.
    #[cfg(feature = "otel")]
    let (m_otel_layer, m_tracer_provider, m_endpoint_p) = match std::env::var("OXEN_OTEL_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()
    {
        Some(endpoint) => {
            validate_otel_endpoint(&endpoint)?;

            let protocol = match std::env::var("OXEN_OTEL_PROTOCOL")
                .map(|x| x.to_lowercase())
                .ok()
                .as_deref()
            {
                Some("http") => Protocol::Http,
                Some("grpc") | None => Protocol::Grpc,
                Some(unknown) => {
                    return Err(TelemetryError::UnknownProtocol(unknown.to_string()));
                }
            };

            match build_otel_layer(app_name, &protocol, &endpoint) {
                (Some(layer), Some(provider)) => {
                    atexit_flush::register(provider.clone());
                    (
                        Some(layer),
                        Some(provider),
                        Some(format!("{protocol} -> {endpoint}")),
                    )
                }
                _ => (None, None, None),
            }
        }

        None => (None, None, None),
    };

    // .try_init() also installs the tracing-log bridge (forwarding log::* calls into tracing)
    // when the `tracing-log` feature is enabled.

    #[cfg(feature = "otel")]
    {
        registry.with(m_otel_layer).try_init()?;

        if let Some(protocol_and_endpoint) = m_endpoint_p {
            log::info!("OpenTelemetry tracing enabled (endpoint: {protocol_and_endpoint})");
        }
    }

    #[cfg(not(feature = "otel"))]
    {
        registry.try_init()?;

        if std::env::var("OXEN_OTEL_ENDPOINT").is_ok() {
            log::error!("OXEN_OTEL_ENDPOINT is set but otel feature is not enabled! (Ignoring)")
        }

        if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_ok() {
            log::error!(
                "OTEL_EXPORTER_OTLP_ENDPOINT is set but otel feature is not enabled! (Ignoring)"
            )
        }
    }

    if let Some(ref log_dir) = maybe_log_dir {
        log::info!(
            "JSON file logging enabled (log directory: {})",
            log_dir.display()
        );
    }

    Ok(TracingGuard {
        _file_guard: m_worker_guard,
        #[cfg(feature = "otel")]
        _tracer_provider: m_tracer_provider,
    })
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

/// Parse an `OXEN_FMT_SPAN` value into a [`FmtSpan`] bitflag.
///
/// Accepts single names (`NEW`, `CLOSE`, `FULL`, etc.), the convenience
/// aliases `1`/`true` (mapped to `CLOSE`), or `|`-separated combinations
/// (e.g. `NEW|CLOSE`, `ACTIVE|FULL`).
fn parse_fmt_span(value: &str) -> FmtSpan {
    let upper = value.to_uppercase();
    if !upper.contains('|') {
        return parse_fmt_span_token(&upper);
    }
    let mut span = FmtSpan::NONE;
    for part in upper.split('|') {
        span |= parse_fmt_span_token(part.trim());
    }
    span
}

/// Parse a single `FmtSpan` token (already uppercased).
fn parse_fmt_span_token(token: &str) -> FmtSpan {
    match token {
        "1" | "TRUE" | "CLOSE" => FmtSpan::CLOSE,
        "NEW" => FmtSpan::NEW,
        "ENTER" => FmtSpan::ENTER,
        "EXIT" => FmtSpan::EXIT,
        "ACTIVE" => FmtSpan::ACTIVE,
        "FULL" => FmtSpan::FULL,
        "NONE" => FmtSpan::NONE,
        other => {
            eprintln!("warning: unknown OXEN_FMT_SPAN component: {other:?}, ignoring");
            FmtSpan::NONE
        }
    }
}
#[cfg(feature = "otel")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Grpc,
    Http,
}

#[cfg(feature = "otel")]
impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Grpc => write!(f, "grpc"),
            Protocol::Http => write!(f, "http"),
        }
    }
}

/// Validates that the OTLP endpoint starts with `http://`.
#[cfg(feature = "otel")]
fn validate_otel_endpoint(endpoint: &str) -> Result<(), TelemetryError> {
    if !endpoint.starts_with("http://") {
        return Err(TelemetryError::InvalidEndpoint(endpoint.to_string()));
    }
    Ok(())
}

/// Build an OpenTelemetry tracing layer that exports spans via OTLP.
///
/// Returns the layer (wrapped in `Option`) and the provider. When the
/// exporter cannot be built, returns `(None, None)`.
#[cfg(feature = "otel")]
fn build_otel_layer<S>(
    app_name: &str,
    protocol: &Protocol,
    endpoint: &str,
) -> (
    Option<tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>>,
    Option<opentelemetry_sdk::trace::SdkTracerProvider>,
)
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use opentelemetry::KeyValue;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};

    let exporter = match protocol {
        Protocol::Http => {
            match opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint)
                .build()
            {
                Ok(e) => e,
                Err(err) => {
                    eprintln!("[ERROR] failed to build OTel HTTP exporter: {err}");
                    return (None, None);
                }
            }
        }
        Protocol::Grpc => {
            match opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
            {
                Ok(e) => e,
                Err(err) => {
                    eprintln!("[ERROR] failed to build OTel gRPC exporter: {err}");
                    return (None, None);
                }
            }
        }
    };

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", app_name.to_string())])
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("oxen");
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);

    (Some(layer), Some(provider))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::fmt::format::FmtSpan;

    #[test]
    fn token_close() {
        assert_eq!(parse_fmt_span_token("CLOSE"), FmtSpan::CLOSE);
        assert_eq!(parse_fmt_span_token("1"), FmtSpan::CLOSE);
        assert_eq!(parse_fmt_span_token("TRUE"), FmtSpan::CLOSE);
        // case-insensitive
        assert_eq!(parse_fmt_span("cLosE"), FmtSpan::CLOSE);
        assert_eq!(parse_fmt_span("tRuE"), FmtSpan::CLOSE);
    }

    #[test]
    fn token_new() {
        assert_eq!(parse_fmt_span_token("NEW"), FmtSpan::NEW);
        // case-insensitive
        assert_eq!(parse_fmt_span("NeW"), FmtSpan::NEW);
    }

    #[test]
    fn token_enter() {
        assert_eq!(parse_fmt_span_token("ENTER"), FmtSpan::ENTER);
        // case-insensitive
        assert_eq!(parse_fmt_span("eNteR"), FmtSpan::ENTER);
    }

    #[test]
    fn token_exit() {
        assert_eq!(parse_fmt_span_token("EXIT"), FmtSpan::EXIT);
        // case-insensitive
        assert_eq!(parse_fmt_span("exIT"), FmtSpan::EXIT);
    }

    #[test]
    fn token_active() {
        assert_eq!(parse_fmt_span_token("ACTIVE"), FmtSpan::ACTIVE);
        // case-insensitive
        assert_eq!(parse_fmt_span("aCtIvE"), FmtSpan::ACTIVE);
    }

    #[test]
    fn token_full() {
        assert_eq!(parse_fmt_span_token("FULL"), FmtSpan::FULL);
        // case-insensitive
        assert_eq!(parse_fmt_span("FUll"), FmtSpan::FULL);
    }

    #[test]
    fn token_none() {
        assert_eq!(parse_fmt_span_token("NONE"), FmtSpan::NONE);
        // case-insensitive
        assert_eq!(parse_fmt_span("NonE"), FmtSpan::NONE);
    }

    #[test]
    fn token_unknown_returns_none() {
        assert_eq!(parse_fmt_span_token("BOGUS"), FmtSpan::NONE);
        // case-insensitive
        assert_eq!(parse_fmt_span("bogus"), FmtSpan::NONE);
    }

    #[test]
    fn combined_new_close() {
        assert_eq!(parse_fmt_span("NEW|CLOSE"), FmtSpan::NEW | FmtSpan::CLOSE);
        // case-insensitive
        assert_eq!(parse_fmt_span("new|close"), FmtSpan::NEW | FmtSpan::CLOSE);
        // spaces around | are OK
        assert_eq!(parse_fmt_span("NEW | CLOSE"), FmtSpan::NEW | FmtSpan::CLOSE);
    }

    #[test]
    fn combined_active_close() {
        assert_eq!(
            parse_fmt_span("ACTIVE|CLOSE"),
            FmtSpan::ACTIVE | FmtSpan::CLOSE
        );
    }

    #[test]
    fn combined_full_new() {
        assert_eq!(parse_fmt_span("FULL|NEW"), FmtSpan::FULL | FmtSpan::NEW);
    }

    #[test]
    fn combined_unknown_component_ignored() {
        assert_eq!(parse_fmt_span("NEW|BOGUS"), FmtSpan::NEW | FmtSpan::NONE);
    }

    #[test]
    fn combined_all_four_lifecycle() {
        assert_eq!(
            parse_fmt_span("NEW|ENTER|EXIT|CLOSE"),
            FmtSpan::NEW | FmtSpan::ENTER | FmtSpan::EXIT | FmtSpan::CLOSE
        );
    }

    #[cfg(feature = "otel")]
    mod otel_tests {
        use super::super::{TelemetryError, validate_otel_endpoint};

        #[test]
        fn valid_http_endpoint() {
            assert!(validate_otel_endpoint("http://localhost:4317").is_ok());
        }

        #[test]
        fn rejects_grpc_scheme() {
            let err = validate_otel_endpoint("grpc://localhost:4317").unwrap_err();
            assert!(matches!(err, TelemetryError::InvalidEndpoint(_)));
        }

        #[test]
        fn rejects_https_scheme() {
            let err = validate_otel_endpoint("https://localhost:4317").unwrap_err();
            assert!(matches!(err, TelemetryError::InvalidEndpoint(_)));
        }

        #[test]
        fn rejects_bare_host_port() {
            let err = validate_otel_endpoint("localhost:4317").unwrap_err();
            assert!(matches!(err, TelemetryError::InvalidEndpoint(_)));
        }

        #[test]
        fn rejects_bare_host() {
            let err = validate_otel_endpoint("localhost").unwrap_err();
            assert!(matches!(err, TelemetryError::InvalidEndpoint(_)));
        }
    }
}
