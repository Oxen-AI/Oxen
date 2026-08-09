#[cfg(not(any(test, feature = "test-utils")))]
use std::io::IsTerminal;
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
    #[cfg(feature = "otel")]
    #[error("Unknown OXEN_OTEL_PROTOCOL value: {0}")]
    UnknownProtocol(String),
    #[cfg(feature = "otel")]
    #[error(
        "OXEN_OTEL_ENDPOINT must be an http:// or https:// URL, or a bare host:port (got: {0})"
    )]
    InvalidEndpoint(String),
}

/// A caller-supplied tracing layer composed into the registry by
/// [`init_tracing_with_layer`]. Keeps this crate free of any dependency on the destination the
/// layer reports to.
pub type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync>;

/// Holds all tracing-related guards. Drop flushes writers and shuts down
/// the OpenTelemetry pipeline (when enabled). The caller **must** keep this
/// alive for the application lifetime.
pub struct TracingGuard {
    _file_guard: Option<WorkerGuard>,
    #[cfg(feature = "otel")]
    _tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl TracingGuard {
    /// Export whatever spans are still queued and stop the OTLP pipeline.
    ///
    /// An async program should await this before its last return to the runtime, and is the reason
    /// this method exists: the exporter's transport is a task on that runtime, so the flush only
    /// succeeds while the runtime is free to poll it. [`Drop`] cannot do that — it blocks the
    /// calling thread, which on a single-threaded runtime (actix's) is the very thread the
    /// transport needs, so the export stalls until the processor's five-second timeout and every
    /// queued span is lost.
    ///
    /// Idempotent, and a no-op without an OTLP endpoint configured.
    pub async fn shutdown(&self) {
        #[cfg(feature = "otel")]
        if let Some(provider) = self._tracer_provider.clone() {
            // The provider's shutdown blocks until the batch processor has drained, so it goes to
            // the blocking pool; awaiting the handle leaves the runtime free to carry the spans.
            match tokio::task::spawn_blocking(move || shutdown_provider(&provider)).await {
                Ok(()) => {}
                Err(e) => eprintln!("warning: OTel tracer provider shutdown task failed: {e}"),
            }
        }
    }
}

/// Shut down `provider`, reporting anything but success and an already-completed shutdown.
///
/// The global subscriber holds an Arc clone of the tracer provider (via OpenTelemetryLayer →
/// SdkTracer → SdkTracerProvider), so dropping the provider alone never brings the Arc refcount to
/// zero — `TracerProviderInner::drop()` never fires, and the `BatchSpanProcessor`'s pending spans
/// are never flushed. Shutting it down explicitly is what exports them.
#[cfg(feature = "otel")]
fn shutdown_provider(provider: &opentelemetry_sdk::trace::SdkTracerProvider) {
    match provider.shutdown() {
        // Already done by an earlier call, the atexit handler, or Drop. All three routes are
        // expected, and the SDK guards against double-shutdown with an atomic flag.
        Ok(()) | Err(opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown) => {}
        Err(e) => eprintln!("warning: OTel tracer provider shutdown failed: {e}"),
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        // The last-resort flush, for a synchronous program and for an async one that returned
        // without awaiting `shutdown`. See that method for why it is not enough on its own.
        #[cfg(feature = "otel")]
        if let Some(ref provider) = self._tracer_provider {
            shutdown_provider(provider);
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
/// **Filtering** is per-layer, not global. `RUST_LOG` (falling back to `default`) gates the log
/// destinations — stderr, the JSON file, and the caller-supplied layer. Span export is gated
/// separately by `OXEN_OTEL_FILTER`, defaulting to `info`, the level `#[tracing::instrument]` and
/// the HTTP root span are recorded at. Traces therefore export at the stock `RUST_LOG` level, and
/// raising log verbosity for debugging does not change what is exported. Either variable set to a
/// value the filter parser cannot use falls back to the level that applies when it is unset, and
/// names the rejected directives on stderr.
///
/// Returns a [TracingGuard]. The caller **must** hold the guard in a named binding for the
/// lifetime of the application — dropping it flushes the non-blocking writer and stops span
/// export. An async program should additionally await [`TracingGuard::shutdown`] before its last
/// return to the runtime; see that method for what dropping alone costs.
pub fn init_tracing(app_name: &str, default: LevelFilter) -> Result<TracingGuard, TelemetryError> {
    init_tracing_with_layer(app_name, default, None)
}

/// [`init_tracing`] with an additional caller-supplied layer composed into the registry.
///
/// `extra` receives every event that passes the `RUST_LOG` filter, including those emitted through
/// the `log` macros via the `tracing-log` bridge.
pub fn init_tracing_with_layer(
    app_name: &str,
    default: LevelFilter,
    extra: Option<BoxedLayer>,
) -> Result<TracingGuard, TelemetryError> {
    let log_directives = log_filter_directives(default);

    // FmtSpan configuration for stderr
    let span_events = std::env::var("OXEN_FMT_SPAN")
        .ok()
        .map(|v| parse_fmt_span(&v))
        .unwrap_or(FmtSpan::NONE);

    // Always: human-readable stderr output (with optional span events).
    // In production, ANSI color codes only when stderr is a terminal, so redirected output
    // (files, CloudWatch) stays clean instead of carrying escape sequences.
    // In test / test-utils builds, route through `TestWriter` so libtest captures the output
    // instead of it interleaving with test progress on the raw terminal.
    #[cfg(any(test, feature = "test-utils"))]
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(tracing_subscriber::fmt::TestWriter::default())
        .with_target(true)
        .with_ansi(false)
        .with_span_events(span_events);
    #[cfg(not(any(test, feature = "test-utils")))]
    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_target(true)
        .with_ansi(std::io::stderr().is_terminal())
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

    // Base registry with all non-OTel layers. `extra` goes innermost so its subscriber type is
    // `Registry`, which is what `BoxedLayer` names.
    //
    // Each log destination carries its own copy of the `RUST_LOG` filter rather than one filter
    // gating the registry, so the OTel layer below can select spans at its own level. A registry
    // filter would apply to that layer too, and the default level is below the level spans are
    // recorded at — every span would be dropped before the exporter ever saw it.
    let log_filter = env_filter(&log_directives, default);
    let registry = tracing_subscriber::registry()
        .with(extra.map(|layer| layer.with_filter(log_filter.clone())))
        .with(m_json_layer.map(|layer| layer.with_filter(log_filter.clone())))
        .with(stderr_layer.with_filter(log_filter));

    // OpenTelemetry layer (feature-gated). Composed separately because the
    // concrete subscriber type (`S`) changes with each `.with()` call and
    // `OpenTelemetryLayer<S, T>` must match the exact inner subscriber.
    #[cfg(feature = "otel")]
    let (m_otel_layer, m_tracer_provider, m_endpoint_p) = match std::env::var("OXEN_OTEL_ENDPOINT")
        .or_else(|_| std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT"))
        .ok()
    {
        Some(endpoint) => {
            let endpoint = normalize_otel_endpoint(&endpoint)?;

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
        let otel_directives = otel_filter_directives();
        let otel_layer = m_otel_layer
            .map(|layer| layer.with_filter(env_filter(&otel_directives, OTEL_DEFAULT_FILTER)));
        registry.with(otel_layer).try_init()?;

        if let Some(protocol_and_endpoint) = m_endpoint_p {
            log::info!(
                "OpenTelemetry tracing enabled (endpoint: {protocol_and_endpoint}, span filter: {otel_directives})"
            );
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

/// Env var selecting which spans and events reach the OTLP exporter, independently of `RUST_LOG`.
#[cfg(feature = "otel")]
const OTEL_FILTER_ENV: &str = "OXEN_OTEL_FILTER";

/// Filter applied to span export when `OXEN_OTEL_FILTER` is unset. `#[tracing::instrument]` and the
/// HTTP root span record at `INFO`, so anything stricter exports empty traces.
#[cfg(feature = "otel")]
const OTEL_DEFAULT_FILTER: LevelFilter = LevelFilter::INFO;

/// The filter directives the log destinations use: `RUST_LOG` when it is set to something
/// non-empty, otherwise the caller's default level.
fn log_filter_directives(default: LevelFilter) -> String {
    non_empty_env(EnvFilter::DEFAULT_ENV).unwrap_or_else(|| default.to_string())
}

/// The filter directives span export uses: `OXEN_OTEL_FILTER`, or [`OTEL_DEFAULT_FILTER`].
#[cfg(feature = "otel")]
fn otel_filter_directives() -> String {
    non_empty_env(OTEL_FILTER_ENV).unwrap_or_else(|| OTEL_DEFAULT_FILTER.to_string())
}

/// Whether the environment already names a service, in which case the app's own name is not used.
///
/// `service_name_var` is `OTEL_SERVICE_NAME`'s raw value, which names nothing when blank.
/// `attributes_name_a_service` is whether `OTEL_RESOURCE_ATTRIBUTES` carried a `service.name`,
/// the other place the OpenTelemetry SDK accepts one.
#[cfg(feature = "otel")]
fn env_names_a_service(service_name_var: Option<&str>, attributes_name_a_service: bool) -> bool {
    attributes_name_a_service || service_name_var.is_some_and(|name| !name.trim().is_empty())
}

/// The value of `name`, or `None` when it is unset or blank.
fn non_empty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

/// Build an [`EnvFilter`] from filter directives, dropping any the parser rejects and falling back
/// to `fallback` when that leaves none standing.
///
/// Layers sharing one set of directives take clones of a single filter: a clone carries the same
/// directives with callsite caches of its own, which is what each layer needs.
fn env_filter(directives: &str, fallback: LevelFilter) -> EnvFilter {
    EnvFilter::builder()
        .with_default_directive(fallback.into())
        .parse_lossy(directives)
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
fn json_file_logging<S>(app_name: &str, log_dir: &Path) -> (impl Layer<S>, WorkerGuard)
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
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

/// Turn a configured OTLP endpoint into an absolute URL the exporter can dial.
///
/// `http://` and `https://` URLs are taken as given — TLS is what every hosted collector speaks. A
/// value with no scheme is a host with an optional port (`collector:4317`), the form the OTLP gRPC
/// convention uses, and gets `http://`. Any other scheme names a transport this exporter cannot
/// speak and is rejected rather than silently retried against a URL that will never connect. Either
/// way the result has to name a host.
#[cfg(feature = "otel")]
fn normalize_otel_endpoint(endpoint: &str) -> Result<String, TelemetryError> {
    let endpoint = endpoint.trim();
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        require_host(endpoint, endpoint)?;
        return Ok(endpoint.to_string());
    }
    if endpoint.is_empty() || endpoint.contains("://") || endpoint.starts_with('/') {
        return Err(TelemetryError::InvalidEndpoint(endpoint.to_string()));
    }
    let with_scheme = format!("http://{endpoint}");
    require_host(&with_scheme, endpoint)?;
    Ok(with_scheme)
}

/// Reject a URL with no host. Reported against `configured`, the value the operator actually set.
///
/// The exporter builds happily from a hostless URL and only fails once it tries to send, so
/// without this a typo like `http://:4317` looks like a working configuration that silently
/// exports nothing.
#[cfg(feature = "otel")]
fn require_host(url: &str, configured: &str) -> Result<(), TelemetryError> {
    let invalid = || TelemetryError::InvalidEndpoint(configured.to_string());
    let parsed = url::Url::parse(url).map_err(|_| invalid())?;
    match parsed.host_str() {
        Some(host) if !host.is_empty() => Ok(()),
        _ => Err(invalid()),
    }
}

/// The path OTLP/HTTP posts spans to, relative to the configured base endpoint.
#[cfg(feature = "otel")]
const OTLP_HTTP_TRACES_PATH: &str = "/v1/traces";

/// The URL OTLP/HTTP posts spans to for a configured base endpoint.
///
/// The exporter appends the signal path only to an endpoint it reads from the environment itself;
/// one handed to the builder is used verbatim, so a base endpoint gets the path here. An endpoint
/// that already names the path is left alone.
///
/// The path is set on the parsed URL rather than concatenated, so a query string an endpoint
/// carries stays a query string instead of swallowing the path appended after it.
#[cfg(feature = "otel")]
fn http_traces_endpoint(endpoint: &str) -> String {
    let Ok(mut url) = url::Url::parse(endpoint) else {
        return endpoint.to_string();
    };
    let base_path = url.path().trim_end_matches('/');
    if base_path.ends_with(OTLP_HTTP_TRACES_PATH) {
        return endpoint.to_string();
    }
    url.set_path(&format!("{base_path}{OTLP_HTTP_TRACES_PATH}"));
    url.to_string()
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
    use opentelemetry::trace::TracerProvider;
    use opentelemetry::{Key, KeyValue};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_otlp::WithTonicConfig;
    use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::resource::{EnvResourceDetector, ResourceDetector};
    use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};

    let exporter = match protocol {
        Protocol::Http => {
            match opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(http_traces_endpoint(endpoint))
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
            // Supplies the platform root certificates an `https://` endpoint's handshake verifies
            // against. An `http://` endpoint ignores it.
            match opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .with_tls_config(ClientTlsConfig::new().with_native_roots())
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

    // `Resource::builder` already applies the standard detectors, so `OTEL_SERVICE_NAME` and
    // `OTEL_RESOURCE_ATTRIBUTES` (where `deployment.environment` is set) land on every span. The
    // attributes added here take precedence over the detectors, so `service.name` is only supplied
    // as the fallback for a deployment that names no service of its own.
    let attributes_name_a_service = EnvResourceDetector::new()
        .detect()
        .get(&Key::from_static_str("service.name"))
        .is_some();
    let mut builder = Resource::builder().with_attributes([KeyValue::new(
        "service.version",
        crate::constants::OXEN_VERSION,
    )]);
    if !env_names_a_service(
        std::env::var("OTEL_SERVICE_NAME").ok().as_deref(),
        attributes_name_a_service,
    ) {
        builder = builder.with_service_name(app_name.to_string());
    }
    let resource = builder.build();

    // A collector that is slow, unreachable, or black-holing must cost the process bounded memory
    // and never back-pressure a request thread: the queue is fixed, and the processor drops spans
    // once it is full rather than blocking the thread that ended the span. A queue deeper than the
    // SDK default absorbs the burst a bulk endpoint produces, and a shorter delay keeps it draining
    // often enough for the next one.
    //
    // The builder's default has already read the `OTEL_BSP_*` env vars, and setting a field here
    // would override whatever it found — so each is only set when the operator left it unset, and
    // tuning a deployment through the standard variables still works.
    let mut batch_config = BatchConfigBuilder::default();
    if std::env::var_os("OTEL_BSP_MAX_QUEUE_SIZE").is_none() {
        batch_config = batch_config.with_max_queue_size(4096);
    }
    if std::env::var_os("OTEL_BSP_SCHEDULE_DELAY").is_none() {
        batch_config = batch_config.with_scheduled_delay(std::time::Duration::from_secs(2));
    }
    let processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config.build())
        .build();

    // Sampling comes from `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG`, which the provider's
    // default configuration reads; absent those it is parent-based always-on, so a service that
    // receives a sampling decision from its caller honors it.
    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(resource)
        .build();

    // W3C `traceparent` / `tracestate`, the only format this stack propagates. Without a global
    // propagator installed, context extraction reads an empty context and every request starts a
    // new trace instead of continuing its caller's.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let tracer = provider.tracer("oxen");
    let layer = tracing_opentelemetry::layer().with_tracer(tracer);

    (Some(layer), Some(provider))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::fmt::format::FmtSpan;

    /// A directive the parser rejects is dropped, so a filter built only from rejected directives
    /// holds none and silences every layer it gates unless a fallback fills the gap.
    #[test]
    fn a_filter_with_no_usable_directive_falls_back_to_the_given_level() {
        for directives in ["liboxen=verbose", "warn=oops=bad", "="] {
            assert_eq!(
                env_filter(directives, LevelFilter::WARN).max_level_hint(),
                Some(LevelFilter::WARN),
                "{directives} should fall back to the given level"
            );
        }
    }

    /// The fallback fills an empty directive set and nothing else, so it never overrides a usable
    /// filter, including one left usable only because a sibling directive was dropped.
    #[test]
    fn usable_directives_win_over_the_fallback() {
        assert_eq!(
            env_filter("debug", LevelFilter::WARN).max_level_hint(),
            Some(LevelFilter::DEBUG)
        );
        assert_eq!(
            env_filter("off", LevelFilter::WARN).max_level_hint(),
            Some(LevelFilter::OFF)
        );
        assert_eq!(
            env_filter("liboxen=verbose,oxen_server=debug", LevelFilter::WARN).max_level_hint(),
            Some(LevelFilter::DEBUG)
        );
    }

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
        use super::super::{
            TelemetryError, env_names_a_service, http_traces_endpoint, normalize_otel_endpoint,
            otel_filter_directives,
        };

        /// A service the operator named through either variable is kept, and the app's own name is
        /// used only when neither does. Overriding a configured `service.name` would rename the
        /// deployment's traces out from under whoever set it.
        #[test]
        fn the_environment_names_the_service_when_it_says_so() {
            // `OTEL_SERVICE_NAME` set to something.
            assert!(env_names_a_service(Some("checkout"), false));
            // `service.name` supplied through `OTEL_RESOURCE_ATTRIBUTES` instead.
            assert!(env_names_a_service(None, true));
            // Both, which is the same answer.
            assert!(env_names_a_service(Some("checkout"), true));
        }

        /// An `OTEL_SERVICE_NAME` that is unset or blank names nothing, matching how the SDK's own
        /// detector treats it. Reading it as a name would export traces under an empty service.
        #[test]
        fn a_blank_service_name_variable_names_nothing() {
            for value in [None, Some(""), Some("   ")] {
                assert!(
                    !env_names_a_service(value, false),
                    "{value:?} should not count as naming a service"
                );
            }
        }

        #[test]
        fn keeps_http_endpoint() {
            assert_eq!(
                normalize_otel_endpoint("http://localhost:4317").unwrap(),
                "http://localhost:4317"
            );
        }

        #[test]
        fn keeps_https_endpoint() {
            assert_eq!(
                normalize_otel_endpoint("https://otlp.vendor.example:443").unwrap(),
                "https://otlp.vendor.example:443"
            );
        }

        #[test]
        fn adds_scheme_to_bare_host_port() {
            assert_eq!(
                normalize_otel_endpoint("localhost:4317").unwrap(),
                "http://localhost:4317"
            );
        }

        #[test]
        fn adds_scheme_to_bare_host() {
            assert_eq!(
                normalize_otel_endpoint("collector").unwrap(),
                "http://collector"
            );
        }

        #[test]
        fn trims_surrounding_whitespace() {
            assert_eq!(
                normalize_otel_endpoint("  http://localhost:4317 ").unwrap(),
                "http://localhost:4317"
            );
        }

        #[test]
        fn rejects_other_schemes() {
            for endpoint in ["grpc://localhost:4317", "unix:///var/run/otel.sock"] {
                let err = normalize_otel_endpoint(endpoint).unwrap_err();
                assert!(matches!(err, TelemetryError::InvalidEndpoint(_)));
            }
        }

        #[test]
        fn rejects_empty_endpoint() {
            let err = normalize_otel_endpoint("   ").unwrap_err();
            assert!(matches!(err, TelemetryError::InvalidEndpoint(_)));
        }

        /// A hostless URL builds an exporter that only fails once it tries to send, so it has to be
        /// rejected at startup rather than looking like a working configuration.
        #[test]
        fn rejects_endpoints_with_no_host() {
            for endpoint in ["http://", "https://", "http://:4317", ":4317"] {
                let result = normalize_otel_endpoint(endpoint);
                assert!(
                    matches!(result, Err(TelemetryError::InvalidEndpoint(_))),
                    "{endpoint} should be rejected, got {result:?}"
                );
            }
        }

        #[test]
        fn http_signal_path_is_appended_to_a_base_endpoint() {
            assert_eq!(
                http_traces_endpoint("http://localhost:4318"),
                "http://localhost:4318/v1/traces"
            );
            assert_eq!(
                http_traces_endpoint("http://localhost:4318/"),
                "http://localhost:4318/v1/traces"
            );
            assert_eq!(
                http_traces_endpoint("https://vendor.example/otlp"),
                "https://vendor.example/otlp/v1/traces"
            );
        }

        #[test]
        fn http_signal_path_is_not_doubled() {
            assert_eq!(
                http_traces_endpoint("http://localhost:4318/v1/traces"),
                "http://localhost:4318/v1/traces"
            );
        }

        /// The signal path belongs on the path component. Concatenating it onto the whole URL puts
        /// it after any query string, producing something the collector never matches.
        #[test]
        fn http_signal_path_keeps_a_query_string_intact() {
            assert_eq!(
                http_traces_endpoint("http://localhost:4318?token=abc"),
                "http://localhost:4318/v1/traces?token=abc"
            );
            assert_eq!(
                http_traces_endpoint("https://vendor.example/otlp?token=abc"),
                "https://vendor.example/otlp/v1/traces?token=abc"
            );
        }

        /// Span export must not be silenced by the stock log level: `#[tracing::instrument]` and
        /// the HTTP root span record at `INFO`, and the server logs at `WARN` by default.
        #[test]
        fn span_export_defaults_to_info() {
            // Reading the env var directly rather than mutating it: a sibling test setting
            // OXEN_OTEL_FILTER would otherwise decide this one's outcome.
            if std::env::var_os("OXEN_OTEL_FILTER").is_none() {
                assert_eq!(otel_filter_directives(), "info");
            }
        }
    }
}
