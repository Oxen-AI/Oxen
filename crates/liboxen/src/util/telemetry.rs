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
    #[error("Unknown {var} value: {value} (expected grpc, http, http/protobuf, or http/json)")]
    UnknownProtocol { var: &'static str, value: String },
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
    #[cfg(feature = "otel")]
    _logger_provider: Option<opentelemetry_sdk::logs::SdkLoggerProvider>,
}

impl TracingGuard {
    /// Export whatever spans and log records are still queued and stop the OTLP pipelines.
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
        {
            let tracer_provider = self._tracer_provider.clone();
            let logger_provider = self._logger_provider.clone();
            if tracer_provider.is_none() && logger_provider.is_none() {
                return;
            }
            // A provider's shutdown blocks until its batch processor has drained, so it goes to
            // the blocking pool; awaiting the handle leaves the runtime free to carry the batches.
            let flush = tokio::task::spawn_blocking(move || {
                if let Some(provider) = tracer_provider {
                    report_shutdown("tracer", provider.shutdown());
                }
                if let Some(provider) = logger_provider {
                    report_shutdown("logger", provider.shutdown());
                }
            });
            if let Err(e) = flush.await {
                eprintln!("warning: OTel provider shutdown task failed: {e}");
            }
        }
    }
}

/// Report the result of shutting down the `what` provider, ignoring an already-completed shutdown.
///
/// The global subscriber holds an Arc clone of each provider (the tracer provider via
/// OpenTelemetryLayer → SdkTracer → SdkTracerProvider, the logger provider via
/// OpenTelemetryTracingBridge → SdkLogger → SdkLoggerProvider), so dropping a provider alone never
/// brings the Arc refcount to zero — its inner `drop()` never fires, and the batch processor's
/// pending records are never flushed. Shutting it down explicitly is what exports them.
#[cfg(feature = "otel")]
fn report_shutdown(what: &str, result: opentelemetry_sdk::error::OTelSdkResult) {
    match result {
        // Already done by an earlier call, the atexit handler, or Drop. All three routes are
        // expected, and the SDK guards against double-shutdown with an atomic flag.
        Ok(()) | Err(opentelemetry_sdk::error::OTelSdkError::AlreadyShutdown) => {}
        Err(e) => eprintln!("warning: OTel {what} provider shutdown failed: {e}"),
    }
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        // The last-resort flush, for a synchronous program and for an async one that returned
        // without awaiting `shutdown`. See that method for why it is not enough on its own.
        #[cfg(feature = "otel")]
        {
            if let Some(ref provider) = self._tracer_provider {
                report_shutdown("tracer", provider.shutdown());
            }
            if let Some(ref provider) = self._logger_provider {
                report_shutdown("logger", provider.shutdown());
            }
        }
    }
}

/// Ensures the OTel providers are shut down (and pending spans and log
/// records flushed) when the process exits, even if [`TracingGuard`] is stored
/// in a static whose `Drop` impl will never run.
///
/// A clone of each provider is stored in a process-global [`OnceLock`] and a C
/// `atexit` callback is registered to call `shutdown()` on it. `shutdown()` is
/// idempotent (guarded by an atomic flag), so it is safe for both
/// [`TracingGuard::drop`] and the `atexit` handler to call it.
#[cfg(feature = "otel")]
mod atexit_flush {
    use std::sync::{Once, OnceLock};

    static TRACER_PROVIDER: OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> = OnceLock::new();
    static LOGGER_PROVIDER: OnceLock<opentelemetry_sdk::logs::SdkLoggerProvider> = OnceLock::new();
    static HANDLER: Once = Once::new();

    extern "C" fn on_exit() {
        if let Some(provider) = TRACER_PROVIDER.get() {
            let _ = provider.shutdown();
        }
        if let Some(provider) = LOGGER_PROVIDER.get() {
            let _ = provider.shutdown();
        }
    }

    /// Flush this tracer provider from the `atexit` handler.
    pub(super) fn register_tracer(provider: opentelemetry_sdk::trace::SdkTracerProvider) {
        let _ = TRACER_PROVIDER.set(provider);
        register();
    }

    /// Flush this logger provider from the `atexit` handler.
    pub(super) fn register_logger(provider: opentelemetry_sdk::logs::SdkLoggerProvider) {
        let _ = LOGGER_PROVIDER.set(provider);
        register();
    }

    /// Register the `atexit` callback. Only the first call has any effect;
    /// subsequent calls are no-ops, so the two providers share one handler.
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
    /// - `on_exit` only accesses `TRACER_PROVIDER` and `LOGGER_PROVIDER`, both
    ///   `OnceLock`s that are `Sync` and fully initialized before the callback
    ///   can ever fire.
    /// - Each provider's `shutdown()` is synchronous, blocking, and
    ///   idempotent (guarded by an atomic flag), so it is safe to call from
    ///   the single-threaded `atexit` context even if [`TracingGuard::drop`]
    ///   already called it.
    fn register() {
        HANDLER.call_once(|| {
            unsafe extern "C" {
                safe fn atexit(f: extern "C" fn()) -> core::ffi::c_int;
            }
            if atexit(on_exit) != 0 {
                eprintln!("warning: failed to register OTel atexit flush handler");
            }
        });
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
/// **OpenTelemetry** (opt-in via `OTEL_EXPORTER_OTLP_ENDPOINT`, requires the `otel` feature):
/// export spans to an OTLP-compatible collector. That variable names the collector and takes
/// `/v1/traces` under HTTP, while `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` names the traces signal
/// directly and takes precedence. Both need an `http://` or `https://` scheme, since a schemeless
/// value leaves the exporter on its own `http://localhost:4318` default. See env var documentation
/// in the server README for configuration details.
///
/// The `OTEL_EXPORTER_OTLP_PROTOCOL` env var selects the transport, accepting `"grpc"`, `"http"`,
/// `"http/protobuf"`, or `"http/json"`, where the last three all mean HTTP. Unset, it defaults to
/// HTTP. Spans are encoded as binary protobuf under either transport.
///
/// **OTLP log export** (opt-in via `OTEL_LOGS_EXPORTER=otlp`, requires the `otel` feature): also
/// export events as OTLP log records, over the endpoint and transport span export already uses.
/// `none` or unset leaves it off, and stdout logging is unaffected either way. Each record carries
/// the trace and span id of the span the event was recorded in, which is what correlates a log
/// line with its trace.
///
/// **Filtering** is per-layer, not global. `RUST_LOG` (falling back to `default`) gates the log
/// destinations — stderr, the JSON file, and the caller-supplied layer. OTLP export is gated
/// separately by `OXEN_OTEL_FILTER`, defaulting to `info`, the level `#[tracing::instrument]` and
/// the HTTP root span are recorded at. Traces therefore export at the stock `RUST_LOG` level, and
/// raising log verbosity for debugging does not change what is exported. Either variable set to a
/// value the filter parser cannot use falls back to the level that applies when it is unset, and
/// names the rejected directives on stderr. `OXEN_OTEL_FILTER` gates spans and OTLP log records
/// alike, except that log records never carry the exporter's own crates; see
/// [`OTEL_LOG_EXCLUDED_TARGETS`].
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
    let (m_otel_layer, m_tracer_provider, m_endpoint_p) =
        match otel_endpoint("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") {
            Some((var, endpoint)) => {
                let protocol = otel_protocol("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")?;

                match build_otel_layer(app_name, &protocol) {
                    (Some(layer), Some(provider)) => {
                        atexit_flush::register_tracer(provider.clone());
                        (
                            Some(layer),
                            Some(provider),
                            Some(format!("{protocol} (protobuf) -> {var}={endpoint}")),
                        )
                    }
                    _ => (None, None, None),
                }
            }

            None => (None, None, None),
        };

    // The OTLP log pipeline, a second exporter over the same endpoint and transport. Independent
    // of the span pipeline: either can be configured, fail to build, or be left off on its own.
    #[cfg(feature = "otel")]
    let (m_log_layer, m_logger_provider, m_log_endpoint_p) =
        match otel_endpoint("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").filter(|_| otel_logs_enabled()) {
            Some((var, endpoint)) => {
                let protocol = otel_protocol("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL")?;

                match build_otel_log_layer(app_name, &protocol) {
                    Some((layer, provider)) => {
                        atexit_flush::register_logger(provider.clone());
                        (
                            Some(layer),
                            Some(provider),
                            Some(format!("{protocol} (protobuf) -> {var}={endpoint}")),
                        )
                    }
                    None => (None, None, None),
                }
            }

            None => (None, None, None),
        };

    // .try_init() also installs the tracing-log bridge (forwarding log::* calls into tracing)
    // when the `tracing-log` feature is enabled.

    #[cfg(feature = "otel")]
    {
        let otel_directives = otel_filter_directives();
        let exporting_spans = m_otel_layer.is_some();
        let otel_layer = m_otel_layer
            .map(|layer| layer.with_filter(env_filter(&otel_directives, OTEL_DEFAULT_FILTER)));
        let log_directives = otel_log_filter_directives();
        let log_layer = m_log_layer
            .map(|layer| layer.with_filter(env_filter(&log_directives, OTEL_DEFAULT_FILTER)));
        registry.with(otel_layer).with(log_layer).try_init()?;

        if let Some(protocol_and_endpoint) = m_endpoint_p {
            log::info!(
                "OpenTelemetry tracing enabled (endpoint: {protocol_and_endpoint}, span filter: {otel_directives})"
            );
        }
        if let Some(protocol_and_endpoint) = m_log_endpoint_p {
            log::info!(
                "OpenTelemetry log export enabled (endpoint: {protocol_and_endpoint}, record filter: {log_directives})"
            );
            // The trace and span id on a record come from the span layer having activated the
            // span's context. Without that layer every record carries an empty trace id, which
            // looks like a backend that will not correlate rather than a server that never sent
            // the ids. Reachable by naming only the logs endpoint.
            if !exporting_spans {
                log::warn!(
                    "OpenTelemetry log export is on without span export, so every log record carries an empty trace id. Configure OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT to correlate records with traces."
                );
            }
        } else if otel_logs_enabled() && otel_endpoint("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").is_none()
        {
            // An opt-in that resolves to no endpoint. Silence here reads as a backend that is
            // dropping the records. A failed exporter build reports itself, so this covers only
            // the case where nothing named an endpoint at all.
            log::error!(
                "OTEL_LOGS_EXPORTER asks for OTLP log export but no OTLP endpoint is configured! (Ignoring)"
            );
        }
    }

    #[cfg(not(feature = "otel"))]
    {
        registry.try_init()?;

        if non_empty_env("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").is_some()
            || non_empty_env("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT").is_some()
            || non_empty_env("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
        {
            log::error!(
                "An OTLP endpoint is configured but the otel feature is not enabled! (Ignoring)"
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
        #[cfg(feature = "otel")]
        _logger_provider: m_logger_provider,
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

/// Env var turning OTLP log export on, per the OpenTelemetry SDK's own configuration: `otlp`
/// exports, and `none` or unset does not.
#[cfg(feature = "otel")]
const OTEL_LOGS_EXPORTER_ENV: &str = "OTEL_LOGS_EXPORTER";

/// Targets held out of OTLP log export unconditionally: the exporter's own crates, and the HTTP
/// and gRPC stacks it sends over.
///
/// Every export is itself network IO that logs, so exporting those lines feeds the exporter a
/// fresh batch for every batch it delivers, and the loop sustains itself for as long as the
/// process runs. It is dormant at the default `info`, where these crates are near-silent, and
/// arrives the moment someone raises `OXEN_OTEL_FILTER` to `debug` to look into an export problem.
/// Measured at `debug` against a collector, a single idle server spent most of a batch on its own
/// connection-pool and HTTP/2 frame chatter, crowding out the records worth reading.
///
/// The transport crates carry this repo's own HTTP client as well, so excluding them costs the
/// backend any client-side detail from a push or a pull. `RUST_LOG` still puts all of it on
/// stderr, which is where that detail is useful, and export at `debug` could not deliver it
/// through the noise anyway.
///
/// Kept as targets rather than a directive string: a stray character inside a hand-written
/// directive list is dropped by the filter parser with nothing but a line on stderr, and the
/// target it named is exported after all.
#[cfg(feature = "otel")]
const OTEL_LOG_EXCLUDED_TARGETS: &[&str] = &[
    "opentelemetry",
    "opentelemetry_sdk",
    "opentelemetry_otlp",
    "reqwest",
    "hyper",
    "hyper_util",
    "h2",
    "tonic",
    "tower",
];

/// Whether `OTEL_LOGS_EXPORTER` asks for OTLP log records.
#[cfg(feature = "otel")]
fn otel_logs_enabled() -> bool {
    logs_exporter_selects_otlp(non_empty_env(OTEL_LOGS_EXPORTER_ENV).as_deref())
}

/// Read a configured `OTEL_LOGS_EXPORTER` as a decision to export log records over OTLP. Off
/// unless the value names the OTLP exporter, so a deployment that has not opted in exports nothing
/// and `none` reads as the "no exporter" it means everywhere else in the OpenTelemetry ecosystem.
///
/// The variable takes a comma-separated list, and `otlp` is the only exporter this build carries,
/// so any list naming it selects it.
#[cfg(feature = "otel")]
fn logs_exporter_selects_otlp(configured: Option<&str>) -> bool {
    configured.is_some_and(|value| {
        value
            .split(',')
            .any(|exporter| exporter.trim().eq_ignore_ascii_case("otlp"))
    })
}

/// The filter directives OTLP log export uses: span export's, with the exporter's own crates
/// silenced.
///
/// Reusing `OXEN_OTEL_FILTER` keeps one variable deciding what leaves the process over OTLP, so a
/// deployment cannot end up exporting spans and log records at levels that disagree. `RUST_LOG`
/// stays in charge of stdout either way.
///
/// [`OTEL_LOG_EXCLUDED_TARGETS`] goes last, and the filter parser lets the later directive for a
/// target win, so the exclusion holds whatever `OXEN_OTEL_FILTER` says about those targets.
#[cfg(feature = "otel")]
fn otel_log_filter_directives() -> String {
    std::iter::once(otel_filter_directives())
        .chain(
            OTEL_LOG_EXCLUDED_TARGETS
                .iter()
                .map(|target| format!("{target}=off")),
        )
        .collect::<Vec<_>>()
        .join(",")
}

/// Whether the environment already names a service, in which case the app's own name is not used.
///
/// `service_name_var` is `OTEL_SERVICE_NAME`'s raw value, and `attributes_service_name` is the
/// `service.name` entry from `OTEL_RESOURCE_ATTRIBUTES`, the other place the OpenTelemetry SDK
/// accepts one. Either names a service only when set to something other than whitespace.
#[cfg(feature = "otel")]
fn env_names_a_service(
    service_name_var: Option<&str>,
    attributes_service_name: Option<&str>,
) -> bool {
    [service_name_var, attributes_service_name]
        .into_iter()
        .flatten()
        .any(|name| !name.trim().is_empty())
}

/// The endpoint variable that enables export of `signal_var`'s signal, and its value, reported for
/// the startup line. The signal's own variable wins over the collector-wide one, per OTLP.
///
/// Read only to decide whether to build an exporter at all: without one the exporter defaults to
/// `http://localhost:4318` and every process would push telemetry there. The exporter reads these
/// same variables itself and applies the OTLP precedence and signal-path rules, so the value is
/// not passed along.
#[cfg(feature = "otel")]
fn otel_endpoint(signal_var: &'static str) -> Option<(&'static str, String)> {
    [signal_var, "OTEL_EXPORTER_OTLP_ENDPOINT"]
        .into_iter()
        .find_map(|var| non_empty_env(var).map(|value| (var, value)))
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

/// The transport OTLP export uses, from `signal_var`'s protocol variable or the general one.
#[cfg(feature = "otel")]
fn otel_protocol(signal_var: &'static str) -> Result<Protocol, TelemetryError> {
    let (var, configured) = [signal_var, "OTEL_EXPORTER_OTLP_PROTOCOL"]
        .into_iter()
        .find_map(|var| non_empty_env(var).map(|value| (var, Some(value))))
        .unwrap_or(("OTEL_EXPORTER_OTLP_PROTOCOL", None));
    parse_otel_protocol(var, configured.as_deref())
}

/// Read a configured protocol as a transport, defaulting to HTTP where nothing is configured.
///
/// An unaccepted value is reported against `var`, the variable it came from.
///
/// `http/protobuf` and `http/json` name an encoding as well as a transport, and both select HTTP.
/// Spans are encoded as binary protobuf either way, which every OTLP/HTTP endpoint accepts.
#[cfg(feature = "otel")]
fn parse_otel_protocol(
    var: &'static str,
    configured: Option<&str>,
) -> Result<Protocol, TelemetryError> {
    match configured
        .map(|value| value.trim().to_lowercase())
        .as_deref()
    {
        None | Some("http" | "http/protobuf" | "http/json") => Ok(Protocol::Http),
        Some("grpc") => Ok(Protocol::Grpc),
        Some(unknown) => Err(TelemetryError::UnknownProtocol {
            var,
            value: unknown.to_string(),
        }),
    }
}

/// The resource identifying this process on every span and log record it exports.
///
/// `Resource::builder` already applies the standard detectors, so `OTEL_SERVICE_NAME` and
/// `OTEL_RESOURCE_ATTRIBUTES` (where `deployment.environment.name` is set) are picked up here. The
/// attributes added on top take precedence over the detectors, so `service.name` is only supplied
/// as the fallback for a deployment that names no service of its own.
#[cfg(feature = "otel")]
fn otel_resource(app_name: &str) -> opentelemetry_sdk::Resource {
    use opentelemetry::{Key, KeyValue};
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::resource::{EnvResourceDetector, ResourceDetector};

    let attributes_service_name = EnvResourceDetector::new()
        .detect()
        .get(&Key::from_static_str("service.name"))
        .map(|name| name.to_string());
    let mut builder = Resource::builder().with_attributes([KeyValue::new(
        "service.version",
        crate::constants::OXEN_VERSION,
    )]);
    if !env_names_a_service(
        std::env::var("OTEL_SERVICE_NAME").ok().as_deref(),
        attributes_service_name.as_deref(),
    ) {
        builder = builder.with_service_name(app_name.to_string());
    }
    builder.build()
}

/// Build an OpenTelemetry tracing layer that exports spans via OTLP.
///
/// The exporter reads the endpoint from the environment, appending the OTLP signal path to a
/// collector endpoint and dialing a traces endpoint as configured.
///
/// Returns the layer (wrapped in `Option`) and the provider. When the
/// exporter cannot be built, returns `(None, None)`.
#[cfg(feature = "otel")]
fn build_otel_layer<S>(
    app_name: &str,
    protocol: &Protocol,
) -> (
    Option<tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>>,
    Option<opentelemetry_sdk::trace::SdkTracerProvider>,
)
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_otlp::WithTonicConfig;
    use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor, SdkTracerProvider};

    let exporter = match protocol {
        Protocol::Http => {
            // Named rather than left to the exporter's default, which is whichever encoding the
            // enabled cargo features imply: any crate in the graph turning on "http-json" would
            // otherwise switch every export to JSON, silently and without a compile error.
            // `opentelemetry_otlp::Protocol` is qualified to keep it apart from this module's own.
            match opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
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

    let resource = otel_resource(app_name);

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

/// Build a tracing layer that exports events as OTLP log records.
///
/// The exporter reads the endpoint from the environment, appending `/v1/logs` to a collector
/// endpoint and dialing a logs endpoint as configured.
///
/// Each record carries the trace and span id of the tracing span the event was recorded in, but
/// only while the registry also holds a `tracing-opentelemetry` layer: that layer is what
/// activates a span's OpenTelemetry context, and the SDK's logger stamps the record from whatever
/// context is current.
///
/// Returns the layer and the provider, or `None` when the exporter cannot be built.
#[cfg(feature = "otel")]
fn build_otel_log_layer(
    app_name: &str,
    protocol: &Protocol,
) -> Option<(
    opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge<
        opentelemetry_sdk::logs::SdkLoggerProvider,
        opentelemetry_sdk::logs::SdkLogger,
    >,
    opentelemetry_sdk::logs::SdkLoggerProvider,
)> {
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_otlp::WithTonicConfig;
    use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
    use opentelemetry_sdk::logs::{BatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider};

    let exporter = match protocol {
        // Named rather than left to the exporter's default, for the reason given in
        // `build_otel_layer`: an enabled "http-json" feature anywhere in the graph would otherwise
        // switch the encoding without a compile error.
        Protocol::Http => opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build()
            .inspect_err(|err| eprintln!("[ERROR] failed to build OTel HTTP log exporter: {err}"))
            .ok()?,
        Protocol::Grpc => opentelemetry_otlp::LogExporter::builder()
            .with_tonic()
            .with_tls_config(ClientTlsConfig::new().with_native_roots())
            .build()
            .inspect_err(|err| eprintln!("[ERROR] failed to build OTel gRPC log exporter: {err}"))
            .ok()?,
    };

    // Bounded like the span queue and for the same reason: a collector that is slow or unreachable
    // costs the process a fixed amount of memory, and the processor drops records once the queue
    // is full rather than back-pressuring the thread that logged. Each field is only set when the
    // operator left the matching `OTEL_BLRP_*` variable unset, so tuning through the standard
    // variables still works.
    let mut batch_config = BatchConfigBuilder::default();
    if std::env::var_os("OTEL_BLRP_MAX_QUEUE_SIZE").is_none() {
        batch_config = batch_config.with_max_queue_size(4096);
    }
    if std::env::var_os("OTEL_BLRP_SCHEDULE_DELAY").is_none() {
        batch_config = batch_config.with_scheduled_delay(std::time::Duration::from_secs(2));
    }
    let processor = BatchLogProcessor::builder(exporter)
        .with_batch_config(batch_config.build())
        .build();

    let provider = SdkLoggerProvider::builder()
        .with_log_processor(processor)
        .with_resource(otel_resource(app_name))
        .build();

    let layer = OpenTelemetryTracingBridge::new(&provider);
    Some((layer, provider))
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
            OTEL_DEFAULT_FILTER, OTEL_LOG_EXCLUDED_TARGETS, Protocol, TelemetryError, env_filter,
            env_names_a_service, logs_exporter_selects_otlp, otel_filter_directives,
            otel_log_filter_directives, parse_otel_protocol,
        };
        use opentelemetry::trace::{SpanId, TraceId};
        use tracing_subscriber::EnvFilter;

        /// Nothing configured is HTTP, the transport `OTEL_EXPORTER_OTLP_PROTOCOL` carries by
        /// default across the OTLP ecosystem.
        #[test]
        fn no_configured_protocol_is_http() {
            assert_eq!(
                parse_otel_protocol("OTEL_EXPORTER_OTLP_PROTOCOL", None).unwrap(),
                Protocol::Http
            );
        }

        /// `OTEL_EXPORTER_OTLP_PROTOCOL` carries values naming an encoding along with the
        /// transport. Both HTTP spellings select HTTP rather than being rejected, so a deployment
        /// copying a stock OTLP snippet exports over the transport it asked for.
        #[test]
        fn standard_protocol_values_select_a_transport() {
            let var = "OTEL_EXPORTER_OTLP_PROTOCOL";
            assert_eq!(
                parse_otel_protocol(var, Some("grpc")).unwrap(),
                Protocol::Grpc
            );
            // Case and surrounding whitespace are normalized before the match, so the HTTP
            // spellings cover both for every value.
            for value in [
                "http",
                "http/protobuf",
                "http/json",
                "HTTP/protobuf",
                " http ",
            ] {
                assert_eq!(
                    parse_otel_protocol(var, Some(value)).unwrap(),
                    Protocol::Http,
                    "{value} should select HTTP"
                );
            }
        }

        /// An unaccepted value names the variable it came from, so an operator who set the
        /// traces-specific one is not sent looking at the general one.
        #[test]
        fn rejects_an_unknown_protocol() {
            for var in [
                "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
                "OTEL_EXPORTER_OTLP_PROTOCOL",
            ] {
                for value in ["https", "http/proto", "tcp"] {
                    let result = parse_otel_protocol(var, Some(value));
                    match result {
                        Err(TelemetryError::UnknownProtocol {
                            var: named,
                            value: v,
                        }) => {
                            assert_eq!(
                                named, var,
                                "the rejected value should name its own variable"
                            );
                            assert_eq!(v, value);
                        }
                        other => panic!("{var}={value} should be rejected, got {other:?}"),
                    }
                }
            }
        }

        /// A service the operator named through either variable is kept, and the app's own name is
        /// used only when neither does. Overriding a configured `service.name` would rename the
        /// deployment's traces out from under whoever set it.
        #[test]
        fn the_environment_names_the_service_when_it_says_so() {
            // `OTEL_SERVICE_NAME` set to something.
            assert!(env_names_a_service(Some("checkout"), None));
            // `service.name` supplied through `OTEL_RESOURCE_ATTRIBUTES` instead.
            assert!(env_names_a_service(None, Some("checkout")));
            // Both, which is the same answer.
            assert!(env_names_a_service(Some("checkout"), Some("billing")));
        }

        /// A blank value names nothing whichever variable carries it, so the app's own name is
        /// still used. Reading one as a name would export traces under an empty service.
        /// `OTEL_RESOURCE_ATTRIBUTES=service.name=` and `service.name=   ` both arrive here blank,
        /// since the SDK trims each value it parses out of that variable.
        #[test]
        fn a_blank_service_name_names_nothing() {
            for blank in [None, Some(""), Some("   ")] {
                assert!(
                    !env_names_a_service(blank, None),
                    "OTEL_SERVICE_NAME {blank:?} should not count as naming a service"
                );
                assert!(
                    !env_names_a_service(None, blank),
                    "service.name {blank:?} should not count as naming a service"
                );
                assert!(
                    !env_names_a_service(blank, blank),
                    "both blank at {blank:?} should not count as naming a service"
                );
            }
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

        /// Only a value naming the OTLP exporter turns log export on. Anything else, `none`
        /// included, leaves a deployment exporting no log records at all.
        #[test]
        fn only_otlp_selects_log_export() {
            for selected in ["otlp", "OTLP", " otlp ", "none,otlp", "otlp,console"] {
                assert!(
                    logs_exporter_selects_otlp(Some(selected)),
                    "{selected} should select OTLP log export"
                );
            }
            for unselected in [None, Some(""), Some("none"), Some("console"), Some("otl")] {
                assert!(
                    !logs_exporter_selects_otlp(unselected),
                    "{unselected:?} should leave log export off"
                );
            }
        }

        /// The log filter carries span export's directives, so one variable decides what leaves
        /// the process over OTLP, and silences every excluded target on top of them.
        #[test]
        fn the_log_filter_extends_the_span_filter() {
            let directives = otel_log_filter_directives();
            assert!(
                directives.starts_with(&otel_filter_directives()),
                "{directives} should begin with the span filter's directives"
            );
            for target in OTEL_LOG_EXCLUDED_TARGETS {
                assert!(
                    directives.contains(&format!("{target}=off")),
                    "{directives} should silence {target}"
                );
            }
        }

        /// Every excluded target has to survive the filter parser. `parse_lossy` drops a directive
        /// it cannot read with nothing but a line on stderr, and the target it named is then
        /// exported after all, which is how a stray space inside the list stays invisible.
        #[test]
        fn every_excluded_target_is_a_usable_directive() {
            let directives = otel_log_filter_directives();
            assert!(
                EnvFilter::builder().parse(&directives).is_ok(),
                "{directives} should parse with no directive dropped"
            );
        }

        /// The exclusion half of the log filter, built without reading the environment so a
        /// sibling test's `OXEN_OTEL_FILTER` cannot change what this asserts.
        fn excluded_directives() -> String {
            OTEL_LOG_EXCLUDED_TARGETS
                .iter()
                .map(|target| format!("{target}=off"))
                .collect::<Vec<_>>()
                .join(",")
        }

        /// The targets the exclusion test below emits an event on.
        const EMITTED_EXCLUDED_TARGETS: &[&str] = &[
            "opentelemetry",
            "opentelemetry_sdk",
            "opentelemetry_otlp",
            "reqwest",
            "hyper",
            "hyper_util",
            "h2",
            "tonic",
            "tower",
        ];

        /// A target added to the exclusion list without an event to match leaves the exclusion
        /// unproven for it, and the test below still passes.
        #[test]
        fn every_excluded_target_is_covered_by_an_event() {
            assert_eq!(EMITTED_EXCLUDED_TARGETS, OTEL_LOG_EXCLUDED_TARGETS);
        }

        /// A log record picks up the trace and span id of the tracing span it was recorded in,
        /// which is what lets a backend show a log line against its trace. `tracing-opentelemetry`
        /// activates the span's OpenTelemetry context on entry and the SDK's logger reads it, so
        /// nothing here wires the two together by hand.
        ///
        /// The exporter's own crates and the transport it sends over stay out of the export even
        /// when the directives ask for them, since exporting what an export logs is what makes an
        /// exporter generate work for itself.
        #[test]
        fn log_records_carry_their_span_and_never_the_exporters_own_crates() {
            use opentelemetry::trace::{TraceContextExt, TracerProvider};
            use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
            use opentelemetry_sdk::logs::{SdkLoggerProvider, SimpleLogProcessor};
            use opentelemetry_sdk::trace::SdkTracerProvider;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            use tracing_subscriber::Layer;
            use tracing_subscriber::layer::SubscriberExt;

            let exporter = CollectingLogExporter::default();
            let logger_provider = SdkLoggerProvider::builder()
                .with_log_processor(SimpleLogProcessor::new(exporter.clone()))
                .build();
            let tracer_provider = SdkTracerProvider::builder().build();

            // Directives asking for every excluded target at its loudest, to prove the exclusion
            // outranks them.
            let asked_for = OTEL_LOG_EXCLUDED_TARGETS
                .iter()
                .map(|target| format!("{target}=trace"))
                .collect::<Vec<_>>()
                .join(",");
            let directives = format!("info,{asked_for},{}", excluded_directives());
            let subscriber = tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("oxen")))
                .with(
                    OpenTelemetryTracingBridge::new(&logger_provider)
                        .with_filter(env_filter(&directives, OTEL_DEFAULT_FILTER)),
                );

            let expected_trace_id = tracing::subscriber::with_default(subscriber, || {
                let span = tracing::info_span!("request");
                let _entered = span.enter();
                tracing::info!("handled the request");
                // One event per excluded target, spelled out because `target:` takes a literal.
                // `every_excluded_target_is_covered_by_an_event` keeps these in step with the
                // constant.
                tracing::error!(target: "opentelemetry", "export failed");
                tracing::error!(target: "opentelemetry_sdk", "export failed");
                tracing::error!(target: "opentelemetry_otlp", "export failed");
                tracing::debug!(target: "reqwest", "starting new connection");
                tracing::debug!(target: "hyper", "flushed frame");
                tracing::debug!(target: "hyper_util", "pooling idle connection");
                tracing::debug!(target: "h2", "send");
                tracing::debug!(target: "tonic", "sending request");
                tracing::debug!(target: "tower", "service ready");
                span.context().span().span_context().trace_id()
            });

            assert_ne!(
                expected_trace_id,
                TraceId::INVALID,
                "the tracing span should have a real trace id to correlate against"
            );

            let exported = exporter.exported();
            assert_eq!(
                exported.len(),
                1,
                "only the application's own event should be exported, got {exported:?}"
            );
            let (trace_id, span_id) = exported[0];
            assert_eq!(trace_id, expected_trace_id);
            assert_ne!(
                span_id,
                SpanId::INVALID,
                "the record should name the span it was recorded in"
            );
        }

        /// Collects the trace and span id of every record handed to it, standing in for a
        /// collector.
        #[derive(Debug, Default, Clone)]
        struct CollectingLogExporter {
            records: std::sync::Arc<std::sync::Mutex<Vec<(TraceId, SpanId)>>>,
        }

        impl CollectingLogExporter {
            fn exported(&self) -> Vec<(TraceId, SpanId)> {
                self.records
                    .lock()
                    .expect("the exporter's records should not be poisoned")
                    .clone()
            }
        }

        impl opentelemetry_sdk::logs::LogExporter for CollectingLogExporter {
            async fn export(
                &self,
                batch: opentelemetry_sdk::logs::LogBatch<'_>,
            ) -> opentelemetry_sdk::error::OTelSdkResult {
                let mut records = self
                    .records
                    .lock()
                    .expect("the exporter's records should not be poisoned");
                for (record, _scope) in batch.iter() {
                    let context = record
                        .trace_context()
                        .expect("an exported record should carry a trace context");
                    records.push((context.trace_id, context.span_id));
                }
                Ok(())
            }
        }
    }
}
