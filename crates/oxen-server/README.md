# oxen-server
The server for remote `oxen` repositories.

Remote repositories have the same internal structure as local ones, with the caveat that all the data is in the `.oxen/` dir and not duplicated into a "local workspace".

**Notable configuration sections**:
- [Prometheus Metrics](#prometheus-metrics)
- [OpenTelemetry Tracing](#opentelemetry-tracing)
- [FmtSpan Events](#fmtspan-events)
- [Stacking Tracing Layers | Writing Spans to Logs & OTel](#stacking-tracing-layers)


## Build

See the [prerequisites](../../README.md#prerequisites) section of the main readme before developing.
Use the standard `cargo ... --workspace` commands and `cargo ... -p oxen-server` commands.


## Run

To run a local Oxen Server, generate a config file and token to authenticate the user:
```bash
cargo run -p oxen-server add-user --email ox@oxen.ai --name Ox --output user_config.toml
```

Copy the config to the default locations:
```bash
mkdir ~/.oxen
mv user_config.toml ~/.oxen/user_config.toml
mkdir -p data/test/config/
cp ~/.oxen/user_config.toml data/test/config/user_config.toml
```

Set where you want the data to be synced to.
The default sync directory is `./data/`.
To change, set the `SYNC_DIR` environment variable to a path:
```bash
export SYNC_DIR=/path/to/sync/dir
```

You can also create a `.env.local` file in the `crates/oxen-server/` directory which can contain the `SYNC_DIR` variable to avoid setting it every time you run the server.

Run the server:
```bash
cargo run -p oxen-server start
```

Or run the compiled binary directly:
```bash
./target/debug/oxen-server start
```

To run the server with live reload, use `bacon`:
```bash
cargo install --locked bacon
```

Then run the server like this:
```bash
bacon server
```

### API Examples

Server defaults to localhost 3000.

You can grab your auth token from the config file above (`~/.oxen/user_config.toml`):
```bash
export TOKEN="<YOUR_TOKEN>"
```

#### List Repositories
```bash
curl -H "Authorization: Bearer $TOKEN" "http://0.0.0.0:3000/api/repos"
```

#### Create Repository
```bash
curl -H "Authorization: Bearer $TOKEN" -X POST -d '{"name": "MyRepo"}' "http://0.0.0.0:3000api/repos"
```


## Logging

Oxen uses structured logging.
It outputs to STDERR by default but can be configured with rotating log files.
See [Logging](../../README.md#logging) for details.

By default, `oxen-server` logs at the `WARN` level. Set `RUST_LOG` to change.
It gates the log destinations only — span export has its own filter, see
[Filtering: logs and spans are separate](#filtering-logs-and-spans-are-separate).


## Prometheus Metrics

`oxen-server` exposes a [Prometheus](https://prometheus.io/)-compatible
metrics endpoint. This allows you to monitor server health, track request
counts, error rates, and other operational metrics using standard Prometheus
tooling.

### Compile-time feature flag

Metrics collection requires the `metrics` Cargo feature. Without it, all
metric collections (`counter!`, `histogram!`, etc.) compile to no-ops —
no counters are recorded and no `/metrics` endpoint is served,
regardless of environment variables.

The `metrics` feature is included in `production`, so a production build
already has it:

```bash
cargo build --workspace --features production
```

To enable metrics alone (without OpenTelemetry tracing or other production
features):

```bash
# just metrics, for any crate
cargo build --workspace --features metrics

# or per-crate
cargo build -p oxen-server --features metrics
cargo build -p oxen        --features metrics
cargo build -p liboxen     --features metrics
```

If `OXEN_METRICS_PORT` is set at runtime (to a value other than `off`)
but the binary was compiled **without** the `metrics` feature, the server
logs an error at startup explaining the mismatch.

### How it works

On startup (when compiled with `metrics`), `oxen-server` launches a
lightweight HTTP server (separate from the main API) that serves metrics
in the Prometheus exposition format. Any counters, gauges, or histograms
recorded via the [`metrics`](https://docs.rs/metrics) crate are
automatically exposed.

### Configuration

The metrics endpoint is **opt-in**. Set `OXEN_METRICS_PORT` to a port number
to enable it.

| Variable | Description | Default |
|---|---|---|
| `OXEN_METRICS_PORT` | Port for the metrics HTTP server (opt-in) | *(none — disabled)* |
| `OXEN_METRICS_PORT=off` | Explicitly disable the metrics endpoint | -- |

```bash
# No metrics server (default)
cargo run -p oxen-server --features metrics start

# Enable metrics on port 9090
OXEN_METRICS_PORT=9090 cargo run -p oxen-server --features metrics start

# Enable metrics on a custom port
OXEN_METRICS_PORT=9100 cargo run -p oxen-server --features metrics start

# Explicitly disable metrics
OXEN_METRICS_PORT=off cargo run -p oxen-server --features metrics start
```

### Verifying with `curl`

```bash
curl http://localhost:9090/metrics
```

This returns all registered metrics in Prometheus text format, e.g.:

```
# TYPE oxen_errors_total counter
oxen_errors_total{module="commits",error="not_found"} 3
```

### Integrating with Prometheus

Add a scrape target to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: oxen-server
    scrape_interval: 15s
    static_configs:
      - targets: ["localhost:9090"]
```

If you run multiple `oxen-server` instances, list each one (or use service
discovery):

```yaml
scrape_configs:
  - job_name: oxen-server
    static_configs:
      - targets:
          - "oxen-1.internal:9090"
          - "oxen-2.internal:9090"
```

### Integrating with Grafana

Once Prometheus is scraping the endpoint, add it as a data source in
[Grafana](https://grafana.com/) and build dashboards using PromQL queries.
For example:

```
rate(oxen_errors_total[5m])
```


## OpenTelemetry Tracing

`oxen-server` can export tracing spans to any OTLP-compatible collector
(Jaeger, Tempo, Honeycomb, Datadog, etc.). The release image is built with the
`otel` feature; a local build needs it named explicitly:

```bash
cargo build -p oxen-server --features otel
```

At runtime, set `OXEN_OTEL_ENDPOINT` to enable export. Nothing is exported
until you do, so a build with the feature compiled in and no endpoint
configured behaves exactly like one without it.

```bash
# gRPC (default protocol) — a bare host:port gets http://
OXEN_OTEL_ENDPOINT=localhost:4317 oxen-server start

# OTLP/HTTP
OXEN_OTEL_ENDPOINT=localhost:4318 OXEN_OTEL_PROTOCOL=http oxen-server start

# A TLS-terminated vendor endpoint
OXEN_OTEL_ENDPOINT=https://otlp.vendor.example:443 oxen-server start
```

| Variable | Description | Default |
|---|---|---|
| `OXEN_OTEL_ENDPOINT` | Collector endpoint: an `http://` or `https://` URL, or a bare `host:port` (which gets `http://`). Absent = export disabled. | *(none)* |
| `OXEN_OTEL_PROTOCOL` | Transport: `grpc` or `http`. Under `http` the OTLP signal path `/v1/traces` is appended to the endpoint unless it already names one, and the payload is binary protobuf. | `grpc` |
| `OXEN_OTEL_FILTER` | Which spans and events are exported. Same syntax as `RUST_LOG`, and independent of it. | `info` |

An `https://` endpoint is verified against the platform's root certificate
store under both transports, so a collector behind a publicly trusted
certificate needs no further configuration. A private CA has to be installed in
that store.

The standard `OTEL_EXPORTER_OTLP_ENDPOINT` variable is also respected as a
fallback if `OXEN_OTEL_ENDPOINT` is not set.

These standard `OTEL_*` variables are read by the SDK itself:

| Variable | Description | Default |
|---|---|---|
| `OTEL_SERVICE_NAME` | `service.name` on every exported span. | `oxen-server` |
| `OTEL_RESOURCE_ATTRIBUTES` | Comma-separated `key=value` resource attributes. This is where `deployment.environment` is set — nothing else supplies it. | *(none)* |
| `OTEL_TRACES_SAMPLER` | `always_on`, `always_off`, `traceidratio`, `parentbased_always_on`, `parentbased_always_off`, `parentbased_traceidratio`. | `parentbased_always_on` |
| `OTEL_TRACES_SAMPLER_ARG` | Sampling probability, `0.0`–`1.0`, for the ratio samplers. | `1.0` |
| `OTEL_BSP_MAX_QUEUE_SIZE`, `OTEL_BSP_SCHEDULE_DELAY`, `OTEL_BSP_MAX_EXPORT_BATCH_SIZE`, `OTEL_BSP_EXPORT_TIMEOUT` | Batch-processor tuning: queue depth, how often a batch drains, batch size, and how long the processor waits on one export. | `4096`, `2000` ms, `512`, `30000` ms |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | How long one export request to the collector may take, for every signal. Distinct from `OTEL_BSP_EXPORT_TIMEOUT` above, which bounds the batch processor rather than the request. | `10000` ms |
| `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT` | The same bound for span exports alone, and takes precedence over `OTEL_EXPORTER_OTLP_TIMEOUT` where both are set. | *(whatever `OTEL_EXPORTER_OTLP_TIMEOUT` resolves to)* |

Every span carries `service.name`, `service.version`, and — when a caller sent
an `x-oxen-request-id` header, or the server minted one — `oxen.request_id`.
`deployment.environment` is there too once `OTEL_RESOURCE_ATTRIBUTES` sets it.
`tracing-actix-web` records a second field named `request_id`; that one is its
own per-request uuid, private to this process. Correlate on `oxen.request_id`.

The default sampler is parent-based, so a caller that has already made a
sampling decision and sent it in `traceparent` is honored. To sample a share of
the traces this server roots:

```bash
OTEL_TRACES_SAMPLER=parentbased_traceidratio OTEL_TRACES_SAMPLER_ARG=0.1
```

When the `otel` feature is not compiled in, no OpenTelemetry dependencies are
included and the env vars are ignored (the server logs an error at startup if
an endpoint is configured, rather than silently dropping it).

### Inbound trace context

The server reads a W3C `traceparent` header and continues the caller's trace
instead of starting a new one, so a request forwarded from another service
appears as a child of that service's span. `tracestate` rides along with it; no
other propagation format is read, and baggage is not.

There is no *outbound* propagation: the server does not inject `traceparent`
into calls it makes.

### Filtering: logs and spans are separate

`RUST_LOG` gates the log destinations — stderr, the JSON file, and error
reporting. `OXEN_OTEL_FILTER` gates span export. They are independent, which
matters because the two want different levels: the server logs at `WARN` by
default, while `#[tracing::instrument]` spans and the HTTP root span are
recorded at `INFO`.

So traces export correctly at the stock log level, and raising `RUST_LOG` for
debugging does not change what is exported:

```bash
# Full traces, warnings and errors only on stderr — the recommended setup.
OXEN_OTEL_ENDPOINT=http://localhost:4317 oxen-server start

# Verbose stderr for a debugging session; the traces are unchanged.
OXEN_OTEL_ENDPOINT=http://localhost:4317 RUST_LOG=warn,liboxen=debug oxen-server start
```

`OXEN_OTEL_FILTER` takes the same directive syntax, so span export can be
narrowed or widened on its own:

```bash
# Only spans and events from the server's own code.
OXEN_OTEL_FILTER="warn,oxen_server=info,tracing_actix_web=info"
```

Two cautions. Anything below `info` exports nothing, because that is the level
the spans are recorded at. And `debug` unlocks well over a thousand call sites
in `liboxen`, many inside per-file loops — each becomes an event attached to
the enclosing span. Scope it to a target rather than setting it globally.

### Quick Start with Jaeger

```bash
# Start Jaeger all-in-one: https://www.jaegertracing.io/docs/2.17/
docker run --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 5778:5778 \
  -p 9411:9411 \
  cr.jaegertracing.io/jaegertracing/jaeger:2.17.0

# Start oxen-server with OTel export
OXEN_OTEL_ENDPOINT=http://localhost:4317 cargo run --features otel -p oxen-server start

# View traces at http://localhost:16686 under service "oxen-server"
```

`bin/otel-metrics-test` runs this end to end — Jaeger in Docker, a full
push/clone/pull, then assertions that the traces arrived, that an inbound
`traceparent` was continued, and that work on the blocking pool stayed inside
the request's trace.


## FmtSpan Events

Span lifecycle events (creation, entry, exit, close) can be emitted as
additional log lines on stderr. This is useful for seeing timing of
`#[instrument]`-annotated functions without a full tracing collector.

Set `OXEN_FMT_SPAN` to enable:

```bash
# Log when spans close (includes elapsed time)
OXEN_FMT_SPAN=CLOSE oxen-server start

# Log all span lifecycle events
OXEN_FMT_SPAN=FULL oxen-server start

# Combine specific events
OXEN_FMT_SPAN="NEW|CLOSE" oxen-server start
```

Accepted values: `NEW`, `CLOSE`, `ENTER`, `EXIT`, `ACTIVE` (enter+exit),
`FULL` (all), `NONE`, `1`/`true` (alias for `CLOSE`).

No feature flag or additional dependencies are required.


## Stacking Tracing Layers

All tracing outputs can be enabled simultaneously. For example, to get stderr
output with span timing, JSON file logs, and OpenTelemetry export:

```bash
OXEN_LOG_DIR='/var/log/oxen' \
OXEN_FMT_SPAN='CLOSE' \
OXEN_OTEL_ENDPOINT='http://localhost:4317' \
RUST_LOG='info' \
oxen-server start
```

`RUST_LOG` here raises the two log destinations. Span export is filtered by
`OXEN_OTEL_FILTER` and is unaffected by it.
