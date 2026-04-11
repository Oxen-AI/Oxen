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

You can also create a `.env.local` file in the `crates/server/` directory which can contain the `SYNC_DIR` variable to avoid setting it every time you run the server.

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
curl -H "Authorization: Bearer $TOKEN" -X POST -d '{"name": "MyRepo"}' "http://0.0.0.0:3000/api/repos"
```

#### Upload a File (Raw PUT)

Upload a file directly using a raw HTTP body (no multipart form needed):

```bash
curl -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: text/plain" \
     -X PUT \
     -d "Hello, World!" \
     "http://0.0.0.0:3000/api/repos/my_namespace/MyRepo/file/main/hello.txt"
```

The existing multipart PUT is still supported. The server detects the format from the `Content-Type` header.

#### Optimistic Concurrency (`oxen-based-on`)

When downloading a file via GET, the response includes an `oxen-revision-id` header with the commit that last modified the file. To prevent overwriting concurrent changes, pass this value back on PUT or DELETE:

```bash
# Get the current revision
REVISION=$(curl -sI -H "Authorization: Bearer $TOKEN" \
  "http://0.0.0.0:3000/api/repos/my_namespace/MyRepo/file/main/hello.txt" \
  | grep -i oxen-revision-id | awk '{print $2}' | tr -d '\r')

# Update with concurrency check
curl -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: text/plain" \
     -H "oxen-based-on: $REVISION" \
     -X PUT \
     -d "Updated content" \
     "http://0.0.0.0:3000/api/repos/my_namespace/MyRepo/file/main/hello.txt"
```

If the file has been modified since the claimed revision, the server returns `400 Bad Request`.


## Logging

Oxen uses structured logging.
It outputs to STDERR by default but can be configured with rotating log files.
See [Logging](../../README.md#logging) for details.

By default, `oxen-server` logs at the `WARN` level. Set `RUST_LOG` to change.


## Prometheus Metrics

`oxen-server` exposes a [Prometheus](https://prometheus.io/)-compatible
metrics endpoint. This allows you to monitor server health, track request
counts, error rates, and other operational metrics using standard Prometheus
tooling.

### How it works

On startup, `oxen-server` launches a lightweight HTTP server (separate from
the main API) that serves metrics in the Prometheus exposition format. Any
counters, gauges, or histograms recorded via the [`metrics`](https://docs.rs/metrics)
crate are automatically exposed.

### Configuration

The metrics endpoint is **enabled by default** on port **9090**.

| Variable | Description | Default |
|---|---|---|
| `OXEN_METRICS_PORT` | Port for the metrics HTTP server | `9090` |
| `OXEN_METRICS_PORT=off` | Disable the metrics endpoint entirely | -- |

```bash
# Use the default port (9090)
cargo run -p oxen-server start

# Use a custom port
OXEN_METRICS_PORT=9100 cargo run -p oxen-server start

# Disable metrics entirely
OXEN_METRICS_PORT=off cargo run -p oxen-server start
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
(Jaeger, Tempo, Honeycomb, Datadog, etc.). This requires building with the
`otel` feature flag:

```bash
cargo build -p oxen-server --features otel
```

At runtime, set `OXEN_OTEL_ENDPOINT` to enable export:

```bash
# gRPC (default protocol)
OXEN_OTEL_ENDPOINT=localhost:4317 oxen-server start

# HTTP/JSON
OXEN_OTEL_ENDPOINT=localhost:4318 OXEN_OTEL_PROTOCOL=http oxen-server start

# Or include the protocol in the endpoint string directly
OXEN_OTEL_ENDPOINT=http://localhost:4318 oxen-server start
OXEN_OTEL_ENDPOINT=grpc://localhost:4318 oxen-server start
```

| Variable | Description | Default |
|---|---|---|
| `OXEN_OTEL_ENDPOINT` | Collector endpoint URL. Absent = disabled. | *(none)* |
| `OXEN_OTEL_PROTOCOL` | Transport: `grpc` or `http` | `grpc` or whatever is listed in `OXEN_OTEL_ENDPOINT` |

The standard `OTEL_EXPORTER_OTLP_ENDPOINT` variable is also respected as a
fallback if `OXEN_OTEL_ENDPOINT` is not set.

When the `otel` feature is not compiled in, no OpenTelemetry dependencies are
included and the env vars are ignored.

### `RUST_LOG` and span visibility

The `RUST_LOG` filter is global — it gates what reaches **all** tracing
outputs, including the OpenTelemetry exporter. `#[tracing::instrument]`
creates spans at `INFO` level by default. The server defaults to
`LevelFilter::WARN` when `RUST_LOG` is not set, which means **all
`#[instrument]` spans are silently dropped** before the OTel layer sees
them.

**To get full traces** in Jaeger (or any collector), explicitly set
`RUST_LOG=info`:

```bash
OXEN_OTEL_ENDPOINT=http://localhost:4317 RUST_LOG=info oxen-server start
```

**Without `RUST_LOG=info`, the OTel exporter is active but receives no spans.**

**The `TracingLogger` HTTP root span is also at `INFO` level, so it is
similarly affected.**

For targeted verbosity (e.g. keep third-party crates quiet), use a
filter directive:

```bash
RUST_LOG="warn,liboxen=info,oxen_server=info,tracing_actix_web=info"
```

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
OXEN_OTEL_ENDPOINT=http://localhost:4317 RUST_LOG=info cargo run --features otel -p oxen-server start

# View traces at http://localhost:16686 under service "oxen-server"
```


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
