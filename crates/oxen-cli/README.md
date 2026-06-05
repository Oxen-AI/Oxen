# oxen-cli
The client for interacting with `oxen` repositories, both locally and remote.

## Build

See the [prerequisites](../../README.md#prerequisites) section of the main readme before developing.
Use the standard `cargo ... --workspace` commands and `cargo ... -p oxen-cli` commands.

## Run

Initialize a new repository or clone an existing one:
```bash
oxen init
oxen clone https://hub.oxen.ai/namespace/repository
```

This will create the `.oxen` dir in your current directory and allow you to run Oxen CLI commands:
```bash
oxen status
oxen add images/
oxen commit -m "added images"
oxen push origin main
```

## Logging

Oxen uses structured logging.
It outputs to STDERR by default but can be configured with rotating log files.
See [Logging](../../README.md#logging) for details.

By default, the `oxen` CLI does not perform any logging. Set `RUST_LOG` to change.

## OpenTelemetry Tracing

The `oxen` CLI can export tracing spans to any OTLP-compatible collector
(Jaeger, Tempo, etc.). This requires building with the `otel` feature flag:

```bash
cargo build -p oxen-cli --features otel
```

At runtime, set **both** `OXEN_OTEL_ENDPOINT` and `RUST_LOG=info`:

```bash
OXEN_OTEL_ENDPOINT=http://localhost:4317 RUST_LOG=info oxen pull
```

### Why `RUST_LOG=info` is required

The CLI defaults to `LevelFilter::OFF` when `RUST_LOG` is not set. The
`RUST_LOG` filter is global — it gates what reaches **all** tracing outputs,
including the OpenTelemetry exporter. Since `#[tracing::instrument]` creates
spans at `INFO` level, they are silently dropped before the OTel layer ever
sees them unless the filter is at `INFO` or below.

Setting `OXEN_OTEL_ENDPOINT` alone is not enough — you must also set
`RUST_LOG=info` (or more targeted, e.g. `RUST_LOG=liboxen=info`) for spans
to be exported.

| Variable | Description | Default |
|---|---|---|
| `OXEN_OTEL_ENDPOINT` | Collector endpoint URL. Absent = disabled. | *(none)* |
| `OXEN_OTEL_PROTOCOL` | Transport: `grpc` or `http` | `grpc` |
| `RUST_LOG` | Must include `info` level for spans to be exported | `off` |

### Quick start with Jaeger

```bash
# Start Jaeger all-in-one: https://www.jaegertracing.io/docs/2.17/
docker run --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 5778:5778 \
  -p 9411:9411 \
  cr.jaegertracing.io/jaegertracing/jaeger:2.17.0

# Run a pull with tracing enabled
OXEN_OTEL_ENDPOINT=http://localhost:4317 RUST_LOG=info cargo run --features otel -p oxen-cli pull

# View traces at http://localhost:16686 under service "oxen"
```

When the `otel` feature is not compiled in, no OpenTelemetry dependencies
are included and the env vars are ignored.
