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

At runtime, set `OTEL_EXPORTER_OTLP_ENDPOINT`:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 oxen pull
```

### Filtering: logs and spans are separate

`RUST_LOG` gates the CLI's log output. Span export is gated separately by
`OXEN_OTEL_FILTER`, which defaults to `info`, the level
`#[tracing::instrument]` records at. So spans export at the stock settings even
though the CLI logs nothing by default, and raising `RUST_LOG` for a debugging
session does not change what is exported.

`OXEN_OTEL_FILTER` takes the same directive syntax as `RUST_LOG`, so export can
be narrowed or widened on its own:

```bash
OXEN_OTEL_FILTER=warn,liboxen=debug OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 oxen pull
```

| Variable | Description | Default |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Collector URL, needing an `http://` or `https://` scheme, which under HTTP has `/v1/traces` appended to it. Absent = disabled, unless `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` names one. | *(none)* |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | The traces endpoint, taking precedence over the above and posted to exactly as configured under HTTP, so include `/v1/traces` in it. | *(none)* |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | Transport: `grpc`, or `http` / `http/protobuf` / `http/json` for HTTP (binary protobuf either way). | `http/protobuf` |
| `OXEN_OTEL_FILTER` | Which spans and events are exported. Same syntax as `RUST_LOG`, and independent of it. | `info` |
| `RUST_LOG` | Log verbosity on stderr. Does not affect span export. | `off` |

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
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 cargo run --features otel -p oxen-cli pull

# View traces at http://localhost:16686 under service "oxen"
```

When the `otel` feature is not compiled in, no OpenTelemetry dependencies
are included and the env vars are ignored.
