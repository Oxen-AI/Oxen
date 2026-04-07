# oxen-server
The server for remote `oxen` repositories.

Remote repositories have the same internal structure as local ones, with the caveat that all the data is in the `.oxen/` dir and not duplicated into a "local workspace".

**Notable configuration sections**:
- [Prometheus Metrics](#prometheus-metrics)

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

Server defaults to localhost 3000:
```bash
set SERVER 0.0.0.0:3000
```

You can grab your auth token from the config file above (`~/.oxen/user_config.toml`):
```bash
set TOKEN <YOUR_TOKEN>
```

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
