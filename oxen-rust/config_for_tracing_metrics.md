# Tracing & Metrics Configuration Guide

Oxen ships with structured logging (via the Rust `tracing` crate) and Prometheus-compatible
metrics (via the `metrics` crate). Both are wired into the CLI (`oxen`) and server
(`oxen-server`) binaries out of the box.

---

## Overview

| Concern | Crate | How it works |
|---------|-------|-------------|
| **Structured logging** | `tracing` + `tracing-subscriber` + `tracing-log` | A `tracing-log` bridge captures all legacy `log::` calls and routes them through `tracing`. Two subscriber layers are always registered: a human-readable **stderr** layer, and an optional **JSON file** layer. |
| **Metrics** | `metrics` + `metrics-exporter-prometheus` | All counters and histograms use the `metrics::counter!` / `metrics::histogram!` macros. In the server a Prometheus HTTP exporter is installed automatically. In the CLI the default no-op recorder silently discards all metric calls (zero overhead). |

There are ~580 metric instrumentation points across ~120 source files covering the CLI,
lib (repositories, API client, merkle tree), and server controllers.

---

## Environment Variables Reference

| Variable | Default | Applies to | Description |
|----------|---------|-----------|-------------|
| `RUST_LOG` | `info` | CLI & Server | Filter directive for `tracing` (standard `EnvFilter` syntax). Controls what appears on stderr **and** in log files. |
| `OXEN_LOG_FILE` | *(unset)* | CLI & Server | Opt-in to JSON file logging. Set to `1` / `true` for the default directory (`~/.oxen/logs/`), or set to a directory path. |
| `OXEN_METRICS_PORT` | `9090` | Server only | TCP port for the Prometheus metrics HTTP endpoint (`/metrics`). |

---

## Use Case 1: Stderr Only (Default)

This is the zero-configuration default. All log output goes to stderr in a
human-readable format.

### CLI

```bash
# Run any oxen command — logs appear on stderr at `info` level
oxen status

# Increase verbosity to see debug-level output from liboxen
RUST_LOG=debug oxen add .

# Show only warnings and errors
RUST_LOG=warn oxen push origin main

# Target a specific module
RUST_LOG="warn,liboxen::repositories::add=debug" oxen add .
```

### Server

```bash
# Start the server — info-level logs on stderr by default
oxen-server start

# Debug-level logging for everything
RUST_LOG=debug oxen-server start

# Debug only for server controllers, warn for everything else
RUST_LOG="warn,oxen_server::controllers=debug" oxen-server start
```

### Sample stderr output

```
2026-03-09T12:00:01.234Z  INFO liboxen::repositories::add: staging files path="images/"
2026-03-09T12:00:01.567Z DEBUG liboxen::core::v_latest::index::commit_merkle_tree: loading merkle tree commit_id="abc123"
2026-03-09T12:00:02.890Z  INFO liboxen::repositories::commits: committed message="add training data"
```

### Filter syntax

`RUST_LOG` uses the standard `EnvFilter` syntax:

| Example | Meaning |
|---------|---------|
| `debug` | Everything at debug and above |
| `warn,liboxen=debug` | Default warn, but debug for `liboxen` |
| `info,liboxen::repositories=trace` | Trace-level for repo ops, info elsewhere |
| `error` | Only errors |

---

## Use Case 2: JSON File Logging

Opt in by setting `OXEN_LOG_FILE`. A rolling daily log file is written in
JSON-per-line format using `tracing-appender`. Stderr output continues as normal.

### Enable with default directory

```bash
# Logs written to ~/.oxen/logs/oxen.YYYY-MM-DD
OXEN_LOG_FILE=1 oxen push origin main

# Same for the server — files named oxen-server.YYYY-MM-DD
OXEN_LOG_FILE=true oxen-server start
```

### Enable with custom directory

```bash
OXEN_LOG_FILE=/var/log/oxen oxen-server start
# Logs written to /var/log/oxen/oxen-server.YYYY-MM-DD
```

The directory is created automatically if it does not exist.

### Sample JSON output

Each line is a self-contained JSON object:

```json
{
  "timestamp": "2026-03-09T12:00:01.234567Z",
  "level": "INFO",
  "target": "liboxen::repositories::add",
  "filename": "src/lib/src/repositories/add.rs",
  "line_number": 38,
  "threadId": 1,
  "fields": {
    "message": "staging files",
    "path": "images/"
  },
  "span": {
    "name": "add"
  }
}
```

### Inspecting logs with jq

```bash
# Stream all errors from today's log
cat ~/.oxen/logs/oxen-server.2026-03-09 | jq 'select(.level == "ERROR")'

# Count log entries per level
cat ~/.oxen/logs/oxen-server.2026-03-09 | jq -r '.level' | sort | uniq -c

# Show all entries from a specific module
cat ~/.oxen/logs/oxen-server.2026-03-09 | jq 'select(.target | startswith("liboxen::repositories::push"))'

# Extract timing histograms recorded in spans
cat ~/.oxen/logs/oxen-server.2026-03-09 | jq 'select(.fields.message | contains("duration"))'
```

### Log rotation

Files roll daily by filename suffix (e.g. `oxen-server.2026-03-09`,
`oxen-server.2026-03-10`). Old files are **not** automatically deleted — set up
your own rotation policy (e.g. `logrotate`, a cron job, or manual cleanup).

---

## Use Case 3: Prometheus Metrics (Server)

The server automatically starts a Prometheus HTTP exporter on startup. No
additional configuration is needed for the default port.

### Default (port 9090)

```bash
oxen-server start
# Metrics available at http://localhost:9090/metrics
```

### Custom port

```bash
OXEN_METRICS_PORT=9191 oxen-server start
# Metrics available at http://localhost:9191/metrics
```

### Verify with curl

```bash
curl -s http://localhost:9090/metrics | head -20
```

### Sample metric output

```
# HELP oxen_server_commits_upload_total oxen_server_commits_upload_total
# TYPE oxen_server_commits_upload_total counter
oxen_server_commits_upload_total 42

# HELP oxen_server_commits_upload_duration_seconds oxen_server_commits_upload_duration_seconds
# TYPE oxen_server_commits_upload_duration_seconds summary
oxen_server_commits_upload_duration_seconds{quantile="0.5"} 1.234
oxen_server_commits_upload_duration_seconds{quantile="0.9"} 3.456
oxen_server_commits_upload_duration_seconds{quantile="0.99"} 7.890
oxen_server_commits_upload_duration_seconds_sum 123.456
oxen_server_commits_upload_duration_seconds_count 42

# HELP oxen_errors_total oxen_errors_total
# TYPE oxen_errors_total counter
oxen_errors_total{module="repositories::push",error="RemoteBranchNotFound"} 3
```

### Prometheus scrape config

Add a scrape target to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: "oxen-server"
    scrape_interval: 15s
    static_configs:
      - targets: ["localhost:9090"]
```

---

## Metrics Without a Collection Endpoint (CLI)

The CLI calls `init_metrics_noop()` at startup. The Rust `metrics` crate's
default behavior when no recorder is installed is to silently discard all
counter/histogram calls. This means:

- **Zero runtime overhead** — macro calls compile to no-ops when no recorder exists.
- **No port is opened** — the CLI never listens on a network socket for metrics.
- All the same `metrics::counter!` / `metrics::histogram!` instrumentation in
  the shared `liboxen` library simply does nothing when invoked from the CLI.

There is nothing to configure for this case.

---

## Metric Naming Reference

All metrics follow the pattern `oxen_{component}_{operation}_total` (counters)
or `oxen_{component}_{operation}_duration_seconds` (histograms).

### Counters (`_total`)

| Prefix | Component | Examples |
|--------|-----------|---------|
| `oxen_cli_*` | CLI commands | `oxen_cli_branch_total`, `oxen_cli_delete_remote_total` |
| `oxen_repo_init_*` | Repository init | `oxen_repo_init_init_total`, `oxen_repo_init_init_with_version_total` |
| `oxen_repo_commit_*` | Commits | `oxen_repo_commit_commit_total`, `oxen_repo_commit_head_commit_total`, `oxen_repo_commit_list_total` |
| `oxen_repo_clone_*` | Clone | `oxen_repo_clone_clone_total`, `oxen_repo_clone_deep_clone_url_total` |
| `oxen_repo_checkout_*` | Checkout | `oxen_repo_checkout_checkout_total`, `oxen_repo_checkout_checkout_theirs_total` |
| `oxen_repo_status_*` | Status | `oxen_repo_status_status_total`, `oxen_repo_status_status_from_opts_total` |
| `oxen_repo_diff_*` | Diffs | `oxen_repo_diff_diff_total`, `oxen_repo_diff_diff_commits_total`, `oxen_repo_diff_tabular_total` |
| `oxen_repo_add_*` | Staging | `oxen_repo_add_add_total` |
| `oxen_repo_push_*` | Push | `oxen_repo_push_push_total` |
| `oxen_repo_pull_*` | Pull | `oxen_repo_pull_pull_total` |
| `oxen_repo_merge_*` | Merge | `oxen_repo_merge_merge_total` |
| `oxen_repo_entries_*` | Entry listing | `oxen_repo_entries_get_directory_total`, `oxen_repo_entries_list_directory_total` |
| `oxen_repo_tree_*` | Tree operations | `oxen_repo_tree_get_root_total` |
| `oxen_merkle_*` | Merkle tree internals | `oxen_merkle_from_commit_total`, `oxen_merkle_load_children_total` |
| `oxen_client_*` | API client (HTTP calls) | `oxen_client_commits_post_push_complete_total`, `oxen_client_entries_download_entry_total` |
| `oxen_server_commits_*` | Server commit endpoints | `oxen_server_commits_upload_total`, `oxen_server_commits_create_total` |
| `oxen_server_repositories_*` | Server repo endpoints | `oxen_server_repositories_create_total`, `oxen_server_repositories_delete_total` |
| `oxen_server_branches_*` | Server branch endpoints | `oxen_server_branches_index_total`, `oxen_server_branches_create_total` |
| `oxen_server_workspaces_*` | Server workspace endpoints | `oxen_server_workspaces_create_total`, `oxen_server_workspaces_commit_total` |
| `oxen_server_tree_*` | Server tree endpoints | `oxen_server_tree_create_nodes_total`, `oxen_server_tree_download_tree_total` |
| `oxen_server_diff_*` | Server diff endpoints | `oxen_server_diff_commits_total`, `oxen_server_diff_file_total` |
| `oxen_server_file_*` | Server file endpoints | `oxen_server_file_get_total`, `oxen_server_file_put_total` |
| `oxen_server_merger_*` | Server merge endpoint | `oxen_server_merger_merge_total` |
| `oxen_errors_total` | Error tracking | Labels: `module`, `error` — e.g. `oxen_errors_total{module="repositories::push",error="RemoteBranchNotFound"}` |

### Histograms (`_duration_seconds`)

| Metric | What it measures |
|--------|-----------------|
| `oxen_repo_add_add_duration_seconds` | Time to stage files |
| `oxen_repo_commit_commit_duration_seconds` | Time to create a commit |
| `oxen_repo_clone_clone_duration_seconds` | Time to clone a repository |
| `oxen_repo_checkout_checkout_duration_seconds` | Time to checkout a branch/commit |
| `oxen_repo_push_push_duration_seconds` | Time to push to remote |
| `oxen_repo_pull_pull_duration_seconds` | Time to pull from remote |
| `oxen_repo_merge_merge_duration_seconds` | Time to merge branches |
| `oxen_repo_tree_*_duration_seconds` | Various tree operations (get root, compress, unpack, write) |
| `oxen_merkle_from_commit_duration_seconds` | Time to build merkle tree from commit |
| `oxen_client_import_upload_zip_duration_seconds` | Time to upload a zip import |
| `oxen_client_versions_*_duration_seconds` | Upload/download version file durations |
| `oxen_client_workspaces_files_*_duration_seconds` | Workspace file staging/download durations |
| `oxen_server_commits_upload_duration_seconds` | Time to process a commit upload |
| `oxen_server_commits_upload_tree_duration_seconds` | Time to upload tree data |
| `oxen_server_file_put_duration_seconds` | Time to write a file via API |
| `oxen_server_merger_merge_duration_seconds` | Time to process a server-side merge |
| `oxen_server_workspaces_commit_duration_seconds` | Time to commit a workspace |
| `oxen_server_prune_prune_duration_seconds` | Time to prune a repository |
| `oxen_server_versions_*_duration_seconds` | Version upload/chunk durations |
| `oxen_server_export_download_zip_duration_seconds` | Time to export a zip download |
| `oxen_server_branches_maybe_create_merge_duration_seconds` | Time to create a merge branch |
| `oxen_server_import_*_duration_seconds` | Import processing durations |

---

## Troubleshooting

### No log output appears

- Ensure `RUST_LOG` is set to a level that includes the messages you expect
  (e.g. `debug` or `info`). The default is `info`.
- Logs go to **stderr**, not stdout. If piping output, make sure you're
  capturing stderr: `oxen status 2>log.txt`

### JSON log files are empty

- Verify `OXEN_LOG_FILE` is set **before** the process starts (not after).
- Check the directory exists and is writable. The directory is auto-created, but
  parent directories must already exist.
- The non-blocking writer flushes on process exit. If the process is killed with
  `SIGKILL`, the last few log lines may be lost. Use `SIGTERM` for graceful shutdown.

### Metrics endpoint returns nothing / connection refused

- The Prometheus exporter only runs in `oxen-server`, not the CLI.
- Verify the port: `curl -s http://localhost:9090/metrics`. If you changed the
  port via `OXEN_METRICS_PORT`, use that port instead.
- Check for port conflicts — another process may already be listening on that port.
  Use `lsof -i :9090` to check.

### Metrics show all zeros

- Counters start at zero and only increment when the corresponding code path
  executes. If no requests have been made, counters will be zero.
- Histograms only appear after the first observation is recorded.

### Too much log output / performance concerns

- Use a targeted filter: `RUST_LOG="warn,liboxen::repositories=info"` to limit
  verbosity to specific modules.
- File logging uses a non-blocking writer with an in-memory buffer, so it has
  minimal impact on request latency.
- Metrics collection via the `metrics` crate uses atomic operations and has
  negligible overhead.
