

<div align="center">
  <a href="https://docs.oxen.ai/" style="padding: 2px;">
    <img src="https://img.shields.io/badge/%F0%9F%93%9A-Documentation-245AF0" alt="Oxen.ai Documentation">
  </a>
  <a href="https://oxen.ai/" style="padding: 2px;">
    <img src="https://img.shields.io/badge/%F0%9F%90%82-Oxen%20Hub-245AF0" alt="Oxen.ai">
  </a>
  <a href="https://crates.io/crates/liboxen" style="padding: 2px;">
    <img src="https://img.shields.io/crates/v/liboxen.svg?color=245AF0" alt="Oxen.ai Crate"/>
  </a>
  <a href="https://pypi.org/project/oxenai/" style="padding: 2px;">
    <img src="https://img.shields.io/pypi/v/oxenai.svg?color=245AF0" alt="PyPi Latest Release"/>
  </a>
  <a href="https://discord.com/invite/s3tBEn7Ptg" style="padding: 2px;">
    <img src="https://img.shields.io/badge/join-discord-245AF0?logo=discord" alt ="Oxen.ai Discord">
  </a>
  <a href="https://twitter.com/oxen_ai" style="padding: 2px;">
    <img src="https://img.shields.io/twitter/url/https/twitter.com/oxenai.svg?style=social&label=Follow%20%40Oxen.ai" alt ="Oxen.ai Twitter">
  </a>
  <br/>
</div>

#

![Oxen.ai Logo](/images/oxen-no-margin-white.svg#gh-dark-mode-only)
![Oxen.ai Logo](/images/oxen-no-margin-black.svg#gh-light-mode-only)

## 🐂 What is Oxen?

Oxen is a lightning fast data version control system for large datasets. We aim to make versioning data as easy as versioning code.

The interface mirrors git, but shines in many areas that git or git-lfs fall short. Oxen is built from the ground up for any data type, and is optimized to handle repositories with millions of files and scales to terrabytes of data.

```bash
oxen init
oxen add images/
oxen add annotations/*.parquet
oxen commit "Adding 200k images and their corresponding annotations"
oxen push origin main
```

Oxen is comprised of a [command line
interface](https://docs.oxen.ai/getting-started/cli), as well as bindings for
[Rust](https://github.com/Oxen-AI/Oxen/tree/main/crates) 🦀, [Python](https://docs.oxen.ai/getting-started/python) 🐍, and [HTTP interfaces](https://docs.oxen.ai/http-api) 🌎 to make it easy to integrate into your workflow.

## 🌾 What kind of data?

Oxen is designed to efficiently manage large data in any format - including images, audio, video, text or tabular data like parquet files with millions of rows. Behind the scenes Oxen can store any blob type, but has specialized metadata extractors for certain filetypes and caches this information in the merkle tree for fast access later.

## 🚀 Built for speed

One of the main reasons datasets are hard to maintain is the pure performance of indexing the data and transferring the data over the network. We wanted to be able to index hundreds of thousands of images, videos, audio files, and text files in seconds.

Watch below as we version **hundreds of thousands of images** in seconds 🔥

<p align="center">
    <img src="https://github.com/Oxen-AI/oxen-release/raw/main/images/cli-celeba.gif?raw=true" alt="oxen cli demo" />
</p>

But speed is only the beginning.

## ✅ Features

Oxen is built around ergonomics, ease of use, and it is easy to learn. If you know how to use git, you know how to use Oxen.

* 🔥 Fast (efficient indexing and syncing of data)
* 🧠 Easy to learn (same commands as git)
* 💪 Handles large files (images, videos, audio, text, parquet, arrow, json, models, etc)
* 🗄️ Index lots of files (millions of images? no problem)
* 📊 Native DataFrame processing (index, compare and serve up DataFrames)
* 📈 Tracks changes over time (never worry about losing the state of your data)
* 🤝 Collaborate with your team (sync to an oxen-server)
* 🌎 [Workspaces](https://docs.oxen.ai/concepts/workspace) to interact with the data without downloading it
* 👀 Better data visualization on [OxenHub](https://oxen.ai)

## 🐮 Learn The Basics

To learn what everything Oxen can do, the full documentation can be found at [https://docs.oxen.ai](https://docs.oxen.ai).

## 🧑‍💻 Getting Started

You can install through homebrew or pip or from our [releases page](https://github.com/Oxen-AI/Oxen/releases).

### 🐂 Install Command Line Tool

Install via [Homebrew](https://brew.sh/):

```bash
brew install oxen
```

### 🐍 Install Python Library

```bash
pip install oxenai
```

### ⬇️ Clone Dataset

Clone your first Oxen repository from the [OxenHub](https://oxen.ai/explore).

<CodeGroup>

```bash
oxen clone https://hub.oxen.ai/ox/CatDogBBox
```

## 🤝 Support

If you have any questions, comments, suggestions, or just want to get in contact with the team, feel free to email us at `hello@oxen.ai`

## 👥 Contributing

This repository contains the Python library that wraps the core Rust codebase. We would love help extending out the python interfaces, the documentation, or the core rust library.

Code bases to contribute to:

* 🦀 [Rust Library & Binaries](https://github.com/Oxen-AI/Oxen/tree/main/crates/)
* 🐍 [Python Interface](https://github.com/Oxen-AI/Oxen/tree/main/oxen-python)
* 📚 [Documentation](https://github.com/Oxen-AI/docs)

See the [CONTRIBUTING.md](./CONTRIBUTING.md) document for guidelines on external contributions.

All contributors are required to follow the [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md). The tl;dr is be nice and respectful during all engagements :)

If you are building anything with Oxen.ai or have any questions we would love to hear from you in our [discord](https://discord.gg/s3tBEn7Ptg).

## Build 🔨

Each codebase has its own build instructions, please refer to the [Rust build instructions](./crates/lib/README.md#-build--run)
and [`oxen-python`'s build instructions](./oxen-python/README.md#build) for specifics.

However, each codebase shares the same pre-requisites and pre-commit hooks.

### Prerequisites

#### Automatic Install

You should use [`bin/install-prereqs`](./bin/install-prereqs) to automatically install the required development tools and toolchains for Rust and Python. Execute that as:

```bash
bin/install-prereqs
```

It supports MacOS and Debian-based Linux distributions. If you have a different OS or distribution, or if you have some error with the install script, you can follow the manual installation steps below.

#### Manual Installation

Oxen is purely written in Rust 🦀. You should install the Rust toolchain with [`rustup`](https://www.rust-lang.org/tools/install).

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Once you have rust, install the following developer tools:
- [`bacon`](https://crates.io/crates/bacon): run the server with reload-on-changes
- [`cargo-machete`](https://github.com/bnjbvr/cargo-machete): identify and remove unused dependencies
- [`cargo-llvm-cov`](https://crates.io/crates/cargo-llvm-cov): calculate test code coverage
- [`cargo-sort`](https://crates.io/crates/cargo-sort): ensure `Cargo.toml` files are organized
- [`cargo-nextest`](https://crates.io/crates/cargo-nextest): run unit tests

You can install all of these at once with the following commands:

```bash
cargo install bacon cargo-machete cargo-llvm-cov cargo-sort
cargo install --locked cargo-nextest
```

Make sure [`cmake`](https://cmake.org/download/) is installed. `cmake` can be installed on macOS with:

```bash
brew install cmake
```

The [Python interface](./oxen-python/README.md) uses [`liboxen`](./crates/lib/) bindings provided by PyO3.

The `oxen-python` codebase requires installing [`uv`](https://docs.astral.sh/uv/getting-started/installation/):

```bash
curl --LsSf https://astral.sh/uv/install.sh | sh
```

If you use [`mise`](https://mise.jdx.dev/) to manage your Python installs, you may run into an error where the oxen-py crate can't find the Python dynamic library to link with, e.g., `dyld[31558]: Library not loaded: @rpath/libpython3.13.dylib`

You can fix it by adding this to your mise config (`~/.config/mise/config.toml`)

```toml
[env]
DYLD_LIBRARY_PATH = "{{ exec(command='mise where python') }}/lib"
```

### Pre-Commit Hooks

We use [pre-commit-hooks](https://pre-commit.com/) to check for commit consistency.

Install with `uv` as a tool:

```bash
uv tool install pre-commit
```

Install `Oxen`'s pre-commit hooks locally using:
```bash
pre-commit install
```

### Production Release Build

For deployment, build with the `production` feature flag and `--release`:

```bash
cargo build --workspace --release --features production
```

This enables:
- **OpenTelemetry tracing** (`otel`) -- export spans to any OTLP-compatible collector (Jaeger, Tempo, Datadog, etc.). See [OpenTelemetry Tracing](crates/server/README.md#opentelemetry-tracing) for runtime configuration.
- **FFmpeg thumbnails** (`ffmpeg`) -- generate video/image thumbnails via FFmpeg (requires FFmpeg libraries installed on the host).
- **Performance logging** (`perf-logging`) -- additional timing instrumentation for internal operations.

Without `--features production`, the default build excludes OTel dependencies and FFmpeg support, keeping the binary smaller for local development.

## Logging

Oxen uses structured logging via the [`tracing`](https://docs.rs/tracing) crate. All log output goes to **stderr** by default in a human-readable format. This applies to the CLI (`oxen`), the server (`oxen-server`), and any code using `liboxen` (including the Python bindings).

### Controlling Log Level

Set the `RUST_LOG` environment variable to control verbosity.

```bash
# Show debug logs from the oxen library
RUST_LOG=debug oxen push origin main

# Show only warnings and errors
RUST_LOG=warn oxen-server start

# Fine-grained: debug for liboxen, warn for everything else
RUST_LOG=warn,liboxen=debug oxen-server start
```

### File Logging with `OXEN_LOG_DIR`

Set `OXEN_LOG_DIR` to enable file-based logging in addition to stderr. This env var is a directory where rotating log files are written. Log files are written as **newline-delimited JSON** (one JSON object per line), rotated daily. Each line includes the timestamp, level, target, thread ID, source file, and line number.

```bash
OXEN_LOG_DIR=./logs/ RUST_LOG=warn oxen clone https://hub.oxen.ai/ox/CatDogBBox
OXEN_LOG_DIR=/var/log/oxen oxen-server start
```

Log files are named `{app_name}.{date}` (e.g. `oxen-server.2026-04-06`) inside the configured directory.

To ingest these logs with standard tooling:

- **Promtail / Grafana Loki** -- point a `file_sd` or static target at the log directory; Loki handles newline-delimited JSON natively.
- **Filebeat / Elasticsearch** -- configure a `filebeat.inputs` entry with `type: filestream` and `parsers: [{ ndjson: {} }]`.
- **Vector** -- use a `file` source with `decoding.codec = "json"`.
- **`jq`** -- for ad-hoc inspection:

```bash
# Stream logs, filter for errors
tail -f ~/.oxen/logs/oxen-server.2026-04-06 | jq 'select(.level == "ERROR")'
```

## Prometheus Metrics

`oxen-server` can expose a Prometheus-compatible metrics endpoint. Requires
the `metrics` compile-time feature (included in `production`) and `OXEN_METRICS_PORT`
at runtime. See [Prometheus Metrics](crates/server/README.md#prometheus-metrics) for details.

## OpenTelemetry Tracing

`oxen-server` can export tracing spans to any OTLP-compatible collector (Jaeger, Tempo, etc.).
Requires building with the `otel` feature flag.
See [OpenTelemetry Tracing](crates/server/README.md#opentelemetry-tracing) for details.

## FmtSpan Events

Span lifecycle events can be emitted as log lines on stderr for lightweight tracing.
See [FmtSpan Events](crates/server/README.md#fmtspan-events) for details.

## OpenTelemetry Tracing

`oxen-server` can export tracing spans to any OTLP-compatible collector (Jaeger, Tempo, etc.).
Requires building with the `otel` feature flag.
See [OpenTelemetry Tracing](crates/server/README.md#opentelemetry-tracing) for details.

## FmtSpan Events

Span lifecycle events can be emitted as log lines on stderr for lightweight tracing.
See [FmtSpan Events](crates/server/README.md#fmtspan-events) for details.

## Why build Oxen?

Oxen was build by a team of machine learning engineers, who have spent countless hours in their careers managing datasets. We have used many different tools, but none of them were as easy to use and as ergonomic as we would like.

If you have ever tried [git lfs](https://git-lfs.com/) to version large datasets and became frustrated, we feel your pain. Solutions like git-lfs are too slow when it comes to the scale of data we need for machine learning.

If you have ever uploaded a large dataset of images, audio, video, or text to a cloud storage bucket with the name:

`s3://data/images_july_2022_final_2_no_really_final.tar.gz`

We built Oxen to be the tool we wish we had.

## Why the name Oxen?

"Oxen" 🐂 comes from the fact that the tooling will plow, maintain, and version your data like a good farmer tends to their fields 🌾. Let Oxen take care of the grunt work of your infrastructure so you can focus on the higher-level problems that matter to your product.

<!---------------------------------------------------------------------------->

[Learn The Basics]: https://img.shields.io/badge/Learn_The_Basics-37a779?style=for-the-badge
