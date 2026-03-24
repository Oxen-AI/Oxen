# 🐂 Oxen

Create a world where everyone can contribute to an Artificial General Intelligence, starting with the data.

# 🌾 What is Oxen?

Oxen at its core is a data version control library, written in Rust. Its goals are to be fast, reliable, and easy to use. It's designed to be used in a variety of ways, from a simple command line tool, to a remote server to sync to, to integrations into other ecosystems such as [python](https://github.com/Oxen-AI/oxen-release).

# 📚 Documentation

The documentation for the Oxen.ai tool chain can be found [here](https://docs.oxen.ai).

# ✅ TODO

- [ ] Configurable storage backends
  - [x] Local filesystem
  - [ ] S3
  - [ ] GCS
  - [ ] Azure
  - [ ] Backblaze
- [ ] Block level deduplication

# 🔨 Build & Run

## Install Prerequisites

See the [prerequisites](../../README.md#prerequisites) section of the main readme to install the needed prerequisites.

## Build

Build the entire workspace (CLI, server, and library):

```bash
cargo build --workspace
```

Or build a specific crate:

```bash
cargo build -p oxen-server
cargo build -p oxen-cli
cargo build -p liboxen
```

If on intel mac, you may need to build with the following

```bash
rustup target install x86_64-apple-darwin
cargo build --workspace --target x86_64-apple-darwin
```

If on Windows, you may need to add the following directories to the 'INCLUDE' environment variable

```text
"C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Tools\MSVC\14.29.30133\include"

"C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Tools\MSVC\14.29.27023\include"

"C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Tools\Llvm\lib\clang\12.0.0\include"
```

These are example paths and will vary between machines. If you install 'C++ Clang tools for Windows' through [Microsoft Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019), the directories can be located from the Visual Studio installation under 'BuildTools\VC\Tools'

## Speed up the build process

### Linux

_Note: Rust 1.90+ on `x86_64-unknown-linux-gnu` [already uses](https://blog.rust-lang.org/2025/09/01/rust-lld-on-1.90.0-stable/) an optimized linker by default. For other variants of Linux, the instructions below may speed things up._

On Linux, you can use the [mold](https://github.com/rui314/mold) linker to speed up builds.

Then create `.cargo/config.toml` in your Oxen repo root with the following
content:

```toml
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-fuse-ld=/usr/local/bin/ld64.mold"]
```

### macOS with Apple Silicon

**For macOS with Apple Silicon**, you can use the [lld](https://lld.llvm.org/) linker.

```bash
brew install llvm
```

Then create `.cargo/config.toml` in your Oxen repo root with the following:

```toml
[target.aarch64-apple-darwin]
rustflags = [ "-C", "link-arg=-fuse-ld=/opt/homebrew/opt/llvm/bin/ld64.lld", ]
```

# Run

## CLI

To run Oxen from the command line, add the `target/debug` directory to the 'PATH' environment variable

```bash
export PATH="$PATH:/path/to/Oxen/target/debug"
```

On Windows, you can use

```powershell
$env:PATH += ";/path/to/Oxen/target/debug"
```

Initialize a new repository or clone an existing one

```bash
oxen init
oxen clone https://hub.oxen.ai/namespace/repository
```

This will create the `.oxen` dir in your current directory and allow you to run Oxen CLI commands

```bash
oxen status
oxen add images/
oxen commit -m "added images"
oxen push origin main
```


## Oxen Server

To run a local Oxen Server, generate a config file and token to authenticate the user

```bash
./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output user_config.toml
```

Copy the config to the default locations

```bash
mkdir ~/.oxen
```

```bash
mv user_config.toml ~/.oxen/user_config.toml
```

```bash
cp ~/.oxen/user_config.toml data/test/config/user_config.toml
```

Set where you want the data to be synced to. The default sync directory is `./data/` to change it set the `SYNC_DIR` environment variable to a path.

```bash
export SYNC_DIR=/path/to/sync/dir
```

You can also create a .env.local file in the /crates/server directory which can contain the SYNC_DIR variable to avoid setting it every time you run the server.

Run the server

```bash
cargo run -p oxen-server -- start
```

Or run the compiled binary directly:

```bash
./target/debug/oxen-server start
```

To run the server with live reload, use `bacon`:

```bash
cargo install --locked bacon
```

Then run the server like this

```bash
bacon server
```


## Nix Flake

If you have [Nix installed](https://github.com/DeterminateSystems/nix-installer)
you can use the flake to build and run the server. This will automatically
install and configure the required build toolchain dependencies for Linux & macOS.

```bash
nix build .#oxen-server
nix build .#oxen-cli
nix build .#liboxen
```

```bash
nix run .#oxen-server -- start
nix run .#oxen-cli -- init
```

To develop with the standard rust toolchain in a Nix dev shell:

```bash
nix develop -c $SHELL
cargo build --workspace
cargo run -p oxen-server -- start
cargo run -p oxen-cli -- init
```

The flake also provides derivations to build OCI (Docker) images with the minimal
set of dependencies required to build and run `oxen` & `oxen-server`.

```bash
nix build .#oci-oxen-server
nix build .#oci-oxen-cli
```

This will export the OCI image and can be loaded with:

```bash
docker load -i result
```

# Unit & Integration Tests

## Manual Test Setup

Here are the steps to manually configure and run tests (see also the [Automatic Test Setup](#automatic-test-setup) section). Make sure your user is configured and server is running on the default port and host, by following these setup steps:

```bash
# Configure a user
mkdir -p data/test/{runs,config}
./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output data/test/config/user_config.toml
# Start the oxen-server
./target/debug/oxen-server start
```

*Note:* tests open up a lot of file handles, so limit num test threads if running everything.

You can also increase the number of open files your system allows ulimit before running tests:

```bash
ulimit -n 10240
```

Then you can run the tests with the `cargo test` or `cargo nextest` (preferred) directly. To run all tests with the default number of threads:

```bash
cargo test --workspace -- --test-threads=$(getconf _NPROCESSORS_ONLN)
```

## Automatic Test Setup

You can use [bin/test-rust](./bin/test-rust) to run tests. It will set up config files, build and run an oxen-server, run the tests against it, and shutdown the server. Any arguments passed to `test-rust` will be passed to `cargo nextest run`, so you can use it to run specific tests or set test threads.

```bash
bin/test-rust
```

It can be faster (in terms of compilation and runtime) to run a specific test. To run a specific library test:

```bash
bin/test-rust --lib test_get_metadata_text_readme
```

To run with all debug output and run a specific test

```bash
env RUST_LOG=warn,liboxen=debug,integration_test=debug bin/test-rust --no-capture test_command_push_clone_pull_push
```

To explicitly set the port for the `oxen-server` used in tests, set `OXEN_PORT`:

```bash
env OXEN_PORT=4000 bin/test-rust
```


# Oxen Server

## Structure

Remote repositories have the same internal structure as local ones, with the caveat that all the data is in the .oxen dir and not duplicated into a "local workspace".

# APIs

Server defaults to localhost 3000

```bash
set SERVER 0.0.0.0:3000
```

You can grab your auth token from the config file above (~/.oxen/user_config.toml)

```bash
set TOKEN <YOUR_TOKEN>
```

## List Repositories

```bash
curl -H "Authorization: Bearer $TOKEN" "http://$SERVER/api/repos"
```

## Create Repository

```bash
curl -H "Authorization: Bearer $TOKEN" -X POST -d '{"name": "MyRepo"}' "http://$SERVER/api/repos"
```

# Docker

Create the docker image

```bash
docker build -t oxen/server:0.46.7 .
```

Run a container on port 3000 with a local filesystem mounted from /var/oxen/data on the host to /var/oxen/data in the container.

```bash
docker run -d -v /var/oxen/data:/var/oxen/data -p 3000:3001 --name oxen oxen/server:0.46.7
```

Or use docker compose

```bash
docker-compose up -d reverse-proxy
```

```bash
docker-compose up -d --scale oxen=4 --no-recreate
```

## Run Benchmark

we use criterion to handle benchmarks.

```bash
cargo bench --workspace
```

To save baseline you can run

```bash
cargo bench --workspace -- --save-baseline bench
```

Which would then store the benchmark under `target/criterion/add`

## Enable ffmpeg

To enable thumbnailing for videos, you will have to build with ffmpeg enabled

```bash
brew install ffmpeg@7
cargo build --workspace --all-features
```

Or for a specific crate:

```bash
cargo build -p oxen-server --features liboxen/ffmpeg
```
