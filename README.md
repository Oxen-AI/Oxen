# 🐂 Oxen

Create a world where everyone can contribute to an Artificial General Intelligence, starting with the data.

# 🌾 What is Oxen?

Oxen at it's core is a data version control library, written in Rust. It's goals are to be fast, reliable, and easy to use. It's designed to be used in a variety of ways, from a simple command line tool, to a remote server to sync to, to integrations into other ecosystems such as [python](https://github.com/Oxen-AI/oxen-release).

# 📚 Documentation

The documentation for the Oxen.ai tool chain can be found [here](https://docs.oxen.ai).

# ✅ TODO

- [ ] Hugging face compatible APIs
  - [ ] Upload model to hub
  - [ ] Download model with `transformers` library
  - [ ] Upload dataset to hub
  - [ ] Download dataset with `datasets` library
- [ ] Configurable storage backends
  - [x] Local filesystem
  - [ ] S3
  - [ ] GCS
  - [ ] Azure
  - [ ] Backblaze
- [ ] Block level deduplication

# 🔨 Build & Run

## Install Dependencies

Oxen is purely written in Rust 🦀. You should install the Rust toolchain with rustup: https://www.rust-lang.org/tools/install.

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

If you are a developer and want to learn more about adding code or the overall architecture [start here](docs/dev/AddLibraryCode.md). Otherwise, a quick start to make sure everything is working follows.

## Build

```
cargo build
```

If on intel mac, you may need to build with the following

```
$ rustup target install x86_64-apple-darwin
$ cargo build --target x86_64-apple-darwin
```

If on Windows, you may need to add the following directories to the 'INCLUDE' environment variable

```
"C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Tools\MSVC\14.29.30133\include"

"C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Tools\MSVC\14.29.27023\include"

"C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Tools\Llvm\lib\clang\12.0.0\include"
```
These are example paths and will vary between machines. If you install 'C++ Clang tools for Windows' through [Microsoft Visual Studio Build Tools](https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019), the directories can be located from the Visual Studio installation under 'BuildTools\VC\Tools'

## Speed up the build process

You can use the [mold](https://github.com/rui314/mold) linker to speed up builds (The MIT-licensed macOS version is [sold](https://github.com/bluewhalesystems/sold)).

Use the following instructions to
install sold and configure cargo to use it for building Oxen:

```
git clone --depth=1 --single-branch https://github.com/bluewhalesystems/sold.git

mkdir sold/build
cd sold/build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ ..
cmake --build . -j $(nproc)
sudo cmake --install .
```

Then create `.cargo/config.toml` in your Oxen repo root with the following
content:

```
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-fuse-ld=/usr/local/bin/ld64.mold"]

[target.x86_64-apple-darwin]
rustflags = ["-C", "link-arg=-fuse-ld=/usr/local/bin/ld64.mold"]

```

**For macOS with Apple Silicon**, you can use the [lld](https://lld.llvm.org/) linker.

```
brew install llvm
```

Then create `.cargo/config.toml` in your Oxen repo root with the following:

```
[target.aarch64-apple-darwin]
rustflags = [ "-C", "link-arg=-fuse-ld=/opt/homebrew/opt/llvm/bin/ld64.lld", ]

```

# Run

## CLI

To run Oxen from the command line, add the `Oxen/target/debug` directory to the 'PATH' environment variable

```
export PATH="$PATH:/path/to/Oxen/target/debug"
```

On Windows, you can use

```
$env:PATH += ";/path/to/Oxen/target/debug"
```

Initialize a new repository or clone an existing one

```
oxen init
oxen clone https://hub.oxen.ai/namespace/repository
```

This will create the `.oxen` dir in your current directory and allow you to run Oxen CLI commands

```
oxen status
oxen add images/
oxen commit -m "added images"
oxen push origin main
```


## Oxen Server

To run a local Oxen Server, generate a config file and token to authenticate the user

```
./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output user_config.toml
```

Copy the config to the default locations

```
mkdir ~/.oxen
```

```
mv user_config.toml ~/.oxen/user_config.toml
```

```
cp ~/.oxen/user_config.toml data/test/config/user_config.toml
```

Set where you want the data to be synced to. The default sync directory is `./data/` to change it set the SYNC_DIR environment variable to a path.

```
export SYNC_DIR=/path/to/sync/dir
```

You can also create a .env.local file in the /src/server directory which can contain the SYNC_DIR variable to avoid setting it every time you run the server.

Run the server

```
./target/debug/oxen-server start
```

To run the server with live reload, first install cargo-watch

```
cargo install cargo-watch
```

On Windows, you may need to use `cargo-watch --locked`

```
cargo install cargo-watch --locked
```

Then run the server like this

```
cargo watch -- cargo run --bin oxen-server start
```


## Nix Flake

If you have [Nix installed](https://github.com/DeterminateSystems/nix-installer)
you can use the flake to build and run the server. This will automatically
install and configure the required build toolchain dependencies for Linux & macOS.

```
nix build .#oxen-server
nix build .#oxen-cli
nix build .#liboxen
```

```
nix run .#oxen-server -- start
nix run .#oxen-cli -- init
```

To develop with the standard rust toolchain in a Nix dev shell:

```
nix develop -c $SHELL
cargo build
cargo run --bin oxen-server start
cargo run --bin oxen start
```

The flake also provides derviations to build OCI (Docker) images with the minimal
set of dependencies required to build and run `oxen` & `oxen-server`.

```
nix build .#oci-oxen-server
nix build .#oci-oxen-cli
```

This will export the OCI image and can be loaded with:

```
docker load -i result
```

# Unit & Integration Tests

Make sure your user is configured and server is running on the default port and host, by following these setup steps:

```bash
# Configure a user
mkdir ./data/test/runs
./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output user_config.toml
cp user_config.toml data/test/config/user_config.toml
# Start the oxen-server
./target/debug/oxen-server start
```

*Note:* tests open up a lot of file handles, so limit num test threads if running everything.

You an also increase the number of open files your system allows ulimit before running tests:

```
ulimit -n 10240
```

```
cargo test -- --test-threads=$(nproc)
```

It can be faster (in terms of compilation and runtime) to run a specific test. To run a specific library test:

```
cargo test --lib test_get_metadata_text_readme
```

To run a specific integration test

```
cargo test --test test_rm test_rm_directory_restore_directory
```

To run with all debug output and run a specific test

```
env RUST_LOG=warn,liboxen=debug,integration_test=debug cargo test -- --nocapture test_command_push_clone_pull_push
```

To set a different test host you can set the `OXEN_TEST_HOST` environment variable

```
env OXEN_TEST_HOST=0.0.0.0:4000 cargo test
```

## Pre-Commit Hook

We use [pre-commit-hooks](https://github.com/pre-commit/pre-commit-hooks) to check for commit consistency.
make sure to install [`pre-commit-hooks`](https://pre-commit.com/) library
and then install the precommits locally using
```
pre-commit install
```

# Oxen Server

## Structure

Remote repositories have the same internal structure as local ones, with the caviate that all the data is in the .oxen dir and not duplicated into a "local workspace".

# APIs

Server defaults to localhost 3000

```
set SERVER 0.0.0.0:3000
```

You can grab your auth token from the config file above (~/.oxen/user_config.toml)

```
set TOKEN <YOUR_TOKEN>
```

## List Repositories

```
curl -H "Authorization: Bearer $TOKEN" "http://$SERVER/api/repos"
```

## Create Repository

```
curl -H "Authorization: Bearer $TOKEN" -X POST -d '{"name": "MyRepo"}' "http://$SERVER/api/repos"
```

# Docker

Create the docker image

```
docker build -t oxen/server:0.6.0 .
```

Run a container on port 3000 with a local filesystem mounted from /var/oxen/data on the host to /var/oxen/data in the container.

```
docker run -d -v /var/oxen/data:/var/oxen/data -p 3000:3001 --name oxen oxen/server:0.6.0
```

Or use docker compose

```
docker-compose up -d reverse-proxy
```

```
docker-compose up -d --scale oxen=4 --no-recreate
```
