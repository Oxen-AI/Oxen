# 🐂 Self Hosting

You can either setup an `oxen-server` instance yourself, or use the hosted version on [OxenHub](https://oxen.ai). To use the hosted OxenHub solution you can sign up [here](https://oxen.ai/register).

## Setup an Oxen Server

To setup a local Oxen Server instance, first [install](Installation.md) the `oxen-server` binary.

The server can be run with access token authentication turned on or off. The server runs with no authentication by default:

```bash
$ oxen-server start
```

To enable authentication, generate a token to give it to the user to access to the server

```bash
$ oxen-server add-user --email YOUR_EMAIL --name YOUR_NAME

User access token created:

XXXXXXXX

To give user access have them run the command `oxen config --auth <HOST> <TOKEN>`
```

You may have different authentication tokens for different hosts. From the client side, you can setup an auth token per host with the `config` command. If you ever need to debug or edit the tokens manually, they are stored in the `~/.config/oxen/user_config.toml` file.

```bash
$ oxen config --auth <HOST> <TOKEN>
$ cat ~/.config/oxen/user_config.toml
```

To run the server with authentication, use the `-a` flag

```bash
$ oxen-server start -a
```

The default directory that Oxen stores data is `data` (relative to the working directory the server was started from), which is convenient for trying things out but probably not what you want in production. To change it set the `SYNC_DIR` environment variable to an absolute path.

```
$ export SYNC_DIR=/Path/To/Data
$ oxen-server start -a

Running 🐂 server on 0.0.0.0:3000
Syncing to directory: /Path/To/Data
[2022-06-08T10:00:48Z INFO  actix_server::builder] Starting 8 workers
[2022-06-08T10:00:48Z INFO  actix_server::server] Actix runtime found; starting in Actix runtime
```

If you want to change the default `IP ADDRESS` and `PORT` you can do so by passing them in with the `-i` and `-p` parameters.

```bash
$ oxen-server start -i 0.0.0.0 -p 4321
```

## Storage Configuration

Oxen has two TOML config files that govern where repository data is stored:

1. A **server-wide** config (optional, loaded via `--config <path>`) that decides which storage backends the server will host new repos on and supplies admin-only S3 settings.
2. A **per-repo** `.oxen/config.toml` inside each server-hosted repository, written when the repo is created, that records which kind of storage *that* repo uses.

These are independent files. The server-wide config is read once at startup; the per-repo config is read every time the server opens a repo.

### Server-wide config (`--config <path>`)

The `--config` flag is optional. When omitted, the server uses a conservative default: only the local-filesystem backend is enabled.

```bash
$ oxen-server start --config /etc/oxen/server.toml
```

The full schema for the file is a single `[storage]` table:

```toml
[storage]
backends = ["local", "s3"]   # required; first element is the server default
s3_bucket = "my-oxen-bucket" # required iff "s3" is in backends
s3_region = "us-west-1"      # required iff "s3" is in backends
```

- **`backends`** — a list of which storage kinds this server is willing to host. The valid values are `"local"` (local-filesystem version storage) and `"s3"` (S3 version storage), spelled in lowercase. The first element is the server's default: when a client creates a new repo without specifying a kind, the server uses that. Each kind must appear at most once; the list must be non-empty.
- **`s3_bucket`** — the S3 bucket the server uses for any S3-backed repo. Required when `"s3"` appears in `backends` and rejected when it doesn't. Each repo gets the prefix `{namespace}/{name}/` inside this bucket; the prefix is not configurable per repo.
  - **`s3_region`** — the AWS region the bucket lives in (e.g. `us-west-1`). Required when `"s3"` appears in `backends`. The server uses it to build the S3 client directly rather than detecting it at runtime; if it's wrong, startup fails when the bucket reachability check runs.

Omitting the `[storage]` section entirely is equivalent to:

```toml
[storage]
backends = ["local"]
```

The server validates the config at startup and exits with a clear error if anything is wrong (empty `backends`, duplicate entries, S3 listed without `s3_bucket` or `s3_region`, etc.). When the S3 backend is enabled it also probes the configured bucket at startup (a `HeadBucket` call) and refuses to boot if the bucket is unreachable — wrong region, missing credentials, or no permission. It will not run with an invalid config.

A few example configurations:

```toml
# Local-only (the default; no config file needed)
[storage]
backends = ["local"]
```

```toml
# S3-only — new repos go to S3 by default
[storage]
backends = ["s3"]
s3_bucket = "oxen-prod-versions"
s3_region = "us-west-1"
```

```toml
# Both backends enabled, local is the server default; clients
# can request "s3" on the wire when they create a repo
[storage]
backends = ["local", "s3"]
s3_bucket = "oxen-prod-versions"
s3_region = "us-west-1"
```

```toml
# Both backends enabled, s3 is the server default
[storage]
backends = ["s3", "local"]
s3_bucket = "oxen-prod-versions"
s3_region = "us-west-1"
```

S3 backends require valid AWS credentials in the server's environment, picked up by the AWS SDK in the usual way (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `~/.aws/credentials` / instance role / etc.). The bucket region comes from `s3_region` in the config above.

### Per-repo `.oxen/config.toml`

When the server creates a repository, it writes a small `[storage]` section into `<repo>/.oxen/config.toml` so that the storage choice is persisted with the repo. The shape is:

```toml
[storage]
kind = "local"                          # or "s3"
versions_path = "/mnt/nfs/oxen-data"    # optional, local backend only
```

- **`kind`** — `"local"` or `"s3"` (lowercase; these are the only valid values today). Set by the server at repo-creation time according to its policy and what the client requested. Do not change this by hand; the server expects the backend kind to match the data on disk (for `local`) or in the bucket (for `s3`).
- **`versions_path`** *(local backend only)* — overrides where the local backend stores version blobs for this repo. By default, version blobs live under `<repo>/.oxen/versions/files`. If `versions_path` starts with `.oxen`, it is interpreted as relative to the repo's root (and so the repo stays portable if the directory is moved); otherwise it is taken as an absolute path. Useful for pointing version storage at an NFS mount or another volume while keeping the rest of the repo's metadata in place.

#### Legacy `[storage.settings]` fallback

Repositories created by older versions of `oxen-server` used a different shape:

```toml
[storage]
type = "local"

[storage.settings]
path = "/mnt/nfs/oxen-data"
```

`oxen-server` continues to read the legacy form on every load — `type` is treated as `kind` and `[storage.settings] path` is promoted to `versions_path` — so an upgrade does not force a migration. No admin action is required.

The file is opportunistically rewritten into the new shape the next time the server has another reason to save the repo's config. In practice that means after a namespace transfer; if that never happens, the legacy keys stay on disk indefinitely and the repo continues to work normally.

## Pushing the Changes

Once you have committed data locally and are ready to share them with colleagues (or the world) you will have to push them to a remote.

You can either create a remote through the web UI on [OxenHub](https://oxen.ai) or if you have setup a server your self, you will have to run the `create-remote` command.

```bash
$ oxen create-remote --name MyNamespace/MyRepoName --host 0.0.0.0:3001 --scheme http
```

Repositories that live on an Oxen Server have the idea of a `namespace` and a `name` to help you organize your repositories.

Once you know your remote repository URL you can add it as a remote.

```bash
$ oxen config --set-remote origin http://<HOST>/MyNamespace/MyRepoName
```

Once a remote is set you can push

```bash
$ oxen push origin main
```

You can change the remote (origin) and the branch (main) to whichever remote and branch you want to push.

## Clone the Changes

To clone a repository from remote server you can use the URL you provided previously, and pull the changes to a new machine.

```bash
$ oxen clone http://<HOST>/MyNamespace/MyRepoName
```
