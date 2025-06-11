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

The default directory that Oxen stores data is `/tmp/oxen_sync`, we definitely do not want this in production. To change it set the SYNC_DIR environment variable to a path.

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
