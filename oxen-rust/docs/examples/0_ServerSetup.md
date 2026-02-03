# Developer Setup for CLI and Server

Build the Oxen Server and Oxen CLI binaries

`cargo build --workspace`

Generate a config file that contains an access token to give it to the user to access to the server

`cargo run -p oxen-server -- add-user --email ox@oxen.ai --name Ox --output user_config.toml`

The user who needs access should copy the config to the ~/.oxen directory, which is where the Oxen CLI looks for it. If the user has not done this step, they will not have access to the server.

`mkdir ~/.oxen`

`mv user_config.toml ~/.oxen/user_config.toml`

Run the server

`cargo run -p oxen-server -- start`

The default sync directory is `./data/` to change it set the SYNC_DIR environment variable to a path.

For example

`env SYNC_DIR=/Users/gregschoeninger/Data/oxen_server cargo run -p oxen-server -- start`

```
Running 🐂 server on 0.0.0.0:3000
Syncing to directory: /Users/gregschoeninger/Data/oxen_server
[2022-06-08T10:00:48Z INFO  actix_server::builder] Starting 8 workers
[2022-06-08T10:00:48Z INFO  actix_server::server] Actix runtime found; starting in Actix runtime
```

If you want to change the default `IP ADDRESS` and `PORT` you can do so by passing them in with the `-i` and `-p` parameters.

`cargo run -p oxen-server -- start -i 0.0.0.0 -p 4004`

To learn how to create a local Oxen repository and push it to the server see the [next tutorial](1_InitAndCommit.md).
