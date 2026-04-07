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
