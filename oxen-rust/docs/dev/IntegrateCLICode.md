# How to add a command to CLI

Once you have written the library code from [this example](AddLibraryCode.md) it's time to integrate it into the CLI. The entry point for parsing command line arguments is in [src/cli/src/main.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/cli/src/main.rs).

We use the [clap](https://docs.rs/clap/latest/clap/) library to parse the arguments.

Subcommands with parameters are easy to add. For example adding a subcommand with two arguments looks something like this:

```rust
let command = Command::new("oxen")
    .version("0.1.0")
    .about("Data management toolchain")
    .subcommand_required(true)
    .arg_required_else_help(true)
    .allow_external_subcommands(true)
    .allow_invalid_utf8_for_external_subcommands(true)
    // ...
    .subcommand(
        Command::new("commit")
            .about("Commit staged data")
            .arg(arg!(<MESSAGE> "Your commit message"))
            .arg_required_else_help(true),
    )
    // ...
```

After the boiler plate command is setup and args are parsed, you can handle the subcommand and grab the args you require.

```rust

let matches = command.get_matches();
match matches.subcommand() {
    // ...
    Some(("commit", sub_matches)) => {
        let message = sub_matches.value_of("MESSAGE").expect("required");

        match dispatch::commit(message) {
            Ok(_) => {}
            Err(err) => {
                eprintln!("{}", err)
            }
        }
    }
    // ...
}
```

Once you know which subcommand to run, and have grabbed your arguments we send the arguments to the [dispatch](https://github.com/Oxen-AI/Oxen/blob/main/src/cli/src/dispatch.rs) module.

Within the dispatch module we instantiate the LocalRepository and pass it to the proper function in the [command](https://github.com/Oxen-AI/Oxen/blob/main/src/lib/src/command.rs) module.

```rust
// src/cli/src/dispatch.rs
pub fn set_remote(name: &str, url: &str) -> Result<(), OxenError> {
    // Create the local repository from the current directory
    let repo_dir = env::current_dir().unwrap();
    let mut repo = LocalRepository::from_dir(&repo_dir)?;

    // Library call
    command::set_remote(&mut repo, name, url)?;

    Ok(())
}
```

You can build with `cargo build --workspace` and run the CLI with:

```shell
cargo run -p oxen-cli -- config --set-remote <REMOTE> <URL>
```

Or run the compiled binary directly:

```shell
./target/debug/oxen config --set-remote <REMOTE> <URL>
```

Tip: For developing it is nice to just have this debug oxen binary in your path so you can use it from anywhere. You can just make a symbolic link to wherever your system looks for binaries in your path.

```shell
ln -s target/debug/oxen /usr/local/bin/oxen
```
