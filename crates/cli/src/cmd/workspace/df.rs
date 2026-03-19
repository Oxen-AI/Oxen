use std::collections::HashMap;

use async_trait::async_trait;
use clap::Command;

use liboxen::error::OxenError;

// subcommands
pub mod get;
use get::WorkspaceDFGetCmd;

pub mod index;
use index::WorkspaceDFIndexCmd;

use crate::cmd::RunCmd;

pub const NAME: &str = "df";
pub struct WorkspaceDfCmd;

#[async_trait]
impl RunCmd for WorkspaceDfCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command = Command::new(NAME).about(
            "Interact with remote data frames. Supported types: csv, tsv, ndjson, jsonl, parquet.",
        );

        let sub_commands = self.get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let sub_commands = self.get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                eprintln!("Unknown df subcommand {name}");
                return Err(OxenError::basic_str(format!(
                    "Unknown df subcommand {name}"
                )));
            };

            // Calling await within an await is making it complain?
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
            })?;
        }
        Ok(())
    }
}

impl WorkspaceDfCmd {
    fn get_subcommands(&self) -> HashMap<String, Box<dyn RunCmd>> {
        let commands: Vec<Box<dyn RunCmd>> =
            vec![Box::new(WorkspaceDFIndexCmd), Box::new(WorkspaceDFGetCmd)];
        let mut runners: HashMap<String, Box<dyn RunCmd>> = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }
}
