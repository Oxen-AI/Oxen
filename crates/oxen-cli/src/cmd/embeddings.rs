use async_trait::async_trait;
use clap::Command;
use std::collections::HashMap;

use crate::{
    cli_error::UnknownSubcommand,
    cmd::{RunCmd, Runners},
};
pub const NAME: &str = "embeddings";

pub mod index;
pub use index::EmbeddingsIndexCmd;

pub mod query;
pub use query::EmbeddingsQueryCmd;

pub struct EmbeddingsCmd;

#[async_trait]
impl RunCmd for EmbeddingsCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command = Command::new(NAME).about("Index and query embeddings from a data frame.");

        // These are all the subcommands for the schemas command
        // including `index` and `query`
        let sub_commands = self.get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
        let sub_commands = self.get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                return Err(UnknownSubcommand {
                    parent: "embeddings",
                    name: name.to_string(),
                })?;
            };

            // Calling await within an await is making it complain?
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
            })?;
        }
        Ok(())
    }
}

impl EmbeddingsCmd {
    fn get_subcommands(&self) -> Runners {
        let commands: [Box<dyn RunCmd>; 2] =
            [Box::new(EmbeddingsIndexCmd), Box::new(EmbeddingsQueryCmd)];
        let mut runners = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }
}
