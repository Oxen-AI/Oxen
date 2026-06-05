use std::collections::HashMap;

use async_trait::async_trait;
use clap::Command;

use crate::{
    cli_error::UnknownSubcommand,
    cmd::{RunCmd, Runners},
};

pub const NAME: &str = "db";

pub mod count;
pub use count::DbCountCmd;

pub mod get;
pub use get::DbGetCmd;

pub mod list;
pub use list::DbListCmd;

pub struct DbCmd;

#[async_trait]
impl RunCmd for DbCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command =
            Command::new(NAME).about("Tools to inspect and interact with the local oxen databases");

        // These are all the subcommands the command
        let sub_commands = self.get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
        // Parse Args
        let sub_commands = self.get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                return Err(UnknownSubcommand {
                    parent: "db",
                    name: name.to_string(),
                })?;
            };

            // Calling await within an await is making it complain?
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
            })?;
        } else {
            return Err(anyhow::anyhow!(
                "No db subcommand provided. Run `oxen db --help` for usage.",
            ));
        }

        Ok(())
    }
}

impl DbCmd {
    fn get_subcommands(&self) -> Runners {
        let commands: [Box<dyn RunCmd>; 3] = [
            Box::new(DbListCmd),
            Box::new(DbGetCmd),
            Box::new(DbCountCmd),
        ];
        let mut runners = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }
}
