pub mod checkout;
pub use checkout::RemoteModeCheckoutCmd;

pub mod commit;
pub use commit::RemoteModeCommitCmd;

pub mod restore;
pub use restore::RemoteModeRestoreCmd;

pub mod status;
pub use status::RemoteModeStatusCmd;

use async_trait::async_trait;
use clap::Command;

use std::collections::HashMap;

use crate::{Runners, cli_error::UnknownSubcommand, cmd::RunCmd};
pub const NAME: &str = "remote_mode";
pub struct RemoteModeCmd;

#[async_trait]
impl RunCmd for RemoteModeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command = Command::new(NAME)
            .about("Remote mode operations")
            .subcommand_required(true)
            .arg_required_else_help(true);

        let sub_commands = Self::get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    // Note: Currently, you can't run `oxen remote-mode status` or other subcommand from the command line
    // They're only accessible via their aliases in remote-mode repos
    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
        let sub_commands = Self::get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                return Err(UnknownSubcommand {
                    parent: "remote mode",
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

impl RemoteModeCmd {
    fn get_subcommands() -> Runners {
        let commands: [Box<dyn RunCmd>; 4] = [
            Box::new(RemoteModeCheckoutCmd),
            Box::new(RemoteModeCommitCmd),
            Box::new(RemoteModeRestoreCmd),
            Box::new(RemoteModeStatusCmd),
        ];
        let mut runners = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }

    pub async fn run_subcommands(
        name: &str,
        sub_matches: &clap::ArgMatches,
    ) -> Result<(), anyhow::Error> {
        let sub_commands = Self::get_subcommands();
        let Some(cmd) = sub_commands.get(name) else {
            return Err(UnknownSubcommand {
                parent: "remote mode",
                name: name.to_string(),
            })?;
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
        })?;

        Ok(())
    }
}
