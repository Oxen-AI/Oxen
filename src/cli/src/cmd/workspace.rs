pub mod add;
pub use add::WorkspaceAddCmd;

pub mod create;
pub use create::WorkspaceCreateCmd;

pub mod commit;
pub use commit::WorkspaceCommitCmd;

pub mod diff;
pub use diff::WorkspaceDiffCmd;

pub mod df;
pub use df::WorkspaceDfCmd;

pub mod delete;
pub use delete::WorkspaceDeleteCmd;

pub mod list;
pub use list::WorkspaceListCmd;

pub mod restore;
pub use restore::WorkspaceRestoreCmd;

pub mod status;
pub use status::WorkspaceStatusCmd;

use async_trait::async_trait;
use clap::Command;

use liboxen::error::OxenError;
use std::collections::HashMap;

use crate::cmd::RunCmd;
pub const NAME: &str = "workspace";
pub struct WorkspaceCmd;

#[async_trait]
impl RunCmd for WorkspaceCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        let mut command = Command::new(NAME)
            .about("Manage workspaces")
            .subcommand_required(true)
            .arg_required_else_help(true);

        // These are all the subcommands for the schemas command
        // including `create`, `add`, `rm`, `commit`, and `status`
        let sub_commands = self.get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let sub_commands = self.get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                eprintln!("Unknown schema subcommand {name}");
                return Err(OxenError::basic_str(format!(
                    "Unknown schema subcommand {name}"
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

impl WorkspaceCmd {
    fn get_subcommands(&self) -> HashMap<String, Box<dyn RunCmd>> {
        let commands: Vec<Box<dyn RunCmd>> = vec![
            Box::new(WorkspaceAddCmd),
            Box::new(WorkspaceCommitCmd),
            Box::new(WorkspaceCreateCmd),
            Box::new(WorkspaceDfCmd),
            Box::new(WorkspaceDiffCmd),
            Box::new(WorkspaceDeleteCmd),
            Box::new(WorkspaceListCmd),
            Box::new(WorkspaceStatusCmd),
        ];
        let mut runners: HashMap<String, Box<dyn RunCmd>> = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }
}
