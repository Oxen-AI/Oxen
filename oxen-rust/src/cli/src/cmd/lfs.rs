pub mod clean;
pub use clean::LfsCleanCmd;

pub mod env;
pub use env::LfsEnvCmd;

pub mod fetch_all;
pub use fetch_all::LfsFetchAllCmd;

pub mod filter_process;
pub use filter_process::LfsFilterProcessCmd;

pub mod init;
pub use init::LfsInitCmd;

pub mod install;
pub use install::LfsInstallCmd;

pub mod pull;
pub use pull::LfsPullCmd;

pub mod push;
pub use push::LfsPushCmd;

pub mod smudge;
pub use smudge::LfsSmudgeCmd;

pub mod status;
pub use status::LfsStatusCmd;

pub mod track;
pub use track::LfsTrackCmd;

pub mod untrack;
pub use untrack::LfsUntrackCmd;

use async_trait::async_trait;
use clap::Command;

use liboxen::error::OxenError;
use std::collections::HashMap;

use crate::cmd::RunCmd;

pub const NAME: &str = "lfs";
pub struct LfsCmd;

#[async_trait]
impl RunCmd for LfsCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        let mut command = Command::new(NAME)
            .about("Oxen large file storage (Git LFS replacement)")
            .subcommand_required(true)
            .arg_required_else_help(true);

        let sub_commands = Self::get_subcommands();
        for cmd in sub_commands.values() {
            command = command.subcommand(cmd.args());
        }
        command
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let sub_commands = Self::get_subcommands();
        if let Some((name, sub_matches)) = args.subcommand() {
            let Some(cmd) = sub_commands.get(name) else {
                eprintln!("Unknown lfs subcommand {name}");
                return Err(OxenError::basic_str(format!(
                    "Unknown lfs subcommand {name}"
                )));
            };

            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(cmd.run(sub_matches))
            })?;
        }
        Ok(())
    }
}

impl LfsCmd {
    fn get_subcommands() -> HashMap<String, Box<dyn RunCmd>> {
        let commands: Vec<Box<dyn RunCmd>> = vec![
            Box::new(LfsCleanCmd),
            Box::new(LfsEnvCmd),
            Box::new(LfsFetchAllCmd),
            Box::new(LfsFilterProcessCmd),
            Box::new(LfsInitCmd),
            Box::new(LfsInstallCmd),
            Box::new(LfsPullCmd),
            Box::new(LfsPushCmd),
            Box::new(LfsSmudgeCmd),
            Box::new(LfsStatusCmd),
            Box::new(LfsTrackCmd),
            Box::new(LfsUntrackCmd),
        ];
        let mut runners: HashMap<String, Box<dyn RunCmd>> = HashMap::new();
        for cmd in commands {
            runners.insert(cmd.name().to_string(), cmd);
        }
        runners
    }
}
