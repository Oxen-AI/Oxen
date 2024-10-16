use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "remote";
pub struct RemoteCmd;

#[async_trait]
impl RunCmd for RemoteCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("List oxen remotes.").arg(
            Arg::new("verbose")
                .long("verbose")
                .short('v')
                .help("Verbose output")
                .action(clap::ArgAction::SetTrue),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let verbose = args.get_flag("verbose");
        if verbose {
            self.list_remotes_verbose()?;
        } else {
            self.list_remotes()?;
        }

        Ok(())
    }
}

impl RemoteCmd {
    pub fn list_remotes(&self) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;

        for remote in repo.remotes().iter() {
            println!("{}", remote.name);
        }

        Ok(())
    }

    pub fn list_remotes_verbose(&self) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;

        for remote in repo.remotes().iter() {
            println!("{}\t{}", remote.name, remote.url);
        }

        Ok(())
    }
}
