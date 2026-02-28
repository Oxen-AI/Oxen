use async_trait::async_trait;
use clap::{Arg, ArgAction, Command};

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "pull";
pub struct LfsPullCmd;

#[async_trait]
impl RunCmd for LfsPullCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Pull and restore large files from Oxen remote or local store")
            .arg(
                Arg::new("local")
                    .long("local")
                    .help("Only restore from the local .oxen/versions/ store (no network)")
                    .action(ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let oxen_dir = repo_root.join(OXEN_HIDDEN_DIR);
        let local_only = args.get_flag("local");

        lfs::sync::pull_from_remote(&repo_root, &oxen_dir, local_only).await?;
        Ok(())
    }
}
