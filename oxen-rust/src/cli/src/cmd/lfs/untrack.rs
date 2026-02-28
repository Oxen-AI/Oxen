use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "untrack";
pub struct LfsUntrackCmd;

#[async_trait]
impl RunCmd for LfsUntrackCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Stop tracking a file pattern with Oxen LFS")
            .arg(
                Arg::new("pattern")
                    .help("File glob pattern to untrack")
                    .required(true),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let pattern = args
            .get_one::<String>("pattern")
            .ok_or_else(|| OxenError::basic_str("pattern is required"))?;

        lfs::gitattributes::untrack_pattern(&repo_root, pattern)?;
        println!("Untracking \"{pattern}\"");
        Ok(())
    }
}
