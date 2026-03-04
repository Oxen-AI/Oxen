use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "clean";
pub struct LfsCleanCmd;

#[async_trait]
impl RunCmd for LfsCleanCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Clean filter for a single file (invoked by Git)")
            .arg(Arg::new("separator").long("").hide(true))
            .arg(
                Arg::new("file")
                    .help("Path to the file being cleaned")
                    .required(false),
            )
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let versions_dir = repo_root.join(OXEN_HIDDEN_DIR).join("versions");

        // Read content from stdin.
        let content = {
            use std::io::Read;
            let mut buf = Vec::new();
            std::io::stdin().read_to_end(&mut buf)?;
            buf
        };

        let result = lfs::filter::clean(&versions_dir, &content).await?;

        // Write result to stdout.
        {
            use std::io::Write;
            std::io::stdout().write_all(&result)?;
            std::io::stdout().flush()?;
        }

        Ok(())
    }
}
