use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "smudge";
pub struct LfsSmudgeCmd;

#[async_trait]
impl RunCmd for LfsSmudgeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Smudge filter for a single file (invoked by Git)")
            .arg(Arg::new("separator").long("").hide(true))
            .arg(
                Arg::new("file")
                    .help("Path to the file being smudged")
                    .required(false),
            )
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let oxen_dir = repo_root.join(OXEN_HIDDEN_DIR);
        let versions_dir = oxen_dir.join("versions");
        let config = lfs::config::LfsConfig::load(&oxen_dir)?;

        // Read pointer data from stdin.
        let pointer_data = {
            use std::io::Read;
            let mut buf = Vec::new();
            std::io::stdin().read_to_end(&mut buf)?;
            buf
        };

        let result = lfs::filter::smudge(&versions_dir, &config, &pointer_data).await?;

        // Write result to stdout.
        {
            use std::io::Write;
            std::io::stdout().write_all(&result)?;
            std::io::stdout().flush()?;
        }

        Ok(())
    }
}
