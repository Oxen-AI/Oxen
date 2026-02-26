use async_trait::async_trait;
use clap::Command;

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "filter-process";
pub struct LfsFilterProcessCmd;

#[async_trait]
impl RunCmd for LfsFilterProcessCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Git long-running filter process (invoked by Git, not by users)")
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let versions_dir = repo_root.join(OXEN_HIDDEN_DIR).join("versions");

        // Run the blocking filter process loop.
        lfs::filter_process::run_filter_process(&versions_dir)?;
        Ok(())
    }
}
