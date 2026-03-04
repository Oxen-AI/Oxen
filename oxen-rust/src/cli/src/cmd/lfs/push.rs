use async_trait::async_trait;
use clap::Command;

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "push";
pub struct LfsPushCmd;

#[async_trait]
impl RunCmd for LfsPushCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Push large files to the configured Oxen remote")
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let oxen_dir = repo_root.join(OXEN_HIDDEN_DIR);

        // Collect remaining args that were passed by the pre-push hook.
        let hook_args: Vec<String> = std::env::args().collect();

        lfs::sync::push_to_remote(&repo_root, &oxen_dir, &hook_args).await?;
        Ok(())
    }
}
