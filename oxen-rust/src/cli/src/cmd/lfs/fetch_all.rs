use async_trait::async_trait;
use clap::Command;

use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::error::OxenError;
use liboxen::lfs;

use crate::cmd::RunCmd;

pub const NAME: &str = "fetch-all";
pub struct LfsFetchAllCmd;

#[async_trait]
impl RunCmd for LfsFetchAllCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about(
            "Resolve and restore ALL tracked pointer files. Errors if any file cannot be resolved.",
        )
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repo_root = std::env::current_dir()?;
        let oxen_dir = repo_root.join(OXEN_HIDDEN_DIR);

        if !oxen_dir.exists() {
            return Err(OxenError::basic_str(
                "Not an oxen lfs repository. Run `oxen lfs init` first.",
            ));
        }

        lfs::sync::fetch_all(&repo_root, &oxen_dir).await
    }
}
