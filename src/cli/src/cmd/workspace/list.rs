use async_trait::async_trait;
use clap::{ArgMatches, Command};

use liboxen::api;
use liboxen::{error::OxenError, model::LocalRepository};

use crate::cmd::RunCmd;
pub const NAME: &str = "list";
pub struct WorkspaceListCmd;

#[async_trait]
impl RunCmd for WorkspaceListCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Lists all workspaces")
    }

    async fn run(&self, _args: &ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        let workspaces = api::client::workspaces::list(&remote_repo).await?;
        for workspace in workspaces {
            println!(
                "{}\t{}\t{}",
                workspace.id, workspace.commit.id, workspace.commit.message
            );
        }
        Ok(())
    }
}
