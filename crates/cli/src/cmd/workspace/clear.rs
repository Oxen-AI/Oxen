use async_trait::async_trait;
use clap::{ArgMatches, Command};

use dialoguer::Confirm;
use liboxen::api;
use liboxen::{error::OxenError, model::LocalRepository};

use crate::cmd::RunCmd;
pub const NAME: &str = "clear";
pub struct WorkspaceClearCmd;

#[async_trait]
impl RunCmd for WorkspaceClearCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Deletes all workspaces").arg(
            clap::Arg::new("remote")
                .short('r')
                .long("remote")
                .help("Remote repository name")
                .required(false),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        let remote_name = args.get_one::<String>("remote");
        let remote_repo = match remote_name {
            Some(name) => {
                let remote = repository
                    .get_remote(name)
                    .ok_or(OxenError::remote_not_set(name))?;
                api::client::repositories::get_by_remote(&remote)
                    .await?
                    .ok_or(OxenError::remote_not_found(remote))?
            }
            None => api::client::repositories::get_default_remote(&repository).await?,
        };

        match Confirm::new()
            .with_prompt(format!(
                "Are you sure you want to delete all workspaces for remote: {}?",
                remote_repo.name
            ))
            .interact()
        {
            Ok(true) => {
                println!("Deleting all workspaces for remote: {}", remote_repo.name);
                api::client::workspaces::clear(&remote_repo).await?;
                println!("All workspaces deleted");
            }
            Ok(false) => {
                return Ok(());
            }
            Err(e) => {
                return Err(OxenError::basic_str(format!(
                    "Error confirming deletion: {e}"
                )));
            }
        }

        Ok(())
    }
}
