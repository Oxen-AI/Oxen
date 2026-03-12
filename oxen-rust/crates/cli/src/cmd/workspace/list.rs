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
        Command::new(NAME).about("Lists all workspaces").arg(
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

        let workspaces = api::client::workspaces::list(&remote_repo).await?;
        if workspaces.is_empty() {
            println!("No workspaces found");
            return Ok(());
        }

        println!("id\tname\tcommit_id\tcommit_message");
        for workspace in workspaces {
            println!(
                "{}\t{}\t{}\t{}",
                workspace.id,
                workspace.name.unwrap_or("".to_string()),
                workspace.commit.id,
                workspace.commit.message
            );
        }
        Ok(())
    }
}
