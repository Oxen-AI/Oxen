use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "delete";
pub struct WorkspaceDeleteCmd;

#[async_trait]
impl RunCmd for WorkspaceDeleteCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Deletes a workspace")
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .required_unless_present("workspace-name")
                    .help("The workspace ID of the workspace")
                    .conflicts_with("workspace-name"),
            )
            .arg(
                Arg::new("workspace-name")
                    .long("workspace-name")
                    .short('n')
                    .required_unless_present("workspace-id")
                    .help("The name of the workspace")
                    .conflicts_with("workspace-id"),
            )
            .arg_required_else_help(true)
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_id = args.get_one::<String>("workspace-id");

        let workspace_identifier = match workspace_id {
            Some(id) => id,
            None => {
                // If no ID is provided, try to get the workspace by name
                if let Some(name) = workspace_name {
                    name
                } else {
                    return Err(OxenError::basic_str(
                        "Either workspace-id or workspace-name must be provided.",
                    ));
                }
            }
        };

        // Now call the delete API using the resolved workspace id.
        api::client::workspaces::delete(&remote_repo, &workspace_identifier).await?;
        println!("Workspace '{workspace_identifier}' deleted successfully");
        Ok(())
    }
}
