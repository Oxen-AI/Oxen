use async_trait::async_trait;
use clap::{Arg, Command};

use dialoguer::Confirm;
use liboxen::api;
use liboxen::constants::DEFAULT_HOST;
use liboxen::error::OxenError;

use crate::cmd::RunCmd;
pub const NAME: &str = "delete-remote";
pub struct DeleteRemoteCmd;

#[async_trait]
impl RunCmd for DeleteRemoteCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
        .about("Deletes a remote repository with the name on the host. Default behavior is to delete a remote on the hub.oxen.ai remote.")
        .arg(
            Arg::new("name")
                .long("name")
                .short('n')
                .help("The namespace/name of the remote repository you want to create. For example: 'ox/my_repo'")
                .required(true)
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("The host you want to create the remote repository on. For example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(namespace_name) = args.get_one::<String>("name") else {
            return Err(OxenError::basic_str(
                "Must supply a namespace/name for the remote repository.",
            ));
        };
        // Default the host to the oxen.ai hub
        let host = args
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string());

        let Some(remote_repo) =
            api::client::repositories::get_by_name_and_host(namespace_name, host).await?
        else {
            return Err(OxenError::basic_str(format!(
                "Remote repository not found: {namespace_name}"
            )));
        };

        // Confirm the user wants to delete the remote repository
        match Confirm::new()
            .with_prompt(format!(
                "Are you sure you want to delete the remote repository: {namespace_name}?"
            ))
            .interact()
        {
            Ok(true) => {
                api::client::repositories::delete(&remote_repo).await?;
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