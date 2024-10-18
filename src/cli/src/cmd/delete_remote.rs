use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::constants::{DEFAULT_HOST, DEFAULT_REMOTE_NAME};
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
                .help("The namespace/name of the remote repository you want to delete. For example: 'ox/my_repo'")
                .required(true)
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("The host you want to delete the remote repository on. For example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .help("The remote you want to delete the repository on. For example: 'origin'")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let Some(namespace_name) = args.get_one::<String>("name") else {
            return Err(OxenError::basic_str(
                "Must supply a namespace/name for the remote repository.",
            ));
        };

        let host = args
            .get_one::<String>("host")
            .map(String::from)
            .unwrap_or(DEFAULT_HOST.to_string());

        let remote = args
            .get_one::<String>("remote")
            .map(String::from)
            .unwrap_or(DEFAULT_REMOTE_NAME.to_string());

        let parts: Vec<&str> = namespace_name.split('/').collect();
        if parts.len() != 2 {
            return Err(OxenError::basic_str(
                "Invalid name format. Must be namespace/name",
            ));
        }

        if let Some(remote_repo) =
            api::remote::repositories::get_by_name_host_and_remote(&namespace_name, &host, &remote)
                .await?
        {
            api::remote::repositories::delete(&remote_repo).await?;
            println!("Deleted remote repository: {}", namespace_name);
        } else {
            eprintln!("Repository does not exist {}", namespace_name);
        }
        Ok(())
    }
}
