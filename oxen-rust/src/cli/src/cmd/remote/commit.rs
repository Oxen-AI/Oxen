use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "commit";
pub struct RemoteCommitCmd;

#[async_trait]
impl RunCmd for RemoteCommitCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Commit the staged files to the repository.")
            .arg(
                Arg::new("message")
                    .help("The message for the commit. Should be descriptive about what changed.")
                    .long("message")
                    .short('m')
                    .required(true)
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(message) = args.get_one::<String>("message") else {
            return Err(OxenError::basic_str(
                "Err: Usage `oxen commit -m <message>`",
            ));
        };

        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        println!("Committing to remote with message: {message}");
        command::remote::commit(&repo, message).await?;

        Ok(())
    }
}
