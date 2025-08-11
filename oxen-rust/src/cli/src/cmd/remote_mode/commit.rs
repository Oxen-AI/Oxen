use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, NewCommitBody};
use liboxen::repositories;

use crate::helpers::check_repo_migration_needed;
use crate::cmd::RunCmd;

pub const NAME: &str = "commit";
pub struct RemoteModeCommitCmd;

#[async_trait]
impl RunCmd for RemoteModeCommitCmd {
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
                "Err: Usage `oxen workspace commit -w <workspace_id> -m <message>`",
            ));
        };

        let repo = LocalRepository::from_current_dir()?;

        check_repo_migration_needed(&repo)?;

        let cfg = UserConfig::get()?;
        let body = NewCommitBody {
            message: message.to_string(),
            author: cfg.name,
            email: cfg.email,
        };

        let _commit = repositories::remote_mode::commit(&repo, &body).await?;

        Ok(())
    }
}
