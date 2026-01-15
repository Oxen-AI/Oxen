use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "commit";
pub struct CommitCmd;

#[async_trait]
impl RunCmd for CommitCmd {
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
            .arg(
                Arg::new("allow_empty")
                    .help("Allow creating a commit with no changes")
                    .long("allow-empty")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let Some(message) = args.get_one::<String>("message") else {
            return Err(OxenError::basic_str(
                "Err: Usage `oxen commit -m <message>`",
            ));
        };

        let allow_empty = args.get_flag("allow_empty");

        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        println!("Committing with message: {message}");

        if allow_empty {
            let branch_name = repositories::branches::current_branch(&repo)?.map(|b| b.name);
            repositories::commits::commit_allow_empty(&repo, message, branch_name.as_deref())?;
        } else {
            repositories::commit(&repo, message)?;
        }

        Ok(())
    }
}
