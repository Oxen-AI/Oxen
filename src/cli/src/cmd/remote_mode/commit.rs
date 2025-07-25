use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, NewCommitBody};
use liboxen::opts::FetchOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

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

        let workspace_identifier = if repo.is_remote_mode() {
            &repo.workspace_name.clone().unwrap()
        } else {
            return Err(OxenError::basic_str(
                "Error: Cannot run remote mode commands outside remote mode repo",
            ));
        };

        check_repo_migration_needed(&repo)?;

        println!("Committing to remote with message: {message}");
        let branch = repositories::branches::current_branch(&repo)?;
        if branch.is_none() {
            log::error!("Remote-mode commit No current branch found");
            return Err(OxenError::must_be_on_valid_branch());
        }
        let branch = branch.unwrap();

        let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
        let cfg = UserConfig::get()?;
        let body = NewCommitBody {
            message: message.to_string(),
            author: cfg.name,
            email: cfg.email,
        };
        api::client::workspaces::commit(&remote_repo, &branch.name, workspace_identifier, &body)
            .await?;

        // Update local tree
        let fetch_opts = FetchOpts::from_branch(&branch.name);
        repositories::fetch::fetch_branch(&repo, &fetch_opts).await?;

        Ok(())
    }
}
