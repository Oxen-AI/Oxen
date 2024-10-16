use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::config::UserConfig;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, NewCommitBody};
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "commit";
pub struct WorkspaceCommitCmd;

#[async_trait]
impl RunCmd for WorkspaceCommitCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Commit the staged files to the repository.")
            .arg(
                Arg::new("workspace_id")
                    .long("workspace_id")
                    .short('w')
                    .required(true)
                    .help("The workspace_id of the workspace"),
            )
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

        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str(
                "Err: Usage `oxen workspace commit -w <workspace_id> -m <message>`",
            ));
        };

        let repo = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repo)?;

        println!("Committing to remote with message: {message}");
        let branch = repositories::branches::current_branch(&repo)?;
        if branch.is_none() {
            log::error!("Workspace commit No current branch found");
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
        api::client::workspaces::commit(&remote_repo, &branch.name, workspace_id, &body).await?;

        Ok(())
    }
}
