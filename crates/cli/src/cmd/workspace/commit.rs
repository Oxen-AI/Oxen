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
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .required_unless_present("workspace-name")
                    .help("The workspace_id of the workspace"),
            )
            .arg(
                Arg::new("workspace-name")
                    .long("workspace-name")
                    .short('n')
                    .required_unless_present("workspace-id")
                    .help("The name of the workspace"),
            )
            .arg(
                Arg::new("message")
                    .help("The message for the commit. Should be descriptive about what changed.")
                    .long("message")
                    .short('m')
                    .required(true)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .help("The branch to commit to"),
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
            let workspace_name = args.get_one::<String>("workspace-name");
            let workspace_id = args.get_one::<String>("workspace-id");
            match workspace_id {
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
            }
        };

        check_repo_migration_needed(&repo)?;

        let branch_name = match args.get_one::<String>("branch") {
            Some(name) => name.clone(),
            None => {
                // No --branch flag, so we need the current branch as a default
                match repositories::branches::current_branch(&repo)? {
                    Some(branch) => branch.name,
                    None => {
                        return Err(OxenError::basic_str(
                            "No current branch. Use --branch to specify a target branch to commit the workspace to.",
                        ));
                    }
                }
            }
        };

        println!("Committing to remote with message: {message}");

        let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
        let cfg = UserConfig::get()?;
        let body = NewCommitBody {
            message: message.to_string(),
            author: cfg.name,
            email: cfg.email,
        };
        api::client::workspaces::commit(&remote_repo, &branch_name, workspace_identifier, &body)
            .await?;

        Ok(())
    }
}
