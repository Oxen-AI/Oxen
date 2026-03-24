use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::api;
use liboxen::constants::DEFAULT_REMOTE_NAME;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "unlock";

pub struct BranchUnlockCmd;

#[async_trait]
impl RunCmd for BranchUnlockCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .arg(
                Arg::new("branch")
                    .help("Branch to unlock")
                    .required(true)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .short('r')
                    .help("Specify the remote to unlock the branch on")
                    .default_value(DEFAULT_REMOTE_NAME)
                    .default_missing_value(DEFAULT_REMOTE_NAME)
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let remote_name = args.get_one::<String>("remote").expect("required");
        let branch = args.get_one::<String>("branch").expect("required");

        let repository = LocalRepository::from_current_dir()?;

        // Get the remote repo
        let remote = repository
            .get_remote(remote_name)
            .ok_or(OxenError::remote_not_set(remote_name))?;
        let remote_repo = api::client::repositories::get_by_remote(&remote)
            .await?
            .ok_or(OxenError::remote_not_found(remote.clone()))?;

        // Unlock the branch
        api::client::branches::unlock(&remote_repo, branch).await?;

        Ok(())
    }
}
