use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use liboxen::repositories;

use crate::helpers::{
    check_remote_version, check_remote_version_blocking, check_repo_migration_needed,
    get_host_from_repo,
};
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};

use crate::cmd::RunCmd;
pub const NAME: &str = "push";
pub struct PushCmd;

#[async_trait]
impl RunCmd for PushCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Push the the files to the remote branch")
            .arg(
                Arg::new("REMOTE")
                    .help("Remote you want to push to")
                    .default_value(DEFAULT_REMOTE_NAME)
                    .default_missing_value(DEFAULT_REMOTE_NAME),
            )
            .arg(
                Arg::new("BRANCH")
                    .help("Branch name to push to")
                    .default_value(DEFAULT_BRANCH_NAME)
                    .default_missing_value(DEFAULT_BRANCH_NAME),
            )
            .arg(
                Arg::new("delete")
                    .long("delete")
                    .short('d')
                    .help("Remove the remote branch")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let remote = args
            .get_one::<String>("REMOTE")
            .expect("Must supply a remote");

        let branch = args
            .get_one::<String>("BRANCH")
            .expect("Must supply a branch");

        // Call into liboxen to push or delete
        if args.get_flag("delete") {
            let repository = LocalRepository::from_current_dir()?;

            let host = get_host_from_repo(&repository)?;
            check_remote_version(host).await?;

            api::client::branches::delete_remote(&repository, remote, branch).await?;
            println!("Deleted remote branch: {remote}/{branch}");
            Ok(())
        } else {
            let repository = LocalRepository::from_current_dir()?;
            let host = get_host_from_repo(&repository)?;

            check_repo_migration_needed(&repository)?;
            check_remote_version_blocking(host.clone()).await?;
            check_remote_version(host).await?;

            repositories::push::push_remote_branch(&repository, remote, branch).await?;
            Ok(())
        }
    }
}
