use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use liboxen::repositories;

use crate::helpers::{
    check_remote_version, check_remote_version_blocking, check_repo_migration_needed,
    get_scheme_and_host_from_repo,
};
use liboxen::constants::DEFAULT_REMOTE_NAME;

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
            .arg(Arg::new("BRANCH").help("Branch name to push to"))
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

        let repo = LocalRepository::from_current_dir()?;
        let current_branch = repositories::branches::current_branch(&repo)?;

        // Default to CURRENT branch
        let branch = if let Some(branch) = args.get_one::<String>("BRANCH") {
            branch
        } else if current_branch.is_some() {
            &current_branch.unwrap().name
        } else {
            return Err(OxenError::basic_str(
                "Error: Cannot push from non-existant branch",
            ));
        };

        // Call into liboxen to push or delete
        if args.get_flag("delete") {
            let repository = LocalRepository::from_current_dir()?;

            let (scheme, host) = get_scheme_and_host_from_repo(&repository)?;

            check_remote_version(scheme, host).await?;

            api::client::branches::delete_remote(&repository, remote, branch).await?;
            println!("Deleted remote branch: {remote}/{branch}");
            Ok(())
        } else {
            let mut repository = LocalRepository::from_current_dir()?;
            repository.set_remote_name(remote);

            let (scheme, host) = get_scheme_and_host_from_repo(&repository)?;

            check_repo_migration_needed(&repository)?;
            check_remote_version_blocking(scheme.clone(), host.clone()).await?;
            check_remote_version(scheme, host).await?;

            match repositories::push::push_remote_branch(&repository, remote, branch).await {
                Ok(_) => Ok(()),
                Err(OxenError::BranchNotFound(branch)) => {
                    let msg = format!("{}\nMake sure you are on the correct branch and have committed your changes.", branch);
                    Err(OxenError::basic_str(msg))
                }
                Err(e) => {
                    println!("Error pushing: {}", e);
                    Err(e)
                }
            }
        }
    }
}
