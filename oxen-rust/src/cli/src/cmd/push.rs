use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::PushOpts;

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
            .arg(
                Arg::new("force")
                    .long("force")
                    .help("Force push")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse args
        let remote = args
            .get_one::<String>("REMOTE")
            .expect("Must supply a remote");
        let delete = args.get_flag("delete");
        let force = args.get_flag("force");

        let repo = LocalRepository::from_current_dir()?;
        let current_branch = repositories::branches::current_branch(&repo)?;

        // Default to CURRENT branch
        let branch_name = if let Some(branch) = args.get_one::<String>("BRANCH") {
            branch.to_string()
        } else if let Some(branch) = current_branch {
            branch.name
        } else {
            return Err(OxenError::basic_str(
                "Error: Cannot push from non-existant branch",
            ));
        };

        let opts = PushOpts {
            remote: remote.to_string(),
            branch: branch_name,
            delete,
            force,
        };

        // Call into liboxen to push or delete
        if opts.delete {
            let (scheme, host) = get_scheme_and_host_from_repo(&repo)?;

            check_remote_version(scheme, host).await?;

            api::client::branches::delete_remote(&repo, &opts.remote, &opts.branch).await?;
            println!("Deleted remote branch: {}/{}", opts.remote, opts.branch);
            Ok(())
        } else {
            let mut repo = LocalRepository::from_current_dir()?;
            repo.set_remote_name(remote);

            let (scheme, host) = get_scheme_and_host_from_repo(&repo)?;

            check_repo_migration_needed(&repo)?;
            check_remote_version_blocking(scheme.clone(), host.clone()).await?;
            check_remote_version(scheme, host).await?;

            match repositories::push::push_remote_branch(&repo, &opts).await {
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
