use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::model::LocalRepository;
use liboxen::{error::OxenError, opts::FetchOpts};

use liboxen::repositories;

use crate::helpers::{
    check_remote_version, check_remote_version_blocking, check_repo_migration_needed,
    get_host_from_repo,
};
use liboxen::constants::{DEFAULT_BRANCH_NAME, DEFAULT_REMOTE_NAME};

use crate::cmd::RunCmd;
pub const NAME: &str = "pull";
pub struct PullCmd;

#[async_trait]
impl RunCmd for PullCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Pull the files up from a remote branch")
            .arg(
                Arg::new("REMOTE")
                    .help("Remote you want to pull from")
                    .default_value(DEFAULT_REMOTE_NAME)
                    .default_missing_value(DEFAULT_REMOTE_NAME),
            )
            .arg(
                Arg::new("BRANCH")
                    .help("Branch name to pull"),
            )
            .arg(
                Arg::new("all")
                    .long("all")
                    .help("This pulls the full commit history, all the data files, and all the commit databases. Useful if you want to have the entire history locally or push to a new remote.")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        let current_branch = repositories::branches::current_branch(&repository)?;

        // Parse args
        let remote = args
            .get_one::<String>("REMOTE")
            .expect("Must supply a remote");
        let branch = if let Some(branch) = args.get_one::<String>("BRANCH") {
            branch
        } else if current_branch.is_some() {
            &current_branch.unwrap().name
        } else {
            DEFAULT_BRANCH_NAME
        };
        let all = args.get_flag("all");

        let host = get_host_from_repo(&repository)?;
        check_repo_migration_needed(&repository)?;
        check_remote_version_blocking(host.clone()).await?;
        check_remote_version(host).await?;

        let mut fetch_opts = FetchOpts::new();
        fetch_opts.branch = branch.to_owned();
        fetch_opts.remote = remote.to_owned();
        fetch_opts.depth = repository.depth();
        fetch_opts.subtree_paths = repository.subtree_paths();
        fetch_opts.all = all;
        repositories::pull_remote_branch(&repository, &fetch_opts).await?;
        Ok(())
    }
}
