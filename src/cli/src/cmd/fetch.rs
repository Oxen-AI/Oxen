use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::fetch_opts::FetchOpts;
use liboxen::repositories;

use crate::helpers::{
    check_remote_version_blocking, check_repo_migration_needed, get_scheme_and_host_from_repo,
};

use crate::cmd::RunCmd;
pub const NAME: &str = "fetch";
pub struct FetchCmd;

#[async_trait]
impl RunCmd for FetchCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Download objects and refs from the remote repository")
            .arg(
                Arg::new("branch")
                    .short('b')
                    .long("branch")
                    .help("Specify the branch to fetch")
                    .value_name("BRANCH"),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        let (scheme, host) = get_scheme_and_host_from_repo(&repository)?;

        check_repo_migration_needed(&repository)?;
        check_remote_version_blocking(scheme.clone(), host.clone()).await?;
        let mut fetch_opts = FetchOpts::new();
        let subtrees = repository.subtree_paths();
        fetch_opts.subtree_paths = subtrees;
        if let Some(branch) = args.get_one::<String>("branch") {
            fetch_opts.branch = branch.clone();
            repositories::fetch_branch(&repository, &fetch_opts).await?;
        } else {
            repositories::fetch_all(&repository, &fetch_opts).await?;
        }

        Ok(())
    }
}
