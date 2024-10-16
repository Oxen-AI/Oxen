use async_trait::async_trait;
use clap::Command;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use liboxen::repositories;

use crate::helpers::{
    check_remote_version_blocking, check_repo_migration_needed, get_host_from_repo,
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
        Command::new(NAME).about("Download objects and refs from the remote repository")
    }

    async fn run(&self, _args: &clap::ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        let host = get_host_from_repo(&repository)?;

        check_repo_migration_needed(&repository)?;
        check_remote_version_blocking(host.clone()).await?;
        repositories::fetch(&repository, false).await?;
        Ok(())
    }
}
