// TODO: Later

use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::{repositories, api};

use std::path::Path;
use colored::Colorize;
use uuid::Uuid;

use crate::cmd::RunCmd;

pub const NAME: &str = "sync";
pub struct RemoteModeSyncCmd;

#[async_trait]
impl RunCmd for RemoteModeSyncCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {

        Command::new(NAME)
            .about("sync ")

    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {

        let mut repo = LocalRepository::from_current_dir()?;
        let Some(branch_name) = repositories::commits::head_commit_maybe()? else {
            DEFAULT_BRANCH_NAME
        };

        // Get head commit
        // Call donwload_dir on the root dir 
        repositories::fetch(&remote_repo, branch_name)?;
        repositories::checkout_remote_mode(&repo, branch)?;

    

        Ok(())
    }
}
