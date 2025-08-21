// TODO: Later

use async_trait::async_trait;
use clap::Command;

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use std::path::PathBuf;

use crate::cmd::{restore::restore_args, RunCmd};

pub const NAME: &str = "restore";
pub struct RemoteModeRestoreCmd;

#[async_trait]
impl RunCmd for RemoteModeRestoreCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        restore_args()
    }

    // TODO: Support multiple paths
    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let path = args.get_one::<String>("PATH").expect("required");
        let repo = LocalRepository::from_current_dir()?;

        let revision = if let Some(source) = args.get_one::<String>("source") {
            source.to_string()
        } else {
            let head_commit = repositories::commits::head_commit(&repo)?;
            head_commit.id
        };

        let paths = vec![PathBuf::from(path)];
        repositories::remote_mode::restore(&repo, &paths, &revision).await?;

        Ok(())
    }
}
