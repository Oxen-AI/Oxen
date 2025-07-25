use crate::helpers::check_repo_migration_needed;
use async_trait::async_trait;
use clap::{ArgMatches, Command};
use liboxen::config::UserConfig;
use std::env;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::opts::RestoreOpts;

use std::path::PathBuf;

use crate::cmd::{restore::restore_args, RunCmd};
pub const NAME: &str = "restore";
pub struct WorkspaceRestoreCmd;

#[async_trait]
impl RunCmd for WorkspaceRestoreCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        restore_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let path = args.get_one::<String>("PATH").expect("required");

        let opts = if let Some(source) = args.get_one::<String>("source") {
            RestoreOpts {
                path: PathBuf::from(path),
                staged: args.get_flag("staged"),
                is_remote: true,
                source_ref: Some(String::from(source)),
            }
        } else {
            RestoreOpts {
                path: PathBuf::from(path),
                staged: args.get_flag("staged"),
                is_remote: true,
                source_ref: None,
            }
        };

        let repo_dir = env::current_dir().unwrap();
        let repository = LocalRepository::from_dir(&repo_dir)?;

        check_repo_migration_needed(&repository)?;

        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        let workspace_id = UserConfig::identifier()?;
        api::client::workspaces::data_frames::restore(
            &remote_repo,
            &workspace_id,
            opts.path.to_owned(),
        )
        .await?;

        Ok(())
    }
}
