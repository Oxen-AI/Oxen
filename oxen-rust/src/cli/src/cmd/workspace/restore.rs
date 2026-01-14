use crate::helpers::check_repo_migration_needed;
use async_trait::async_trait;
use clap::{ArgMatches, Command};
use liboxen::config::UserConfig;
use std::env;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use std::collections::HashSet;
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
        let paths: Vec<PathBuf> = args
            .get_many::<String>("paths")
            .expect("Must supply paths")
            .map(|p| -> Result<PathBuf, OxenError> {
                let path = PathBuf::from(p);
                Ok(path)
            })
            .collect::<Result<Vec<PathBuf>, OxenError>>()?;

        let paths: HashSet<PathBuf> = HashSet::from_iter(paths.iter().cloned());

        let repo_dir = env::current_dir().unwrap();
        let repository = LocalRepository::from_dir(&repo_dir)?;

        check_repo_migration_needed(&repository)?;

        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        let workspace_identifier = if repository.is_remote_mode() {
            repository.workspace_name.unwrap()
        } else {
            UserConfig::identifier()?
        };

        // TODO: Refactor this to restore all paths in 1 API call
        for path in paths {
            api::client::workspaces::data_frames::restore(
                &remote_repo,
                &workspace_identifier,
                &path,
            )
            .await?;
        }

        Ok(())
    }
}
