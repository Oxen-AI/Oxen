use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::{api, error::OxenError, model::LocalRepository, opts::AddOpts};

use crate::cmd::{add::add_args, RunCmd};
pub const NAME: &str = "add";
pub struct WorkspaceAddCmd;

#[async_trait]
impl RunCmd for WorkspaceAddCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        add_args().arg(
            Arg::new("workspace_id")
                .long("workspace_id")
                .short('w')
                .required(true)
                .help("The workspace_id of the workspace"),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let Some(workspace_id) = args.get_one::<String>("workspace_id") else {
            return Err(OxenError::basic_str("Must supply a workspace id"));
        };

        let opts = AddOpts {
            paths,
            is_remote: false,
            directory: None,
        };

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        for path in opts.paths.iter() {
            api::client::workspaces::files::add(
                &repository,
                &remote_repo,
                workspace_id,
                path,
                &opts,
            )
            .await?;
        }

        Ok(())
    }
}
