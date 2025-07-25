use async_trait::async_trait;
use clap::Arg;
use clap::Command;

use liboxen::api;
use liboxen::constants::DEFAULT_PAGE_NUM;
use liboxen::constants::DEFAULT_PAGE_SIZE;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::DiffCmd;
use crate::cmd::RunCmd;
use crate::helpers::check_repo_migration_needed;

pub const NAME: &str = "diff";
pub struct WorkspaceDiffCmd;

#[async_trait]
impl RunCmd for WorkspaceDiffCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        DiffCmd.args().arg(
            Arg::new("workspace-id")
                .long("workspace-id")
                .short('w')
                .help("The workspace to compare against.")
                .action(clap::ArgAction::Set),
        )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let opts = DiffCmd::parse_args(args);
        let repo = LocalRepository::from_current_dir()?;

        let workspace_id = if repo.is_remote_mode() {
            Some(repo.workspace_name.clone().unwrap())
        } else if let Some(id) = args.get_one::<String>("workspace-id") {
            Some(id.to_string())
        } else {
            return Err(OxenError::basic_str("Must supply a workspace id."));
        }
        .unwrap();

        check_repo_migration_needed(&repo)?;

        let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
        let diff = api::client::workspaces::data_frames::diff(
            &remote_repo,
            &workspace_id,
            &opts.path_1,
            DEFAULT_PAGE_NUM,
            DEFAULT_PAGE_SIZE,
        )
        .await?;
        let remote_df = diff.view.to_df();
        println!("{:?}", remote_df);

        // TODO: Allow them to save a remote diff to disk

        Ok(())
    }
}
