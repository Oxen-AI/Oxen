use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::view::workspaces::WorkspaceResponse;
use liboxen::{api, error::OxenError, model::LocalRepository};

use crate::cmd::RunCmd;

pub const NAME: &str = "download";

pub struct WorkspaceDownloadCmd;

#[async_trait]
impl RunCmd for WorkspaceDownloadCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Download files from a workspace")
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .required_unless_present("workspace-name")
                    .conflicts_with("workspace-name")
                    .help("The workspace ID of the workspace"),
            )
            .arg(
                Arg::new("workspace-name")
                    .long("workspace-name")
                    .short('n')
                    .required_unless_present("workspace-id")
                    .conflicts_with("workspace-id")
                    .help("The name of the workspace"),
            )
            .arg(
                Arg::new("file")
                    .long("file")
                    .short('f')
                    .required(true)
                    .help("The path of the file to download from the workspace"),
            )
            .arg(
                Arg::new("output")
                    .long("output")
                    .short('o')
                    .help("The output path where the file should be saved"),
            )
            .arg_required_else_help(true)
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), anyhow::Error> {
        // Parse Args
        let file_path = args
            .get_one::<String>("file")
            .map_or_else(|| Err(anyhow::anyhow!("Must supply --file (-f)")), Ok)?;

        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_id = args.get_one::<String>("workspace-id");
        let output_path = args
            .get_one::<String>("output")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(file_path));

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;

        let workspace: WorkspaceResponse = match (workspace_id, workspace_name) {
            (Some(workspace_id), None) => {
                let Some(workspace) =
                    api::client::workspaces::get(&remote_repo, workspace_id).await?
                else {
                    return Err(anyhow::anyhow!(
                        "Workspace ID={workspace_id} does not exist"
                    ));
                };
                workspace
            }
            (None, Some(workspace_name)) => {
                let Some(workspace) =
                    api::client::workspaces::get_by_name(&remote_repo, workspace_name).await?
                else {
                    return Err(anyhow::anyhow!(
                        "Workspace name={workspace_name} does not exist"
                    ));
                };
                workspace
            }
            // This should never be reached due to clap's required_unless_present
            _ => {
                return Err(anyhow::anyhow!(
                    "Either --workspace-id or --workspace-name must be provided.",
                ));
            }
        };

        match api::client::workspaces::files::download(
            &remote_repo,
            &workspace.id,
            file_path,
            Some(&output_path),
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(OxenError::PathDoesNotExist(_)) => Err(OxenError::resource_not_found(
                "File not found in workspace staged DB or base repo",
            ))?,
            unexpected_error => Ok(unexpected_error?),
        }
    }
}
