use std::path::{Path, PathBuf};

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::error::StringError;
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

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let file_path = args
            .get_one::<String>("file")
            .map_or_else(|| Err(OxenError::basic_str("Must supply --file (-f)")), Ok)?;

        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_id = args.get_one::<String>("workspace-id");
        let output_path = args
            .get_one::<String>("output")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(file_path));

        let repository = LocalRepository::from_current_dir()?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;

        match (workspace_id, workspace_name) {
            (Some(workspace_id), None) => {
                let workspace = api::client::workspaces::get(&remote_repo, workspace_id)
                    .await?
                    .ok_or_else(|| {
                        OxenError::basic_str(format!(
                            "Workspace with ID '{workspace_id}' does not exist.",
                        ))
                    })?;

                match api::client::workspaces::files::download(
                    &remote_repo,
                    workspace_id,
                    file_path,
                    Some(&output_path),
                )
                .await
                {
                    Err(OxenError::ResourceNotFound(_)) => {
                        api::client::entries::download_entry(
                            &remote_repo,
                            Path::new(file_path),
                            &output_path,
                            workspace.commit.id,
                        )
                        .await
                    }
                    Err(e) => Err(e),
                    Ok(_) => Ok(()),
                }
            }

            (None, Some(workspace_name)) => {
                match api::client::workspaces::get_by_name(&remote_repo, workspace_name).await? {
                    Some(workspace) => {
                        // Workspace exists, download file from it
                        match api::client::workspaces::files::download(
                            &remote_repo,
                            &workspace.id,
                            file_path,
                            Some(&output_path),
                        )
                        .await
                        {
                            Err(e) => match e {
                                // file not in workspace --> fall back to getting from commit
                                // Workspace doesn't exist, fall back to downloading from commit
                                OxenError::ResourceNotFound(_) => {
                                    api::client::entries::download_entry(
                                        &remote_repo,
                                        Path::new(file_path),
                                        &output_path,
                                        workspace.commit.id,
                                    )
                                    .await
                                }
                                // cannot handle other errors
                                _ => Err(e),
                            },
                            // successfully downloaded
                            Ok(_) => Ok(()),
                        }
                    }
                    None => Err(OxenError::WorkspaceNotFound(Box::new(StringError::new(
                        format!("Workspace named {workspace_name} does not exist"),
                    )))),
                }
            }

            _ => {
                // This should never be reached due to clap's required_unless_present
                Err(OxenError::basic_str(
                    "Either --workspace-id or --workspace-name must be provided.",
                ))
            }
        }
    }
}
