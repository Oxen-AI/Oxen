use std::path::{Path, PathBuf};

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::model::RemoteRepository;
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

        let workspace: WorkspaceResponse = match (workspace_id, workspace_name) {
            (Some(workspace_id), None) => api::client::workspaces::get(&remote_repo, workspace_id)
                .await
                .and_then(|w| match w {
                    Some(workspace) => Ok(workspace),
                    None => Err(workspace_not_found(&format!("ID={workspace_id}"))),
                })?,
            (None, Some(workspace_name)) => {
                api::client::workspaces::get_by_name(&remote_repo, workspace_name)
                    .await
                    .and_then(|w| match w {
                        Some(workspace) => Ok(workspace),
                        None => Err(workspace_not_found(&format!("name={workspace_name}"))),
                    })?
            }
            // This should never be reached due to clap's required_unless_present
            _ => Err(OxenError::basic_str(
                "Either --workspace-id or --workspace-name must be provided.",
            ))?,
        };

        download(&remote_repo, &workspace, file_path, output_path.as_path()).await
    }
}

/// CLI-facing error message for when a workspace is not found.
fn workspace_not_found(message: &str) -> OxenError {
    // TODO: should be a `OxenError::WorkspaceNotFound` but this is debug-rendered in the CLI at the moment.
    //       To control formatting exactly, it's an `OxenError::Basic`.
    //
    //       CLI error rendering needs to be updated so we can use error variants properly & have precise
    //       error message formatting control.
    // OxenError::WorkspaceNotFound(Box::new(StringError::new(format!("Workspace {message} does not exist"))))
    OxenError::basic_str(format!("Workspace {message} does not exist"))
}

/// Downloads a file from a remote workspace, or from the workspace's base commit, to a local filepath.
async fn download(
    remote_repo: &RemoteRepository,
    workspace: &WorkspaceResponse,
    file_path: &str,
    output_path: &Path,
) -> Result<(), OxenError> {
    // try to download file from workspace
    match api::client::workspaces::files::download(
        remote_repo,
        &workspace.id,
        file_path,
        Some(output_path),
    )
    .await
    {
        // found it in the workspace!
        Ok(_) => Ok(()),
        // file doesn't exist in workspace
        Err(OxenError::PathDoesNotExist(_)) => {
            // try to download from the workspace's base commit
            match api::client::entries::download_entry(
                remote_repo,
                Path::new(file_path),
                output_path,
                &workspace.commit.id,
            )
            .await
            {
                // found it in the commit!
                Ok(_) => Ok(()),
                // did not find it :(
                Err(OxenError::PathDoesNotExist(_)) => Err(OxenError::basic_str(
                    "File not found in workspace staged DB or base repo",
                )),
                unexpected_error => unexpected_error,
            }
        }
        unexpected_error => unexpected_error,
    }
}
