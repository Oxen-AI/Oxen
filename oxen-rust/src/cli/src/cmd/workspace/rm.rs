use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, ArgGroup, ArgMatches, Command};

use liboxen::{api, util, core::oxenignore, error::OxenError, model::LocalRepository};

use crate::cmd::{rm::rm_args, RunCmd};
pub const NAME: &str = "rm";
pub struct WorkspaceRmCmd;

#[async_trait]
impl RunCmd for WorkspaceRmCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // If in remote repo, workspace id can be found from the config instead
        let is_remote_repo = match LocalRepository::from_current_dir() {
            Ok(repo) => repo.is_remote_mode(),
            Err(_) => false,
        };

        rm_args()
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .help("The workspace ID of the workspace")
                    .conflicts_with("workspace-name"),
            )
            .arg(
                Arg::new("workspace-name")
                    .long("workspace-name")
                    .short('n')
                    .help("The name of the workspace")
                    .conflicts_with("workspace-id"),
            )
            .group(
                ArgGroup::new("workspace-identifier")
                    .args(["workspace-id", "workspace-name"])
                    .required(!is_remote_repo),
            )
            .arg(
                Arg::new("directory")
                    .long("directory")
                    .short('d')
                    .help("The destination directory to add the workspace to")
                    .default_value("."),
            )
            .arg_required_else_help(true)
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let mut paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let repository = LocalRepository::from_current_dir()?;

        let (workspace_identifier, _directory) = if repository.is_remote_mode() {
            (&repository.workspace_name.clone().unwrap(), ".")
        } else {
            let directory = args.get_one::<String>("directory").unwrap(); // safe to unwrap because we have a default value

            let workspace_name = args.get_one::<String>("workspace-name");
            let workspace_id = args.get_one::<String>("workspace-id");
            match workspace_id {
                Some(id) => (id, directory.as_str()),
                None => {
                    // If no ID is provided, try to get the workspace by name
                    if let Some(name) = workspace_name {
                        (name, directory.as_str())
                    } else {
                        return Err(OxenError::basic_str(
                            "Either workspace-id or workspace-name must be provided.",
                        ));
                    }
                }
            }
        };

        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;

        // Handle .oxenignore filtering
        let ignore = oxenignore::create(&repository);
        if let Some(ignore) = &ignore {
            paths.retain(|path| !ignore.matched(path, path.is_dir()).is_ignore());
        }

        // If no paths left after filtering, return early
        if paths.is_empty() {
            return Err(OxenError::basic_str(
                "No files to remove after filtering with .oxenignore.",
            ));
        }

        // TODO: Use directory
        if args.get_flag("staged") {
            api::client::workspaces::files::rm_files_from_staged(
                &remote_repo,
                workspace_identifier,
                paths,
            )
            .await?;
        } else {
            api::client::workspaces::files::rm_files(&repository, &remote_repo, workspace_identifier, paths)
                .await?;
            
        }

        Ok(())
    }
}
