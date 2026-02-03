use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::api;
use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::LocalRepository;
use liboxen::model::RemoteRepository;
use liboxen::model::StagedData;
use liboxen::model::StagedEntry;
use liboxen::model::StagedEntryStatus;
use liboxen::util;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::helpers::{
    check_remote_version, check_remote_version_blocking, get_scheme_and_host_from_repo,
};

use crate::cmd::RunCmd;
pub const NAME: &str = "status";
pub struct WorkspaceStatusCmd;

#[async_trait]
impl RunCmd for WorkspaceStatusCmd {
    fn name(&self) -> &str {
        NAME
    }
    fn args(&self) -> Command {
        Command::new(NAME)
            .about("See at what files are ready to be added or committed")
            // DEPRECATED: (start) announce migration now + remove in the next release
            .arg(
                Arg::new("workspace")
                    .long("workspace")
                    .required_unless_present("workspace-name")
                    .required_unless_present("workspace-id")
                    .conflicts_with("workspace-name")
                    .conflicts_with("workspace-id")
                    .help("DEPRECATED OPTION -- renamed to '--workspace-id'"),
            )
            // DEPRECATED: (end)
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .required_unless_present("workspace-name")
                    .required_unless_present("workspace") // DEPRECATED: remove
                    .conflicts_with("workspace-name")
                    .conflicts_with("workspace") // DEPRECATED: remove
                    .help("The workspace_id of the workspace"),
            )
            .arg(
                Arg::new("workspace-name")
                    .long("workspace-name")
                    .short('n')
                    .required_unless_present("workspace-id")
                    .required_unless_present("workspace") // DEPRECATED: remove
                    .conflicts_with("workspace") // DEPRECATED: remove
                    .conflicts_with("workspace-id")
                    .help("The name of the workspace"),
            )
            .arg(
                Arg::new("skip")
                    .long("skip")
                    .short('s')
                    .help("Allows you to skip and paginate through the file list preview.")
                    .default_value("0")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("limit")
                    .long("limit")
                    .short('l')
                    .help("Allows you to view more file list preview.")
                    .default_value("10")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("print_all")
                    .long("print_all")
                    .short('a')
                    .help("If present, does not truncate the output of status at all.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(Arg::new("path").required(false))
    }

    /// Parse CLI arguments and execute the workspace status command.
    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let directory = args.get_one::<String>("path").map(PathBuf::from);

        // let workspace_id = args.get_one::<String>("workspace-id");
        // DEPRECATED: (start) remove this logic and replace with the above commented-out line once we remove '--workspace'
        let workspace_id = match args.get_one::<String>("workspace-id") {
            None => match args.get_one::<String>("workspace") {
                None => None,
                something => {
                    eprintln!("DEPRECATION WARNING: '--workspace' option has been renamed to '--workspace-id'. '--workspace' will be **REMOVED** in a future release!");
                    something
                }
            },
            something => something,
        };
        // DEPRECATED: (end)
        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_identifier = match workspace_id {
            Some(id) => id,
            None => {
                // it's OK to use the name here since the server will resolve the input
                // as a workspace name (it first tries to understand it as a workspace ID,
                // it falls-back to treating it as a workspace name if that fails)
                if let Some(name) = workspace_name {
                    name
                } else {
                    return Err(OxenError::basic_str(
                        "Either workspace-id or workspace-name must be provided.",
                    ));
                }
            }
        };

        let skip = args
            .get_one::<String>("skip")
            .expect("Must supply skip")
            .parse::<usize>()
            .expect("skip must be a valid integer.");
        let limit = args
            .get_one::<String>("limit")
            .expect("Must supply limit")
            .parse::<usize>()
            .expect("limit must be a valid integer.");
        let print_all = args.get_flag("print_all");

        let is_remote = true;
        let opts = StagedDataOpts {
            paths: vec![PathBuf::from("")],
            skip,
            limit,
            print_all,
            is_remote,
            ignore: None,
        };

        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

        let repository = LocalRepository::from_dir(&repo_dir)?;

        let (scheme, host) = get_scheme_and_host_from_repo(&repository)?;

        check_remote_version_blocking(scheme.clone(), host.clone()).await?;
        check_remote_version(scheme, host).await?;

        let directory = directory.unwrap_or(PathBuf::from("."));

        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;

        let repo_status =
            Self::status(&remote_repo, workspace_identifier, &directory, &opts).await?;
        repo_status.print_with_params(&opts);

        Ok(())
    }
}

impl WorkspaceStatusCmd {
    /// Retrieves the status of the workspace's files as compared to the workspace's commit.
    pub(crate) async fn status(
        remote_repo: &RemoteRepository,
        workspace_id: &str,
        directory: impl AsRef<Path>,
        opts: &StagedDataOpts,
    ) -> Result<StagedData, OxenError> {
        let page_size = opts.limit;
        let page_num = opts.skip / page_size;

        let remote_status = api::client::workspaces::changes::list(
            remote_repo,
            workspace_id,
            directory,
            page_num,
            page_size,
        )
        .await?;

        let mut status = StagedData::empty();
        status.staged_dirs = remote_status.added_dirs;
        let added_files: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.added_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Added),
                )
            }));
        let added_mods: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.modified_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Modified),
                )
            }));
        status.staged_files = added_files.into_iter().chain(added_mods).collect();

        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use liboxen::api;
    use liboxen::error::OxenError;

    #[tokio::test]
    async fn test_workspace_status_by_name() -> Result<(), OxenError> {
        oxen_test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let w_id = "test_workspace_id";
            let w_name = "my_test_workspace_name";
            let opts = StagedDataOpts {
                paths: vec![PathBuf::from("")],
                skip: 0,
                limit: 10,
                print_all: false,
                is_remote: true,
                ignore: None,
            };

            // Create a named workspace
            api::client::workspaces::create_with_name(&remote_repo, "main", w_id, w_name).await?;
            let status = WorkspaceStatusCmd::status(&remote_repo, w_id, ".", &opts).await?;
            assert_eq!(status.staged_files.len(), 0);

            // add a file to it and ensure that we can retrieve status via name
            api::client::workspaces::files::upload_single_file(
                &remote_repo,
                w_name,
                "",
                oxen_test::test_img_file(),
            )
            .await?;
            let status = WorkspaceStatusCmd::status(&remote_repo, w_name, ".", &opts).await?;
            assert_eq!(status.staged_files.len(), 1);

            Ok(remote_repo)
        })
        .await
    }
}
