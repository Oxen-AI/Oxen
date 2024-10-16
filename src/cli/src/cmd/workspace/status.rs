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

use crate::helpers::{check_remote_version, check_remote_version_blocking, get_host_from_repo};

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
            .arg(
                Arg::new("workspace")
                    .long("workspace")
                    .short('w')
                    .help("Pass in the workspace id.")
                    .action(clap::ArgAction::Set),
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

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let directory = args.get_one::<String>("path").map(PathBuf::from);

        let Some(workspace_id) = args.get_one::<String>("workspace") else {
            return Err(OxenError::basic_str("Must supply workspace id."));
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
            skip,
            limit,
            print_all,
            is_remote,
        };

        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;

        let repository = LocalRepository::from_dir(&repo_dir)?;

        let host = get_host_from_repo(&repository)?;

        check_remote_version_blocking(host.clone()).await?;

        check_remote_version(host).await?;

        let directory = directory.unwrap_or(PathBuf::from("."));

        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        let repo_status = Self::status(&remote_repo, workspace_id, &directory, &opts).await?;
        repo_status.print_with_params(&opts);

        Ok(())
    }
}

impl WorkspaceStatusCmd {
    async fn status(
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
                    PathBuf::from(e.filename),
                    StagedEntry::empty_status(StagedEntryStatus::Added),
                )
            }));
        let added_mods: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.modified_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename),
                    StagedEntry::empty_status(StagedEntryStatus::Modified),
                )
            }));
        status.staged_files = added_files.into_iter().chain(added_mods).collect();

        Ok(status)
    }
}
