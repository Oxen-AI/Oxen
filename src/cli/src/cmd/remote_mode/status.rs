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

use liboxen::core::v_latest::status::status_from_opts_and_staged_data;

use crate::cmd::RunCmd;
pub const NAME: &str = "status";
pub struct RemoteModeStatusCmd;

#[async_trait]
impl RunCmd for RemoteModeStatusCmd {
    fn name(&self) -> &str {
        NAME
    }
    fn args(&self) -> Command {
        Command::new(NAME)
            // TODO: Update about message
            .about("See at what files are ready to be added or committed")
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
            // TODO: Implement
            .arg(
                Arg::new("paths")
                    .num_args(0..)
                    .trailing_var_arg(true) // Collect all remaining args as paths
                    .help("Specify one or more paths"),
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;
        let repository = LocalRepository::from_dir(&repo_dir)?;

        let workspace_id = if repository.is_remote_mode() {
            repository.workspace_name.clone().unwrap()
        } else {
            // TODO: New error type
            return Err(OxenError::basic_str(
                "New err type, can't do rmeote mode command outside remote mode repo",
            ));
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

        let paths = args
            .get_many::<String>("paths")
            .map(|vals| vals.map(|v| repository.path.join(v)).collect())
            .unwrap_or_else(|| vec![repository.path.clone()]);

        // Get workspace status
        let mut is_remote = true;
        let remote_opts = StagedDataOpts {
            paths: paths.clone(),
            skip,
            limit,
            print_all,
            is_remote,
            ignore: None,
        };

        let (scheme, host) = get_scheme_and_host_from_repo(&repository)?;

        check_remote_version_blocking(scheme.clone(), host.clone()).await?;
        check_remote_version(scheme, host).await?;

        // TODO: Implement path-based workspace status
        let directory = PathBuf::from(".");

        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;
        let mut repo_status =
            Self::status(&remote_repo, &workspace_id, &directory, &remote_opts).await?;

        // Get local status
        is_remote = false;
        let local_opts = StagedDataOpts {
            paths: paths.clone(),
            skip,
            limit,
            print_all,
            is_remote,
            ignore: None,
        };

        status_from_opts_and_staged_data(&repository, &local_opts, &mut repo_status)?;

        // TODO: Make custom status message clarifying 'untracked' dirs and files
        repo_status.print_with_params(&local_opts);

        Ok(())
    }
}

impl RemoteModeStatusCmd {
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
        let staged_removals: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.removed_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Removed),
                )
            }));
        status.staged_files = added_files
            .into_iter()
            .chain(added_mods)
            .chain(staged_removals)
            .collect();

        Ok(status)
    }
}
