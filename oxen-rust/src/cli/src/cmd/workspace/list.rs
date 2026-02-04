use async_trait::async_trait;
use clap::{ArgMatches, Command};
use colored::Colorize;
use liboxen::view::RemoteStagedStatus;
use std::path::Path;

use liboxen::api;
use liboxen::constants;
use liboxen::{error::OxenError, model::LocalRepository};

use crate::cmd::RunCmd;
pub const NAME: &str = "list";
pub struct WorkspaceListCmd;

#[async_trait]
impl RunCmd for WorkspaceListCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME).about("Lists all workspaces").arg(
            clap::Arg::new("remote")
                .short('r')
                .long("remote")
                .help("Remote repository name")
                .required(false),
        )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repository = LocalRepository::from_current_dir()?;
        let remote_name = args.get_one::<String>("remote");
        let remote_repo = match remote_name {
            Some(name) => {
                let remote = repository
                    .get_remote(name)
                    .ok_or(OxenError::remote_not_set(name))?;
                api::client::repositories::get_by_remote(&remote)
                    .await?
                    .ok_or(OxenError::remote_not_found(remote))?
            }
            None => api::client::repositories::get_default_remote(&repository).await?,
        };

        let workspaces = api::client::workspaces::list(&remote_repo).await?;
        if workspaces.is_empty() {
            println!("No workspaces found");
            return Ok(());
        }

        println!("id\tname\tcommit_id\tcommit_message\tstatus");
        for workspace in workspaces {
            let ws_changes: WorkspaceChanges = api::client::workspaces::changes::list(
                &remote_repo,
                &workspace.id,
                Path::new(""),
                constants::DEFAULT_PAGE_NUM,
                constants::DEFAULT_PAGE_SIZE,
            )
            .await
            .into();

            println!(
                "{}\t{}\t{}\t{}\t{}",
                workspace.id,
                workspace.name.unwrap_or("".to_string()),
                workspace.commit.id,
                workspace.commit.message,
                ws_changes,
            );
        }
        Ok(())
    }
}

/// The status of file changes in a workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkspaceChanges {
    /// There are no changes in the workspace as compared to its commit.
    Clean,
    /// There are some file changes: tracks added, modified, and deleted files.
    Changes {
        added: usize,
        modified: usize,
        deleted: usize,
    },
    /// Unable to determine the status of the workspace.
    Unknown,
}

/// Converts a Result<RemoteStagedStatus, OxenError> into either a Clean or Changes if its Ok otherwise its Unknown.
impl From<Result<RemoteStagedStatus, OxenError>> for WorkspaceChanges {
    fn from(result: Result<RemoteStagedStatus, OxenError>) -> Self {
        match result {
            Ok(status) => (&status).into(),
            Err(_) => WorkspaceChanges::Unknown,
        }
    }
}

/// Converts a RemoteStagedStatus into either a Clean or Changes variant of a WorkspaceChanges.
impl From<&RemoteStagedStatus> for WorkspaceChanges {
    fn from(status: &RemoteStagedStatus) -> Self {
        let added = status.added_files.total_entries;
        let modified = status.modified_files.total_entries;
        let deleted = status.removed_files.total_entries;

        if added == 0 && modified == 0 && deleted == 0 {
            WorkspaceChanges::Clean
        } else {
            WorkspaceChanges::Changes {
                added,
                modified,
                deleted,
            }
        }
    }
}

impl std::fmt::Display for WorkspaceChanges {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkspaceChanges::Clean => write!(f, "clean"),
            WorkspaceChanges::Changes {
                added,
                modified,
                deleted,
            } => {
                let status_entry = {
                    let mut parts = Vec::new();
                    if *added > 0 {
                        parts.push(format!("{}", format!("+{}", added).green()));
                    }
                    if *modified > 0 {
                        parts.push(format!("{}", format!("~{}", modified).yellow()));
                    }
                    if *deleted > 0 {
                        parts.push(format!("{}", format!("-{}", deleted).red()));
                    }
                    parts.join(" ")
                };
                write!(f, "{}", status_entry)
            }
            WorkspaceChanges::Unknown => write!(f, "unknown"),
        }
    }
}
