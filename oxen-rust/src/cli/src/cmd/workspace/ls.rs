use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use liboxen::api;
use liboxen::error;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util;

use std::path::PathBuf;

use crate::cmd::RunCmd;
pub const NAME: &str = "ls";
pub struct WorkspaceLsCmd;

#[async_trait]
impl RunCmd for WorkspaceLsCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("List files in a workspace directory")
            .arg(
                Arg::new("workspace-id")
                    .long("workspace-id")
                    .short('w')
                    .required_unless_present("workspace-name")
                    .conflicts_with("workspace-name")
                    .help("The workspace_id of the workspace"),
            )
            .arg(
                Arg::new("workspace-name")
                    .long("workspace-name")
                    .short('n')
                    .required_unless_present("workspace-id")
                    .conflicts_with("workspace-id")
                    .help("The name of the workspace"),
            )
            .arg(Arg::new("path").help("Directory path to list (defaults to root)"))
            .arg(
                Arg::new("page")
                    .long("page")
                    .short('p')
                    .help("Page number")
                    .default_value("1")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("page-size")
                    .long("page-size")
                    .help("Number of entries per page")
                    .default_value("100")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let workspace_id = args.get_one::<String>("workspace-id");
        let workspace_name = args.get_one::<String>("workspace-name");
        let workspace_identifier = match workspace_id {
            Some(id) => id,
            None => {
                if let Some(name) = workspace_name {
                    name
                } else {
                    return Err(OxenError::basic_str(
                        "Either workspace-id or workspace-name must be provided.",
                    ));
                }
            }
        };

        let path = args
            .get_one::<String>("path")
            .map(PathBuf::from)
            .unwrap_or_default();

        let page: usize = args
            .get_one::<String>("page")
            .expect("Must supply page")
            .parse()
            .expect("page must be a valid integer");
        let page_size: usize = args
            .get_one::<String>("page-size")
            .expect("Must supply page-size")
            .parse()
            .expect("page-size must be a valid integer");

        let repo_dir = util::fs::get_repo_root_from_current_dir()
            .ok_or(OxenError::basic_str(error::NO_REPO_FOUND))?;
        let repository = LocalRepository::from_dir(&repo_dir)?;
        let remote_repo = api::client::repositories::get_default_remote(&repository).await?;

        let result = api::client::workspaces::ls::list(
            &remote_repo,
            workspace_identifier,
            &path,
            page,
            page_size,
        )
        .await?;

        // Print entries
        for entry in &result.entries {
            let prefix = if entry.is_dir() { "d " } else { "  " };
            let status = match &entry {
                liboxen::view::entries::EMetadataEntry::WorkspaceMetadataEntry(ws) => {
                    ws.changes.as_ref().map_or("", |c| match c.status {
                        liboxen::model::StagedEntryStatus::Added => " [added]",
                        liboxen::model::StagedEntryStatus::Modified => " [modified]",
                        liboxen::model::StagedEntryStatus::Removed => " [removed]",
                        _ => "",
                    })
                }
                _ => "",
            };
            println!(
                "{}{}{} ({} bytes)",
                prefix,
                entry.filename(),
                status,
                entry.size()
            );
        }

        if result.total_pages > 1 {
            println!(
                "\nPage {}/{} ({} total entries)",
                result.page_number, result.total_pages, result.total_entries
            );
        }

        Ok(())
    }
}
