use async_trait::async_trait;
use clap::{ArgMatches, Command};

use liboxen::api;
use liboxen::constants;
use liboxen::opts::PaginateOpts;
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
        Command::new(NAME)
            .about("Lists workspaces")
            .arg(
                clap::Arg::new("remote")
                    .short('r')
                    .long("remote")
                    .help("Remote repository name")
                    .required(false),
            )
            .arg(
                clap::Arg::new("page")
                    .long("page")
                    .help("Page number to fetch")
                    .value_parser(clap::value_parser!(usize))
                    .required(false),
            )
            .arg(
                clap::Arg::new("page-size")
                    .long("page-size")
                    .help("Number of workspaces per page")
                    .value_parser(clap::value_parser!(usize))
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
                    .ok_or_else(|| OxenError::RemoteNotSet(name.clone()))?;
                api::client::repositories::get_by_remote(&remote).await?
            }
            None => api::client::repositories::get_default_remote(&repository).await?,
        };

        let page_opts = PaginateOpts {
            page_num: args
                .get_one::<usize>("page")
                .copied()
                .unwrap_or(constants::DEFAULT_PAGE_NUM),
            page_size: args
                .get_one::<usize>("page-size")
                .copied()
                .unwrap_or(constants::DEFAULT_PAGE_SIZE),
        };

        let paginated = api::client::workspaces::list(&remote_repo, &page_opts).await?;
        if paginated.entries.is_empty() {
            println!("No workspaces found");
            return Ok(());
        }

        println!("id\tname\tcommit_id\tcommit_message");
        for workspace in paginated.entries {
            println!(
                "{}\t{}\t{}\t{}",
                workspace.id,
                workspace.name.unwrap_or("".to_string()),
                workspace.commit.id,
                workspace.commit.message
            );
        }
        println!(
            "\nPage {} of {} ({} total)",
            paginated.pagination.page_number,
            paginated.pagination.total_pages,
            paginated.pagination.total_entries,
        );
        Ok(())
    }
}
