use std::path::Path;

use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use colored::Colorize;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::{api, repositories};
use uuid::Uuid;

use crate::cmd::RunCmd;
pub const NAME: &str = "create";
pub struct WorkspaceCreateCmd;

#[async_trait]
impl RunCmd for WorkspaceCreateCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Creates a new workspace")
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .help("The branch to create the workspace from"),
            )
            .arg(
                Arg::new("name")
                    .long("name")
                    .short('n')
                    .help("The name of the workspace"),
            )
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), OxenError> {
        let repo = LocalRepository::from_current_dir()?;
        let Ok(Some(branch)) = repositories::branches::current_branch(&repo) else {
            return Err(OxenError::basic_str(
                "Cannot create workspace without a branch",
            ));
        };

        let branch_name = match args.get_one::<String>("branch") {
            Some(branch_name) => branch_name,
            None => &branch.name,
        };

        let name = args.get_one::<String>("name").map(|s| s.to_string());

        let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
        // Generate a random workspace id
        let workspace_id = Uuid::new_v4().to_string();
        let workspace = api::client::workspaces::create_with_path(
            &remote_repo,
            &branch_name,
            &workspace_id,
            Path::new("/"),
            name,
        )
        .await?;
        match workspace.status.as_str() {
            "resource_created" => {
                println!("{}", "Workspace created successfully!".green().bold());
            }
            "resource_found" => {
                println!("{}", "Workspace already exists".yellow().bold());
            }
            other => {
                println!(
                    "{}",
                    format!("Unexpected workspace status: {other}").red()
                );
            }
        }
        println!("{} {}", "Workspace ID:".green().bold(), workspace.id.bold());

        Ok(())
    }
}
