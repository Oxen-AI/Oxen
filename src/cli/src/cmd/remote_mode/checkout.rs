use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::{api, repositories};

use colored::Colorize;
use std::path::Path;
use uuid::Uuid;

use crate::cmd::RunCmd;

pub const NAME: &str = "checkout";
pub struct RemoteModeCheckoutCmd;

#[async_trait]
impl RunCmd for RemoteModeCheckoutCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        Command::new(NAME)
            .about("Checks out a branch in the repository")
            .arg(Arg::new("name").help("Name of the branch or commit id to checkout"))
            .arg(
                Arg::new("create")
                    .long("create")
                    .short('b')
                    .value_name("BRANCH_NAME")
                    .num_args(1),
            )
            .group(
                clap::ArgGroup::new("checkout_args")
                    .args(["name", "create"])
                    .required(true),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        let mut repo = LocalRepository::from_current_dir()?;

        // Parse Args
        if let Some(name) = args.get_one::<String>("create") {
            self.create_checkout_branch(&mut repo, name).await?
        } else if let Some(name) = args.get_one::<String>("name") {
            self.checkout_remote_mode(&mut repo, name).await?;
        }

        Ok(())
    }
}

impl RemoteModeCheckoutCmd {
    pub async fn checkout_remote_mode(
        &self,
        repo: &mut LocalRepository,
        name: &str,
    ) -> Result<(), OxenError> {
        match repositories::checkout(repo, name).await {
            Ok(Some(branch)) => {
                println!("Checked out branch: {}", branch.name);
            }
            Ok(None) => {
                println!("Checked out commit: {}", name);
            }
            Err(OxenError::RevisionNotFound(name)) => {
                println!("Revision not found: {}\n\nIf the branch exists on the remote, run\n\n  oxen fetch -b {}\n\nto update the local copy, then try again.", name, name);
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn create_checkout_branch(
        &self,
        repo: &mut LocalRepository,
        branch_name: &str,
    ) -> Result<(), OxenError> {
        repositories::branches::create_checkout(repo, branch_name)?;

        // Generate a random workspace id
        let workspace_id = Uuid::new_v4().to_string();

        // Use the branch name as the workspace name
        let workspace_name = format!("{}: {workspace_id}", branch_name);
        let Some(remote) = repo.remote() else {
            return Err(OxenError::basic_str(
                "Error: local repository has no remote",
            ));
        };
        let remote_repo = api::client::repositories::get_by_remote(&remote)
            .await?
            .ok_or_else(|| OxenError::remote_repo_not_found(branch_name))?;

        // Create the remote branch from the commit
        let head_commit = repositories::commits::head_commit(repo)?;
        api::client::branches::create_from_commit(&remote_repo, &branch_name, &head_commit).await?;

        let workspace = api::client::workspaces::create_with_path(
            &remote_repo,
            &branch_name,
            &workspace_id,
            Path::new("/"),
            Some(workspace_name.clone()),
        )
        .await?;

        match workspace.status.as_str() {
            "resource_created" => {
                println!(
                    "{}",
                    "Remote-mode repository initialized successfully!"
                        .green()
                        .bold()
                );
            }
            "resource_found" => {
                let err_msg = format!(
                    "Remote-mode repo for workspace {} already exists",
                    workspace_id.clone()
                );
                println!("{}", err_msg.yellow().bold());
                return Err(OxenError::basic_str(format!(
                    "Error: Remote-mode repo already exists for workspace {}",
                    workspace_id
                )));
            }
            other => {
                println!(
                    "{}",
                    format!("Unexpected workspace status: {}", other).red()
                );
            }
        }
        println!("{} {}", "Workspace ID:".green().bold(), workspace.id.bold());

        // Add the new branch name to workspaces
        repo.add_workspace(&workspace_name);
        repo.set_workspace(&workspace_name)?;
        repo.save()?;

        Ok(())
    }
}
