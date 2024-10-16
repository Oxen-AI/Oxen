use async_trait::async_trait;
use clap::{Arg, Command};
use colored::Colorize;

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::{check_remote_version, check_remote_version_blocking, get_host_from_repo};

pub mod unlock;

pub const NAME: &str = "branch";

pub struct BranchCmd;

#[async_trait]
impl RunCmd for BranchCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the init command
        Command::new(NAME)
            .about("Manage branches in repository")
            .subcommand(unlock::BranchUnlockCmd.args())
            .arg(Arg::new("name").help("Name of the branch").exclusive(true))
            .arg(
                Arg::new("all")
                    .long("all")
                    .short('a')
                    .help("List both local and remote branches")
                    .exclusive(true)
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .short('r')
                    .help("List all the remote branches")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("force-delete")
                    .long("force-delete")
                    .short('D')
                    .help("Force remove the local branch")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("delete")
                    .long("delete")
                    .short('d')
                    .help("Remove the local branch if it is safe to")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("move")
                    .long("move")
                    .short('m')
                    .help("Rename the current local branch.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("show-current")
                    .long("show-current")
                    .help("Print the current branch")
                    .exclusive(true)
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Find the repository
        let repo = LocalRepository::from_current_dir()?;

        // Parse Args
        if let Some(subcommand) = args.subcommand() {
            match subcommand {
                (unlock::NAME, args) => unlock::BranchUnlockCmd.run(args).await,
                (cmd, _) => Err(OxenError::basic_str(format!("Unknown subcommand {cmd}"))),
            }
        } else if args.get_flag("all") {
            self.list_all_branches(&repo).await
        } else if let Some(remote_name) = args.get_one::<String>("remote") {
            if let Some(branch_name) = args.get_one::<String>("delete") {
                self.delete_remote_branch(&repo, remote_name, branch_name)
                    .await
            } else {
                self.list_remote_branches(&repo, remote_name).await
            }
        } else if let Some(name) = args.get_one::<String>("name") {
            self.create_branch(&repo, name)
        } else if let Some(name) = args.get_one::<String>("delete") {
            self.delete_branch(&repo, name)
        } else if let Some(name) = args.get_one::<String>("force-delete") {
            self.force_delete_branch(&repo, name)
        } else if let Some(name) = args.get_one::<String>("move") {
            self.rename_current_branch(&repo, name)
        } else if args.get_flag("show-current") {
            self.show_current_branch(&repo)
        } else {
            self.list_branches(&repo)
        }
    }
}

impl BranchCmd {
    pub async fn list_all_branches(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        self.list_branches(repo)?;

        for remote in repo.remotes().iter() {
            self.list_remote_branches(repo, &remote.name).await?;
        }

        Ok(())
    }

    pub fn list_branches(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        let branches = repositories::branches::list(repo)?;
        let current_branch = repositories::branches::current_branch(repo)?;

        for branch in branches.iter() {
            if current_branch.is_some() && current_branch.as_ref().unwrap().name == branch.name {
                let branch_str = format!("* {}", branch.name).green();
                println!("{branch_str}")
            } else {
                println!("  {}", branch.name)
            }
        }

        Ok(())
    }

    pub fn show_current_branch(&self, repo: &LocalRepository) -> Result<(), OxenError> {
        if let Some(current_branch) = repositories::branches::current_branch(repo)? {
            println!("{}", current_branch.name);
        }
        Ok(())
    }

    pub fn create_branch(&self, repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
        repositories::branches::create_from_head(repo, name)?;
        Ok(())
    }

    pub fn delete_branch(&self, repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
        repositories::branches::delete(repo, name)?;
        Ok(())
    }

    pub fn force_delete_branch(&self, repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
        repositories::branches::force_delete(repo, name)?;
        Ok(())
    }

    pub fn rename_current_branch(
        &self,
        repo: &LocalRepository,
        name: &str,
    ) -> Result<(), OxenError> {
        repositories::branches::rename_current_branch(repo, name)?;
        Ok(())
    }

    pub async fn list_remote_branches(
        &self,
        repo: &LocalRepository,
        remote_name: &str,
    ) -> Result<(), OxenError> {
        let host = get_host_from_repo(repo)?;
        check_remote_version_blocking(host.clone()).await?;
        check_remote_version(host).await?;

        let remote = repo
            .get_remote(remote_name)
            .ok_or(OxenError::remote_not_set(remote_name))?;
        let remote_repo = api::client::repositories::get_by_remote(&remote)
            .await?
            .ok_or(OxenError::remote_not_found(remote.clone()))?;

        let branches = api::client::branches::list(&remote_repo).await?;
        for branch in branches.iter() {
            println!("{}\t{}", &remote.name, branch.name);
        }
        Ok(())
    }

    pub async fn delete_remote_branch(
        &self,
        repo: &LocalRepository,
        remote_name: &str,
        branch_name: &str,
    ) -> Result<(), OxenError> {
        let host = get_host_from_repo(repo)?;
        check_remote_version(host).await?;

        api::client::branches::delete_remote(repo, remote_name, branch_name).await?;
        Ok(())
    }
}
