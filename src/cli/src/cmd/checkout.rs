use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

use crate::cmd::RunCmd;
pub const NAME: &str = "checkout";
pub struct CheckoutCmd;

#[async_trait]
impl RunCmd for CheckoutCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Checks out a branches in the repository")
            .arg(Arg::new("name").help("Name of the branch or commit id to checkout"))
            .arg(
                Arg::new("create")
                    .long("create")
                    .short('b')
                    .help("Create the branch and check it out")
                    .exclusive(true)
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("ours")
                    .long("ours")
                    .help("Checkout the content of the base branch and take it as the working directories version. Will overwrite your working file.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("theirs")
                    .long("theirs")
                    .help("Checkout the content of the merge branch and take it as the working directories version. Will overwrite your working file.")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Find the repository
        let repo = LocalRepository::from_current_dir()?;

        // Parse Args
        if let Some(name) = args.get_one::<String>("create") {
            self.create_checkout_branch(&repo, name)?
        } else if args.get_flag("ours") {
            let Some(name) = args.get_one::<String>("name") else {
                return Err(OxenError::basic_str(
                    "Err: Usage `oxen checkout --ours <name>`",
                ));
            };

            self.checkout_ours(&repo, name)?
        } else if args.get_flag("theirs") {
            let Some(name) = args.get_one::<String>("name") else {
                return Err(OxenError::basic_str(
                    "Err: Usage `oxen checkout --theirs <name>`",
                ));
            };

            self.checkout_theirs(&repo, name)?
        } else if let Some(name) = args.get_one::<String>("name") {
            self.checkout(&repo, name).await?;
        }
        Ok(())
    }
}

impl CheckoutCmd {
    pub async fn checkout(&self, repo: &LocalRepository, name: &str) -> Result<(), OxenError> {
        repositories::checkout(repo, name).await?;
        Ok(())
    }

    pub fn checkout_theirs(&self, repo: &LocalRepository, path: &str) -> Result<(), OxenError> {
        repositories::checkout::checkout_theirs(repo, path)?;
        Ok(())
    }

    pub fn checkout_ours(&self, repo: &LocalRepository, path: &str) -> Result<(), OxenError> {
        repositories::checkout::checkout_ours(repo, path)?;
        Ok(())
    }

    pub fn create_checkout_branch(
        &self,
        repo: &LocalRepository,
        name: &str,
    ) -> Result<(), OxenError> {
        repositories::branches::create_checkout(repo, name)?;
        Ok(())
    }
}
