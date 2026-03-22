use async_trait::async_trait;
use clap::{Arg, Command};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

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
            repositories::remote_mode::create_checkout(&mut repo, name).await?
        } else if let Some(name) = args.get_one::<String>("name") {
            repositories::remote_mode::checkout(&mut repo, name).await?;
        }

        Ok(())
    }
}
