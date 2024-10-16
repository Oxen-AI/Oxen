use async_trait::async_trait;
use clap::{arg, Arg, Command};

use liboxen::api;
use liboxen::constants::DEFAULT_BRANCH_NAME;
use liboxen::error::OxenError;
use liboxen::opts::CloneOpts;
use liboxen::repositories;

use crate::cmd::RunCmd;
use crate::helpers::{check_remote_version, check_remote_version_blocking};

pub const NAME: &str = "clone";
pub struct CloneCmd;

#[async_trait]
impl RunCmd for CloneCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Clone a repository by its URL")
            .arg_required_else_help(true)
            .arg(arg!(<URL> "URL of the repository you want to clone"))
            .arg(
                Arg::new("shallow")
                    .long("shallow")
                    .help("A shallow clone doesn't actually clone the data files. Useful if you want to soley interact with the remote.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("all")
                    .long("all")
                    .help("This downloads the full commit history, all the data files, and all the commit databases. Useful if you want to have the entire history locally or push to a new remote.")
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("branch")
                    .long("branch")
                    .short('b')
                    .help("The branch you want to pull to when you clone.")
                    .default_value(DEFAULT_BRANCH_NAME)
                    .default_missing_value(DEFAULT_BRANCH_NAME)
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let url = args.get_one::<String>("URL").expect("required");
        let shallow = args.get_flag("shallow");
        let all = args.get_flag("all");
        let branch = args
            .get_one::<String>("branch")
            .expect("Must supply a branch");

        let dst = std::env::current_dir().expect("Could not get current working directory");
        // Get the name of the repo from the url
        let name = url.split('/').last().unwrap();
        let dst = dst.join(name);

        let opts = CloneOpts {
            url: url.to_string(),
            dst,
            shallow,
            all,
            branch: branch.to_string(),
        };

        let host = api::client::get_host_from_url(&opts.url)?;
        check_remote_version_blocking(host.clone()).await?;
        check_remote_version(host).await?;

        repositories::clone(&opts).await?;
        Ok(())
    }
}
