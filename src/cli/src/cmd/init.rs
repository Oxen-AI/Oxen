use std::path::PathBuf;

use async_trait::async_trait;
use clap::{arg, Arg, Command};
use liboxen::core::versions::MinOxenVersion;
use liboxen::error::OxenError;

use crate::cmd::RunCmd;
use crate::helpers::{check_remote_version, get_host_or_default};
use liboxen::repositories;

pub const INIT: &str = "init";

pub struct InitCmd;

#[async_trait]
impl RunCmd for InitCmd {
    fn name(&self) -> &str {
        INIT
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(INIT)
            .about("Initializes a local repository")
            .arg(arg!([PATH] "The directory to establish the repo in. Defaults to the current directory."))
            .arg(
                Arg::new("oxen-version")
                    .short('v')
                    .long("oxen-version")
                    .help("The oxen version to use, if you want to test older CLI versions (default: latest)")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let default = String::from(".");
        let path = args.get_one::<String>("PATH").unwrap_or(&default);

        let version_str = args
            .get_one::<String>("oxen-version")
            .map(|s| s.to_string());
        let oxen_version = MinOxenVersion::or_latest(version_str)?;

        // Make sure the remote version is compatible
        let host = get_host_or_default()?;
        check_remote_version(host).await?;

        // Initialize the repository
        let directory = dunce::canonicalize(PathBuf::from(&path))?;
        repositories::init::init_with_version(&directory, oxen_version)?;
        println!("🐂 repository initialized at: {directory:?}");
        Ok(())
    }
}
