use std::path::PathBuf;
use std::str::FromStr;

use async_trait::async_trait;
use clap::{Arg, Command, arg};
use liboxen::core::db::merkle_node::MerkleNodeBackend;

use crate::cmd::RunCmd;
use crate::helpers::{check_remote_version, get_scheme_and_host_or_default};
use crate::util;
use liboxen::repositories;

pub const INIT: &str = "init";

const AFTER_INIT_MSG: &str = "
    📖 If this is your first time using Oxen, check out the CLI docs at:
            https://docs.oxen.ai/getting-started/cli

    💬 For more support, or to chat with the Oxen team, join our Discord:
            https://discord.gg/s3tBEn7Ptg
";

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
                Arg::new("merkle-backend")
                    .long("merkle-backend")
                    .help("Which engine backs the repo's Merkle node store (default: filesystem)")
                    .value_parser(["filesystem", "lmdb"])
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error> {
        //
        // parse args
        //
        let path = args
            .get_one::<String>("PATH")
            .map(|x| x.as_str())
            .unwrap_or(".");
        log::info!("Repository path: {}", path);

        let merkle_backend = args
            .get_one::<String>("merkle-backend")
            .map(|s| MerkleNodeBackend::from_str(s))
            .transpose()?;

        // Make sure the remote version is compatible
        let (scheme, host) = get_scheme_and_host_or_default()?;

        check_remote_version(scheme, host).await?;

        // Initialize the repository
        let directory = util::fs::canonicalize(PathBuf::from(&path))?;
        repositories::init::init_with_version_and_storage_config(&directory, None, merkle_backend)
            .await?;
        println!("🐂 repository initialized at: {directory:?}");
        println!("{AFTER_INIT_MSG}");
        Ok(())
    }
}
