use std::path::PathBuf;
use std::str::FromStr;

use async_trait::async_trait;
use clap::{Arg, Command, arg};
use liboxen::config::repository_config::{MerkleStoreKind, RepoConfigError};
use liboxen::core::versions::MinOxenVersion;
use liboxen::error::OxenError;
use strum::VariantNames;

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
    #[inline(always)]
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
            .arg(
                Arg::new("merkle-store")
                    .long("merkle-store")
                    .help(
                        "Backend for the repository's Merkle tree node storage. \
                         'file' is the original on-disk format (default, backwards-compatible). \
                         'lmdb' uses an LMDB database for the tree store — not supported on \
                         virtual file systems."
                    )
                    .default_value(<&'static str>::from(MerkleStoreKind::default()))
                    .default_missing_value(<&'static str>::from(MerkleStoreKind::default()))
                    .value_parser(<MerkleStoreKind as VariantNames>::VARIANTS.to_vec())
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        //
        // parse args
        //
        let path = args
            .get_one::<String>("PATH")
            .map(|x| x.as_str())
            .unwrap_or(".");
        log::info!("Repository path: {}", path);

        let version_str = args
            .get_one::<String>("oxen-version")
            .map(|s| s.to_string());
        let oxen_version = MinOxenVersion::or_latest(version_str)?;
        log::info!("Oxen version: {}", oxen_version);

        // Should not fail because MerkleStoreKind::VARIANTS is from a derive macro, which picks up
        // all actually defined variants. And the `from_str` impl. is also generated from derive macros
        // that look at enum structure.
        let merkle_store_kind = match args.get_one::<String>("merkle-store") {
            Some(token) => {
                MerkleStoreKind::from_str(token).map_err(RepoConfigError::UnknownMerkeKind)?
            }
            None => MerkleStoreKind::default(),
        };
        log::debug!("🌲 Merkle store kind: {}", merkle_store_kind);

        //
        // validate args
        //
        // Make sure the remote version is compatible
        let (scheme, host) = get_scheme_and_host_or_default()?;

        check_remote_version(scheme, host).await?;

        //
        // Initialize the repository
        //
        let directory = util::fs::canonicalize(PathBuf::from(&path))?;
        repositories::init::init_with_version_storage_and_merkle_store(
            &directory,
            oxen_version,
            None,
            merkle_store_kind,
        )
        .await?;
        println!("🐂 repository initialized at: {directory:?}");
        println!("{AFTER_INIT_MSG}");
        Ok(())
    }
}
