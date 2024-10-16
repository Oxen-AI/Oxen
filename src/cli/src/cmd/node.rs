use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::core::v0_19_0::index::CommitMerkleTree;
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, MerkleHash};

use std::str::FromStr;

use crate::cmd::RunCmd;
pub const NAME: &str = "node";
pub struct NodeCmd;

#[async_trait]
impl RunCmd for NodeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Inspect an oxen merkle tree node")
            .arg(Arg::new("node").required(true).action(clap::ArgAction::Set))
            // add --verbose flag
            .arg(
                Arg::new("verbose")
                    .long("verbose")
                    .short('v')
                    .help("Verbose output")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let node_hash = args.get_one::<String>("node").expect("Must supply node");

        let repository = LocalRepository::from_current_dir()?;
        let node_hash = MerkleHash::from_str(node_hash)?;
        let node = CommitMerkleTree::read_node(&repository, &node_hash, false)?;

        println!("{:?}", node);
        if args.get_flag("verbose") {
            if let Some(node) = node {
                println!("{} children", node.children.len());
                for child in node.children {
                    println!("{:?}", child);
                }
            }
        }

        Ok(())
    }
}
