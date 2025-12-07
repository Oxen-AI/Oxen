use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;

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
            // add --verbose flag
            .arg(
                Arg::new("verbose")
                    .long("verbose")
                    .short('v')
                    .help("Verbose output")
                    .action(clap::ArgAction::SetTrue),
            )
            // add --node flag
            .arg(
                Arg::new("node")
                    .long("node")
                    .short('n')
                    .conflicts_with("file")
                    .required_unless_present("file")
                    .help("Node hash to inspect"),
            )
            // add --file flag
            .arg(
                Arg::new("file")
                    .long("file")
                    .short('f')
                    .conflicts_with("node")
                    .required_unless_present("node")
                    .help("File path to inspect"),
            )
            // add --revision flag
            .arg(
                Arg::new("revision")
                    .long("revision")
                    .short('r')
                    .help("Which revision to start at"),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Find the repository
        let repository = LocalRepository::from_current_dir()?;

        // if the --file flag is set, we need to get the node for the file
        if let Some(file) = args.get_one::<String>("file") {
            let commit = if let Some(revision) = args.get_one::<String>("revision") {
                repositories::revisions::get(&repository, revision)?.unwrap()
            } else {
                repositories::commits::head_commit(&repository)?
            };
            let Some(node) = repositories::entries::get_file(&repository, &commit, file)? else {
                return Err(OxenError::basic_str(format!(
                    "Error: file {:?} not found in commit {:?}",
                    file, commit.id
                )));
            };

            println!("{node:?}");
            return Ok(());

        // otherwise, get the node based on the node hash
        } else if let Some(node_hash) = args.get_one::<String>("node") {
            let node_hash = node_hash.parse()?;
            let Some(node) = repositories::tree::get_node_by_id(&repository, &node_hash)? else {
                return Err(OxenError::basic_str(format!(
                    "Error: node {node_hash:?} not found in repo"
                )));
            };

            println!("{node:?}");
            if args.get_flag("verbose") {
                println!("{} children", node.children.len());
                for child in node.children {
                    println!("{child:?}");
                }
            }
        } else {
            return Err(OxenError::basic_str("Must supply file path or node hash"));
        }

        Ok(())
    }
}
