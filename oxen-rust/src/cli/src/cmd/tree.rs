use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::core::v_latest::index::CommitMerkleTree;
use liboxen::error::OxenError;
use liboxen::model::{Commit, LocalRepository, MerkleHash};
use liboxen::repositories;
use std::time::Instant;

use std::str::FromStr;

use crate::cmd::RunCmd;
pub const NAME: &str = "tree";
pub struct TreeCmd;

#[async_trait]
impl RunCmd for TreeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Print the merkle tree 🌲 of a commit.")
            .arg(
                Arg::new("commit")
                    .long("commit")
                    .short('c')
                    .help("The commit to print the tree of.")
                    .default_value("HEAD")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("node")
                    .long("node")
                    .short('n')
                    .help("The node to print the tree of.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("path")
                    .long("path")
                    .short('p')
                    .help("The path to print the tree of.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("depth")
                    .long("depth")
                    .short('d')
                    .help("How many levels deep to traverse the tree. -1 for all.")
                    .default_value("-1")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("legacy")
                    .long("legacy")
                    .help("To use the legacy lookup method")
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let depth = args
            .get_one::<String>("depth")
            .expect("Must supply depth")
            .parse::<i32>()
            .expect("depth must be a valid integer.");
        let commit_id = args
            .get_one::<String>("commit")
            .expect("Must supply commit");
        let repo = LocalRepository::from_current_dir()?;

        let commit = if commit_id == "HEAD" {
            repositories::commits::head_commit(&repo)?
        } else {
            let Some(commit) = repositories::commits::get_by_id(&repo, commit_id)? else {
                return Err(OxenError::basic_str(format!(
                    "Commit {} not found",
                    commit_id
                )));
            };
            commit
        };

        let path = args.get_one::<String>("path");
        if let Some(node) = args.get_one::<String>("node") {
            self.print_node(&repo, node, depth)?;
        } else {
            self.print_tree(&repo, &commit, path, depth)?;
        }

        Ok(())
    }
}

impl TreeCmd {
    fn print_node(&self, repo: &LocalRepository, node: &str, depth: i32) -> Result<(), OxenError> {
        let node_hash = MerkleHash::from_str(node)?;
        let tree = CommitMerkleTree::read_node(repo, &node_hash, true)?.unwrap();
        CommitMerkleTree::print_node_depth(&tree, depth);

        Ok(())
    }

    fn print_tree(
        &self,
        repo: &LocalRepository,
        commit: &Commit,
        path: Option<&String>,
        depth: i32,
    ) -> Result<(), OxenError> {
        let load_start = Instant::now(); // Start timing
        let tree = match (repo.subtree_paths(), repo.depth()) {
            (Some(subtrees), Some(depth)) => {
                println!("Working with subtrees: {:?}", subtrees);
                println!("Depth: {}", depth);
                println!("Loading first tree...");
                CommitMerkleTree::from_path_depth(repo, commit, subtrees.first().unwrap(), depth)?
            }
            (_, _) => {
                if let Some(path) = path {
                    CommitMerkleTree::from_path(repo, commit, path, true)?
                } else {
                    CommitMerkleTree::from_commit(repo, commit)?
                }
            }
        };
        let load_duration = load_start.elapsed(); // Calculate duration
        let print_start = Instant::now(); // Start timing
        tree.print_depth(depth);
        let print_duration = print_start.elapsed(); // Calculate duration
        println!("Time to load tree: {:?}", load_duration);
        println!("Time to print tree: {:?}", print_duration);
        Ok(())
    }
}
