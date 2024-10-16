use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::core::db;
use liboxen::core::v0_10_0::index::{CommitDirEntryReader, CommitEntryReader, ObjectDBReader};
use liboxen::core::v0_19_0::index::CommitMerkleTree;
use liboxen::error::OxenError;
use liboxen::model::{Commit, LocalRepository, MerkleHash};
use liboxen::repositories;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
        if args.get_flag("legacy") {
            if let Some(_node) = args.get_one::<String>("node") {
                self.print_legacy(&repo, &commit, path, true)?;
            } else {
                self.print_legacy(&repo, &commit, path, false)?;
            }
        } else if let Some(node) = args.get_one::<String>("node") {
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
        let tree = if let Some(path) = path {
            CommitMerkleTree::from_path(repo, commit, path, true)?
        } else {
            CommitMerkleTree::from_commit(repo, commit)?
        };
        let load_duration = load_start.elapsed(); // Calculate duration

        // List directories in the .oxen/tree dir
        // This is to benchmark how fast we can open the individual nodes..
        /*type TreeNode = HashMap<u128, MerkleNode>;
        let mut data: Vec<TreeNode> = vec![];
        let path = Path::new(&repo.path).join(".oxen").join("tree");
        let mut total_open_duration = Duration::new(0, 0);
        let mut total_map_duration = Duration::new(0, 0);
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                println!("Opening tree at {:?}", path);
                let open_start = Instant::now(); // Start timing
                let mut tree_db = MerkleNodeDB::open(&path, true)?;
                let open_duration = open_start.elapsed(); // Calculate duration
                println!("Time to open tree: {:?}", open_duration);
                total_open_duration += open_duration;
                let map_start = Instant::now(); // Start timing
                let vals: TreeNode = tree_db.map()?;
                let map_duration = map_start.elapsed(); // Calculate duration
                println!("Time to map tree: {:?}", map_duration);
                total_map_duration += map_duration;
                data.push(vals);
                println!("Tree size: {:?}", tree_db.size());
                println!("--------------------");
            }
        }

        println!("Avg open time: {:?}", total_open_duration.as_millis() as f32 / data.len() as f32);
        println!("Avg map time: {:?}", total_map_duration.as_millis() as f32 / data.len() as f32);
        */

        let print_start = Instant::now(); // Start timing
        tree.print_depth(depth);
        let print_duration = print_start.elapsed(); // Calculate duration
        println!("Time to load tree: {:?}", load_duration);
        println!("Time to print tree: {:?}", print_duration);
        Ok(())
    }

    fn print_legacy(
        &self,
        repo: &LocalRepository,
        commit: &Commit,
        path: Option<&String>,
        single_entry: bool,
    ) -> Result<(), OxenError> {
        let path = path.unwrap_or(&commit.id);
        // Read a full dir
        if single_entry {
            // Just get a single entry
            let path = Path::new(path);
            let filename = path.file_name().unwrap().to_str().unwrap();
            let parent = path.parent().unwrap();
            let object_reader = ObjectDBReader::new(repo, &commit.id)?;
            let entry_reader = liboxen::core::v0_10_0::index::CommitDirEntryReader::new(
                repo,
                &commit.id,
                parent,
                object_reader.clone(),
            )?;
            println!("looking up entry {}", filename);
            let entry = entry_reader.get_entry(filename)?;
            println!("Got entry {:?}", entry);
        } else {
            // Get a paginated list of entries
            /*
            let page = 1;
            let page_size = 100;
            let (paginated_entries, _dir) = repositories::entries::list_directory(
                &repo,
                &commit,
                &Path::new(path),
                &commit.id,
                page,
                page_size,
            )?;
            println!("Got {:?} entries", paginated_entries.entries.len());
            */

            let start_load = Instant::now();
            let start_load_obj = Instant::now();
            let object_reader = ObjectDBReader::new(repo, &commit.id)?;
            let load_obj_duration = start_load_obj.elapsed();
            println!("Time to load object reader: {:?}", load_obj_duration);

            let db_path = CommitDirEntryReader::dir_hash_db(&repo.path, &commit.id);
            let opts = db::key_val::opts::default();
            let dir_hashes_db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open_for_read_only(&opts, db_path, false)?;

            let start_load_entry_reader = Instant::now();
            let reader = CommitEntryReader::new(repo, commit)?;
            let mut dirs = reader.list_dirs()?;
            dirs.sort();
            let load_entry_reader_duration = start_load_entry_reader.elapsed();
            println!(
                "Time to load entry reader: {:?}",
                load_entry_reader_duration
            );
            println!("Got {:?} dirs", dirs.len());

            // Create a nested structure to represent the tree
            let mut tree: Vec<(usize, PathBuf)> = Vec::new();

            for dir in dirs {
                let depth = dir.components().count();
                tree.push((depth + 1, dir));
            }

            // Iterate and print the tree structure
            let mut files_per_dir: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();
            for (_, path) in &tree {
                files_per_dir.insert(path.clone(), Vec::new());
                let entry_reader = CommitDirEntryReader::new_from_hash_db(
                    &repo.path,
                    &commit.id,
                    &dir_hashes_db,
                    path,
                    object_reader.clone(),
                )?;
                let entries = entry_reader.list_entries()?;
                for entry in entries {
                    files_per_dir
                        .entry(path.clone())
                        .or_default()
                        .push(entry.path);
                }
            }
            let load_duration = start_load.elapsed();

            let start_print = Instant::now();
            for (depth, dir) in tree {
                let indent = "  ".repeat(depth - 1);
                println!("{}├─ {:?}", indent, dir);
                let files = files_per_dir.get(&dir).unwrap();
                for file in files {
                    let indent = "  ".repeat(depth);
                    println!("{}├─ {:?}", indent, file);
                }
            }
            let print_duration = start_print.elapsed();
            println!("Time to load: {:?}", load_duration);
            println!("Time to print: {:?}", print_duration);
        }

        Ok(())
    }
}
