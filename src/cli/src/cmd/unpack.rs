use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::core::v0_10_0::index::CommitEntryReader;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use rocksdb::DBWithThreadMode;
use rocksdb::IteratorMode;
use rocksdb::MultiThreaded;

use crate::cmd::RunCmd;
pub const NAME: &str = "unpack";
pub struct UnpackCmd;

#[async_trait]
impl RunCmd for UnpackCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Unpack your bags! Let's see how well we can decompress data. TODO: This is not a real command, just an experiment.")
            .arg(
                Arg::new("files")
                    .required(true)
                    .action(clap::ArgAction::Append),
            )
            // Number of commits back to pack
            .arg(
                Arg::new("number")
                    .long("number")
                    .short('n')
                    .help("How many commits back to pack")
                    .default_value("1")
                    .action(clap::ArgAction::Set),
            )
            // Output file
            .arg(
                Arg::new("output")
                    .long("output")
                    .short('o')
                    .help("Output file")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let n = args
            .get_one::<String>("number")
            .expect("Must supply number")
            .parse::<usize>()
            .expect("number must be a valid integer.");

        let reconstructed_path = args
            .get_one::<String>("output")
            .expect("Must supply output path");

        if paths.len() != 1 {
            return Err(OxenError::basic_str("Must supply exactly one file"));
        }

        let path = &paths[0];

        // The idea here is that if we split the file into chunks and hash the chunks
        // Then we can store these at the bottom of the merkle tree
        // The questions are:
        //   1) How much storage space to we save?
        //   2) How much time does it take to reconstruct the original file?
        //   3) What does the performance look like loading this into duckdb to query?
        //   4) Can we just upload the changed chunk in this case rather than whole new version?

        // Traverse back in file history, split file into chunks.
        let repo = LocalRepository::from_current_dir()?;
        let commits = repositories::commits::list(&repo)?;

        // Take the nth commit as the file to reconstruct
        let commit = &commits[n - 1];

        // Get the entry to reconstruct
        let commit_entry_reader = CommitEntryReader::new(&repo, commit)?;
        let Some(entry) = commit_entry_reader.get_entry(path)? else {
            return Err(OxenError::basic_str("File not found in commit"));
        };
        let file_hash = entry.hash;
        println!("Reconstructing file hash: {:?}", file_hash);

        // Time the total time taken to read the files
        let start = std::time::Instant::now();
        println!("Reading indices from disk...");

        let opts = liboxen::core::db::key_val::opts::default();
        let output_dir = Path::new("chunks");
        let chunks_idx = output_dir.join("idx");
        let ids_db_path = chunks_idx.join(file_hash);

        let idx_db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open(&opts, ids_db_path)?;
        let iter = idx_db.iterator(IteratorMode::Start);

        let mut indices: Vec<(usize, u128)> = vec![];
        for val in iter {
            match val {
                Ok((k, v)) => {
                    let k = u128::from_be_bytes((*k).try_into().unwrap());
                    let v = usize::from_be_bytes((*v).try_into().unwrap());
                    indices.push((v, k));
                }
                Err(_) => return Err(OxenError::basic_str("Error iterating over indices")),
            }
        }

        println!("Got {} indices", indices.len());

        // sort indices by first value
        indices.sort_by_key(|k| k.0);

        // Read all the chunks from the chunks db
        let chunks_db = output_dir.join("db");

        let db: DBWithThreadMode<MultiThreaded> =
            DBWithThreadMode::open_for_read_only(&opts, chunks_db, true)?;

        let mut file = File::create(reconstructed_path)?;
        for (_, hash) in &indices {
            let hash = hash.to_be_bytes().to_vec();
            let chunk = db.get(hash)?.unwrap();
            file.write_all(&chunk)?;
        }

        // Time the total time taken to read the files
        let end = std::time::Instant::now();
        println!("Total time taken: {:?}", end.duration_since(start));

        Ok(())
    }
}
