// use std::collections::HashMap;
// use std::fs::File;
// use std::io::Read;
// use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
// use liboxen::model::{CommitEntry, LocalRepository};
// use liboxen::util::hasher;
// use liboxen::{repositories, util};
// use rocksdb::DBWithThreadMode;
// use rocksdb::MultiThreaded;

use crate::cmd::RunCmd;
pub const NAME: &str = "pack";
pub struct PackCmd;

#[async_trait]
impl RunCmd for PackCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Pack your bags, let's see how well we can compress data. TODO: This is not a real command, just an experiment.")
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
            // Chunk size
            .arg(
                Arg::new("chunk_size")
                    .long("chunk_size")
                    .short('c')
                    .help("Chunk size in KB")
                    .default_value("4")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let _paths: Vec<PathBuf> = args
            .get_many::<String>("files")
            .expect("Must supply files")
            .map(PathBuf::from)
            .collect();

        let _n = args
            .get_one::<String>("number")
            .expect("Must supply number")
            .parse::<usize>()
            .expect("number must be a valid integer.");

        let _chunk_size = args
            .get_one::<String>("chunk_size")
            .expect("Must supply chunk size")
            .parse::<usize>()
            .expect("Chunk size must be a valid integer.");

        if _paths.len() != 1 {
            return Err(OxenError::basic_str("Must supply exactly one file"));
        }
        /*

        let _path = &paths[0];

        // Traverse back in file history, split file into chunks.
        let _repo = LocalRepository::from_current_dir()?;
        let _commits = repositories::commits::list(&repo)?;

        // Take first n commits
        let commits = commits.into_iter().take(n);
        let mut entries: Vec<CommitEntry> = vec![];
        for commit in commits {
            let commit_entry_reader = CommitEntryReader::new(&repo, &commit)?;
            let Some(entry) = commit_entry_reader.get_entry(path)? else {
                continue;
            };
            entries.push(entry);
        }

        // Time the total time taken to read the files
        let start = std::time::Instant::now();

        // Chunk each file into 16kb chunks
        let mut chunks: HashMap<u128, Vec<u8>> = HashMap::new();
        let mut version_to_chunks: HashMap<String, Vec<(usize, u128)>> = HashMap::new();
        let chunk_size = chunk_size * 1024;
        let mut latest_size: u64 = 0;
        let mut total_size: u64 = 0;
        let mut compressed_size: u64 = 0;
        println!("Compressing {} versions of {:?}...", n, path);
        // enumerate with index
        for (i, entry) in entries.into_iter().enumerate() {
            let version_file = util::fs::version_path(&repo, &entry);

            // Open File
            let mut file = File::open(&version_file)?;

            // Read chunks
            let mut chunk_idx = 0;
            let mut buffer = vec![0; chunk_size]; // 16KB buffer
            while let Ok(bytes_read) = file.read(&mut buffer) {
                if bytes_read == 0 {
                    if i == 0 {
                        latest_size = total_size;
                    }
                    break; // End of file
                }
                // Shrink buffer to size of bytes read
                buffer.truncate(bytes_read);

                // Process the buffer here
                // println!("Read {} bytes from {:?}", bytes_read, version_file);
                let hash = hasher::hash_buffer_128bit(&buffer);

                let pair = (chunk_idx, hash);
                if !version_to_chunks.contains_key(&entry.hash) {
                    version_to_chunks.insert(entry.hash.clone(), vec![pair]);
                } else {
                    version_to_chunks
                        .entry(entry.hash.clone())
                        .or_default()
                        .push(pair);
                }

                // Add to chunks
                if let std::collections::hash_map::Entry::Vacant(e) = chunks.entry(hash) {
                    e.insert(buffer.clone());
                    compressed_size += bytes_read as u64;
                }
                total_size += bytes_read as u64;
                chunk_idx += 1;
            }
        }

        // Time the total time taken to read the files
        let end = std::time::Instant::now();
        println!("Time to compress: {:?}", end.duration_since(start));

        println!("Writing chunks to disk...");

        // TODO: Write all chunks to our u128 kv db
        // TODO: Write another db of idx -> hash so that we can reconstruct the file
        // TODO: Time the reconstruction of the file from chunks to Polars DF

        // Write all the chunks to the chunks db
        let output_dir = Path::new("chunks");
        let chunks_db = output_dir.join("db");
        // mkdir if not exists
        util::fs::create_dir_all(output_dir)?;

        let opts = liboxen::core::db::key_val::opts::default();
        let db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open(&opts, chunks_db)?;
        for (hash, chunk) in &chunks {
            // liboxen::core::db::u128_kv_db::put(&db, *hash, chunk)?;
            db.put(hash.to_be_bytes(), chunk)?;
        }

        // Write all the chunk ids to the chunks db
        let chunks_idx = output_dir.join("idx");
        for (file_hash, ids) in &version_to_chunks {
            let ids_db_path = chunks_idx.join(file_hash);
            let idx_db: DBWithThreadMode<MultiThreaded> =
                DBWithThreadMode::open(&opts, &ids_db_path)?;
            println!("Writing {} indices to {:?}", ids.len(), ids_db_path);
            for (idx, block_hash) in ids {
                let block_hash = block_hash.to_be_bytes().to_vec();
                let idx = idx.to_be_bytes().to_vec();
                idx_db.put(block_hash, idx)?;
            }
        }

        // Time the total time taken to read the files
        let end = std::time::Instant::now();
        println!("Total time taken: {:?}", end.duration_since(start));

        println!("Uncompressed size: {}", bytesize::ByteSize::b(total_size));
        println!(
            "Compressed size: {}",
            bytesize::ByteSize::b(compressed_size)
        );
        println!(
            "Latest version size: {}",
            bytesize::ByteSize::b(latest_size)
        );
        println!(
            "🎉 Total space saved: {}",
            bytesize::ByteSize::b(total_size - compressed_size)
        );
         */

        Ok(())
    }
}
