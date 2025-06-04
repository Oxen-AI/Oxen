use std::{
    fs::{self, File},
    io::{self, Read, Write, BufReader, BufWriter, Seek, SeekFrom},
    path::{Path, PathBuf}
};

use serde::{Deserialize, Serialize};
use std::error::Error;
use crate::chunker::{Chunker, map_bincode_error};
use crate::xhash;
use rayon::prelude::*;

const METADATA_FILE_NAME: &str = "metadata.bin";

#[derive(Serialize, Deserialize, Debug)]
struct ChunkMetadata {
    original_file_name: String,
    original_file_size: u64,
    chunk_size: usize,
    chunks: Vec<String>,
}

/*
Multi Threaded Chunker operates on fixed size chunks
*/

pub struct FixedSizeMultiChunker {
    chunk_size: usize,
    concurrency: usize,
}

impl FixedSizeMultiChunker {
    pub fn new(chunk_size: usize, concurrency: usize) -> Result<Self, io::Error> {
        if chunk_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Chunk size cannot be zero",
            ));
        }
        if concurrency == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Concurrency must be greater than zero",
            ));
        }
        Ok(Self {
            chunk_size,
            concurrency,
        })
    }

    fn split_and_hash_chunks_rayon(
        &self,
        input_path: &Path,
        output_dir: &Path,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        let input_file_metadata = fs::metadata(input_path)?;
        let total_size = input_file_metadata.len();

        if total_size == 0 {
            println!("Input file is empty. No chunks created.");
            return Ok(Vec::new());
        }

        fs::create_dir_all(output_dir)?;

        let chunk_size_u64 = self.chunk_size as u64;
        let num_chunks = (total_size + chunk_size_u64 - 1) / chunk_size_u64;

        println!(
            "Splitting '{}' ({} bytes) into {} chunks of approx. {} bytes each using Rayon...",
            input_path.display(),
            total_size,
            num_chunks,
            self.chunk_size
        );


        let tasks: Vec<(usize, u64, usize)> = (0..num_chunks)
            .map(|i| {
                let start = i * chunk_size_u64;
                let actual_chunk_size = std::cmp::min(chunk_size_u64, total_size - start) as usize;
                (i as usize, start, actual_chunk_size)
            })
            .collect();

        let results: Vec<Result<(usize, String), io::Error>> = tasks
            .into_par_iter()
            .map(|(chunk_index, start_offset, chunk_data_size)| -> Result<(usize, String), io::Error> {
                if chunk_data_size == 0 { 
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Chunk size is zero"));
                }

                let mut infile = File::open(input_path)?;
                infile.seek(SeekFrom::Start(start_offset))?;

                let mut buffer = vec![0; chunk_data_size];
                infile.read_exact(&mut buffer)?;

                let hash_str = xhash::hash_buffer_128bit(&buffer).to_string();
                let output_filepath = output_dir.join(&hash_str);

                if !output_filepath.exists() {
                    let mut outfile = File::create(&output_filepath)?;
                    outfile.write_all(&buffer)?;
                } else {
                    // println!(
                    //     "  Chunk {} (HASH: {}) already exists. Skipping write. {}",
                    //     chunk_index, hash_str, output_filepath.display()
                    // );
                }
                Ok((chunk_index, hash_str))
            })
            .collect();

        let mut indexed_hashes: Vec<(usize, String)> = Vec::with_capacity(num_chunks as usize);
        for result in results {
            match result {
                Ok(pair) => indexed_hashes.push(pair),
                Err(e) => return Err(Box::new(e)), 
            }
        }

        indexed_hashes.sort_by_key(|k| k.0);

        let chunk_filenames: Vec<String> = indexed_hashes.into_iter().map(|(_, hash)| hash).collect();

        println!("Rayon-based file splitting and hashing complete.");
        Ok(chunk_filenames)
    }
}


impl Chunker for FixedSizeMultiChunker {
    fn name(&self) -> &'static str {
        "fixed-size-64k-multithreaded"
    }

    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        fs::create_dir_all(output_dir)?;

        let original_file_metadata = fs::metadata(input_file)
            .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("Failed to read input file metadata '{}': {}", input_file.display(), e)))?;
        
        let original_file_size = original_file_metadata.len();
        let original_file_name = input_file
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("unknown_file"))
            .to_string_lossy()
            .into_owned();

        let chunk_filenames = self.split_and_hash_chunks_rayon(input_file, output_dir)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Chunking process failed: {}", e)))?;

        let metadata = ChunkMetadata {
            original_file_name,
            original_file_size,
            chunk_size: self.chunk_size,
            chunks: chunk_filenames,
        };

        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(
            File::create(&metadata_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Failed to create metadata file '{}': {}", metadata_path.display(), e)))?
        );

        bincode::serialize_into(metadata_file, &metadata)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Metadata serialization error: {}", e)))?;

        Ok(output_dir.to_path_buf())
    }

    fn unpack(&self, chunk_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error> {
        let metadata_path = chunk_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufReader::new(File::open(&metadata_path)?);

        let metadata: ChunkMetadata = bincode::deserialize_from(metadata_file)
            .map_err(map_bincode_error)?;

        let mut output_file = BufWriter::new(File::create(output_path)?);

        for chunk_filename in &metadata.chunks {
            let chunk_path = chunk_dir.join(chunk_filename);

            if !chunk_path.exists() {
                 return Err(io::Error::new(
                     io::ErrorKind::NotFound,
                     format!("Chunk file not found during unpack: {}", chunk_path.display())
                 ));
            }
            let mut chunk_file = BufReader::new(File::open(&chunk_path)?);
            io::copy(&mut chunk_file, &mut output_file)?;
        }

        output_file.flush()?;

        Ok(output_path.to_path_buf())
    }

    fn get_chunk_hashes(&self, input_dir: &Path) -> Result<Vec<String>, io::Error>{

        let metadata_path = input_dir.join(METADATA_FILE_NAME);
        let metadata_file = fs::File::open(&metadata_path)?;
        let metadata: ChunkMetadata = bincode::deserialize_from(metadata_file)
            .map_err(map_bincode_error)?;

        Ok(metadata.chunks)
    }
}
