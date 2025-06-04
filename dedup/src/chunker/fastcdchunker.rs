use std::{fs, io::{self, BufWriter, Write}, path::{Path, PathBuf}};
use fastcdc::v2020;
use serde::{Deserialize, Serialize};
use crate::{chunker::Chunker, xhash};

#[derive(Serialize, Deserialize)]
struct ChunkMetadata {
    original_file_name: String,
    original_file_size: u64,
    chunks: Vec<String>,
}

const METADATA_FILE_NAME: &str = "metadata.bin";

fn map_bincode_error(e: bincode::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("Bincode error: {}", e))
}

/*
FastCDC Algorithm for first reading the file and creating chunk locations.
and then writing the chunks to disk.

Multithreaded implementation is yet to be implemented.
*/

pub struct FastCDChunker {
    _chunk_size: usize,
    _concurrency: usize,
    min_chunk_size: u32,
    avg_chunk_size: u32,
    max_chunk_size: u32,
}

impl FastCDChunker {
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

        let min_chunk_size = 4096;
        let avg_chunk_size = chunk_size as u32;
        let max_chunk_size = chunk_size as u32 * 2;


        Ok(Self {
            _chunk_size: chunk_size,
            _concurrency: concurrency,
            min_chunk_size,
            avg_chunk_size,
            max_chunk_size,
        })
    }

}

impl Chunker for FastCDChunker {
    fn name(&self) -> &'static str {
        "fastcdc-chunker"
    }

    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        fs::create_dir_all(output_dir)?;

        let file_content = fs::read(input_file)?;

        let metadata = input_file.metadata()?;
        let original_file_size = metadata.len();

        let original_file_name = input_file
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("unknown_file"))
            .to_string_lossy()
            .into_owned();

        let chunker = v2020::FastCDC::new(
            &file_content,
            self.min_chunk_size,
            self.avg_chunk_size,
            self.max_chunk_size,
        );

        let mut chunk_filenames: Vec<String> = Vec::new();

        for chunk in chunker {
            let chunk_data_slice = &file_content[chunk.offset..chunk.offset + chunk.length];

            let chunk_filename = xhash::hash_buffer_128bit(chunk_data_slice).to_string();

            let chunk_path = output_dir.join(&chunk_filename);

            chunk_filenames.push(chunk_filename.clone());

        
            let mut chunk_file = BufWriter::new(fs::File::create(&chunk_path)?);
            chunk_file.write_all(chunk_data_slice)?;
            chunk_file.flush()?;
            // println!("Wrote chunk: {}", &chunk_filename);
        }

        let metadata = ChunkMetadata {
            original_file_name,
            original_file_size,
            chunks: chunk_filenames,
        };

        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(fs::File::create(&metadata_path)?);

        bincode::serialize_into(metadata_file, &metadata)
            .map_err(map_bincode_error)?;

        Ok(output_dir.to_path_buf())
    }

    fn unpack(&self, input_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error> {
        println!("To Implement: Unpacking files from {:?}", input_dir);
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
