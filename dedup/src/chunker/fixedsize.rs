use std::{
    path::{Path, PathBuf},
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
};
use serde::{Serialize, Deserialize};
use bincode; // Import bincode

use crate::chunker::Chunker;
use crate::xhash;

const METADATA_FILE_NAME: &str = "metadata.bin"; 

#[derive(Serialize, Deserialize, Debug)]
struct ChunkMetadata {
    original_file_name: String,
    original_file_size: u64,
    chunk_size: usize, 
    chunks: Vec<String>, 
}

pub struct FixedSizeChunker {
    chunk_size: usize,
}

fn map_bincode_error(err: bincode::Error) -> io::Error {

    io::Error::new(io::ErrorKind::Other, format!("Bincode error: {:?}", err))
}

impl FixedSizeChunker {

    pub fn new(chunk_size: usize) -> Result<Self, io::Error> {
        if chunk_size == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Chunk size cannot be zero"));
        }
        Ok(Self { chunk_size })
    }
}

impl Chunker for FixedSizeChunker {
    fn name(&self) -> &'static str {
        "fixed-size-chunker"
    }

    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {

        fs::create_dir_all(output_dir)?;

        let mut input = BufReader::new(File::open(input_file)?);
        let metadata = input_file.metadata()?;
        let original_file_size = metadata.len();

        let original_file_name = input_file
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("unknown_file"))
            .to_string_lossy()
            .into_owned();

        let mut buffer = vec![0; self.chunk_size];
        let mut chunk_index = 0;
        let mut chunk_filenames: Vec<String> = Vec::new();

        loop {
            let bytes_read = input.read(&mut buffer)?;

            if bytes_read == 0 {
                break;
            }

            let chunk_filename = xhash::hash_buffer_128bit(&buffer[..bytes_read]).to_string();
            let chunk_path = output_dir.join(&chunk_filename);
            chunk_filenames.push(chunk_filename);

            let mut chunk_file = BufWriter::new(File::create(&chunk_path)?);
            chunk_file.write_all(&buffer[..bytes_read])?;
            chunk_file.flush()?;

            chunk_index = chunk_index+1;

            if bytes_read < self.chunk_size {
                break;
            }
        }


        let metadata = ChunkMetadata {
            original_file_name,
            original_file_size,
            chunk_size: self.chunk_size,
            chunks: chunk_filenames,
        };

        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(File::create(&metadata_path)?);

        bincode::serialize_into(metadata_file, &metadata)
            .map_err(map_bincode_error)?;

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
}