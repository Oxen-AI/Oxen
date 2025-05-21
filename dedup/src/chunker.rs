use std::{collections::HashMap, io, path::{Path, PathBuf}};
use clap::ValueEnum;
use thiserror::Error;
pub use crate::chunker::fixedsize::FixedSizeChunker;
pub use crate::chunker::fixedsize_multithreaded::FixedSizeMultiChunker;
pub use crate::chunker::copier::Copier;
pub use crate::chunker::fastcdchunker::FastCDChunker;

pub mod fixedsize;
pub mod copier;
pub mod fixedsize_multithreaded;
pub mod oxendedup;
pub mod fastcdchunker;

fn map_bincode_error(err: bincode::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("Bincode error: {:?}", err))
}

#[derive(Error, Debug)]
pub enum FrameworkError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Bincode serialization/deserialization error: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("Chunker '{name}' not found")]
    ChunkerNotFound { name: String },
    #[error("System time error: {message}")]
    TimeError {
        message: String,
        source: std::time::SystemTimeError,
    },
    #[error("File Verification failed")]
    VerificationFailed,

    #[error("Internal error: {message}")]
    InternalError{
        message: String,
    },
}

struct ChunkMetadata {
    original_file_name: String,
    original_file_size: u64,
    chunk_size: usize,
    chunks: Vec<String>,
}

pub type FrameworkResult<T> = Result<T, FrameworkError>;

pub trait Chunker {
    
    fn name(&self) -> &'static str;

    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error>;

    fn unpack(&self, input_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error>;

    fn get_chunk_hashes(&self, input_dir: &Path) -> Result<Vec<String>, io::Error>;
}

pub fn get_chunker(algorithm: &Algorithm, chunk_size: usize) -> FrameworkResult<Box<dyn Chunker>> {
    let mut chunkers: HashMap<&'static str, Box<dyn Chunker>> = HashMap::new();

    for variant in Algorithm::value_variants() {
        let chunker = variant.create_chunker(chunk_size); 
        let name = variant.as_str(); 

        chunkers.insert(name, chunker);
    }
    let chunker_name = algorithm.as_str();
    chunkers.remove(chunker_name)
        .ok_or_else(|| FrameworkError::ChunkerNotFound { name: chunker_name.to_string() })
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum Algorithm {
    #[value(name = "fixed-size")]
    FixedSize,
    #[value(name = "fixed-size-multithreaded")]
    FixedSizeMultiThreaded,
    #[value(name = "copier")]
    Copier,
    #[value(name = "fastcdc")]
    FastCDC,
}

impl Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Algorithm::FixedSize => "fixed-size",
            Algorithm::Copier => "copier",
            Algorithm::FixedSizeMultiThreaded => "fixed-size-multithreaded",
            Algorithm::FastCDC => "fastcdc",
        }
    }

    fn create_chunker(&self, chunk_size: usize) -> Box<dyn Chunker> {
        match self {
            Algorithm::FixedSize => {
                let chunker = FixedSizeChunker::new(chunk_size).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
            Algorithm::Copier => {
                let mover = Copier {};
                Box::new(mover)
            },
            Algorithm::FixedSizeMultiThreaded => {
                let chunker = FixedSizeMultiChunker::new(chunk_size, 16).expect("Failed to create FixedSizeMultiChunker");
                Box::new(chunker)
            },
            Algorithm::FastCDC => {
                let chunker = FastCDChunker::new(chunk_size, 16).expect("Failed to create FastCDChunker");
                Box::new(chunker)
            },
        }
    }
}
