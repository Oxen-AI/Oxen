pub mod fixedsize;
use std::{collections::HashMap, io, path::{Path, PathBuf}};
use clap::ValueEnum;
use thiserror::Error;
pub use crate::chunker::fixedsize::FixedSizeChunker;

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

pub type FrameworkResult<T> = Result<T, FrameworkError>;

pub trait Chunker {

    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error>;

    fn unpack(&self, input_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error>;

    fn name(&self) -> &'static str;
}

pub fn get_chunker(algorithm: &Algorithm) -> FrameworkResult<Box<dyn Chunker>> {
    let mut chunkers: HashMap<&'static str, Box<dyn Chunker>> = HashMap::new();

    for variant in Algorithm::value_variants() {
        let chunker = variant.create_chunker(); 
        let name = variant.as_str(); 

        chunkers.insert(name, chunker);
    }
    let chunker_name = algorithm.as_str();
    chunkers.remove(chunker_name)
        .ok_or_else(|| FrameworkError::ChunkerNotFound { name: chunker_name.to_string() })
}

#[derive(ValueEnum, Clone, Debug, PartialEq)]
pub enum Algorithm {
    #[value(name = "fixed-size-4k")]
    FixedSize4k,
    #[value(name = "fixed-size-64k")]
    FixedSize64k,
    
}

impl Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Algorithm::FixedSize4k => "fixed-size-4k",
            Algorithm::FixedSize64k => "fixed-size-64k",
        }
    }

    fn create_chunker(&self) -> Box<dyn Chunker> {
        match self {
            Algorithm::FixedSize4k => {
                let chunker = FixedSizeChunker::new(4096).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
            Algorithm::FixedSize64k => {
                let chunker = FixedSizeChunker::new(65536).expect("Failed to create FixedSizeChunker");
                Box::new(chunker)
            },
        }
    }
}
