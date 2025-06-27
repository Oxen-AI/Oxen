use std::{
    fs::{self, File}, 
    io::{self, BufReader, BufWriter, Write}, path::{Path, PathBuf}
};
use crate::chunker::Chunker;

/*
Super simple chunker that just copies the file to a new location.
Our current baseline implemented in Oxen.
*/
pub struct Copier {}

const FILE_NAME: &str = "file_blob";

impl Chunker for Copier {

    fn name(&self) -> &'static str {
        "mover"
    }
    
    fn pack(&self, input_file: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        println!("Packing file: {:?}", input_file);
        fs::create_dir_all(output_dir)?;
        let file_name = input_file
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("unknown_file"))
        .to_string_lossy()
        .into_owned();
    let output_path = output_dir.join(&file_name);
    
    let mut input = BufReader::new(File::open(input_file)?);
    let mut output = BufWriter::new(File::create(FILE_NAME)?);
    
    io::copy(&mut input, &mut output)?;
    
    output.flush()?;
    println!("Packed file: {:?}", output_path);
    
    Ok(output_path)
    }

    fn unpack(&self, input_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error>{
        let mut input = BufReader::new(File::open(input_dir.join(FILE_NAME))?);
        let mut output = BufWriter::new(File::create(output_path)?);
        
        io::copy(&mut input, &mut output)?;
        
        Ok(output_path.to_path_buf())
    }

    fn get_chunk_hashes(&self, input_dir: &Path) -> Result<Vec<String>, io::Error>{
        let hashes = Vec::new();
        Ok(hashes) //returning empty vector because we are not hashing anything
    }

}