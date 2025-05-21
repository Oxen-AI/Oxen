use xxhash_rust::xxh3::{Xxh3, xxh3_128};
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::fs::{File};



pub fn hash_file_128bit(file_path: &Path) -> io::Result<u128> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Xxh3::new();
    let mut buffer = [0; 8192]; 

    loop {
        let n = reader.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }
    Ok(hasher.digest128()) 
}


pub fn hash_buffer_128bit(buffer: &[u8]) -> u128 {
    xxh3_128(buffer)
}