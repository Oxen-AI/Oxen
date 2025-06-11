use std::{
    path::{Path, PathBuf, StripPrefixError},
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
};
use serde::{Serialize, Deserialize};
use bincode; // Import bincode

use crate::chunker::{Chunker};
use crate::xhash;

const METADATA_FILE_NAME: &str = "metadata.bin";

fn map_bincode_error(err: bincode::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("Bincode error: {:?}", err))
}

fn map_strip_prefix_error(_: StripPrefixError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, "Failed to get relative path")
}

#[derive(Serialize, Deserialize, Debug)]
struct ArchiveEntry {
    path: PathBuf, // Relative path within the original structure
    is_dir: bool,
    chunks: Option<Vec<String>>, // Chunk hashes (only for files)
    size: Option<u64>, // Original file size (only for files)
}

#[derive(Serialize, Deserialize, Debug)]
struct ArchiveMetadata {
    chunk_size: usize,
    entries: Vec<ArchiveEntry>,
}


pub struct FixedSizeChunker {
    chunk_size: usize,
}

impl FixedSizeChunker {

    pub fn new(chunk_size: usize) -> Result<Self, io::Error> {
        if chunk_size == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Chunk size cannot be zero"));
        }
        Ok(Self { chunk_size })
    }

    // Helper function to process a single file, chunk it, and add to entries
    fn process_file(
        &self,
        file_path: &Path,
        base_input_path: &Path, // The root path being packed (file or directory)
        output_dir: &Path,
        archive_entries: &mut Vec<ArchiveEntry>,
    ) -> Result<(), io::Error> {
        let mut input = BufReader::new(File::open(file_path)?);
        let metadata = file_path.metadata()?;
        let original_file_size = metadata.len();

        let relative_path = file_path
            .strip_prefix(base_input_path)
            .map_err(map_strip_prefix_error)?
            .to_path_buf();

        let mut buffer = vec![0; self.chunk_size];
        let mut chunk_filenames: Vec<String> = Vec::new();

        loop {
            let bytes_read = input.read(&mut buffer)?;

            if bytes_read == 0 {
                break;
            }

            let chunk_hash = xhash::hash_buffer_128bit(&buffer[..bytes_read]).to_string();
            let chunk_path = output_dir.join(&chunk_hash);
            chunk_filenames.push(chunk_hash);

            // Only write the chunk if it doesn't exist (basic deduplication)
            if !chunk_path.exists() {
                let mut chunk_file = BufWriter::new(File::create(&chunk_path)?);
                chunk_file.write_all(&buffer[..bytes_read])?;
                chunk_file.flush()?;
            }


            if bytes_read < self.chunk_size {
                break;
            }
        }

        archive_entries.push(ArchiveEntry {
            path: relative_path,
            is_dir: false,
            chunks: Some(chunk_filenames),
            size: Some(original_file_size),
        });

        Ok(())
    }

    // Helper function for recursive directory packing
    fn pack_directory_recursive(
        &self,
        current_dir: &Path,
        base_input_path: &Path,
        output_dir: &Path,
        archive_entries: &mut Vec<ArchiveEntry>,
    ) -> Result<(), io::Error> {
        for entry_result in fs::read_dir(current_dir)? {
            let entry = entry_result?;
            let entry_path = entry.path();

            let metadata = entry_path.metadata()?;

            if metadata.is_dir() {
                 // Add directory entry *before* recursing into it
                 let relative_path = entry_path
                     .strip_prefix(base_input_path)
                     .map_err(map_strip_prefix_error)?
                     .to_path_buf();

                 archive_entries.push(ArchiveEntry {
                     path: relative_path,
                     is_dir: true,
                     chunks: None,
                     size: None,
                 });

                 self.pack_directory_recursive(&entry_path, base_input_path, output_dir, archive_entries)?;

            } else if metadata.is_file() {
                 self.process_file(&entry_path, base_input_path, output_dir, archive_entries)?;
            }
            // Ignore other types like symlinks for now
        }
        Ok(())
    }
}

impl Chunker for FixedSizeChunker {
    fn name(&self) -> &'static str {
        "fixed-size-chunker"
    }

    fn pack(&self, input_path: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        fs::create_dir_all(output_dir)?;

        let mut archive_metadata = ArchiveMetadata {
            chunk_size: self.chunk_size,
            entries: Vec::new(),
        };

        let input_metadata = fs::metadata(input_path)?;

        if input_metadata.is_file() {
            // If input is a single file, process it directly
            // The base input path for a single file is its parent directory,
            // so the relative path becomes just the filename.
            let base_input_path = input_path.parent().unwrap_or_else(|| Path::new("."));
             self.process_file(input_path, base_input_path, output_dir, &mut archive_metadata.entries)?;

        } else if input_metadata.is_dir() {
            // If input is a directory, add an entry for the root directory itself
            // and then recurse through its contents.
            archive_metadata.entries.push(ArchiveEntry {
                 path: PathBuf::from("."), // Represents the root of the packed directory
                 is_dir: true,
                 chunks: None,
                 size: None,
            });
            self.pack_directory_recursive(input_path, input_path, output_dir, &mut archive_metadata.entries)?;
        } else {
             return Err(io::Error::new(io::ErrorKind::InvalidInput, "Input path must be a file or a directory"));
        }


        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(File::create(&metadata_path)?);

        bincode::serialize_into(metadata_file, &archive_metadata)
            .map_err(map_bincode_error)?;

        Ok(output_dir.to_path_buf())
    }

    fn unpack(&self, chunk_dir: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        let metadata_path = chunk_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufReader::new(File::open(&metadata_path)?);

        let archive_metadata: ArchiveMetadata = bincode::deserialize_from(metadata_file)
            .map_err(map_bincode_error)?;

        // Ensure the base output directory exists
        fs::create_dir_all(output_dir)?;

        // Iterate through entries and recreate the structure
        for entry in &archive_metadata.entries {
            let entry_output_path = output_dir.join(&entry.path);

            if entry.is_dir {
                fs::create_dir_all(&entry_output_path)?;
            } else {
                // Ensure parent directory exists for the file
                if let Some(parent) = entry_output_path.parent() {
                     fs::create_dir_all(parent)?;
                }

                let mut output_file = BufWriter::new(File::create(&entry_output_path)?);

                if let Some(chunks) = &entry.chunks {
                    for chunk_filename in chunks {
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
                }
                 output_file.flush()?;

                 // Optional: Verify unpacked size
                 if let Some(original_size) = entry.size {
                     let unpacked_size = fs::metadata(&entry_output_path)?.len();
                     if unpacked_size != original_size {
                          // This could be a warning or an error depending on desired strictness
                          // For simplicity, let's just allow it, but in a real system,
                          // you might want to log this or return an error.
                         // println!("Warning: Unpacked size mismatch for {}", entry_output_path.display());
                     }
                 }

            }
        }

        Ok(output_dir.to_path_buf())
    }

    fn get_chunk_hashes(&self, input_dir: &Path) -> Result<Vec<String>, io::Error>{
        let metadata_path = input_dir.join(METADATA_FILE_NAME);
        let metadata_file = fs::File::open(&metadata_path)?;
        let archive_metadata: ArchiveMetadata = bincode::deserialize_from(metadata_file)
            .map_err(map_bincode_error)?;

        let mut all_chunks: Vec<String> = Vec::new();

        // Collect all chunk hashes from all file entries
        for entry in archive_metadata.entries.into_iter() {
            if !entry.is_dir {
                if let Some(chunks) = entry.chunks {
                    all_chunks.extend(chunks);
                }
            }
        }

        Ok(all_chunks)
    }

}