use std::{ 
    fs::{self, File},
    io::{self, BufReader, BufWriter, Write}, 
    path::{Path, PathBuf},
    collections::{HashMap, HashSet}
};
use fastcdc::v2020;
use serde::{Deserialize, Serialize};
use crate::{chunker::{Chunker, ArchiveDedupStats}, xhash};

#[derive(Serialize, Deserialize, Debug)]
struct ArchiveEntry {
    path: PathBuf,
    is_dir: bool,
    chunks: Option<Vec<String>>,
    size: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ArchiveMetadata {
    min_chunk_size: u32,
    avg_chunk_size: u32,
    max_chunk_size: u32,
    entries: Vec<ArchiveEntry>,
    dedup_stats: Option<ArchiveDedupStats>,
}

const METADATA_FILE_NAME: &str = "metadata.bin";

fn map_bincode_error(e: bincode::Error) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("Bincode error: {}", e))
}

struct PackStatsCollector {
    chunk_data: HashMap<String, (usize, usize, HashSet<PathBuf>)>, 
    unique_chunks_written_hashes: HashSet<String>,
}

/*
FastCDC Algorithm for first reading the file and creating chunk locations.
and then writing the chunks to disk.

Multithreaded implementation is yet to be implemented.
*/

impl PackStatsCollector {
    fn new() -> Self {
        Self {
            chunk_data: HashMap::new(),
            unique_chunks_written_hashes: HashSet::new(),
        }
    }

    fn record_chunk_generated(&mut self, hash: &str, size: usize, file_rel_path: &Path) {
        let entry = self.chunk_data.entry(hash.to_string()).or_insert((0, size, HashSet::new()));
        entry.0 += 1; 
        entry.2.insert(file_rel_path.to_path_buf()); 
    }

    fn record_chunk_written_to_disk(&mut self, hash: &str) {
        self.unique_chunks_written_hashes.insert(hash.to_string());
    }

    fn calculate_final_stats(&self) -> ArchiveDedupStats {
        let mut total_logical_chunks_referenced = 0;
        let mut total_logical_size_bytes = 0;
        
        for (occurrences, size, _files_set) in self.chunk_data.values() {
            total_logical_chunks_referenced += occurrences;
            total_logical_size_bytes += (*occurrences as u64) * (*size as u64);
        }

        let unique_chunks_physically_stored = self.unique_chunks_written_hashes.len();
        let mut total_physical_size_bytes = 0;
        
        for hash in &self.unique_chunks_written_hashes {
            if let Some((_occurrences, size, _files_set_ref)) = self.chunk_data.get(hash) {
                total_physical_size_bytes += *size as u64;
            }
        }
        
        let total_space_saved_bytes = total_logical_size_bytes.saturating_sub(total_physical_size_bytes);

        let overlapping_unique_chunks_between_files = self.chunk_data.values().filter(|(_, _, files_set)| files_set.len() > 1).count();

        ArchiveDedupStats {
            overlapping_unique_chunks_between_files,
            total_space_saved_bytes,
            total_logical_chunks_referenced,
            unique_chunks_physically_stored,
            total_logical_size_bytes,
            total_physical_size_bytes,
        }
    }
}

/*
FastCDC Algorithm for first reading the file and creating chunk locations.
and then writing the chunks to disk.
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

    // This method is called by the trait implementation of get_archive_stats.
    pub fn get_archive_stats(&self, archive_dir: &Path) -> Result<Option<ArchiveDedupStats>, io::Error> {
        let metadata_path = archive_dir.join(METADATA_FILE_NAME);
        if !metadata_path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, 
                format!("Metadata file not found in {}", archive_dir.display())));
        }

        let metadata_file = BufReader::new(File::open(&metadata_path)?);
        let archive_metadata: ArchiveMetadata = bincode::deserialize_from(metadata_file)
            .map_err(map_bincode_error)?;

        Ok(archive_metadata.dedup_stats)
    }

}

impl Chunker for FastCDChunker {
    fn name(&self) -> &'static str {
        "fastcdc-chunker"
    }

    fn pack(&self, input_files: &Vec<PathBuf>, output_dir: &Path, _ignored_dirs: &Vec<PathBuf>) -> Result<PathBuf, io::Error> {
        fs::create_dir_all(output_dir)?;

        if input_files.is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "No input files provided for pack operation"));
        }

        let mut final_archive_entries: Vec<ArchiveEntry> = Vec::new();
        let mut stats_collector = PackStatsCollector::new();

        for current_disk_path in input_files {
            let file_metadata = fs::metadata(current_disk_path)
                .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("Input file not found: {}. Error: {}", current_disk_path.display(), e)))?;

            if !file_metadata.is_file() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                    format!("Input path is not a file: {}. FastCDChunker currently only processes files.", current_disk_path.display())));
            }

            let archive_entry_path = current_disk_path.clone();
            if archive_entry_path.is_absolute() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                    format!("Absolute input paths are not supported for archive creation: {}. Please use relative paths.", current_disk_path.display())));
            }

            let file_content = fs::read(current_disk_path)?;
            let original_file_size = file_metadata.len();

            let fastcdc_chunker_instance = v2020::FastCDC::new(
                &file_content,
                self.min_chunk_size,
                self.avg_chunk_size,
                self.max_chunk_size,
            );

            let mut chunk_hashes_for_entry: Vec<String> = Vec::new();

            for chunk in fastcdc_chunker_instance {
                let chunk_data_slice = &file_content[chunk.offset..chunk.offset + chunk.length];
                let chunk_hash_str = xhash::hash_buffer_128bit(chunk_data_slice).to_string();
                let chunk_path_on_disk = output_dir.join(&chunk_hash_str);

                chunk_hashes_for_entry.push(chunk_hash_str.clone());
                stats_collector.record_chunk_generated(&chunk_hash_str, chunk.length, &archive_entry_path);

                if !chunk_path_on_disk.exists() {
                    let mut chunk_file_writer = BufWriter::new(fs::File::create(&chunk_path_on_disk)?);
                    chunk_file_writer.write_all(chunk_data_slice)?;
                    chunk_file_writer.flush()?;
                    stats_collector.record_chunk_written_to_disk(&chunk_hash_str);
                }
            }

            let archive_entry = ArchiveEntry {
                path: archive_entry_path,
                is_dir: false, // FastCDChunker handles files only for now
                chunks: Some(chunk_hashes_for_entry),
                size: Some(original_file_size),
            };
            final_archive_entries.push(archive_entry);
        }

        let final_stats = stats_collector.calculate_final_stats();

        let archive_metadata = ArchiveMetadata {
            min_chunk_size: self.min_chunk_size,
            avg_chunk_size: self.avg_chunk_size,
            max_chunk_size: self.max_chunk_size,
            entries: final_archive_entries,
            dedup_stats: Some(final_stats),
        };

        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(fs::File::create(&metadata_path)?);

        bincode::serialize_into(metadata_file, &archive_metadata)
            .map_err(map_bincode_error)?;

        Ok(output_dir.to_path_buf())
    }

    fn unpack(&self, input_dir: &Path, output_unpack_dir: &Path) -> Result<PathBuf, io::Error> {
        let metadata_path = input_dir.join(METADATA_FILE_NAME);
        let metadata_file_reader = BufReader::new(File::open(&metadata_path)?);

        let archive_metadata: ArchiveMetadata = bincode::deserialize_from(metadata_file_reader)
            .map_err(map_bincode_error)?;

        fs::create_dir_all(output_unpack_dir)?; // Ensure base output directory exists

        for entry in &archive_metadata.entries {
            let current_entry_output_path = output_unpack_dir.join(&entry.path);

            if entry.is_dir {
                // Though FastCDChunker doesn't create dir entries currently,
                // this handles them if they were to be supported.
                fs::create_dir_all(&current_entry_output_path)?;
            } else {
                // Ensure parent directory for the file exists
                if let Some(parent_dir) = current_entry_output_path.parent() {
                    fs::create_dir_all(parent_dir)?;
                }

                let mut output_file_writer = BufWriter::new(File::create(&current_entry_output_path)?);

                if let Some(chunks) = &entry.chunks {
                    for chunk_filename in chunks {
                        let chunk_path = input_dir.join(chunk_filename);
                        if !chunk_path.exists() {
                            return Err(io::Error::new(
                                io::ErrorKind::NotFound,
                                format!("Chunk file not found during unpack: {}", chunk_path.display())
                            ));
                        }
                        let mut chunk_file_reader = BufReader::new(File::open(&chunk_path)?);
                        io::copy(&mut chunk_file_reader, &mut output_file_writer)?;
                    }
                }
                output_file_writer.flush()?;

                if let Some(original_size) = entry.size {
                    let unpacked_size = fs::metadata(&current_entry_output_path)?.len();
                    if unpacked_size != original_size {
                        eprintln!("Warning: Unpacked size mismatch for {}. Expected {}, got {}.",
                                  current_entry_output_path.display(), original_size, unpacked_size);
                    }
                }
            }
        }
        Ok(output_unpack_dir.to_path_buf())
    }

    fn get_chunk_hashes(&self, input_dir: &Path) -> Result<Vec<String>, io::Error>{
        let metadata_path = input_dir.join(METADATA_FILE_NAME);
        let metadata_file_reader = BufReader::new(fs::File::open(&metadata_path)?);
        let archive_metadata: ArchiveMetadata = bincode::deserialize_from(metadata_file_reader)
            .map_err(map_bincode_error)?;
        
        let mut all_chunks: Vec<String> = Vec::new();
        for entry in archive_metadata.entries {
            if !entry.is_dir {
                if let Some(chunks) = entry.chunks {
                    all_chunks.extend(chunks);
                }
            }
        }
        Ok(all_chunks)
    }

    fn get_archive_stats(&self, archive_dir: &Path) -> Result<Option<ArchiveDedupStats>, io::Error>{
        self.get_archive_stats(archive_dir)
    }
}
