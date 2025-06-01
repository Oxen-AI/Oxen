use std::{
    path::{Path, PathBuf, StripPrefixError},
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    collections::{HashMap, HashSet},
};
use serde::{Serialize, Deserialize};
use bincode;

use crate::chunker::{Chunker, ArchiveDedupStats};
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
    path: PathBuf,
    is_dir: bool,
    chunks: Option<Vec<String>>,
    size: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ArchiveMetadata {
    chunk_size: usize,
    entries: Vec<ArchiveEntry>,
    dedup_stats: Option<ArchiveDedupStats>, // New field to store stats
}


// Internal helper struct for collecting statistics during packing - unchanged
struct PackStatsCollector {
    chunk_data: HashMap<String, (usize, usize, HashSet<PathBuf>)>,
    unique_chunks_written_hashes: HashSet<String>,
}

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

        let mut overlapping_unique_chunks_between_files = 0;
        for (_occurrences, _size, files_set) in self.chunk_data.values() {
            if files_set.len() > 1 {
                overlapping_unique_chunks_between_files += 1;
            }
        }

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

    // process_file_for_stats and pack_directory_recursive_for_stats remain largely the same,
    // as their role is to feed data into PackStatsCollector.
    fn process_file_for_stats(
        &self,
        file_path: &Path,
        base_input_path: &Path, 
        output_dir: &Path,
        archive_entries: &mut Vec<ArchiveEntry>,
        stats_collector: &mut PackStatsCollector, 
    ) -> Result<(), io::Error> {
        let mut input = BufReader::new(File::open(file_path)?);
        let metadata = file_path.metadata()?;
        let original_file_size = metadata.len();

        let relative_path = file_path
            .strip_prefix(base_input_path)
            .map_err(map_strip_prefix_error)?
            .to_path_buf();

        let mut buffer = vec![0; self.chunk_size];
        let mut chunk_hashes_for_entry: Vec<String> = Vec::new();

        loop {
            let bytes_read = input.read(&mut buffer)?;
            if bytes_read == 0 { break; }

            let chunk_content = &buffer[..bytes_read];
            let chunk_hash_str = xhash::hash_buffer_128bit(chunk_content).to_string();
            
            stats_collector.record_chunk_generated(&chunk_hash_str, bytes_read, &relative_path);

            let chunk_path = output_dir.join(&chunk_hash_str);
            chunk_hashes_for_entry.push(chunk_hash_str.clone());

            if !chunk_path.exists() {
                let mut chunk_file = BufWriter::new(File::create(&chunk_path)?);
                chunk_file.write_all(chunk_content)?;
                chunk_file.flush()?;
                stats_collector.record_chunk_written_to_disk(&chunk_hash_str);
            }

            if bytes_read < self.chunk_size { break; }
        }

        archive_entries.push(ArchiveEntry {
            path: relative_path,
            is_dir: false,
            chunks: Some(chunk_hashes_for_entry),
            size: Some(original_file_size),
        });
        Ok(())
    }

    fn pack_directory_recursive_for_stats(
        &self,
        current_dir: &Path,
        base_input_path: &Path,
        output_dir: &Path,
        archive_entries: &mut Vec<ArchiveEntry>,
        stats_collector: &mut PackStatsCollector,
        ignored_dirs: &Vec<PathBuf>,
    ) -> Result<(), io::Error> {
        for entry_result in fs::read_dir(current_dir)? {
            let entry = entry_result?;
            let entry_path = entry.path();
            let metadata = entry_path.metadata()?;
            let relative_path_for_entry = entry_path
                 .strip_prefix(base_input_path)
                 .map_err(map_strip_prefix_error)?
                 .to_path_buf();

            if metadata.is_dir() {
                if ignored_dirs.contains(&relative_path_for_entry) {
                    // Directory is in the ignore list, skip processing it
                    continue; 
                }
                 archive_entries.push(ArchiveEntry {
                     path: relative_path_for_entry.clone(),
                     is_dir: true,
                     chunks: None,
                     size: None,
                 });
                 self.pack_directory_recursive_for_stats(&entry_path, base_input_path, output_dir, archive_entries, stats_collector, ignored_dirs)?;
            } else if metadata.is_file() {
                 self.process_file_for_stats(&entry_path, base_input_path, output_dir, archive_entries, stats_collector)?;
            }
        }
        Ok(())
    }
    pub fn pack_and_get_stats(&self, input_path: &Path, output_dir: &Path, ignored_dirs: &Vec<PathBuf>) -> Result<(PathBuf, ArchiveDedupStats), io::Error> {
        fs::create_dir_all(output_dir)?;
        
        let mut stats_collector = PackStatsCollector::new();
        let mut entries_vec: Vec<ArchiveEntry> = Vec::new(); // Temporary vec for entries

        let input_metadata = fs::metadata(input_path)?;

        if input_metadata.is_file() {
            let base_input_path_for_file = input_path.parent().unwrap_or_else(|| Path::new("."));
            self.process_file_for_stats(
                input_path, 
                base_input_path_for_file,
                output_dir, 
                &mut entries_vec, 
                &mut stats_collector // ignored_dirs is not directly used by process_file_for_stats
            )?;
        } else if input_metadata.is_dir() {
            entries_vec.push(ArchiveEntry {
                 path: PathBuf::from("."), 
                 is_dir: true,
                 chunks: None,
                 size: None,
            });
            self.pack_directory_recursive_for_stats(
                input_path, input_path, output_dir, 
                &mut entries_vec, &mut stats_collector, ignored_dirs
            )?;
        } else {
             return Err(io::Error::new(io::ErrorKind::InvalidInput, "Input path must be a file or a directory"));
        }

        let final_stats = stats_collector.calculate_final_stats(); // Calculate stats

        let archive_metadata = ArchiveMetadata { // Construct metadata with stats
            chunk_size: self.chunk_size,
            entries: entries_vec,
            dedup_stats: Some(final_stats.clone()), // Store a clone of the stats
        };

        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(File::create(&metadata_path)?);
        bincode::serialize_into(metadata_file, &archive_metadata).map_err(map_bincode_error)?;

        Ok((output_dir.to_path_buf(), final_stats))
    }

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

impl Chunker for FixedSizeChunker {
    fn name(&self) -> &'static str {
        "fixed-size-chunker"
    }

    fn pack(&self, input_path: &Path, output_dir: &Path, ignored_dirs: &Vec<PathBuf>) -> Result<PathBuf, io::Error> {
        self.pack_and_get_stats(input_path, output_dir, ignored_dirs)
            .map(|(output_path_buf, _stats)| output_path_buf)
    }

    fn unpack(&self, chunk_dir: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        let metadata_path = chunk_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufReader::new(File::open(&metadata_path)?);

        let archive_metadata: ArchiveMetadata = bincode::deserialize_from(metadata_file)
            .map_err(map_bincode_error)?;

        fs::create_dir_all(output_dir)?;

        for entry in &archive_metadata.entries {
            let entry_output_path = output_dir.join(&entry.path);

            if entry.is_dir {
                fs::create_dir_all(&entry_output_path)?;
            } else {
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
                 if let Some(original_size) = entry.size {
                     let unpacked_size = fs::metadata(&entry_output_path)?.len();
                     if unpacked_size != original_size {
                         // eprintln!("Warning: Unpacked size mismatch for {}. Expected {}, got {}.",
                         //    entry_output_path.display(), original_size, unpacked_size);
                     }
                 }
            }
        }
        Ok(output_dir.to_path_buf())
    }

    fn get_chunk_hashes(&self, input_dir: &Path) -> Result<Vec<String>, io::Error>{
        let metadata_path = input_dir.join(METADATA_FILE_NAME);
        let metadata_file = fs::File::open(&metadata_path)?;
        let reader = BufReader::new(metadata_file);
        let archive_metadata: ArchiveMetadata = bincode::deserialize_from(reader)
            .map_err(map_bincode_error)?;

        let mut all_chunks: Vec<String> = Vec::new();
        for entry in archive_metadata.entries.into_iter() {
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