use std::{
    path::{Path, PathBuf, StripPrefixError},
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Write},
    collections::{HashMap, HashSet},
    time::{Instant, Duration},
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
    dedup_stats: Option<ArchiveDedupStats>,
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

    fn calculate_final_stats(&self, pack_duration: Duration) -> ArchiveDedupStats {
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

        let mut total_intra_file_space_saved_bytes = 0;
        let mut overlapping_unique_chunks_between_files = 0;

        for (occurrences, size, files_set) in self.chunk_data.values() {
            let n_occurrences = *occurrences as u64;
            let chunk_size_val = *size as u64;
            let n_files = files_set.len() as u64;

            if files_set.len() > 1 {
                overlapping_unique_chunks_between_files += 1;
            }

            if n_occurrences > n_files {
                total_intra_file_space_saved_bytes += (n_occurrences - n_files) * chunk_size_val;
            }
        }

        ArchiveDedupStats {
            overlapping_unique_chunks_between_files,
            total_space_saved_bytes,
            total_logical_chunks_referenced,
            total_intra_file_space_saved_bytes,
            unique_chunks_physically_stored,
            total_logical_size_bytes,
            total_physical_size_bytes,
            time_to_pack: pack_duration,
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

    fn process_single_file_for_archive(
        &self,
        disk_file_path: &Path,
        archive_entry_path: &Path, 
        output_dir: &Path, 
        stats_collector: &mut PackStatsCollector,
    ) -> Result<ArchiveEntry, io::Error> {
        let mut input_file_reader = BufReader::new(File::open(disk_file_path)?);
        let metadata = disk_file_path.metadata()?;
        let original_file_size = metadata.len();

        let mut buffer = vec![0; self.chunk_size];
        let mut chunk_hashes_for_entry: Vec<String> = Vec::new();

        loop {
            let bytes_read = input_file_reader.read(&mut buffer)?;
            if bytes_read == 0 { break; }

            let chunk_content = &buffer[..bytes_read];
            let chunk_hash_str = xhash::hash_buffer_128bit(chunk_content).to_string();
            
            // Path used for stats_collector refers to its path *in the archive*
            stats_collector.record_chunk_generated(&chunk_hash_str, bytes_read, archive_entry_path);

            let chunk_path_on_disk = output_dir.join(&chunk_hash_str);
            chunk_hashes_for_entry.push(chunk_hash_str.clone());

            if !chunk_path_on_disk.exists() {
                let mut chunk_file_writer = BufWriter::new(File::create(&chunk_path_on_disk)?);
                chunk_file_writer.write_all(chunk_content)?;
                chunk_file_writer.flush()?;
                stats_collector.record_chunk_written_to_disk(&chunk_hash_str);
            }
            if bytes_read < self.chunk_size { break; }
        }

        Ok(ArchiveEntry {
            path: archive_entry_path.to_path_buf(),
            is_dir: false,
            chunks: Some(chunk_hashes_for_entry),
            size: Some(original_file_size),
        })
    }

    fn pack_directory_recursive_for_archive(
        &self,
        disk_current_dir: &Path,
        base_dir_for_ignored_check: &Path, 
        archive_path_prefix: &Path, 
        output_dir: &Path,
        archive_entries_list: &mut Vec<ArchiveEntry>,
        stats_collector: &mut PackStatsCollector,
        ignored_dirs: &Vec<PathBuf>,
    ) -> Result<(), io::Error> {
        for entry_result in fs::read_dir(disk_current_dir)? {
            let entry = entry_result?;
            let disk_entry_path = entry.path(); 
            let metadata = disk_entry_path.metadata()?;

            let path_relative_to_scan_root = disk_entry_path
                 .strip_prefix(base_dir_for_ignored_check)
                 .map_err(map_strip_prefix_error)?
                 .to_path_buf();

            let final_archive_path = archive_path_prefix.join(&path_relative_to_scan_root);

            if metadata.is_dir() {
                if ignored_dirs.contains(&path_relative_to_scan_root) {
                    continue; 
                }
                 archive_entries_list.push(ArchiveEntry {
                     path: final_archive_path.clone(),
                     is_dir: true,
                     chunks: None,
                     size: None,
                 });

                self.pack_directory_recursive_for_archive(
                    &disk_entry_path, 
                    base_dir_for_ignored_check, 
                    archive_path_prefix, 
                    output_dir, 
                    archive_entries_list, 
                    stats_collector, 
                    ignored_dirs
                )?;
            } else if metadata.is_file() {
                let file_entry = self.process_single_file_for_archive(
                    &disk_entry_path,
                    &final_archive_path,
                    output_dir,
                    stats_collector,
                )?;
                archive_entries_list.push(file_entry);
            }
        }
        Ok(())
    }

    pub fn pack_and_get_stats(&self, input_paths: &Vec<PathBuf>, output_dir: &Path, ignored_dirs: &Vec<PathBuf>) -> Result<(PathBuf, ArchiveDedupStats), io::Error> {
        fs::create_dir_all(output_dir)?;

        let pack_start_time = Instant::now();
        
        let mut stats_collector = PackStatsCollector::new();
        let mut final_archive_entries: Vec<ArchiveEntry> = Vec::new();

        for current_disk_path in input_paths {
            let input_metadata = fs::metadata(current_disk_path)
                .map_err(|e| io::Error::new(io::ErrorKind::NotFound, format!("Input path not found: {}. Error: {}", current_disk_path.display(), e)))?;

            let archive_path_for_this_input_item = current_disk_path.clone();

            if archive_path_for_this_input_item.is_absolute() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                    format!("Absolute input paths are not directly supported for archive creation: {}. Please use relative paths.", current_disk_path.display())));
            }

            if input_metadata.is_file() {
                let file_entry = self.process_single_file_for_archive(
                    current_disk_path,
                    &archive_path_for_this_input_item,
                    output_dir,
                    &mut stats_collector,
                )?;
                final_archive_entries.push(file_entry);

            } else if input_metadata.is_dir() {

                let current_dir_name_as_path = current_disk_path.file_name().map(PathBuf::from).unwrap_or_default();
                if !current_dir_name_as_path.as_os_str().is_empty() && ignored_dirs.contains(&current_dir_name_as_path) {
                    continue; 
                }

                final_archive_entries.push(ArchiveEntry {
                    path: archive_path_for_this_input_item.clone(),
                    is_dir: true,
                    chunks: None,
                    size: None,
                });
                
                self.pack_directory_recursive_for_archive(
                    current_disk_path,       
                    current_disk_path,
                    &archive_path_for_this_input_item,
                    output_dir,
                    &mut final_archive_entries,
                    &mut stats_collector,
                    ignored_dirs,
                )?;

            } else {
                 return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("Input path {} is not a file or a directory", current_disk_path.display())));
            }
        }

        let pack_duration = pack_start_time.elapsed();
        let final_stats = stats_collector.calculate_final_stats(pack_duration);

        let archive_metadata = ArchiveMetadata {
            chunk_size: self.chunk_size,
            entries: final_archive_entries,
            dedup_stats: Some(final_stats.clone()),
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

    fn pack(&self, input_paths: &Vec<PathBuf>, output_dir: &Path, ignored_dirs: &Vec<PathBuf>) -> Result<PathBuf, io::Error> {
        self.pack_and_get_stats(input_paths, output_dir, ignored_dirs)
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