use std::{
    fs::{self, File},
    io::{self, Read, Write, BufReader, BufWriter, Seek, SeekFrom},
    path::{Path, PathBuf, StripPrefixError},
    collections::{HashMap, HashSet},
    time::{Instant, Duration},
};

use serde::{Deserialize, Serialize};
use crate::chunker::{Chunker, map_bincode_error as common_map_bincode_error, ArchiveDedupStats};
use crate::xhash;
use rayon::prelude::*;

const METADATA_FILE_NAME: &str = "metadata.bin";

fn map_bincode_error(err: bincode::Error) -> io::Error {
    common_map_bincode_error(err)
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
struct MultiChunkArchiveMetadata {
    chunk_size: usize,
    entries: Vec<ArchiveEntry>,
    dedup_stats: Option<ArchiveDedupStats>,
}

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
                // This chunk appears more times in total than the number of unique files it's in.
                // This means it must be repeated within at least one file.
                // The number of "excess" occurrences (intra-file duplications) is (n_occurrences - n_files).
                total_intra_file_space_saved_bytes += (n_occurrences - n_files) * chunk_size_val;
            }
        }

        ArchiveDedupStats {
            overlapping_unique_chunks_between_files,
            total_intra_file_space_saved_bytes,
            total_space_saved_bytes,
            total_logical_chunks_referenced,
            unique_chunks_physically_stored,
            total_logical_size_bytes,
            total_physical_size_bytes,
            time_to_pack: pack_duration,
        }
    }
}


pub struct FixedSizeMultiChunker {
    chunk_size: usize,
    concurrency: usize,
}

impl FixedSizeMultiChunker {
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
        Ok(Self {
            chunk_size,
            concurrency,
        })
    }

    fn process_single_file_multithreaded(
        &self,
        disk_file_path: &Path,
        archive_entry_path: &Path,
        output_dir: &Path,
        stats_collector: &mut PackStatsCollector,
    ) -> Result<ArchiveEntry, io::Error> {
        let input_file_metadata = fs::metadata(disk_file_path)?;
        let original_file_size = input_file_metadata.len();

        if original_file_size == 0 {
            return Ok(ArchiveEntry {
                path: archive_entry_path.to_path_buf(),
                is_dir: false,
                chunks: Some(Vec::new()),
                size: Some(0),
            });
        }

        let chunk_size_u64 = self.chunk_size as u64;
        let num_chunks = (original_file_size + chunk_size_u64 - 1) / chunk_size_u64;

        let tasks: Vec<(usize, u64, usize)> = (0..num_chunks)
            .map(|i| {
                let start = i * chunk_size_u64;
                let actual_chunk_size = std::cmp::min(chunk_size_u64, original_file_size - start) as usize;
                (i as usize, start, actual_chunk_size)
            })
            .collect();

        let results: Vec<Result<(usize, String, usize, bool), io::Error>> = tasks
            .into_par_iter()
            .map(|(_chunk_index, start_offset, chunk_data_size)| -> Result<(usize, String, usize, bool), io::Error> {
                if chunk_data_size == 0 { 
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Chunk size is zero"));
                }

                let mut infile = File::open(disk_file_path)?;
                infile.seek(SeekFrom::Start(start_offset))?;

                let mut buffer = vec![0; chunk_data_size];
                infile.read_exact(&mut buffer)?;

                let hash_str = xhash::hash_buffer_128bit(&buffer).to_string();
                let chunk_path_on_disk = output_dir.join(&hash_str);
                let mut wrote_chunk = false;

                match fs::OpenOptions::new().write(true).create_new(true).open(&chunk_path_on_disk) {
                    Ok(mut outfile) => {
                        outfile.write_all(&buffer)?;
                        outfile.flush()?;
                        wrote_chunk = true;
                    }
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                        // Chunk already exists
                    }
                    Err(e) => return Err(e),
                }
                Ok((_chunk_index, hash_str, chunk_data_size, wrote_chunk))
            })
            .collect();

        let mut indexed_chunk_info: Vec<(usize, String, usize, bool)> = Vec::with_capacity(num_chunks as usize);
        for result in results {
            match result {
                Ok(data) => indexed_chunk_info.push(data),
                Err(e) => return Err(e), 
            }
        }
        indexed_chunk_info.sort_by_key(|k| k.0);


        let mut chunk_hashes_for_entry: Vec<String> = Vec::new();
        for (_idx, hash_str, bytes_read, was_written) in indexed_chunk_info {
            stats_collector.record_chunk_generated(&hash_str, bytes_read, archive_entry_path);
            if was_written {
                stats_collector.record_chunk_written_to_disk(&hash_str);
            }
            chunk_hashes_for_entry.push(hash_str);
        }

        Ok(ArchiveEntry {
            path: archive_entry_path.to_path_buf(),
            is_dir: false,
            chunks: Some(chunk_hashes_for_entry),
            size: Some(original_file_size),
        })
    }

    fn pack_directory_recursive_multithreaded(
        &self,
        disk_current_dir: &Path,
        base_dir_for_ignored_check: &Path, 
        archive_path_prefix: &Path, 
        output_dir: &Path, // Chunk storage
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
                self.pack_directory_recursive_multithreaded(
                    &disk_entry_path, 
                    base_dir_for_ignored_check, 
                    archive_path_prefix, 
                    output_dir, 
                    archive_entries_list, 
                    stats_collector, 
                    ignored_dirs
                )?;
            } else if metadata.is_file() {
                let file_entry = self.process_single_file_multithreaded(
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

            if current_disk_path.is_absolute() {
                return Err(io::Error::new(io::ErrorKind::InvalidInput,
                    format!("Absolute input paths are not directly supported for archive creation: {}. Please use relative paths.", current_disk_path.display())));
            }
            let archive_path_for_this_input_item = current_disk_path.clone();

            if input_metadata.is_file() {
                let file_entry = self.process_single_file_multithreaded(
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
                
                self.pack_directory_recursive_multithreaded(
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

        let archive_metadata = MultiChunkArchiveMetadata {
            chunk_size: self.chunk_size,
            entries: final_archive_entries,
            dedup_stats: Some(final_stats.clone()),
        };

        let metadata_path = output_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufWriter::new(File::create(&metadata_path)?);
        bincode::serialize_into(metadata_file, &archive_metadata).map_err(map_bincode_error)?;

        Ok((output_dir.to_path_buf(), final_stats))
    }
}


impl Chunker for FixedSizeMultiChunker {
    fn name(&self) -> &'static str {
        "fixed-size-64k-multithreaded"
    }

    fn pack(&self, input_paths: &Vec<PathBuf>, output_dir: &Path, ignored_dirs: &Vec<PathBuf>) -> Result<PathBuf, io::Error> {
        self.pack_and_get_stats(input_paths, output_dir, ignored_dirs)
            .map(|(output_path_buf, _stats)| output_path_buf)
    }

    fn unpack(&self, chunk_dir: &Path, output_dir: &Path) -> Result<PathBuf, io::Error> {
        let metadata_path = chunk_dir.join(METADATA_FILE_NAME);
        let metadata_file = BufReader::new(File::open(&metadata_path)?);

        let archive_metadata: MultiChunkArchiveMetadata = bincode::deserialize_from(metadata_file)
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
        let archive_metadata: MultiChunkArchiveMetadata = bincode::deserialize_from(reader)
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

    fn get_archive_stats(&self, archive_dir: &Path) -> Result<Option<ArchiveDedupStats>, io::Error> {
        let metadata_path = archive_dir.join(METADATA_FILE_NAME);
        if !metadata_path.exists() {
            return Err(io::Error::new(io::ErrorKind::NotFound, 
                format!("Metadata file not found in {}", archive_dir.display())));
        }
        let metadata_file = BufReader::new(File::open(&metadata_path)?);
        let archive_metadata: MultiChunkArchiveMetadata = bincode::deserialize_from(metadata_file)
            .map_err(map_bincode_error)?;
        Ok(archive_metadata.dedup_stats)
    }
}
