//! Chunks files in order to deduplicate chunks across large files that are changed
//!
//! The idea here is that we can split the file into chunks and hash the chunks
//! These chunks are stored at the bottom of the merkle tree
//!
//! It saves us:
//! * Storage across commits
//! * Time to upload changes
//!
//! Need to balance this with:
//! * Time to reconstruct the file
//! * Time to query the file
//!

use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocksdb::DBWithThreadMode;
use rocksdb::SingleThreaded;

use crate::constants::CHUNKS_DIR;
use crate::constants::TREE_DIR;
use crate::core::db;
use crate::core::db::key_val::u128_kv_db;
use crate::error::OxenError;
use crate::model::CommitEntry;
use crate::model::LocalRepository;
use crate::util;
use crate::util::hasher;
use crate::util::progress_bar::oxen_progress_bar;
use crate::util::progress_bar::ProgressBarType;

// static chunk size of 16kb
pub const CHUNK_SIZE: usize = 16 * 1024;
const SHARD_CAPACITY: usize = 1000 * CHUNK_SIZE;

/// Chunk Shard DB keeps track of which hash belongs in which shard file
/// Is a simple kv pair from u128 hash to a u32 shard file number
/// Each shard file contains ~1000 hashes and their associated chunk data.
/// When a shard gets too big we close it and start a new one.
pub struct ChunkShardDB {
    db: DBWithThreadMode<SingleThreaded>,
}

impl ChunkShardDB {
    fn db_path(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(TREE_DIR)
            .join(Path::new(CHUNKS_DIR))
    }

    pub fn new(repo: &LocalRepository) -> Result<Self, OxenError> {
        let path = Self::db_path(repo);
        let opts = db::key_val::opts::default();
        let db = DBWithThreadMode::open(&opts, dunce::simplified(&path))?;
        Ok(Self { db })
    }

    pub fn has_key(&self, hash: u128) -> bool {
        u128_kv_db::has_key(&self.db, hash)
    }

    pub fn get(&self, hash: u128) -> Result<Option<u32>, OxenError> {
        u128_kv_db::get(&self.db, hash)
    }

    pub fn put(&self, hash: u128, shard_idx: u32) -> Result<(), OxenError> {
        let value = shard_idx.to_le_bytes();
        u128_kv_db::put_buf(&self.db, hash, &value)?;
        Ok(())
    }
}

/// ChunkShardIndex is the index at the top of the shard file
#[derive(Serialize, Deserialize)]
pub struct ChunkShardIndex {
    // hash -> (offset, size)
    pub hash_offsets: HashMap<u128, (u32, u32)>,
}

impl Default for ChunkShardIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkShardIndex {
    pub fn new() -> Self {
        Self {
            hash_offsets: HashMap::new(),
        }
    }
}

pub struct ChunkShardFile {
    pub path: PathBuf,
    pub file: File,
    pub index: ChunkShardIndex,
    pub data_start: usize,
    pub offset: usize,
    pub data: Vec<u8>,
}

impl ChunkShardFile {
    pub fn db_path(repo: &LocalRepository) -> PathBuf {
        util::fs::oxen_hidden_dir(&repo.path)
            .join(TREE_DIR)
            .join("shards")
    }

    pub fn shard_path(repo: &LocalRepository, file_idx: u32) -> PathBuf {
        let path = Self::db_path(repo);
        path.join(format!("shard_{}", file_idx))
    }

    pub fn shard_idx(path: &Path) -> u32 {
        let file_stem = path.file_stem().unwrap();
        let file_stem_str = file_stem.to_str().unwrap();
        let idx_str = file_stem_str.split('_').nth(1).unwrap();
        idx_str.parse::<u32>().unwrap()
    }

    pub fn open(repo: &LocalRepository, file_idx: u32) -> Result<ChunkShardFile, OxenError> {
        log::debug!("Opening shard file: {:?}", Self::shard_path(repo, file_idx));
        let path = Self::shard_path(repo, file_idx);
        let file = File::open(&path)?;
        // allocate the data buffer
        let shard_file = ChunkShardFile {
            path,
            file,
            index: ChunkShardIndex::new(),
            data_start: 0,
            offset: 0,
            data: Vec::new(),
        };
        Ok(shard_file)
    }

    pub fn create(repo: &LocalRepository, file_idx: u32) -> Result<ChunkShardFile, OxenError> {
        log::debug!(
            "Creating shard file: {:?}",
            Self::shard_path(repo, file_idx)
        );
        let path = Self::shard_path(repo, file_idx);
        let file = File::create(&path)?;
        let index = ChunkShardIndex::new();
        Ok(ChunkShardFile {
            path,
            file,
            index,
            data_start: 0,
            offset: 0,
            data: vec![0; SHARD_CAPACITY],
        })
    }

    pub fn has_capacity(&self, buffer_len: usize) -> bool {
        (self.offset + buffer_len) < SHARD_CAPACITY
    }

    pub fn add_buffer(&mut self, hash: u128, buffer: &[u8]) -> Result<(), OxenError> {
        if !self.has_capacity(buffer.len()) {
            return Err(OxenError::basic_str("Shard is full"));
        }

        self.index
            .hash_offsets
            .insert(hash, (self.offset as u32, buffer.len() as u32));
        self.data[self.offset..self.offset + buffer.len()].copy_from_slice(buffer);
        self.offset += buffer.len();
        Ok(())
    }

    pub fn get_buffer(&mut self, hash: u128) -> Result<Vec<u8>, OxenError> {
        let offset = self.index.hash_offsets[&hash];
        let start = self.data_start + offset.0 as usize;
        log::debug!(
            "Reading chunk from shard: [{:?}] for hash: {} at start {} offset: {} size: {}",
            self.path,
            hash,
            start,
            offset.0,
            offset.1
        );
        self.file.seek(SeekFrom::Start(start as u64))?;
        let mut buffer = vec![0u8; offset.1 as usize];
        let bytes_read = self.file.read(&mut buffer)?;
        log::debug!("read {} bytes", bytes_read);
        Ok(buffer)
    }

    pub fn read_index(&mut self) -> Result<(), OxenError> {
        // read the index size
        let mut buffer = [0u8; 4]; // u32 is 4 bytes
        let bytes_read = self.file.read(&mut buffer)?;
        log::debug!("read {} bytes", bytes_read);
        let index_size = u32::from_le_bytes(buffer) as usize;

        let mut index_bytes = vec![0u8; index_size];
        let bytes_read = self.file.read(&mut index_bytes)?;
        log::debug!("read {} bytes", bytes_read);
        self.index = bincode::deserialize(&index_bytes)?;
        self.data_start = index_size + 8; // 4 for size of index and 4 for data size

        log::debug!(
            "Read index of size {} with {:?} hashes",
            bytesize::ByteSize::b(index_size as u64),
            self.index.hash_offsets.len()
        );

        Ok(())
    }

    pub fn read_data(&mut self) -> Result<(), OxenError> {
        // read the buffer size
        let mut buffer = [0u8; 4]; // u32 is 4 bytes
        let bytes_read = self.file.read(&mut buffer)?;
        log::debug!("read {} bytes", bytes_read);
        self.offset = u32::from_le_bytes(buffer) as usize;

        log::debug!("read data with {:?} bytes", self.offset);

        // read the buffer
        let mut buffer = vec![0u8; self.offset];
        let bytes_read = self.file.read(&mut buffer)?;
        log::debug!("read {} bytes", bytes_read);

        // Allocate the full size for the buffer
        self.data = vec![0u8; SHARD_CAPACITY];
        // Copy the data into the buffer
        self.data[..self.offset].copy_from_slice(&buffer);
        Ok(())
    }

    pub fn save(&mut self) -> Result<(), OxenError> {
        log::debug!("Saving shard file: {:?}", self.path);
        // Overwrite existing file
        self.file = File::create(&self.path)?;
        // write the index to the file
        let index_bytes = bincode::serialize(&self.index)?;
        log::debug!("Saving shard index: {:?}", index_bytes.len());
        // write index size to the file
        self.file
            .write_all(&(index_bytes.len() as u32).to_le_bytes())?;
        // write index to the file
        self.file.write_all(&index_bytes)?;
        // write the data size
        self.file.write_all(&(self.offset as u32).to_le_bytes())?;
        // write only the data that has been written
        let data = &self.data[..self.offset];
        log::debug!("Saving shard data: {:?}", data.len());
        self.file.write_all(data)?;
        self.file.sync_all()?;
        log::debug!("Saved shard file: {:?}", self.path);
        Ok(())
    }
}

/// ChunkShardManager reads how many shards we have and moves to the next one if the current one is full
pub struct ChunkShardManager {
    repo: LocalRepository,
    db: ChunkShardDB,
    current_idx: i32,
    current_file: Option<ChunkShardFile>,
}

impl ChunkShardManager {
    pub fn new(repo: &LocalRepository) -> Result<Self, OxenError> {
        let chunk_db = ChunkShardDB::new(repo)?;
        Ok(Self {
            repo: repo.clone(),
            current_idx: -1,
            current_file: None,
            db: chunk_db,
        })
    }

    pub fn open_for_write(&mut self) -> Result<(), OxenError> {
        log::debug!("Opening chunk shard manager");
        // find all the current shard files
        let shard_dir = ChunkShardFile::db_path(&self.repo);
        if !shard_dir.exists() {
            util::fs::create_dir_all(&shard_dir)?;
        }
        let mut shard_paths: Vec<PathBuf> = std::fs::read_dir(&shard_dir)?
            .map(|x| x.unwrap().path())
            .collect::<Vec<PathBuf>>();

        // sort the shard paths by the file index
        shard_paths.sort_by(|a, b| {
            let a_idx = ChunkShardFile::shard_idx(a);
            let b_idx = ChunkShardFile::shard_idx(b);
            a_idx.cmp(&b_idx)
        });

        let mut current_idx = 0;
        let mut current_file: Option<ChunkShardFile> = None;
        for path in shard_paths {
            log::debug!("Opening shard file: {:?}", path);
            let file_idx = ChunkShardFile::shard_idx(&path);
            if let Ok(mut shard_file) = ChunkShardFile::open(&self.repo, file_idx) {
                shard_file.read_index()?;
                log::debug!("Opened shard file: {:?}", path);
                if shard_file.has_capacity(CHUNK_SIZE) {
                    log::debug!("Shard [{}] has capacity, using it", file_idx);
                    shard_file.read_data()?;
                    current_idx = file_idx;
                    current_file = Some(shard_file);
                }
            }
        }

        if current_file.is_none() {
            log::debug!(
                "Creating new shard file: {:?}",
                ChunkShardFile::shard_path(&self.repo, current_idx)
            );
            current_file = Some(ChunkShardFile::create(&self.repo, current_idx)?);
        }

        log::debug!("Current shard index: {:?}", current_idx);
        self.current_idx = current_idx as i32; // can always cast u32 to i32
        self.current_file = current_file;
        Ok(())
    }

    pub fn has_chunk(&self, hash: u128) -> bool {
        self.db.has_key(hash)
    }

    pub fn read_chunk(&mut self, hash: u128) -> Result<Vec<u8>, OxenError> {
        let shard_idx = self
            .db
            .get(hash)?
            .ok_or(OxenError::basic_str("Chunk not found"))?;
        log::debug!(
            "Reading chunk from shard: [{}] for hash: {}",
            shard_idx,
            hash
        );
        // Cache the current shard file for faster reads of the same shard
        if shard_idx as i32 != self.current_idx {
            self.current_file = Some(ChunkShardFile::open(&self.repo, shard_idx)?);
            self.current_file.as_mut().unwrap().read_index()?;
            self.current_idx = shard_idx as i32;
        }
        let buffer = self.current_file.as_mut().unwrap().get_buffer(hash)?;
        Ok(buffer)
    }

    pub fn write_chunk(&mut self, hash: u128, chunk: &[u8]) -> Result<u32, OxenError> {
        let Some(current_file) = self.current_file.as_mut() else {
            return Err(OxenError::basic_str("Not open for writing"));
        };

        // log::debug!("Writing chunk {} -> {} to shard: [{}]", hash, chunk.len(), self.current_idx);
        // Save the lookup from hash to shard_idx
        self.db.put(hash, self.current_idx as u32)?;
        // Add the chunk to the current file
        current_file.add_buffer(hash, chunk)?;
        // If the file is full, save it and start a new one
        if !current_file.has_capacity(chunk.len()) {
            log::debug!(
                "Shard file is full with {} saving {}",
                current_file.offset,
                self.current_idx
            );
            current_file.save()?;
            self.current_idx += 1;
            log::debug!("Shard file is full, starting new one {}", self.current_idx);
            self.current_file = Some(ChunkShardFile::create(&self.repo, self.current_idx as u32)?);
        }
        Ok(self.current_idx as u32)
    }

    pub fn save_all(&mut self) -> Result<(), OxenError> {
        let Some(current_file) = self.current_file.as_mut() else {
            return Err(OxenError::basic_str("Not open for writing"));
        };

        current_file.save()?;
        Ok(())
    }
}

pub struct FileChunker {
    repo: LocalRepository,
}

impl FileChunker {
    pub fn new(repo: &LocalRepository) -> Self {
        Self { repo: repo.clone() }
    }

    pub fn save_chunks(
        &self,
        entry: &CommitEntry,
        csm: &mut ChunkShardManager,
    ) -> Result<Vec<u128>, OxenError> {
        let version_file = util::fs::version_path(&self.repo, entry);
        let mut read_file = File::open(&version_file)?;

        // Create a progress bar for larger files
        let mut progress_bar: Option<Arc<ProgressBar>> =
            if entry.num_bytes > (CHUNK_SIZE * 10) as u64 {
                Some(oxen_progress_bar(entry.num_bytes, ProgressBarType::Bytes))
            } else {
                None
            };

        // Read/Write chunks
        let mut buffer = vec![0; CHUNK_SIZE]; // 16KB buffer
        let mut hashes: Vec<u128> = Vec::new();
        let mut num_new_chunks = 0;
        while let Ok(bytes_read) = read_file.read(&mut buffer) {
            if bytes_read == 0 {
                break; // End of file
            }
            // Shrink buffer to size of bytes read
            buffer.truncate(bytes_read);

            // Save the chunk to the database
            let hash = hasher::hash_buffer_128bit(&buffer);
            if !csm.has_chunk(hash) {
                csm.write_chunk(hash, &buffer)?;
                num_new_chunks += 1;
            }
            hashes.push(hash);
            if let Some(progress_bar) = progress_bar.as_mut() {
                progress_bar.inc(bytes_read as u64);
            }
        }
        if entry.num_bytes > CHUNK_SIZE as u64 {
            println!(
                "Saved {} new chunks out of {} for {:?}",
                num_new_chunks,
                hashes.len(),
                entry.path
            );
        }

        // Flush the progress to disk
        csm.save_all()?;

        Ok(hashes)
    }
}
