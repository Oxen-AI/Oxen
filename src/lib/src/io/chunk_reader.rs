use polars::io::mmap::MmapBytesReader;
// use polars::io::mmap::ReaderBytes;

use crate::core::v0_19_0::index::file_chunker::ChunkShardManager;
use crate::core::v0_19_0::index::file_chunker::CHUNK_SIZE;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::LocalRepository;

use std::io::Read;
use std::io::Seek;

pub struct ChunkReader {
    pub repo: LocalRepository,
    node: FileNode,
    offset: u64,
    csm: ChunkShardManager,
    // data: Vec<u8>,
}

impl ChunkReader {
    pub fn new(repo: LocalRepository, node: FileNode) -> Result<Self, OxenError> {
        // let num_bytes = node.num_bytes as usize;
        // let mut data: Vec<u8> = vec![0; num_bytes];

        // log::debug!("reading all data... {num_bytes}");

        // let mut total_read = 0;
        // for chunk_hash in &node.chunk_hashes {
        //     let hash_str = format!("{:x}", chunk_hash);
        //     let chunk_path = util::fs::chunk_path(&repo, &hash_str);

        //     let mut file = std::fs::File::open(chunk_path).unwrap();
        //     let mut file_data = Vec::new();
        //     file.read_to_end(&mut file_data).unwrap();

        //     data[total_read..total_read+file_data.len()].copy_from_slice(&file_data);

        //     total_read += file_data.len();
        //     log::debug!("read data... {total_read}/{num_bytes}");
        // }

        let csm = ChunkShardManager::new(&repo)?;
        Ok(Self {
            repo,
            node,
            offset: 0,
            csm,
            // data,
        })
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        log::debug!(
            "--START-- read {} from chunked file at offset {} / {}",
            buf.len(),
            self.offset,
            self.node.num_bytes
        );
        if self.offset >= self.node.num_bytes {
            log::debug!(
                "Reached end of file at offset: {} >= {}",
                self.offset,
                self.node.num_bytes
            );
            self.offset = 0;
            return Ok(0);
        }

        // FileNode has a vector of chunks
        // Each chunk has a size of CHUNK_SIZE
        // We need to read the chunk at the offset and copy the data to the buffer

        // chunk_index is which file chunk we are reading
        let mut chunk_index = self.offset / CHUNK_SIZE as u64;
        // chunk_offset is the offset within the chunk
        let mut chunk_offset = self.offset % CHUNK_SIZE as u64;

        log::debug!("Chunk index: {:?} offset {:?}", chunk_index, chunk_offset);
        log::debug!("Chunk hashes len {:?}", self.node.chunk_hashes.len());

        // read chunks until we fill the buffer
        let mut total_read = 0;
        while total_read < buf.len() as u64 && chunk_index < self.node.chunk_hashes.len() as u64 {
            log::debug!("-start- read {:?}/{}", total_read, buf.len());
            log::debug!(
                "chunk_index {}/{} chunk_offset {:?}",
                chunk_index,
                self.node.chunk_hashes.len(),
                chunk_offset
            );

            // Find the hashed chunk file
            let chunk_hash = self.node.chunk_hashes[chunk_index as usize];
            let chunk_data = self.csm.read_chunk(chunk_hash).unwrap();
            let chunk_data_len = chunk_data.len() as u64;

            log::debug!("Chunk file size {:?}", chunk_data_len);

            let bytes_to_copy =
                std::cmp::min(buf.len() as u64 - total_read, chunk_data_len - chunk_offset);

            log::debug!("Bytes to copy {:?}", bytes_to_copy);

            if bytes_to_copy == 0 {
                break;
            }

            buf[total_read as usize..(total_read + bytes_to_copy) as usize].copy_from_slice(
                &chunk_data[chunk_offset as usize..(chunk_offset + bytes_to_copy) as usize],
            );

            total_read += bytes_to_copy;
            chunk_offset += bytes_to_copy;

            if chunk_offset >= CHUNK_SIZE as u64 {
                chunk_offset = 0;
                chunk_index += 1;
            }

            self.offset += bytes_to_copy;
            log::debug!("Total read {:?}/{}", total_read, buf.len());
            log::debug!("-end- Offset {:?} / {}", self.offset, self.node.num_bytes);
        }

        log::debug!("--END-- Total read {:?}", total_read);

        Ok(total_read as usize)
    }
}

impl Seek for ChunkReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        log::debug!("Seek in chunked file {:?} / {}", pos, self.node.num_bytes);
        self.offset = match pos {
            std::io::SeekFrom::Start(offset) => offset,
            std::io::SeekFrom::Current(offset) => self.offset + offset as u64,
            std::io::SeekFrom::End(offset) => (self.node.num_bytes as i64 + offset) as u64,
        };
        log::debug!("New offset {:?}", self.offset);
        Ok(self.offset)
    }
}

impl MmapBytesReader for ChunkReader {
    // fn to_bytes(&self) -> Option<&[u8]> {
    //     Some(&self.data)
    // }
}
