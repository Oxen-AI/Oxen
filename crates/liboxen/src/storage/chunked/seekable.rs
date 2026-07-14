//! [`SeekableVersionReader`]: a sync `Read + Seek` over the reconstructed bytes of
//! a chunked version.
//!
//! Block-backed versions have no contiguous file, so consumers that need random
//! access (eager Parquet/IPC readers, format sniffers) read through this adapter:
//! `seek` just moves the cursor with no IO, and each `read` resolves only the chunk
//! covering the cursor — a parquet footer read decodes a few tail chunks, never the
//! whole file. The most recently decoded chunk is cached, so sequential reads and
//! the read-a-little/seek-a-little patterns of columnar readers don't re-decode per
//! call.
//!
//! Sync on purpose: columnar readers are sync, and the consumer runs inside one
//! `spawn_blocking` per `docs/async_policy.md`.

use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::error::OxenError;

use super::block_engine::{BlockEngine, ChunkPayloadCursor};
use super::manifest::ChunkManifest;

/// Sync `Read + Seek` over a chunked version's logical bytes.
pub struct SeekableVersionReader {
    engine: Arc<BlockEngine>,
    manifest: ChunkManifest,
    cursor: ChunkPayloadCursor,
    /// Logical position in the reconstructed file. May sit past EOF (like `File`);
    /// reads there return 0.
    pos: u64,
    /// The most recently decoded chunk: `(chunk_index, raw_bytes)`.
    cached_chunk: Option<(usize, Vec<u8>)>,
}

impl SeekableVersionReader {
    pub fn new(engine: Arc<BlockEngine>, manifest: ChunkManifest) -> Self {
        Self {
            engine,
            manifest,
            cursor: ChunkPayloadCursor::new(),
            pos: 0,
            cached_chunk: None,
        }
    }

    /// Logical size of the reconstructed file.
    pub fn file_size(&self) -> u64 {
        self.manifest.file_size
    }

    /// The decoded bytes of chunk `idx`, from cache or via one block range read.
    fn chunk_bytes(&mut self, idx: usize) -> Result<&[u8], OxenError> {
        if self.cached_chunk.as_ref().map(|(i, _)| *i) != Some(idx) {
            let raw = self
                .cursor
                .read_chunk(&self.engine, &self.manifest.chunks[idx])?;
            self.cached_chunk = Some((idx, raw));
        }
        // The branch above guarantees the cache is filled; re-match instead of unwrap.
        match &self.cached_chunk {
            Some((_, raw)) => Ok(raw),
            None => Err(OxenError::basic_str("chunk cache unexpectedly empty")),
        }
    }
}

impl Read for SeekableVersionReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let Some(idx) = self.manifest.chunk_index_at(self.pos) else {
            return Ok(0); // at or past EOF
        };
        let chunk_offset = self.manifest.chunks[idx].offset;
        let start_in_chunk = (self.pos - chunk_offset) as usize;
        let raw = self.chunk_bytes(idx).map_err(std::io::Error::other)?;
        let n = out.len().min(raw.len() - start_in_chunk);
        out[..n].copy_from_slice(&raw[start_in_chunk..start_in_chunk + n]);
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for SeekableVersionReader {
    fn seek(&mut self, target: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match target {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(delta) => self.manifest.file_size.checked_add_signed(delta),
            SeekFrom::Current(delta) => self.pos.checked_add_signed(delta),
        };
        match new_pos {
            Some(pos) => {
                self.pos = pos;
                Ok(pos)
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EntryDataType;
    use crate::storage::chunked::policy::encode_policy;

    struct TestReader {
        _dir: tempfile::TempDir,
        reader: SeekableVersionReader,
        data: Vec<u8>,
    }

    /// Ingest deterministic multi-chunk data and open a seekable reader over it.
    fn test_reader(len: usize) -> TestReader {
        let dir = tempfile::tempdir().expect("create temp dir");
        let engine = BlockEngine::open(&dir.path().join("blocks"), &dir.path().join("chunk_index"))
            .expect("open block engine");

        let mut data = Vec::with_capacity(len + 64);
        let mut state = 0xC0FFEEu64;
        let mut row = 0u64;
        while data.len() < len {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            data.extend_from_slice(format!("{row},image_{state:x}.jpg\n").as_bytes());
            row += 1;
        }
        data.truncate(len);

        let policy = encode_policy(&EntryDataType::Tabular, "csv");
        let manifest = engine
            .ingest(&mut &data[..], &policy)
            .expect("ingest test data");
        assert!(manifest.chunks.len() > 2, "test data must span chunks");

        let reader = SeekableVersionReader::new(Arc::new(engine), manifest);
        TestReader {
            _dir: dir,
            reader,
            data,
        }
    }

    /// Sequential read of the whole reader equals the source bytes.
    #[test]
    fn sequential_read_matches_source() {
        let mut t = test_reader(1_000_000);
        let mut out = Vec::new();
        t.reader.read_to_end(&mut out).expect("read to end");
        assert_eq!(out, t.data);
    }

    /// Seeks from Start/End/Current land on the right bytes, including reads that
    /// straddle chunk boundaries — the access pattern of a parquet footer read.
    #[test]
    fn seeks_land_on_right_bytes() {
        let mut t = test_reader(1_000_000);
        let len = t.data.len() as u64;

        let mut buf = [0u8; 64];
        // Footer-style: last 8 bytes via SeekFrom::End.
        assert_eq!(t.reader.seek(SeekFrom::End(-8)).expect("seek"), len - 8);
        t.reader.read_exact(&mut buf[..8]).expect("read tail");
        assert_eq!(&buf[..8], &t.data[t.data.len() - 8..]);

        // Straddle the first chunk boundary.
        let boundary = t.reader.manifest.chunks[0].len as u64;
        t.reader.seek(SeekFrom::Start(boundary - 10)).expect("seek");
        t.reader.read_exact(&mut buf[..20]).expect("read straddle");
        assert_eq!(
            &buf[..20],
            &t.data[(boundary - 10) as usize..(boundary + 10) as usize]
        );

        // Relative seek backwards from the current position.
        let pos = t.reader.seek(SeekFrom::Current(-20)).expect("seek");
        assert_eq!(pos, boundary - 10);
        t.reader.read_exact(&mut buf[..1]).expect("read byte");
        assert_eq!(buf[0], t.data[(boundary - 10) as usize]);

        // First byte.
        t.reader.seek(SeekFrom::Start(0)).expect("seek");
        t.reader.read_exact(&mut buf[..1]).expect("read first");
        assert_eq!(buf[0], t.data[0]);
    }

    /// Reads at and past EOF return 0 bytes; a negative absolute position errors.
    #[test]
    fn eof_and_invalid_seeks() {
        let mut t = test_reader(600_000);
        let len = t.data.len() as u64;

        t.reader.seek(SeekFrom::Start(len)).expect("seek to EOF");
        let mut buf = [0u8; 16];
        assert_eq!(t.reader.read(&mut buf).expect("read at EOF"), 0);

        t.reader
            .seek(SeekFrom::Start(len + 1000))
            .expect("seek past EOF is allowed");
        assert_eq!(t.reader.read(&mut buf).expect("read past EOF"), 0);

        assert!(t.reader.seek(SeekFrom::End(-(len as i64) - 1)).is_err());
        t.reader.seek(SeekFrom::Start(0)).expect("seek home");
        assert!(t.reader.seek(SeekFrom::Current(-1)).is_err());
    }

    /// Random-access reads match slices of the source (fixed seeds).
    #[test]
    fn random_access_matches_source() {
        let mut t = test_reader(900_000);
        let len = t.data.len();
        let mut state = 0x5EED5EEDu64;
        for _ in 0..200 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let offset = (state as usize) % len;
            let size = ((state >> 32) as usize % 4096).min(len - offset);
            let mut buf = vec![0u8; size];
            t.reader.seek(SeekFrom::Start(offset as u64)).expect("seek");
            t.reader.read_exact(&mut buf).expect("read");
            assert_eq!(buf, &t.data[offset..offset + size], "range {offset}+{size}");
        }
    }
}
