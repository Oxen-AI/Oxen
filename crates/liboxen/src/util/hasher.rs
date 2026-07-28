use crate::error::OxenError;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::util;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::Path;
use xxhash_rust::xxh3::{Xxh3, xxh3_128};

pub fn hash_buffer(buffer: &[u8]) -> String {
    let val = xxh3_128(buffer);
    format!("{val:x}")
}

pub fn hash_str<S: AsRef<str>>(buffer: S) -> String {
    let buffer = buffer.as_ref().as_bytes();
    hash_buffer(buffer)
}

pub fn hash_str_sha256<S: AsRef<str>>(str: S) -> String {
    let mut hasher = Sha256::new();
    hasher.update(str.as_ref().as_bytes());
    let result = hasher.finalize();
    format!("{result:x}")
}

pub fn hash_buffer_128bit(buffer: &[u8]) -> u128 {
    xxh3_128(buffer)
}

pub fn hash_file_contents_with_retry(path: &Path) -> Result<String, OxenError> {
    // Not sure why some tests were failing....the file didn't get written fast enough
    // So added this method to retry a few times
    let mut timeout = 1;
    let mut retries = 0;
    let total_retries = 5;
    loop {
        match hash_file_contents(path) {
            Ok(hash) => return Ok(hash),
            Err(err) => {
                // sleep and try again
                retries += 1;
                // exponential backoff
                timeout *= 2;
                log::warn!("Error: sleeping {timeout}s failed to hash file {path:?}");
                std::thread::sleep(std::time::Duration::from_secs(timeout));
                if retries > total_retries {
                    return Err(err);
                }
            }
        }
    }
}

pub fn get_hash_given_metadata(
    path: &Path,
    metadata: &std::fs::Metadata,
) -> Result<u128, OxenError> {
    if metadata.len() < 1_000_000_000 {
        hash_small_file_contents(path)
    } else {
        hash_large_file_contents(path)
    }
}

pub fn get_combined_hash(
    oxen_metadata_hash: Option<u128>,
    content_hash: u128,
) -> Result<u128, OxenError> {
    match oxen_metadata_hash {
        Some(oxen_metadata) => {
            let mut hasher = Xxh3::new();
            hasher.update(&content_hash.to_le_bytes());
            hasher.update(&oxen_metadata.to_le_bytes());
            Ok(hasher.digest128())
        }
        None => Ok(content_hash),
    }
}

pub fn maybe_get_metadata_hash(
    oxen_metadata: &Option<GenericMetadata>,
) -> Result<Option<u128>, OxenError> {
    if let Some(metadata) = oxen_metadata {
        let mut hasher = Xxh3::new();
        let metadata_str = serde_json::to_string(&metadata).unwrap();
        hasher.update(metadata_str.as_bytes());
        Ok(Some(hasher.digest128()))
    } else {
        Ok(None)
    }
}

pub fn get_metadata_hash(oxen_metadata: &Option<GenericMetadata>) -> Result<u128, OxenError> {
    let mut hasher = Xxh3::new();
    let metadata_str = serde_json::to_string(&oxen_metadata).unwrap();
    hasher.update(metadata_str.as_bytes());
    Ok(hasher.digest128())
}

pub fn u128_hash_file_contents(path: &Path) -> Result<u128, OxenError> {
    // If file is < 1GB, one-shot hash for speed
    // If file is > 1GB, stream hash to avoid memory overage issues
    let file_size = util::fs::metadata(path)?.len();

    if file_size < 1_000_000_000 {
        hash_small_file_contents(path)
    } else {
        hash_large_file_contents(path)
    }
}

pub fn hash_file_contents(path: &Path) -> Result<String, OxenError> {
    // If file is < 1GB, one-shot hash for speed
    // If file is > 1GB, stream hash to avoid memory overage issues
    let file_size = util::fs::metadata(path)?.len();

    if file_size < 1_000_000_000 {
        Ok(format!("{:x}", hash_small_file_contents(path)?))
    } else {
        Ok(format!("{:x}", hash_large_file_contents(path)?))
    }
}

fn hash_small_file_contents(path: &Path) -> Result<u128, OxenError> {
    match File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut buffer = Vec::new();
            match reader.read_to_end(&mut buffer) {
                Ok(_) => {
                    let result = hash_buffer_128bit(&buffer);
                    Ok(result)
                }
                Err(_) => {
                    eprintln!("Could not read file for hashing {path:?}");
                    Err(OxenError::basic_str("Could not read file for hashing"))
                }
            }
        }
        Err(err) => {
            let err =
                format!("util::hasher::hash_file_contents Could not open file {path:?} {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

fn hash_large_file_contents(path: &Path) -> Result<u128, OxenError> {
    let file = File::open(path).map_err(|err| {
        eprintln!("Could not open file {path:?} due to {err:?}");
        OxenError::basic_str(format!("Could not open file {path:?} due to {err:?}"))
    })?;

    let mut reader = BufReader::new(file);
    let mut hasher = Xxh3::new();
    let mut buffer = [0; 4096];

    loop {
        let count = reader.read(&mut buffer).map_err(|_| {
            eprintln!("Could not read file for hashing {path:?}");
            OxenError::basic_str("Could not read file for hashing")
        })?;

        if count == 0 {
            break;
        }

        hasher.update(&buffer[..count]);
    }

    Ok(hasher.digest128())
}

/// Wraps a `std::io::Read` and feeds every byte successfully read into an `Xxh3` hasher;
/// `digest128()` returns the running XXH3-128. Composable with anything that reads from a
/// `Read` (e.g. `std::io::copy`).
///
/// For an async equivalent, compose `tokio_util::io::InspectReader` with a closure that calls
/// `Xxh3::update` — we don't bundle one here because the current verified helpers hash inside
/// the `spawn_blocking` writer task rather than via a reader-side wrapper.
pub struct HashingReader<'a, R: ?Sized> {
    inner: &'a mut R,
    hasher: Xxh3,
}

impl<'a, R: Read + ?Sized> HashingReader<'a, R> {
    pub fn new(inner: &'a mut R) -> Self {
        Self {
            inner,
            hasher: Xxh3::new(),
        }
    }

    pub fn digest128(&self) -> u128 {
        self.hasher.digest128()
    }
}

impl<R: Read + ?Sized> Read for HashingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.hasher.update(&buf[..n]);
        }
        Ok(n)
    }
}

/// Wraps a `std::io::Write` and feeds every byte successfully written into an `Xxh3` hasher;
/// `digest128()` returns the running XXH3-128. The write-side counterpart to [`HashingReader`],
/// for payloads produced by an encoder that owns the writer and so offers no reader to wrap.
pub struct HashingWriter<'a, W: ?Sized> {
    inner: &'a mut W,
    hasher: Xxh3,
}

impl<'a, W: Write + ?Sized> HashingWriter<'a, W> {
    pub fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            hasher: Xxh3::new(),
        }
    }

    pub fn digest128(&self) -> u128 {
        self.hasher.digest128()
    }
}

impl<W: Write + ?Sized> Write for HashingWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        if n > 0 {
            self.hasher.update(&buf[..n]);
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod hashing_reader_tests {
    use super::*;

    #[test]
    fn sync_reader_matches_one_shot() {
        let payload = b"the quick brown fox jumps over the lazy dog";
        let mut source = &payload[..];
        let mut hashing = HashingReader::new(&mut source);
        let mut sink = Vec::new();
        hashing.read_to_end(&mut sink).unwrap();
        assert_eq!(sink, payload);
        assert_eq!(hashing.digest128(), xxh3_128(payload));
    }

    #[test]
    fn sync_reader_empty_input() {
        let mut source: &[u8] = &[];
        let mut hashing = HashingReader::new(&mut source);
        let mut sink = Vec::new();
        hashing.read_to_end(&mut sink).unwrap();
        assert!(sink.is_empty());
        assert_eq!(hashing.digest128(), xxh3_128(b""));
    }
}

#[cfg(test)]
mod hashing_writer_tests {
    use super::*;

    #[test]
    fn sync_writer_matches_one_shot() -> Result<(), OxenError> {
        let payload = b"the quick brown fox jumps over the lazy dog";
        let mut sink = Vec::new();
        let digest = {
            let mut hashing = HashingWriter::new(&mut sink);
            hashing.write_all(payload)?;
            hashing.flush()?;
            hashing.digest128()
        };
        assert_eq!(digest, xxh3_128(payload));
        assert_eq!(sink, payload);
        Ok(())
    }

    /// Bytes arriving across many `write` calls hash the same as the concatenation in one shot —
    /// the property every streaming caller relies on.
    #[test]
    fn sync_writer_accumulates_across_writes() -> Result<(), OxenError> {
        let chunks: [&[u8]; 3] = [b"hello ", b"brave ", b"world"];
        let expected = chunks.concat();
        let mut sink = Vec::new();
        let digest = {
            let mut hashing = HashingWriter::new(&mut sink);
            for chunk in chunks {
                hashing.write_all(chunk)?;
            }
            hashing.digest128()
        };
        assert_eq!(digest, xxh3_128(&expected));
        assert_eq!(sink, expected);
        Ok(())
    }

    #[test]
    fn sync_writer_empty_input() -> Result<(), OxenError> {
        let mut sink = Vec::new();
        let digest = HashingWriter::new(&mut sink).digest128();
        assert_eq!(digest, xxh3_128(b""));
        assert!(sink.is_empty());
        Ok(())
    }

    /// A short write hashes only the bytes the inner writer accepted; hashing the whole buffer
    /// would digest bytes that never reached the file.
    #[test]
    fn sync_writer_hashes_only_accepted_bytes() -> Result<(), OxenError> {
        struct ShortWriter {
            written: Vec<u8>,
        }
        impl Write for ShortWriter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let n = buf.len().min(4);
                self.written.extend_from_slice(&buf[..n]);
                Ok(n)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut inner = ShortWriter {
            written: Vec::new(),
        };
        let digest = {
            let mut hashing = HashingWriter::new(&mut inner);
            let accepted = hashing.write(b"0123456789")?;
            assert_eq!(accepted, 4, "inner writer should accept only 4 bytes");
            hashing.digest128()
        };
        assert_eq!(inner.written, b"0123");
        assert_eq!(digest, xxh3_128(b"0123"));
        Ok(())
    }
}
