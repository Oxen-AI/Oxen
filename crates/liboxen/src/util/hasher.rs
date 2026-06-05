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
