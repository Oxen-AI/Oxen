//! Gzip decompression helpers.

use std::io::Read;

use flate2::read::GzDecoder;

use crate::error::OxenError;

/// Inflate gzipped `data`, erroring if the inflated output would exceed `max_decompressed` bytes.
/// `max_decompressed` protects against gzip bomb attacks by limiting the decompressed size.
pub fn decompress_gzip_capped(data: &[u8], max_decompressed: u64) -> Result<Vec<u8>, OxenError> {
    let mut decompressed = Vec::with_capacity(data.len());
    GzDecoder::new(data)
        .take(max_decompressed + 1)
        .read_to_end(&mut decompressed)
        .map_err(|e| {
            OxenError::internal_error(format!("Failed to decompress gzipped data: {e}"))
        })?;
    if decompressed.len() as u64 > max_decompressed {
        return Err(OxenError::GzipTooLarge {
            limit: max_decompressed,
        });
    }
    Ok(decompressed)
}
