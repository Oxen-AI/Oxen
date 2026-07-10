//! Gzip decompression helpers.

use std::io::Read;

use flate2::read::GzDecoder;

use crate::error::OxenError;

/// Inflate gzipped `data`, erroring if the inflated output would exceed `max_decompressed` bytes.
/// `max_decompressed` protects against gzip bomb attacks by limiting the decompressed size.
pub fn decompress_gzip_capped(data: &[u8], max_decompressed: u64) -> Result<Vec<u8>, OxenError> {
    let mut decoder = GzDecoder::new(data).take(max_decompressed + 1);
    let mut decompressed = Vec::with_capacity(data.len());
    decoder.read_to_end(&mut decompressed).map_err(|e| {
        OxenError::internal_error(format!("Failed to decompress gzipped data: {e}"))
    })?;
    let decompressed_size = decompressed.len() as u64;
    if decompressed_size > max_decompressed {
        return Err(OxenError::internal_error(format!(
            "Decompressed size {decompressed_size} exceeds the {max_decompressed} byte limit"
        )));
    }
    Ok(decompressed)
}
