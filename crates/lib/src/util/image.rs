use crate::error::OxenError;
use image::imageops;
use std::path::Path;

pub fn resize_and_save(
    src: impl AsRef<Path>,
    dst: impl AsRef<Path>,
    dims: u32,
) -> Result<(), OxenError> {
    let src_path = src.as_ref();
    let img = match image::open(src_path) {
        Ok(img) => img,
        Err(e) => return Err(OxenError::basic_str(e.to_string())),
    };

    // If the path ends in .jpg or .jpeg, convert to RGB
    let ext = src_path
        .extension()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if ext == "jpg" || ext == "jpeg" {
        let img = img.to_rgb8();
        let resized = imageops::resize(&img, dims, dims, imageops::Nearest);
        resized.save(dst)?;
    } else {
        let resized = imageops::resize(&img, dims, dims, imageops::Nearest);
        resized.save(dst)?;
    }

    Ok(())
}
