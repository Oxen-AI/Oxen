//! Resized copies of image version files and thumbnails of video version files, made on request
//! and cached in the version store beside the file they come from.

use std::io::Cursor;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(feature = "ffmpeg")]
use std::sync::OnceLock;

use bytes::Bytes;
use futures::StreamExt;
use image::imageops::FilterType;
use image::{DynamicImage, ImageFormat};
#[cfg(feature = "ffmpeg")]
use image::{ImageBuffer, Rgb};
use tokio_stream::Stream;
#[cfg(feature = "ffmpeg")]
use video_rs::decode::Decoder;

use crate::constants::MAX_RESIZE_DIMENSION;
use crate::error::OxenError;
use crate::model::metadata::metadata_image::ImgResize;
use crate::model::metadata::metadata_video::VideoThumbnail;
use crate::storage::version_store::VersionStore;

pub async fn handle_image_resize(
    version_store: Arc<dyn VersionStore>,
    file_hash: String,
    file_path: &Path,
    img_resize: ImgResize,
) -> Result<
    (
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        u64,
    ),
    OxenError,
> {
    log::debug!("img_resize {img_resize:?}");
    check_resize_dimensions(img_resize.width, img_resize.height)?;
    let width = img_resize.width.map(|w| w.to_string()).unwrap_or_default();
    let height = img_resize.height.map(|h| h.to_string()).unwrap_or_default();
    // The extension picks the cached copy's format, so a path with none gets a name no format
    // matches, which refuses the resize.
    let extension = file_path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default();
    let derived_filename = format!("{width}x{height}.{extension}");

    let stream =
        resize_cache_image_version_store(version_store, &file_hash, &derived_filename, img_resize)
            .await?;
    log::debug!("Got the resized image! {derived_filename}");

    Ok(stream)
}

/// Refuses a requested width or height of zero or over [`MAX_RESIZE_DIMENSION`].
fn check_resize_dimensions(width: Option<u32>, height: Option<u32>) -> Result<(), OxenError> {
    for (param, value) in [("width", width), ("height", height)] {
        if let Some(value) = value
            && !(1..=MAX_RESIZE_DIMENSION).contains(&value)
        {
            return Err(OxenError::InvalidResizeDimension { param, value });
        }
    }
    Ok(())
}

/// `img` resized as a request asks, never past its own size. A width and a height give the exact
/// size, and one of them alone gives a square the image is fitted into, keeping its aspect ratio.
fn resize_within_source(
    img: DynamicImage,
    width: Option<u32>,
    height: Option<u32>,
) -> DynamicImage {
    let filter = FilterType::Lanczos3;
    match (width, height) {
        (Some(width), Some(height)) => {
            img.resize_exact(width.min(img.width()), height.min(img.height()), filter)
        }
        (Some(side), None) | (None, Some(side)) => {
            if side >= img.width().max(img.height()) {
                img
            } else {
                img.resize(side, side, filter)
            }
        }
        (None, None) => img,
    }
}

/// The name a video thumbnail of the requested size is cached under. A requested timestamp has no
/// part in it, since every thumbnail is taken from the video's first frame.
#[cfg(feature = "ffmpeg")]
fn video_thumbnail_filename(width: Option<u32>, height: Option<u32>) -> String {
    let extension = "jpg";
    let (width_str, height_str) = match (width, height) {
        (Some(w), Some(h)) => (w.to_string(), h.to_string()),
        (Some(w), None) => (w.to_string(), "auto".to_string()),
        (None, Some(h)) => ("auto".to_string(), h.to_string()),
        (None, None) => ("320".to_string(), "240".to_string()),
    };
    format!("thumbnail_{width_str}x{height_str}.{extension}")
}

async fn resize_cache_image_version_store(
    version_store: Arc<dyn VersionStore>,
    img_hash: &str,
    derived_filename: &str,
    resize: ImgResize,
) -> Result<
    (
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        u64,
    ),
    OxenError,
> {
    if version_store
        .derived_version_exists(img_hash, derived_filename)
        .await?
    {
        log::debug!("In the resize cache! {derived_filename}");
        let content_length = version_store
            .get_version_derived_size(img_hash, derived_filename)
            .await?;
        let stream = version_store
            .get_version_derived_stream(img_hash, derived_filename)
            .await?;
        return Ok((stream.boxed(), content_length));
    }

    log::debug!("create resized image {derived_filename} from hash {img_hash}");
    // Refuse a copy the server cannot write before reading or decoding anything.
    let output_format = ImageFormat::from_path(derived_filename)?;
    let image_data = version_store.get_version(img_hash).await?;

    let buf = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, OxenError> {
        let img = image::load_from_memory(&image_data)?;
        let mut buf = Vec::new();
        resize_within_source(img, resize.width, resize.height)
            .write_to(&mut Cursor::new(&mut buf), output_format)?;
        Ok(buf)
    })
    .await??;
    // Pre-`Bytes::from` once so we can share the same allocation between the version-store write
    // and the streamed response without copying. `Bytes::clone` is an atomic refcount bump.
    let bytes = Bytes::from(buf);
    let content_length = bytes.len() as u64;
    version_store
        .store_version_derived(img_hash, derived_filename, bytes.clone())
        .await?;

    let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        futures::stream::once(async move { Ok(bytes) }).boxed();

    Ok((stream, content_length))
}

/// Initialize the FFmpeg/libav globals once per process for video decoding.
#[cfg(feature = "ffmpeg")]
fn ensure_video_decoding_initialized() -> Result<(), OxenError> {
    static INIT: OnceLock<Result<(), String>> = OnceLock::new();
    INIT.get_or_init(|| video_rs::init().map_err(|e| e.to_string()))
        .clone()
        .map_err(|e| OxenError::internal_error(format!("Failed to initialize video decoding: {e}")))
}

/// Generate a video's JPEG thumbnail with video-rs (FFmpeg) and store it in the version store.
#[cfg(feature = "ffmpeg")]
async fn generate_video_thumbnail_version_store(
    version_store: Arc<dyn VersionStore>,
    video_hash: &str,
    derived_filename: &str,
    thumbnail: VideoThumbnail,
    dir: &Path,
) -> Result<(), OxenError> {
    log::debug!(
        "generate thumbnail derived_filename {derived_filename} from video hash {video_hash}"
    );
    if version_store
        .derived_version_exists(video_hash, derived_filename)
        .await?
    {
        return Ok(());
    }

    // video-rs decodes from a filesystem path, so materialize the video file. The guard
    // `version_file` is held on the async side (its drop runs after the blocking generation below)
    // so a materialized S3 temp outlives the read; the closure takes a plain path copy.
    let version_file = version_store.materialize(video_hash, dir).await?;
    let version_path = version_file.to_pathbuf();

    // Output dimensions bound the thumbnail; `DynamicImage::thumbnail` preserves aspect ratio.
    let (output_width, output_height) = match (thumbnail.width, thumbnail.height) {
        (Some(w), Some(h)) => (w, h),
        (Some(w), None) => (w, w), // Use width for both if only width specified
        (None, Some(h)) => (h, h), // Use height for both if only height specified
        (None, None) => (320, 240),
    };

    // The thumbnail is taken from the first decoded frame; thumbnail.timestamp is not yet honored.
    let buf = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, OxenError> {
        ensure_video_decoding_initialized()?;

        let mut decoder = Decoder::new(version_path.as_path()).map_err(|e| {
            OxenError::internal_error(format!("Failed to open video for thumbnailing: {e}"))
        })?;

        let (width, height) = decoder.size();
        let (_timestamp, frame) = decoder
            .decode()
            .map_err(|e| OxenError::internal_error(format!("Failed to decode video frame: {e}")))?;

        // video-rs hands back an RGB frame as a contiguous (height, width, 3) ndarray.
        let pixels = frame
            .as_slice()
            .ok_or_else(|| OxenError::internal_error("Decoded video frame was not contiguous"))?;
        let img: ImageBuffer<Rgb<u8>, Vec<u8>> =
            ImageBuffer::from_raw(width, height, pixels.to_vec()).ok_or_else(|| {
                OxenError::internal_error("Failed to build image buffer from video frame")
            })?;

        // A box at or over the frame's size returns the frame as it is, since `thumbnail` would
        // scale it up to fill the box.
        let frame = DynamicImage::ImageRgb8(img);
        let thumb = if output_width >= width && output_height >= height {
            frame
        } else {
            frame.thumbnail(output_width, output_height)
        };

        let mut buf = Vec::new();
        thumb
            .write_to(&mut Cursor::new(&mut buf), image::ImageFormat::Jpeg)
            .map_err(|e| OxenError::internal_error(format!("Failed to encode thumbnail: {e}")))?;
        Ok(buf)
    })
    .await??;

    // Save the thumbnail image
    version_store
        .store_version_derived(video_hash, derived_filename, buf.into())
        .await?;

    log::debug!("saved thumbnail {derived_filename}");
    Ok(())
}

/// Handle video thumbnail generation: generate thumbnail if needed and return a stream.
/// Only enabled if the 'ffmpeg' feature is enabled.
#[allow(unused_variables)]
pub async fn handle_video_thumbnail(
    version_store: Arc<dyn VersionStore>,
    file_hash: String,
    video_thumbnail: VideoThumbnail,
    dir: &Path,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>, OxenError> {
    check_resize_dimensions(video_thumbnail.width, video_thumbnail.height)?;

    #[cfg(not(feature = "ffmpeg"))]
    {
        let _ = (version_store, file_hash, video_thumbnail, dir);
        Err(OxenError::ThumbnailingNotEnabled)
    }

    #[cfg(feature = "ffmpeg")]
    {
        log::debug!("video_thumbnail {video_thumbnail:?}");
        let derived_filename =
            video_thumbnail_filename(video_thumbnail.width, video_thumbnail.height);

        generate_video_thumbnail_version_store(
            version_store.clone(),
            &file_hash,
            &derived_filename,
            video_thumbnail,
            dir,
        )
        .await?;

        log::debug!("In the thumbnail cache! {derived_filename}");
        let stream = version_store
            .get_version_derived_stream(&file_hash, &derived_filename)
            .await?;
        Ok(stream.boxed())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_resize_within_source_never_scales_past_the_source() {
        let resized = |width, height| {
            let img =
                super::resize_within_source(image::DynamicImage::new_rgb8(4, 2), width, height);
            (img.width(), img.height())
        };
        assert_eq!(resized(Some(2), Some(1)), (2, 1));
        assert_eq!(
            resized(Some(8), Some(1)),
            (4, 1),
            "each side stops at the source's"
        );
        assert_eq!(resized(Some(8), Some(8)), (4, 2));
        assert_eq!(
            resized(None, Some(2)),
            (2, 1),
            "one side gives a square the image fits in, keeping its aspect ratio"
        );
        assert_eq!(resized(Some(512), None), (4, 2));
    }
}
