//! Helper functions to get metadata from the video files.
//!

use crate::{error::OxenError, model::metadata::MetadataVideo};

use mp4::{Mp4Track, TrackType};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Detects the video metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataVideo, OxenError> {
    let path = path.as_ref();
    let f = match File::open(path) {
        Ok(f) => f,
        Err(e) => return Err(OxenError::file_error(path, e)),
    };

    let size = f.metadata()?.len();
    let reader = BufReader::new(f);

    match mp4::Mp4Reader::read_header(reader, size) {
        Ok(video) => {
            let duration = video.duration().as_secs_f64();

            let video_tracks: Vec<&Mp4Track> = video
                .tracks()
                .values()
                .filter(|t| t.track_type().unwrap() == TrackType::Video)
                .collect();

            let video = video_tracks
                .first()
                .ok_or(OxenError::basic_str("Could not get video track"))?;

            Ok(MetadataVideo::new(
                duration,
                video.width() as usize,
                video.height() as usize,
            ))
        }
        Err(err) => {
            let err = format!("Could not get video metadata {:?}", err);
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::metadata::MetadataVideo;
    use crate::model::EntryDataType;
    use crate::repositories;
    use crate::test;

    use approx::assert_relative_eq;

    #[test]
    fn test_get_metadata_video_mp4() {
        let file = test::test_video_file_with_name("basketball.mp4");
        let metadata = repositories::metadata::get(file).unwrap();
        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 23599);
        assert_eq!(metadata.data_type, EntryDataType::Video);
        assert_eq!(metadata.mime_type, "video/mp4");

        let metadata: MetadataVideo = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataVideo(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.video.width, 128);
        assert_eq!(metadata.video.height, 176);
        assert_relative_eq!(metadata.video.num_seconds, 1.6);
    }

    #[test]
    fn test_get_metadata_video_mov() {
        let file = test::test_video_file_with_name("dog_skatez.mov");
        let metadata = repositories::metadata::get(file).unwrap();
        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 11657299);
        assert_eq!(metadata.data_type, EntryDataType::Video);
        assert_eq!(metadata.mime_type, "video/quicktime");

        // We do not know how to parse mov files yet
        assert!(metadata.metadata.is_none());
    }
}
