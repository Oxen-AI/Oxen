//! Helper functions to get metadata from the video files.
//!

use crate::{error::OxenError, model::metadata::MetadataVideo};

use mp4::{Mp4Reader, TrackType};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::time::Duration;

/// Detects the video metadata of the file `data` reads.
pub fn get_metadata(mut data: impl Read + Seek) -> Result<MetadataVideo, OxenError> {
    let size = data.seek(SeekFrom::End(0))?;
    data.rewind()?;

    match Mp4Reader::read_header(BufReader::new(data), size) {
        Ok(video) => {
            // `Mp4Reader::duration` panics on a timescale of 0
            let mvhd = &video.moov.mvhd;
            let millis = (mvhd.duration.checked_mul(1000))
                .and_then(|duration| duration.checked_div(u64::from(mvhd.timescale)))
                .ok_or_else(|| OxenError::internal_error("Could not get video duration"))?;
            let duration = Duration::from_millis(millis).as_secs_f64();

            // The video track with the lowest ID, passing over tracks of a type the reader does not
            // recognize, such as timecode tracks
            let track = video
                .tracks()
                .values()
                .filter(|track| matches!(track.track_type(), Ok(TrackType::Video)))
                .min_by_key(|track| track.track_id())
                .ok_or_else(|| OxenError::internal_error("Could not get video track"))?;

            Ok(MetadataVideo::new(
                duration,
                track.width() as usize,
                track.height() as usize,
            ))
        }
        Err(err) => {
            let err = format!("Could not get video metadata {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::EntryDataType;
    use crate::model::metadata::MetadataVideo;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::repositories;
    use crate::test;

    use approx::assert_relative_eq;
    use std::io::Cursor;

    #[test]
    fn test_get_metadata_video_mp4() {
        let file = test::test_video_file_with_name("basketball.mp4");
        let metadata = repositories::metadata::get(file).unwrap();
        println!("metadata: {metadata:?}");

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

        // A video with timecode tracks, a type the MP4 reader does not recognize, gets the metadata
        // of its first video track: track 1 is 32x24 and track 2 is 16x16.
        let file = test::test_video_file_with_name("two_videos_timecode.mp4");
        let metadata = repositories::metadata::get(file)
            .expect("two_videos_timecode.mp4 is readable and has a recognized video type");
        let Some(GenericMetadata::MetadataVideo(metadata)) = metadata.metadata else {
            panic!(
                "two_videos_timecode.mp4 has video metadata, not {:?}",
                metadata.metadata
            );
        };
        assert_eq!((metadata.video.width, metadata.video.height), (32, 24));
        assert_relative_eq!(metadata.video.num_seconds, 1.0);

        // A video whose movie header has a timescale of 0 fails to give metadata rather than
        // panicking. In a version 0 header the timescale follows the version and flags and two
        // 4-byte timestamps.
        let mut bytes = std::fs::read(test::test_video_file_with_name("basketball.mp4"))
            .expect("basketball.mp4 is readable");
        let mvhd = (bytes.windows(4))
            .position(|fourcc| fourcc == b"mvhd")
            .expect("basketball.mp4 has a movie header");
        assert_eq!(
            bytes[mvhd + 4],
            0,
            "basketball.mp4's movie header is version 0"
        );
        bytes[mvhd + 16..mvhd + 20].fill(0);
        assert!(super::get_metadata(Cursor::new(bytes)).is_err());
    }

    #[test]
    fn test_get_metadata_video_mov() {
        let file = test::test_video_file_with_name("dog_skatez.mov");
        let expected_size = std::fs::metadata(&file).unwrap().len();
        let metadata = repositories::metadata::get(&file).unwrap();
        println!("metadata: {metadata:?}");

        assert_eq!(metadata.size, expected_size);
        assert_eq!(metadata.data_type, EntryDataType::Video);
        assert_eq!(metadata.mime_type, "video/quicktime");

        let metadata: MetadataVideo = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataVideo(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.video.width, 64);
        assert_eq!(metadata.video.height, 64);
        assert_relative_eq!(metadata.video.num_seconds, 1.0);
    }
}
