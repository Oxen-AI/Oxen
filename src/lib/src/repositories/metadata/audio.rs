//! Helper functions to get metadata from audio files.
//!

use crate::{error::OxenError, model::metadata::MetadataAudio};

use lofty::file::AudioFile;
use lofty::probe::Probe;
use std::path::Path;

/// Detects the audio metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataAudio, OxenError> {
    let path = path.as_ref();
    match Probe::open(path) {
        Ok(tagged_file) => match tagged_file.read() {
            Ok(tagged_file) => {
                let properties = tagged_file.properties();
                let duration = properties.duration();
                let seconds = duration.as_secs_f64();
                let rate = properties.sample_rate().unwrap_or(0);
                let channels = properties.channels().unwrap_or(0);

                Ok(MetadataAudio::new(
                    seconds,
                    channels as usize,
                    rate as usize,
                ))
            }
            Err(err) => {
                log::error!("Could not read audio stream: {}", err);
                Err(OxenError::basic_str("Could not read audio stream"))
            }
        },
        Err(err) => {
            log::error!("Could not open audio stream: {}", err);
            Err(OxenError::basic_str("Could not open audio stream"))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::metadata::MetadataAudio;
    use crate::model::EntryDataType;
    use crate::repositories;
    use crate::test;

    use approx::assert_relative_eq;

    #[test]
    fn test_get_metadata_audio_flac() {
        let file = test::test_audio_file_with_name("121-121726-0005.flac");
        let metadata = repositories::metadata::get(file).unwrap();

        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");

        assert!(metadata.metadata.is_some());
        let metadata: MetadataAudio = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataAudio(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.audio.num_channels, 1);
        assert_eq!(metadata.audio.sample_rate, 16000);
        assert_relative_eq!(metadata.audio.num_seconds, 3.1);
    }

    #[test]
    fn test_get_metadata_audio_wav() {
        let file = test::test_audio_file_with_name("121-121726-0005.wav");
        let metadata = repositories::metadata::get(file).unwrap();

        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 99278);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-wav");

        assert!(metadata.metadata.is_some());
        let metadata: MetadataAudio = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataAudio(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.audio.num_channels, 1);
        assert_eq!(metadata.audio.sample_rate, 16000);
        assert_relative_eq!(metadata.audio.num_seconds, 3.1);
    }
}
