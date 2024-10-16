//! Helper functions to get metadata from the images.
//!

use crate::error::OxenError;
use crate::model::metadata::metadata_image::MetadataImage;

use std::fs::File;

use image::ImageReader;
use std::io::BufReader;
use std::path::Path;

/// Detects the image metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataImage, OxenError> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let reader = ImageReader::new(reader).with_guessed_format()?;

    match reader.into_dimensions() {
        Ok((width, height)) => Ok(MetadataImage::new(width, height)),
        Err(e) => {
            log::error!("Could not get image metadata {:?}", e);
            Err(OxenError::basic_str("Could not get image metadata"))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::model::entry::entry_data_type::EntryDataType;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::metadata::MetadataImage;
    use crate::repositories;
    use crate::test;

    #[test]
    fn test_get_metadata_img_rgb() {
        let file = test::test_img_file_with_name("cat_1.jpg");

        let data = repositories::metadata::get(file).unwrap();

        assert_eq!(data.data_type, EntryDataType::Image);
        assert_eq!(data.mime_type, "image/jpeg");

        assert!(data.metadata.is_some());
        let metadata: MetadataImage = match data.metadata.unwrap() {
            GenericMetadata::MetadataImage(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.image.width, 499);
        assert_eq!(metadata.image.height, 375);
    }

    #[test]
    fn test_get_metadata_img_rgba() {
        let file = test::test_img_file_with_name("cat_rgba.png");
        let data = repositories::metadata::get(file).unwrap();

        assert_eq!(data.data_type, EntryDataType::Image);
        assert_eq!(data.mime_type, "image/png");

        assert!(data.metadata.is_some());
        let metadata: MetadataImage = match data.metadata.unwrap() {
            GenericMetadata::MetadataImage(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.image.width, 499);
        assert_eq!(metadata.image.height, 375);
    }

    #[test]
    fn test_get_metadata_img_grayscale() {
        let file = test::test_img_file_with_name("cat_grayscale.jpg");

        let data = repositories::metadata::get(file).unwrap();

        assert_eq!(data.data_type, EntryDataType::Image);
        assert_eq!(data.mime_type, "image/jpeg");

        assert!(data.metadata.is_some());
        let metadata: MetadataImage = match data.metadata.unwrap() {
            GenericMetadata::MetadataImage(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.image.width, 499);
        assert_eq!(metadata.image.height, 375);
    }

    #[test]
    fn test_get_metadata_img_mnist() {
        let file = test::test_img_file_with_name("mnist_7.png");
        let data = repositories::metadata::get(file).unwrap();

        assert_eq!(data.data_type, EntryDataType::Image);
        assert_eq!(data.mime_type, "image/png");

        assert!(data.metadata.is_some());
        let metadata: MetadataImage = match data.metadata.unwrap() {
            GenericMetadata::MetadataImage(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.image.width, 28);
        assert_eq!(metadata.image.height, 28);
    }
}
