//! Helper functions to get metadata from the text files.
//!

use crate::error::OxenError;
use crate::model::metadata::MetadataText;
use crate::util::fs::{LineCounts, line_counts};

use std::io::Read;

/// Detects the text metadata of the file `data` reads.
pub(crate) fn get_metadata(data: impl Read) -> Result<MetadataText, OxenError> {
    Ok(from_counts(&line_counts(data, true)?))
}

/// The text metadata of a file with `lines`, which counted its characters.
pub(crate) fn from_counts(lines: &LineCounts) -> MetadataText {
    let (lines_count, chars_count) = lines.lines(false);
    MetadataText::new(lines_count, chars_count.unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use crate::model::EntryDataType;
    use crate::model::metadata::MetadataText;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::repositories;
    use crate::test;

    #[test]
    fn test_get_metadata_text_readme() {
        let file = test::test_text_file_with_name("README");
        let metadata = repositories::metadata::get(file).unwrap();

        assert!(metadata.size >= 44); // not sure why 46 on windows
        assert_eq!(metadata.data_type, EntryDataType::Text);
        assert_eq!(metadata.mime_type, "text/plain");

        let metadata: MetadataText = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataText(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.text.num_lines, 3);
        assert!(metadata.text.num_chars == 44 || metadata.text.num_chars == 46);
        // unix vs windows
    }

    #[test]
    fn test_get_metadata_text_readme_md() {
        let file = test::test_text_file_with_name("README.md");
        let metadata = repositories::metadata::get(file).unwrap();

        assert!(metadata.size >= 50); // not sure why 53 on windows
        assert_eq!(metadata.data_type, EntryDataType::Text);
        assert_eq!(metadata.mime_type, "text/markdown");

        let metadata: MetadataText = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataText(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.text.num_lines, 4);
        assert!(metadata.text.num_chars == 50 || metadata.text.num_chars == 53);
        // unix vs windows
    }
}
