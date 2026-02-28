//! Helper functions to get metadata from the text files.
//!

use crate::error::OxenError;
use crate::model::metadata::MetadataText;
use crate::opts::CountLinesOpts;
use crate::util;

use std::path::Path;

/// Detects the text metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataText, OxenError> {
    let mut opts = CountLinesOpts::empty();
    opts.with_chars = true;

    let (lines_count, chars_count) = util::fs::count_lines(path, opts)?;
    let chars_count = chars_count.unwrap_or(0);

    Ok(MetadataText::new(lines_count, chars_count))
}

#[cfg(test)]
mod tests {
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::metadata::MetadataText;
    use crate::model::EntryDataType;
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
