//! Helper functions to get metadata from tabular files.
//!

use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::metadata::MetadataTabular;

use std::path::Path;

/// Detects the tabular metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataTabular, OxenError> {
    let path = path.as_ref();
    let size = tabular::get_size(path)?;
    let schema = tabular::get_schema(path)?;
    Ok(MetadataTabular::new(size.width, size.height, schema))
}

pub fn get_metadata_with_extension(
    path: impl AsRef<Path>,
    extension: &str,
) -> Result<MetadataTabular, OxenError> {
    let path = path.as_ref();
    let size = tabular::get_size_with_extension(path, Some(extension))?;
    let schema = tabular::get_schema_with_extension(path, Some(extension))?;
    Ok(MetadataTabular::new(size.width, size.height, schema))
}

#[cfg(test)]
mod tests {
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::metadata::MetadataTabular;
    use crate::model::EntryDataType;
    use crate::repositories;
    use crate::test;

    #[test]
    fn test_get_metadata_tabular() {
        let file = test::test_text_file_with_name("celeb_a_200k.csv");
        let metadata = repositories::metadata::get(file).unwrap();

        assert!(metadata.size >= 9604701); // not sure why different on windows
        assert_eq!(metadata.data_type, EntryDataType::Tabular);
        assert_eq!(metadata.mime_type, "text/plain");

        let metadata: MetadataTabular = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataTabular(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.tabular.width, 11);
        assert_eq!(metadata.tabular.height, 200_000);
    }
}
