//! Helper functions to get metadata from tabular files.
//!

use crate::core::df::tabular::{self, DataFrameSource};
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

/// Detects the tabular metadata of `source`, a file with `extension`.
pub(crate) fn get_metadata_with_extension(
    source: &DataFrameSource,
    extension: &str,
) -> Result<MetadataTabular, OxenError> {
    let size = tabular::get_size_with_extension(source, Some(extension))?;
    let schema = tabular::get_schema_with_extension(source, Some(extension))?;
    Ok(MetadataTabular::new(size.width, size.height, schema))
}

#[cfg(test)]
mod tests {
    use crate::model::EntryDataType;
    use crate::model::metadata::MetadataTabular;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::repositories;
    use crate::test;

    use std::fs;

    #[test]
    fn test_get_metadata_tabular() {
        let file = test::test_csv_file_with_name("mixed_data_types.csv");
        let metadata = repositories::metadata::get(&file).unwrap();

        assert_eq!(metadata.size, fs::metadata(&file).unwrap().len());
        assert_eq!(metadata.data_type, EntryDataType::Tabular);
        assert_eq!(metadata.mime_type, "text/plain");

        let metadata: MetadataTabular = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataTabular(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.tabular.width, 5);
        assert_eq!(metadata.tabular.height, 4);
    }
}
