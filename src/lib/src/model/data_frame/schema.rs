pub mod custom_data_type;
pub mod data_type;
pub mod field;
pub mod staged_schema;

pub use custom_data_type::CustomDataType;
pub use data_type::DataType;
pub use field::Field;

use crate::util::hasher;
use itertools::Itertools;
use polars::prelude::SchemaExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashMap, fmt, path::PathBuf};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub hash: String,
    pub fields: Vec<Field>,
    // Optional string metadata on the schema, to allow for user driven features.
    pub metadata: Option<Value>,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Schema) -> bool {
        self.hash == other.hash && self.fields == other.fields
    }
}

impl Schema {
    pub fn empty() -> Self {
        Schema {
            hash: "".to_string(),
            fields: vec![],
            metadata: None,
        }
    }
    pub fn new(fields: Vec<Field>) -> Schema {
        Schema {
            hash: Schema::hash_fields(&fields),
            fields: fields.to_owned(),
            metadata: None,
        }
    }

    pub fn to_polars(&self) -> polars::prelude::Schema {
        let mut schema = polars::prelude::Schema::default();
        for field in self.fields.iter() {
            let data_type = DataType::from_string(&field.dtype);
            schema.with_column(
                field.name.to_owned().into(),
                DataType::to_polars(&data_type),
            );
        }

        schema
    }

    pub fn from_polars(schema: &polars::prelude::Schema) -> Schema {
        let mut fields: Vec<Field> = vec![];
        for field in schema.iter_fields() {
            let f = Field::new(field.name(), field.dtype().to_string().as_str());
            fields.push(f);
        }

        Schema {
            hash: Schema::hash_fields(&fields),
            fields,
            metadata: None,
        }
    }

    /// Checks if the provided schema matches this schema given a hash or path
    pub fn matches_ref(&self, schema_ref: impl AsRef<str>) -> bool {
        let schema_ref = schema_ref.as_ref();
        self.hash == schema_ref
    }

    /// Add metadata to a column
    pub fn add_column_metadata(&mut self, name: &str, metadata: &Value) {
        log::debug!("add_column_metadata {} {}", name, metadata);
        if let Some(f) = self.fields.iter_mut().find(|f| f.name == name) {
            f.metadata = Some(metadata.to_owned());
        }
        self.hash = Schema::hash_fields(&self.fields);
    }

    /// Write metadata from schema columns to the schema
    pub fn update_metadata_from_schema(&mut self, schema: &Schema) {
        if let Some(metadata) = &schema.metadata {
            self.metadata = Some(metadata.to_owned());
        }
        for field in schema.fields.iter() {
            if let Some(f) = self.fields.iter_mut().find(|f| f.name == field.name) {
                if field.metadata.is_some() {
                    f.metadata.clone_from(&field.metadata);
                }
            }
        }
        self.hash = Schema::hash_fields(&self.fields);
    }

    pub fn has_all_field_names(&self, schema: &polars::prelude::Schema) -> bool {
        log::debug!(
            "matches_polars checking size {} == {}",
            self.fields.len(),
            schema.len()
        );
        if self.fields.len() != schema.len() {
            // Print debug logic to help figure out why schemas don't match
            log::debug!("====schema.len {}====", schema.len());
            for field in schema.iter_fields() {
                log::debug!("schema.field: {}", field.name());
            }

            log::debug!("====self.fields.len {}====", self.fields.len());
            for field in self.fields.iter() {
                log::debug!("self.field: {}", field.name);
            }

            return false;
        }

        let mut has_all_fields = true;
        for field in schema.iter_fields() {
            if !self.has_field_name(&field.name) {
                has_all_fields = false;
                break;
            }
        }

        has_all_fields
    }

    pub fn has_same_field_names(&self, schema: &polars::prelude::Schema) -> bool {
        let self_field_names: std::collections::HashSet<String> =
            self.fields.iter().map(|f| f.name.clone()).collect();
        let schema_field_names: std::collections::HashSet<String> =
            schema.iter_fields().map(|f| f.name().to_string()).collect();

        log::debug!("Comparing field names between self and provided schema");
        log::debug!("self are {:?}", self_field_names);
        log::debug!("schema are {:?}", schema_field_names);
        if self_field_names != schema_field_names {
            return false;
        }
        true
    }

    pub fn has_field(&self, field: &Field) -> bool {
        self.fields
            .iter()
            .any(|f| f.name == field.name && f.dtype == field.dtype)
    }

    pub fn has_field_names(&self, fields: &[impl AsRef<str>]) -> bool {
        fields.iter().all(|field| self.has_field_name(field))
    }

    pub fn has_field_name(&self, name: impl AsRef<str>) -> bool {
        let name = name.as_ref();
        self.fields.iter().any(|f| f.name == name)
    }

    pub fn has_column(&self, name: impl AsRef<str>) -> bool {
        let name = name.as_ref().to_lowercase(); // Convert the parameter to lowercase
        self.fields.iter().any(|f| f.name.to_lowercase() == name) // Compare lowercase versions
    }

    pub fn get_field(&self, name: impl AsRef<str>) -> Option<&Field> {
        let name = name.as_ref();
        self.fields.iter().find(|f| f.name == name)
    }

    fn hash_fields(fields: &Vec<Field>) -> String {
        let mut hash_buffers: Vec<String> = vec![];
        for f in fields {
            hash_buffers.push(format!("{}{}", f.name, f.dtype));
            if let Some(metadata) = &f.metadata {
                hash_buffers.push(metadata.to_string());
            }
        }

        let buffer_str = hash_buffers.join("");
        let buffer = buffer_str.as_bytes();
        hasher::hash_buffer(buffer)
    }

    pub fn fields_to_csv(&self) -> String {
        self.fields.iter().map(|f| f.name.to_owned()).join(",")
    }

    pub fn fields_names(&self) -> Vec<String> {
        self.fields
            .iter()
            .filter(|f| {
                println!("{:?}", f);

                // Perform the actual filter condition check
                f.changes
                    .as_ref()
                    .map_or(true, |changes| changes.status != "deleted")
            })
            .map(|f| f.name.clone()) // Assuming name is a String and needs to be cloned
            .collect()
    }

    /// Compare the schemas, looking for added fields
    pub fn added_fields(&self, other: &Schema) -> Vec<Field> {
        let mut fields: Vec<Field> = vec![];

        // if field is in current schema but not in commit, it was added
        for current_field in self.fields.iter() {
            if !other.fields.iter().any(|f| f.name == current_field.name) {
                fields.push(current_field.clone());
            }
        }

        fields
    }

    /// Compare the schemas, looking for removed fields
    pub fn removed_fields(&self, other: &Schema) -> Vec<Field> {
        let mut fields: Vec<Field> = vec![];

        // if field is in commit history but not in current, it was removed
        for commit_field in other.fields.iter() {
            if !self.fields.iter().any(|f| f.name == commit_field.name) {
                fields.push(commit_field.clone());
            }
        }

        fields
    }

    /// Find the common fields between two schemas
    pub fn common_fields(&self, other: &Schema) -> Vec<Field> {
        let mut fields: Vec<Field> = vec![];

        // if field is in both schemas, it was common
        for current_field in self.fields.iter() {
            if other.fields.iter().any(|f| f.name == current_field.name) {
                fields.push(current_field.clone());
            }
        }

        fields
    }

    /// Create a common schema between the two
    pub fn common(&self, other: &Schema) -> Schema {
        let mut fields: Vec<Field> = vec![];

        // if field is in both schemas, it was common
        for current_field in self.fields.iter() {
            if other.fields.iter().any(|f| f.name == current_field.name) {
                fields.push(current_field.clone());
            }
        }

        Schema {
            hash: Schema::hash_fields(&fields),
            fields,
            metadata: None,
        }
    }

    pub fn schemas_to_string(schemas: HashMap<PathBuf, Schema>) -> String {
        let mut table = comfy_table::Table::new();
        table.set_header(vec!["path", "hash", "fields"]);

        // Sort by path
        let mut schemas: Vec<(PathBuf, Schema)> = schemas.into_iter().collect();
        schemas.sort_by(|a, b| a.0.cmp(&b.0));

        for (path, schema) in schemas.iter() {
            let fields_str = Field::fields_to_string_with_limit(&schema.fields);
            table.add_row(vec![
                path.to_string_lossy(),
                schema.hash.clone().into(),
                fields_str.into(),
            ]);
        }
        table.to_string()
    }

    pub fn num_bytes(&self) -> u64 {
        let bytes = serde_json::to_string(&self).unwrap().as_bytes().len();
        bytes as u64
    }

    pub fn verbose_str(&self) -> String {
        let mut table = comfy_table::Table::new();
        table.set_header(vec!["name", "dtype", "metadata"]);

        for field in self.fields.iter() {
            let mut row = vec![field.name.to_string(), field.dtype.to_string()];

            if let Some(val) = &field.metadata {
                row.push(val.to_string())
            } else {
                row.push(String::from(""))
            }
            table.add_row(row);
        }
        if let Some(metadata) = &self.metadata {
            format!("\n{}\n\n{}", metadata, table)
        } else {
            format!("{}", table)
        }
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field_strs: Vec<String> = self
            .fields
            .iter()
            .map(|f| format!("{}:{}", f.name, f.dtype))
            .collect();
        let fields_str = field_strs.join(", ");
        write!(f, "{fields_str}")
    }
}

impl std::error::Error for Schema {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::model::data_frame::schema::Field;
    use crate::model::data_frame::schema::Schema;

    #[test]
    fn test_schemas_to_string_one_field() {
        let mut schemas = HashMap::new();
        schemas.insert(
            PathBuf::from("file.csv"),
            Schema {
                hash: "1234".to_string(),
                fields: vec![Field::new("file", "")],
                metadata: None,
            },
        );
        let table = Schema::schemas_to_string(schemas);
        println!("{}", table);
        assert_eq!(
            table,
            r"
+----------+------+--------+
| path     | hash | fields |
+==========================+
| file.csv | 1234 | [file] |
+----------+------+--------+"
                .trim()
        )
    }

    #[test]
    fn test_schemas_to_string_many_fields() {
        let mut schemas = HashMap::new();
        schemas.insert(
            PathBuf::from("another/file.csv"),
            Schema {
                hash: "1234".to_string(),
                fields: vec![
                    Field::new("file", "str"),
                    Field::new("x", "i64"),
                    Field::new("y", "i64"),
                    Field::new("w", "f64"),
                    Field::new("h", "f64"),
                ],
                metadata: None,
            },
        );
        let table = Schema::schemas_to_string(schemas);
        println!("{}", table);

        assert_eq!(
            table,
            r"
+------------------+------+----------------+
| path             | hash | fields         |
+==========================================+
| another/file.csv | 1234 | [file, ..., h] |
+------------------+------+----------------+"
                .trim()
        )
    }

    #[test]
    fn test_schemas_multiple_to_string_no_name() {
        let mut schemas = HashMap::new();

        schemas.insert(
            PathBuf::from("numero_uno.csv"),
            Schema {
                hash: "1234".to_string(),
                fields: vec![
                    Field::new("file", "str"),
                    Field::new("x", "i64"),
                    Field::new("y", "i64"),
                    Field::new("w", "f64"),
                    Field::new("h", "f64"),
                ],
                metadata: None,
            },
        );
        schemas.insert(
            PathBuf::from("numero_dos.csv"),
            Schema {
                hash: "5432".to_string(),
                fields: vec![Field::new("file", "str"), Field::new("x", "i64")],
                metadata: None,
            },
        );
        let table = Schema::schemas_to_string(schemas);
        println!("{}", table);

        assert_eq!(
            table,
            r"
+----------------+------+----------------+
| path           | hash | fields         |
+========================================+
| numero_dos.csv | 5432 | [file, x]      |
|----------------+------+----------------|
| numero_uno.csv | 1234 | [file, ..., h] |
+----------------+------+----------------+"
                .trim()
        )
    }
}
