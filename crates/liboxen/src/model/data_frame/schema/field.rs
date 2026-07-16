use polars::prelude::PlSmallStr;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::model::data_frame::schema::DataType;

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct Field {
    pub name: String,
    pub dtype: String,
    pub metadata: Option<Value>,
    #[serde(default)]
    pub changes: Option<Changes>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct PreviousField {
    pub name: String,
    pub dtype: String,
    pub metadata: Option<Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct Changes {
    pub status: String,
    pub previous: Option<PreviousField>,
}

impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        self.name == other.name && self.dtype == other.dtype && self.metadata == other.metadata
    }
}

impl Field {
    pub fn new(name: &str, dtype: &str) -> Self {
        Field {
            name: name.to_owned(),
            dtype: dtype.to_owned(),
            metadata: None,
            changes: None,
        }
    }

    pub fn to_sql(&self) -> String {
        let dtype = DataType::from_string(&self.dtype).to_sql();
        format!("{} {}", self.name, dtype)
    }

    pub fn to_polars(&self) -> polars::prelude::Field {
        polars::prelude::Field::new(
            PlSmallStr::from(self.name.to_owned()),
            DataType::from_string(&self.dtype).to_polars(),
        )
    }

    pub fn fields_to_string_with_limit<V: AsRef<Vec<Field>>>(fields: V) -> String {
        let fields = fields.as_ref();
        let max_num = 2;
        if fields.len() > max_num {
            let name_0 = fields[0].name.to_owned();
            let name_3 = fields[fields.len() - 1].name.to_owned();

            let combined_names = [name_0, String::from("..."), name_3].join(", ");
            format!("[{combined_names}]")
        } else {
            let names: Vec<String> = fields.iter().map(|f| f.name.to_owned()).collect();

            let combined_names = names.join(", ");

            format!("[{combined_names}]")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    // Mirrors the on-disk `Field` layout from before `changes` was briefly
    // dropped: `changes` is the trailing element. Merkle nodes embed schema
    // fields and are serialized positionally via `rmp_serde::to_vec`, so blobs
    // written by older versions still carry (or omit) this trailing element and
    // must keep deserializing into the current `Field`.
    #[derive(Serialize)]
    struct OldField {
        name: String,
        dtype: String,
        metadata: Option<Value>,
        changes: Option<OldChanges>,
    }

    #[derive(Serialize)]
    struct OldChanges {
        status: String,
        previous: Option<Box<OldField>>,
    }

    #[test]
    fn test_deserialize_field_missing_changes_element() {
        // The interim format dropped `changes` entirely (three-element array).
        #[derive(Serialize)]
        struct FieldWithoutChanges {
            name: String,
            dtype: String,
            metadata: Option<Value>,
        }
        let bytes = rmp_serde::to_vec(&FieldWithoutChanges {
            name: "col".to_owned(),
            dtype: "str".to_owned(),
            metadata: None,
        })
        .expect("serialize field without changes");

        let field: Field =
            rmp_serde::from_slice(&bytes).expect("deserialize interim format into current Field");
        assert_eq!(field.name, "col");
        assert_eq!(field.dtype, "str");
        assert!(field.changes.is_none());
    }

    #[test]
    fn test_deserialize_field_with_null_changes() {
        let bytes = rmp_serde::to_vec(&OldField {
            name: "col".to_owned(),
            dtype: "str".to_owned(),
            metadata: None,
            changes: None,
        })
        .expect("serialize old field with null changes");

        let field: Field =
            rmp_serde::from_slice(&bytes).expect("deserialize old format into current Field");
        assert_eq!(field.name, "col");
        assert_eq!(field.dtype, "str");
        assert!(field.changes.is_none());
    }
}
