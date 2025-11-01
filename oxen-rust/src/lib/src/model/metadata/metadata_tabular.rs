use serde::{Deserialize, Serialize};
use bitcode::{Encode, Decode};

use crate::model::Schema;

#[derive(Deserialize, Serialize, Debug, Clone, Encode, Decode)]
pub struct MetadataTabular {
    pub tabular: MetadataTabularImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone, Encode, Decode)]
pub struct MetadataTabularImpl {
    pub width: usize,
    pub height: usize,
    pub schema: Schema,
}

impl MetadataTabular {
    pub fn new(width: usize, height: usize, schema: Schema) -> Self {
        Self {
            tabular: MetadataTabularImpl {
                width,
                height,
                schema,
            },
        }
    }
}

impl std::fmt::Display for MetadataTabular {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "MetadataTabular({}x{}) ({} fields) {}",
            self.tabular.width,
            self.tabular.height,
            self.tabular.schema.fields.len(),
            self.tabular
                .schema
                .fields
                .iter()
                .map(|field| format!("{}: {}", field.name, field.dtype))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}
