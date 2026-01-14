use serde::{Deserialize, Serialize};

use crate::model::Schema;
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
pub struct MetadataTabular {
    pub tabular: MetadataTabularImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone, ToSchema)]
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
