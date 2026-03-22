use liboxen::model::Schema;
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass]
pub struct PySchema {
    pub hash: String,
    // Map from field name to field type
    pub fields: HashMap<String, String>,
    // Optional string metadata on the schema, to allow for user driven features.
    pub metadata: Option<String>,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (hash, metadata))]
    pub fn new(hash: String, metadata: Option<String>) -> Self {
        Self {
            hash,
            fields: HashMap::new(),
            metadata,
        }
    }

    #[getter]
    pub fn hash(&self) -> &str {
        &self.hash
    }

    #[getter]
    pub fn fields(&self) -> HashMap<String, String> {
        self.fields.clone()
    }

    #[getter]
    pub fn metadata(&self) -> Option<String> {
        self.metadata.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Schema(hash={}, fields={:?}, metadata={:?})",
            self.hash, self.fields, self.metadata
        )
    }
}

impl From<Schema> for PySchema {
    fn from(schema: Schema) -> PySchema {
        PySchema {
            hash: schema.hash,
            fields: schema
                .fields
                .into_iter()
                .map(|field| (field.name, field.dtype))
                .collect(),
            metadata: schema.metadata.map(|m| serde_json::to_string(&m).unwrap()),
        }
    }
}
