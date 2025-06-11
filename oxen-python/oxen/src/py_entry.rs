use pyo3::prelude::*;

use liboxen::view::entries::EMetadataEntry;

#[pyclass]
pub struct PyEntry {
    _entry: EMetadataEntry,
}

#[pymethods]
impl PyEntry {
    fn __repr__(&self) -> String {
        format!("{:?}", self._entry)
    }

    fn __str__(&self) -> String {
        self._entry.filename().to_owned()
    }

    #[getter]
    pub fn hash(&self) -> String {
        self._entry.hash().to_string()
    }

    #[getter]
    pub fn filename(&self) -> &str {
        &self._entry.filename()
    }

    #[getter]
    pub fn data_type(&self) -> String {
        self._entry.data_type().to_string()
    }

    #[getter]
    pub fn mime_type(&self) -> &str {
        &self._entry.mime_type()
    }

    #[getter]
    pub fn size(&self) -> u64 {
        self._entry.size()
    }

    #[getter]
    pub fn is_dir(&self) -> bool {
        self._entry.is_dir()
    }

    #[getter]
    pub fn path(&self) -> String {
        if let Some(resource) = &self._entry.resource() {
            resource.path.to_string_lossy().to_string()
        } else {
            self._entry.filename().to_owned()
        }
    }
}

impl From<EMetadataEntry> for PyEntry {
    fn from(entry: EMetadataEntry) -> PyEntry {
        PyEntry { _entry: entry }
    }
}
