use pyo3::prelude::*;

use liboxen::view::PaginatedDirEntries;

use crate::py_entry::PyEntry;

#[pyclass]
struct StringIter {
    inner: std::vec::IntoIter<String>,
}

#[pymethods]
impl StringIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<String> {
        slf.inner.next()
    }
}

#[pyclass]
pub struct PyPaginatedDirEntries {
    _entries: PaginatedDirEntries,
}

#[pymethods]
impl PyPaginatedDirEntries {
    fn __repr__(&self) -> String {
        format!("PaginatedDirEntries(page_size={}, page_number={}, total_pages={}, total_entries={})", self._entries.page_size, self._entries.page_number, self._entries.total_pages, self._entries.total_entries)
    }

    fn __str__(&self) -> String {
        let result: String = self._entries.entries
            .iter().map(|e| {
                if e.is_dir {
                    format!("{}/", e.filename)
                } else {
                    format!("{}", e.filename)
                }
            })
            .collect::<Vec<String>>()
            .join("\n");
        result
    }

    fn __len__(&self) -> usize {
        self._entries.entries.len()
    }

    fn __getitem__(&self, index: isize) -> PyResult<PyEntry> {
        let index = if index < 0 {
            self._entries.entries.len() as isize + index
        } else {
            index
        };
        if index < 0 || index >= self._entries.entries.len() as isize {
            Err(pyo3::exceptions::PyIndexError::new_err("Index out of bounds"))
        } else {
            Ok(PyEntry::from(self._entries.entries[index as usize].to_owned()))
        }
    }

    #[getter]
    pub fn page_size(&self) -> usize {
        self._entries.page_size
    }

    #[getter]
    pub fn page_number(&self) -> usize {
        self._entries.page_number
    }

    #[getter]
    pub fn total_pages(&self) -> usize {
        self._entries.total_pages
    }

    #[getter]
    pub fn total_entries(&self) -> usize {
        self._entries.total_entries
    }

    #[getter]
    pub fn entries(&self) -> Vec<PyEntry> {
        self._entries.entries
            .iter()
            .map(|e| PyEntry::from(e.to_owned()))
            .collect()
    }
}

impl From<PaginatedDirEntries> for PyPaginatedDirEntries {
    fn from(entries: PaginatedDirEntries) -> PyPaginatedDirEntries {
        PyPaginatedDirEntries { _entries: entries }
    }
}