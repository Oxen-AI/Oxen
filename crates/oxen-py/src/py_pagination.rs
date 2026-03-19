use pyo3::prelude::*;

use liboxen::view::Pagination as OxenPagination;

#[pyclass]
pub struct PyPagination {
    _pagination: OxenPagination,
}

#[pymethods]
impl PyPagination {
    #[getter]
    pub fn page_size(&self) -> usize {
        self._pagination.page_size
    }

    #[getter]
    pub fn page_number(&self) -> usize {
        self._pagination.page_number
    }

    #[getter]
    pub fn total_pages(&self) -> usize {
        self._pagination.total_pages
    }

    #[getter]
    pub fn total_entries(&self) -> usize {
        self._pagination.total_entries
    }
}

impl From<OxenPagination> for PyPagination {
    fn from(pagination: OxenPagination) -> PyPagination {
        PyPagination {
            _pagination: pagination,
        }
    }
}
