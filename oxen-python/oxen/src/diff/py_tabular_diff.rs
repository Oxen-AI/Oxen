use liboxen::model::diff::tabular_diff::TabularDiff;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

use liboxen::model::data_frame::schema::Schema;
use liboxen::model::diff::AddRemoveModifyCounts;

#[pyclass]
pub struct PyTabularDiffMods {
    pub rows: AddRemoveModifyCounts,
}

#[pyclass]
pub struct PyTabularDiffSummary {
    pub modifications: PyTabularDiffMods,
    pub schema: Schema,
}

#[pyclass]
pub struct PyTabularDiff {
    pub summary: PyTabularDiffSummary,
    pub data: PyDataFrame,
}

#[pymethods]
impl PyTabularDiff {
    fn __repr__(&self) -> String {
        let df = self.data.as_ref();
        format!(
            "PyTabularDiff(shape=({},{}))\n{:?}",
            df.height(),
            df.width(),
            df
        )
    }

    #[getter]
    fn data(&self) -> PyResult<PyDataFrame> {
        Ok(self.data.clone())
    }
}

impl From<&TabularDiff> for PyTabularDiff {
    fn from(other: &TabularDiff) -> Self {
        let df = &other.contents;
        let summary = &other.summary;
        let rows = AddRemoveModifyCounts {
            added: summary.modifications.row_counts.added,
            removed: summary.modifications.row_counts.removed,
            modified: summary.modifications.row_counts.modified,
        };
        let mods = PyTabularDiffMods { rows };
        let summary = PyTabularDiffSummary {
            modifications: mods,
            schema: Schema::from_polars(&df.schema()),
        };
        let data = PyDataFrame(df.clone());
        Self { summary, data }
    }
}

impl From<TabularDiff> for PyTabularDiff {
    fn from(other: TabularDiff) -> Self {
        Self::from(&other)
    }
}
