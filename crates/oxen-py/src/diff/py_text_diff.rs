use pyo3::prelude::*;

use liboxen::model::diff::ChangeType;
use liboxen::model::diff::TextDiff;

#[derive(Debug, Clone, PartialEq)]
#[pyclass(eq, eq_int)]
pub enum PyChangeType {
    Added,
    Removed,
    Modified,
    Unchanged,
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct PyLineDiff {
    pub modification: PyChangeType,
    pub text: String,
}

#[pymethods]
impl PyLineDiff {
    fn __repr__(&self) -> String {
        format!(
            "PyLineDiff(modification={:?}, text={})",
            self.modification, self.text
        )
    }

    #[getter]
    fn value(&self) -> String {
        match self.modification {
            PyChangeType::Added => format!("+ {}", self.text),
            PyChangeType::Removed => format!("- {}", self.text),
            PyChangeType::Modified => format!("𝝙 {}", self.text),
            PyChangeType::Unchanged => format!("  {}", self.text),
        }
    }

    #[getter]
    fn modification(&self) -> PyChangeType {
        self.modification.clone()
    }

    #[getter]
    fn text(&self) -> String {
        self.text.clone()
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct PyTextDiff {
    pub lines: Vec<PyLineDiff>,
}

#[pymethods]
impl PyTextDiff {
    fn __repr__(&self) -> String {
        format!("PyTextDiff(lines={})", self.lines.len())
    }

    #[getter]
    fn lines(&self) -> PyResult<Vec<PyLineDiff>> {
        Ok(self.lines.clone())
    }
}

impl From<&TextDiff> for PyTextDiff {
    fn from(other: &TextDiff) -> Self {
        let lines = other
            .lines
            .iter()
            .map(|line| {
                let modification = match line.modification {
                    ChangeType::Added => PyChangeType::Added,
                    ChangeType::Removed => PyChangeType::Removed,
                    ChangeType::Modified => PyChangeType::Modified,
                    ChangeType::Unchanged => PyChangeType::Unchanged,
                };
                PyLineDiff {
                    modification,
                    text: line.text.clone(),
                }
            })
            .collect();
        Self { lines }
    }
}

impl From<TextDiff> for PyTextDiff {
    fn from(other: TextDiff) -> Self {
        Self::from(&other)
    }
}
