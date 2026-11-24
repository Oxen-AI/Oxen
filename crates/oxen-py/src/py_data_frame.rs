//! Passes polars data frames to and from Python as `polars.DataFrame`s, through the Arrow C stream
//! interface (the `__arrow_c_stream__` PyCapsule protocol).

use std::ffi::CStr;
use std::sync::{Mutex, PoisonError};

use polars::prelude::{CompatLevel, DataFrame, PlSmallStr, PolarsResult, polars_err};
use polars_arrow::array::{Array, StructArray, new_empty_array};
use polars_arrow::datatypes::{ArrowDataType, Field as ArrowField};
use polars_arrow::ffi::{ArrowArrayStream, ArrowArrayStreamReader, export_iterator};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyCapsule, PyType};

/// The capsule name the PyCapsule protocol requires for an `ArrowArrayStream`.
const STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

/// A data frame that converts to a `polars.DataFrame` when returned to Python, and is extracted
/// from one when passed in.
#[derive(Clone)]
pub struct PyDataFrame(pub DataFrame);

impl<'py> IntoPyObject<'py> for PyDataFrame {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = ArrowStream(Mutex::new(Some(export(self.0))));
        polars_data_frame_class(py)?.call1((stream,))
    }
}

impl<'a, 'py> FromPyObject<'a, 'py> for PyDataFrame {
    type Error = PyErr;

    fn extract(obj: Borrowed<'a, 'py, PyAny>) -> PyResult<Self> {
        if !obj.is_instance(polars_data_frame_class(obj.py())?)? {
            let type_name = obj.get_type().name()?;
            return Err(PyTypeError::new_err(format!(
                "expected a polars.DataFrame, got {type_name}"
            )));
        }
        let capsule = obj.call_method0("__arrow_c_stream__")?;
        let capsule = capsule.cast::<PyCapsule>()?;
        let pointer = capsule
            .pointer_checked(Some(STREAM_CAPSULE_NAME))?
            .cast::<ArrowArrayStream>();
        // SAFETY: a capsule named `arrow_array_stream` holds an `ArrowArrayStream`, per the
        // PyCapsule protocol. Replacing it with an empty one moves the stream out and leaves a null
        // `release` behind, which tells the capsule's destructor the stream has been consumed.
        let stream = unsafe { pointer.as_ptr().replace(ArrowArrayStream::empty()) };
        let df = import(stream).map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(df))
    }
}

fn polars_data_frame_class(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    static DATA_FRAME: PyOnceLock<Py<PyType>> = PyOnceLock::new();
    DATA_FRAME.import(py, "polars", "DataFrame")
}

/// An exported `ArrowArrayStream`, offered once through `__arrow_c_stream__`.
#[pyclass(frozen)]
struct ArrowStream(Mutex<Option<ArrowArrayStream>>);

#[pymethods]
impl ArrowStream {
    /// Hands the stream over in its capsule. The producer may ignore `requested_schema`, and this
    /// one always does.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let stream = self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| PyValueError::new_err("the Arrow stream was already consumed"))?;
        PyCapsule::new_with_value(py, stream, STREAM_CAPSULE_NAME)
    }
}

/// Exports the frame as a stream of struct arrays, one per chunk, each field a column.
fn export(mut df: DataFrame) -> ArrowArrayStream {
    let compat_level = CompatLevel::newest();
    df.align_chunks_par();
    let fields = df
        .columns()
        .iter()
        .map(|column| column.field().to_arrow(compat_level))
        .collect();
    let dtype = ArrowDataType::Struct(fields);
    let batches = df
        .iter_chunks(compat_level, false)
        .map(|batch| {
            let height = batch.height();
            StructArray::try_new(dtype.clone(), height, batch.into_arrays(), None)
                .map(|array| array.boxed())
        })
        .collect::<Vec<_>>();
    export_iterator(
        Box::new(batches.into_iter()),
        ArrowField::new(PlSmallStr::EMPTY, dtype, false),
    )
}

/// Reads a stream of struct arrays into one frame, each field a column.
fn import(stream: ArrowArrayStream) -> PolarsResult<DataFrame> {
    // SAFETY: `stream` came from a PyCapsule named `arrow_array_stream`, whose producer promises a
    // valid Arrow C stream.
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(Box::new(stream))? };
    let mut df = into_data_frame(new_empty_array(reader.field().dtype().clone()))?;
    // SAFETY: as above, the producer promises arrays that follow the Arrow C data interface.
    while let Some(array) = unsafe { reader.next() } {
        df.vstack_mut_owned(into_data_frame(array?)?)?;
    }
    Ok(df)
}

fn into_data_frame(array: Box<dyn Array>) -> PolarsResult<DataFrame> {
    let array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .cloned()
        .ok_or_else(
            || polars_err!(ComputeError: "an Arrow stream of a data frame must hold struct arrays"),
        )?;
    DataFrame::try_from(array)
}
