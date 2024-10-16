// This is the new dataframe format, depreciate JsonDataFrameSliceResponse

use std::io::BufWriter;
use std::str;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use super::data_frames::DataFrameColumnChange;
use super::data_frames::DataFrameRowChange;
use super::StatusMessage;
use crate::constants;
use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::data_frame::DataFrameSchemaSize;
use crate::model::Commit;
use crate::model::DataFrameSize;
use crate::opts::df_opts::DFOptsView;

use crate::view::entries::ResourceVersion;
use crate::view::Pagination;
use crate::{model::Schema, opts::DFOpts};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameView {
    pub schema: Schema,
    pub size: DataFrameSize,
    pub data: serde_json::Value,
    pub pagination: Pagination,
    #[serde(flatten)]
    pub opts: DFOptsView,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameViews {
    pub source: DataFrameSchemaSize,
    pub view: JsonDataFrameView,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameViewResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub data_frame: JsonDataFrameViews,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    pub derived_resource: Option<DerivedDFResource>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WorkspaceJsonDataFrameViewResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub data_frame: Option<JsonDataFrameViews>,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    pub derived_resource: Option<DerivedDFResource>,
    pub is_indexed: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameRowResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub diff: Option<Vec<DataFrameRowChange>>,
    pub data_frame: JsonDataFrameViews,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    pub derived_resource: Option<DerivedDFResource>,
    pub row_id: Option<String>,
    pub row_index: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BatchUpdateResponse {
    pub row_id: String,
    pub code: i32,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameColumnResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub diff: Option<Vec<DataFrameColumnChange>>,
    pub data_frame: JsonDataFrameViews,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    pub derived_resource: Option<DerivedDFResource>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DFResourceType {
    Compare,
    Diff,
    Query,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DerivedDFResource {
    pub resource_id: String,
    pub path: String,
    pub resource_type: DFResourceType,
}

impl JsonDataFrameView {
    pub fn from_df_opts(df: DataFrame, og_schema: Schema, opts: &DFOpts) -> JsonDataFrameView {
        let full_width = df.width();
        let full_height = df.height();

        let page_size = opts.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
        let page = opts.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;

        let total_pages = (full_height as f64 / page_size as f64).ceil() as usize;

        let mut opts = opts.clone();

        if df.height() == 0 {
            return JsonDataFrameView::empty_with_schema(&og_schema, full_height, &opts);
        };

        opts.slice = Some(format!("{}..{}", start, end));
        let opts_view = DFOptsView::from_df_opts(&opts);
        let mut sliced_df = tabular::transform(df, opts).unwrap();

        // Merge the metadata from the original schema
        let mut slice_schema = Schema::from_polars(&sliced_df.schema());
        slice_schema.update_metadata_from_schema(&og_schema);

        JsonDataFrameView {
            schema: slice_schema,
            size: DataFrameSize {
                height: full_height,
                width: full_width,
            },
            data: JsonDataFrameView::json_from_df(&mut sliced_df),
            pagination: Pagination {
                page_number: page,
                page_size,
                total_pages,
                total_entries: full_height,
            },
            opts: opts_view,
        }
    }

    pub fn from_df_opts_unpaginated(
        df: DataFrame,
        og_schema: Schema,
        og_height: usize,
        opts: &DFOpts,
    ) -> JsonDataFrameView {
        let full_width = df.width();
        let view_height = df.height();

        // Unpaginated means we don't need to slice the df
        let mut opts = opts.clone();
        opts.slice = None;

        let opts_view = DFOptsView::from_df_opts(&opts);
        let mut sliced_df = tabular::transform(df, opts.clone()).unwrap();

        // Merge the metadata from the original schema
        let mut slice_schema = Schema::from_polars(&sliced_df.schema());
        log::debug!("OG schema {:?}", og_schema);
        log::debug!("Pre-Slice schema {:?}", slice_schema);
        slice_schema.update_metadata_from_schema(&og_schema);
        log::debug!("Slice schema {:?}", slice_schema);

        let page_size = opts.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
        let page_number = opts.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

        let total_pages = (og_height as f64 / page_size as f64).ceil() as usize;

        JsonDataFrameView {
            schema: slice_schema,
            size: DataFrameSize {
                height: view_height,
                width: full_width,
            },
            data: JsonDataFrameView::json_from_df(&mut sliced_df),
            pagination: Pagination {
                page_number,
                page_size,
                total_pages,
                total_entries: og_height,
            },
            opts: opts_view,
        }
    }

    pub fn to_df(&self) -> DataFrame {
        if self.data == serde_json::Value::Null {
            DataFrame::empty()
        } else {
            // The fields were coming out of order, so we need to reorder them
            let columns = self.schema.fields_names();
            log::debug!("Got columns: {:?}", columns);

            match &self.data {
                serde_json::Value::Array(arr) => {
                    if !arr.is_empty() {
                        let data = self.data.to_string();
                        let content = Cursor::new(data.as_bytes());
                        log::debug!("Deserializing df: [{}]", data);
                        let df = JsonReader::new(content).finish().unwrap();

                        let opts = DFOpts::from_column_names(columns);
                        tabular::transform(df, opts).unwrap()
                    } else {
                        let cols = columns
                            .iter()
                            .map(|name| {
                                Column::Series(Series::new(
                                    PlSmallStr::from_str(name),
                                    Vec::<&str>::new(),
                                ))
                            })
                            .collect::<Vec<Column>>();
                        DataFrame::new(cols).unwrap()
                    }
                }
                _ => {
                    log::error!("Could not parse non-array json data: {:?}", self.data);
                    DataFrame::empty()
                }
            }
        }
    }

    pub fn json_from_df(df: &mut DataFrame) -> serde_json::Value {
        log::debug!("Serializing df: [{}]", df);

        sanitize_df_for_serialization(df).expect("Error cleaning df before serialization");

        // TODO: catch errors
        let data: Vec<u8> = Vec::new();
        let mut buf = BufWriter::new(data);

        let mut writer = JsonWriter::new(&mut buf).with_json_format(JsonFormat::Json);
        writer.finish(df).expect("Could not write df json buffer");

        let buffer = buf.into_inner().expect("Could not get buffer");

        let json_str = str::from_utf8(&buffer).unwrap();

        serde_json::from_str(json_str).unwrap()
    }

    fn empty_with_schema(
        schema: &Schema,
        total_entries: usize,
        opts: &DFOpts,
    ) -> JsonDataFrameView {
        let mut default_df = DataFrame::empty();
        JsonDataFrameView {
            schema: schema.to_owned(),
            size: DataFrameSize {
                height: 0,
                width: schema.fields_names().len(),
            },
            data: JsonDataFrameView::json_from_df(&mut default_df),
            pagination: Pagination {
                page_number: 0,
                page_size: 0,
                total_pages: 0,
                total_entries,
            },
            opts: DFOptsView::from_df_opts(opts),
        }
    }
}

impl JsonDataFrameViews {
    pub fn from_df_and_opts(df: DataFrame, og_schema: Schema, opts: &DFOpts) -> JsonDataFrameViews {
        let source = DataFrameSchemaSize::from_df(&df, &og_schema);
        let view = JsonDataFrameView::from_df_opts(df, og_schema, opts);
        JsonDataFrameViews { source, view }
    }

    // To avoid duplicate pagination when the pagination has already been applied
    // most commonly from duckdb / other sql
    pub fn from_df_and_opts_unpaginated(
        df: DataFrame,
        og_schema: Schema,
        og_height: usize,
        opts: &DFOpts,
    ) -> JsonDataFrameViews {
        let source = DataFrameSchemaSize::from_df(&df, &og_schema);
        let view = JsonDataFrameView::from_df_opts_unpaginated(df, og_schema, og_height, opts);
        JsonDataFrameViews { source, view }
    }
}

fn sanitize_df_for_serialization(df: &mut DataFrame) -> Result<(), OxenError> {
    let schema = df.schema();

    for (idx, _field) in schema.iter_fields().enumerate() {
        let series = df.select_at_idx(idx).unwrap(); // Index is in bounds, we passed it from the loop

        let new_series = match series.dtype() {
            DataType::Binary => Some(cast_binary_to_string_with_fallback(series, "[binary]")),
            DataType::Struct(subfields) => {
                let mut cast_series = series.clone();
                for subfield in subfields {
                    if let DataType::Binary = subfield.dtype() {
                        cast_series = cast_binary_to_string_with_fallback(
                            series,
                            &format!("struct[{}]", subfields.len()),
                        );
                        break;
                    }
                }
                Some(cast_series)
            }
            DataType::List(subtype) => match **subtype {
                DataType::Binary => {
                    Some(cast_binary_to_string_with_fallback(series, "List[binary]"))
                }
                _ => None,
            },
            _ => None,
        };

        if let Some(new_series) = new_series {
            df.replace_column(idx, new_series)?;
        }
    }

    Ok(())
}

fn cast_binary_to_string_with_fallback(series: &Column, out: &str) -> Column {
    let res = series.cast(&DataType::String);
    if let Ok(series) = res {
        series
    } else {
        let mut vec = vec![out];
        vec.resize(series.len(), out);
        Column::new(series.name().clone(), vec)
    }
}
