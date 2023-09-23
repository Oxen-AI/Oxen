// TODO: This is the new dataframe format, depreciate JsonDataFrameSliceResponse

use std::io::BufWriter;
use std::str;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use super::StatusMessage;
use crate::constants;
use crate::core::df::tabular;
use crate::model::Commit;
use crate::model::DataFrameSize;
use crate::opts::PaginateOpts;
use crate::view::entry::ResourceVersion;
use crate::view::Pagination;
use crate::{model::Schema, opts::DFOpts};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameSource {
    pub schema: Schema,
    pub size: DataFrameSize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameView {
    pub schema: Schema,
    pub size: DataFrameSize,
    pub data: serde_json::Value,
    pub pagination: Pagination,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameViews {
    pub source: JsonDataFrameSource,
    pub view: JsonDataFrameView,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameViewResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub data_frame: JsonDataFrameViews,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
}

impl JsonDataFrameSource {
    pub fn from_df(df: &DataFrame, schema: &Schema) -> JsonDataFrameSource {
        JsonDataFrameSource {
            schema: schema.to_owned(),
            size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
        }
    }
}

impl JsonDataFrameView {
    pub fn view_from_pagination(
        df: DataFrame,
        og_schema: Schema,
        opts: &PaginateOpts,
    ) -> JsonDataFrameView {
        let full_width = df.width();
        let full_height = df.height();

        let page_size = opts.page_size;
        let page = opts.page_num;

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;

        let total_pages = (full_height as f64 / page_size as f64).ceil() as usize;

        let mut opts = DFOpts::empty();
        opts.slice = Some(format!("{}..{}", start, end));
        let mut sliced_df = tabular::transform(df, opts).unwrap();

        // Merge the metadata from the original schema
        let mut slice_schema = Schema::from_polars(&sliced_df.schema());
        log::debug!("OG schema {:?}", og_schema);
        log::debug!("Pre-Slice schema {:?}", slice_schema);
        slice_schema.update_metadata_from_schema(&og_schema);
        log::debug!("Slice schema {:?}", slice_schema);

        JsonDataFrameView {
            schema: slice_schema,
            size: DataFrameSize {
                height: full_height,
                width: full_width,
            },
            data: JsonDataFrameView::json_data(&mut sliced_df),
            pagination: Pagination {
                page_number: page,
                page_size,
                total_pages,
                total_entries: full_height,
            },
        }
    }

    pub fn from_df_opts(df: DataFrame, og_schema: Schema, opts: &DFOpts) -> JsonDataFrameView {
        let full_width = df.width();
        let full_height = df.height();

        let page_size = opts.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
        let page = opts.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;

        let total_pages = (full_height as f64 / page_size as f64).ceil() as usize;

        let mut opts = opts.clone();
        opts.slice = Some(format!("{}..{}", start, end));
        let mut sliced_df = tabular::transform(df, opts).unwrap();

        // Merge the metadata from the original schema
        let mut slice_schema = Schema::from_polars(&sliced_df.schema());
        log::debug!("OG schema {:?}", og_schema);
        log::debug!("Pre-Slice schema {:?}", slice_schema);
        slice_schema.update_metadata_from_schema(&og_schema);
        log::debug!("Slice schema {:?}", slice_schema);

        JsonDataFrameView {
            schema: slice_schema,
            size: DataFrameSize {
                height: full_height,
                width: full_width,
            },
            data: JsonDataFrameView::json_data(&mut sliced_df),
            pagination: Pagination {
                page_number: page,
                page_size,
                total_pages,
                total_entries: full_height,
            },
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
                            .map(|name| Series::new(name, Vec::<&str>::new()))
                            .collect::<Vec<Series>>();
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

    fn json_data(df: &mut DataFrame) -> serde_json::Value {
        log::debug!("Serializing df: [{}]", df);

        // TODO: catch errors
        let data: Vec<u8> = Vec::new();
        let mut buf = BufWriter::new(data);

        let mut writer = JsonWriter::new(&mut buf).with_json_format(JsonFormat::Json);
        writer.finish(df).expect("Could not write df json buffer");

        let buffer = buf.into_inner().expect("Could not get buffer");

        let json_str = str::from_utf8(&buffer).unwrap();

        serde_json::from_str(json_str).unwrap()
    }
}